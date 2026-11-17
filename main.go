// Command print-queue is a durable, per-printer print queue for Prusa Connect.
package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"

	"github.com/prusa3d/prusa-connect/sdk/accessapi"
	"github.com/prusa3d/prusa-connect/sdk/busapi"
	"github.com/prusa3d/prusa-connect/sdk/deviceapi"
	"github.com/prusa3d/prusa-connect/sdk/event"
	"github.com/prusa3d/prusa-connect/sdk/extension"
	"github.com/prusa3d/prusa-connect/sdk/moduledb"
	"github.com/prusa3d/prusa-connect/sdk/routesapi"
	"github.com/prusa3d/prusa-connect/sdk/storageapi"
	"github.com/prusa3d/prusa-connect/sdk/wire"
	"github.com/prusa3d/prusa-connect/sdk/wirerpc"
)

const (
	name    = "print-queue"
	release = extension.Version("0.2.0")
)

//go:embed openapi.json
var openAPI []byte

//go:embed migrations/sqlite/* migrations/postgres/*
var migrationsFS embed.FS

var schema = moduledb.Schema{Migrations: migrationsFS}

var errNotAuthorized = errors.New("caller is not authorized for this printer")

type module struct {
	root   context.Context
	conn   *wirerpc.Conn
	fleet  *deviceapi.Client
	bus    *busapi.Client
	access *accessapi.Client
	files  *storageapi.Client

	mu    sync.RWMutex
	store *queueStore
	close context.CancelFunc
	wait  *sync.WaitGroup
	db    interface{ Close() error }
}

// Settings is intentionally empty: the first usable queue has no operator
// knobs. Database and permissions are declared in module.yaml.
type Settings struct{}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	line, err := (wire.Handshake{
		Protocol: wire.ProtocolMajor,
		MinMinor: wire.MinMinor,
		MaxMinor: wire.MaxMinor,
		Address:  wire.StdioAddress,
	}).Line()
	if err != nil {
		return fmt.Errorf("render handshake: %w", err)
	}
	if _, err = os.Stdout.WriteString(line); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}

	serves := wirerpc.Methods{}
	conn, err := wirerpc.NewConn(stdio{}, wirerpc.Options{
		Role:    wirerpc.RoleChild,
		Methods: serves,
	})
	if err != nil {
		return fmt.Errorf("build connection: %w", err)
	}

	m := &module{
		root:   ctx,
		conn:   conn,
		fleet:  deviceapi.NewClient(conn),
		bus:    busapi.NewClient(conn),
		access: accessapi.NewClient(conn),
		files:  storageapi.NewClient(conn),
	}
	maps.Copy(serves, m.methods())

	if err = conn.Serve(ctx); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	m.shutdown()
	return nil
}

func (m *module) methods() wirerpc.Methods {
	return wirerpc.Methods{
		wirerpc.MethodInfo: func(context.Context, wirerpc.Call) (any, error) {
			return routesapi.Describe(wirerpc.Info{
				Name:     name,
				Version:  release,
				Contract: wirerpc.Contract,
				Serves:   []wirerpc.Method{wirerpc.MethodConfigure},
			}, openAPI)
		},
		wirerpc.MethodConfigure: wirerpc.ConfigureReporting(m.configure),
		wirerpc.MethodShutdown: func(context.Context, wirerpc.Call) (any, error) {
			m.shutdown()
			return nil, nil
		},
		routesapi.MethodServe: routesapi.Serve(m),
	}
}

func (m *module) configure(ctx context.Context, _ Settings, grants wirerpc.Grants) (wirerpc.Configured, error) {
	if grants.Database == nil {
		return wirerpc.Configured{}, errors.New("the module needs a database; reinstall with database approval")
	}

	db, report, err := moduledb.Open(ctx, *grants.Database, schema)
	if err != nil {
		return wirerpc.Configured{}, fmt.Errorf("open queue database: %w", err)
	}
	store := newQueueStore(db, string(grants.Database.Engine))
	workerCtx, cancel := context.WithCancel(m.root)
	var wait sync.WaitGroup
	wait.Go(func() { m.eventLoop(workerCtx, store) })
	wait.Go(func() { m.dispatchLoop(workerCtx, store) })

	m.mu.Lock()
	oldCancel, oldWait, oldDB := m.close, m.wait, m.db
	m.store, m.close, m.wait, m.db = store, cancel, &wait, db
	m.mu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldWait != nil {
		oldWait.Wait()
	}
	if oldDB != nil {
		_ = oldDB.Close()
	}

	return wirerpc.Configured{Database: report.State()}, nil
}

func (m *module) shutdown() {
	m.mu.Lock()
	cancel, wait, db := m.close, m.wait, m.db
	m.close, m.wait, m.db, m.store = nil, nil, nil, nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if wait != nil {
		wait.Wait()
	}
	if db != nil {
		_ = db.Close()
	}
}

func (m *module) currentStore() *queueStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store
}

func (m *module) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	store := m.currentStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "NOT_CONFIGURED", "the queue database is not ready")
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/queues":
		m.listQueues(w, r, store)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/queues/") && strings.HasSuffix(r.URL.Path, "/items"):
		m.enqueue(w, r, store, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/queues/"), "/items"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/queues/"):
		m.getQueue(w, r, store, strings.TrimPrefix(r.URL.Path, "/queues/"))
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/queue-items/"):
		m.deleteItem(w, r, store, strings.TrimPrefix(r.URL.Path, "/queue-items/"))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/move") && strings.HasPrefix(r.URL.Path, "/queue-items/"):
		m.moveItem(w, r, store, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/queue-items/"), "/move"))
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
	}
}

type queueEnvelope struct {
	Device deviceapi.Device `json:"device"`
	Items  []queueItem      `json:"items"`
}

func (m *module) listQueues(w http.ResponseWriter, r *http.Request, store *queueStore) {
	principal := routesapi.PrincipalOf(r)
	page, err := m.fleet.List(r.Context(), deviceapi.ListParams{Principal: principal, Limit: deviceapi.MaxPageLimit})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	items, err := store.list(r.Context(), "")
	if err != nil {
		writeMappedError(w, err)
		return
	}
	byDevice := map[string][]queueItem{}
	for _, item := range items {
		byDevice[item.DeviceID] = append(byDevice[item.DeviceID], item)
	}
	out := make([]queueEnvelope, 0, len(page.Items))
	for _, device := range page.Items {
		out = append(out, queueEnvelope{Device: device, Items: nonNilItems(byDevice[device.ID])})
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": out})
}

func (m *module) getQueue(w http.ResponseWriter, r *http.Request, store *queueStore, deviceID string) {
	device, err := m.fleet.Get(r.Context(), deviceapi.GetParams{Principal: routesapi.PrincipalOf(r), ID: deviceID})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	items, err := store.list(r.Context(), deviceID)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queueEnvelope{Device: device, Items: nonNilItems(items)})
}

func nonNilItems(items []queueItem) []queueItem {
	if items == nil {
		return []queueItem{}
	}
	return items
}

type enqueueRequest struct {
	Storage     string `json:"storage"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	DisplayName string `json:"display_name"`
}

func (m *module) enqueue(w http.ResponseWriter, r *http.Request, store *queueStore, deviceID string) {
	principal := routesapi.PrincipalOf(r)
	if deviceID == "" || strings.Contains(deviceID, "/") {
		writeError(w, http.StatusBadRequest, "INVALID_DEVICE", "invalid device id")
		return
	}
	var input enqueueRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	if input.Storage == "" || input.Source == "" {
		writeError(w, http.StatusBadRequest, "INVALID_FILE", "storage and source are required")
		return
	}
	if len(input.Storage) > storageapi.MaxStorageBytes || len(input.Source) > storageapi.MaxPathBytes {
		writeError(w, http.StatusBadRequest, "INVALID_FILE", "storage or source exceeds the core limit")
		return
	}
	if err := validateRelativePath(input.Source, storageapi.MaxPathBytes); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FILE", err.Error())
		return
	}
	if input.Destination == "" {
		input.Destination = path.Base(input.Source)
	}
	if input.DisplayName == "" {
		input.DisplayName = path.Base(input.Source)
	}
	if err := validateRelativePath(input.Destination, deviceapi.MaxPathBytes); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_DESTINATION", err.Error())
		return
	}
	if err := m.authorizedOperate(r.Context(), principal, deviceID); err != nil {
		writeMappedError(w, err)
		return
	}
	file, err := m.files.Stat(r.Context(), storageapi.StatParams{Principal: principal, Storage: input.Storage, Path: input.Source})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if file.IsDir || file.Irregular {
		writeError(w, http.StatusBadRequest, "INVALID_FILE", "source must name a regular file")
		return
	}
	digest, err := m.sourceDigest(r.Context(), principal, input.Storage, input.Source, file)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	item, err := store.enqueue(r.Context(), newQueueItem{
		DeviceID: deviceID, Storage: input.Storage, Source: input.Source,
		SourceSize: file.Size, SourceMTime: file.MTime, SourceSHA256: digest,
		Destination: input.Destination, DisplayName: input.DisplayName, SubmittedBy: principal,
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (m *module) deleteItem(w http.ResponseWriter, r *http.Request, store *queueStore, id string) {
	item, err := store.get(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if err = m.authorizedOperate(r.Context(), routesapi.PrincipalOf(r), item.DeviceID); err != nil {
		writeMappedError(w, err)
		return
	}
	item, err = store.deletePending(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

type moveRequest struct {
	Position int `json:"position"`
}

func (m *module) moveItem(w http.ResponseWriter, r *http.Request, store *queueStore, id string) {
	item, err := store.get(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	if err = m.authorizedOperate(r.Context(), routesapi.PrincipalOf(r), item.DeviceID); err != nil {
		writeMappedError(w, err)
		return
	}
	var input moveRequest
	if err = decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	if input.Position < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_POSITION", "position must be zero or greater")
		return
	}
	item, err = store.move(r.Context(), id, input.Position)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (m *module) authorizedOperate(ctx context.Context, principal, deviceID string) error {
	if principal == "" {
		return errors.New("no authenticated principal")
	}
	decision, err := m.access.Check(ctx, accessapi.CheckParams{
		Subject: principal, Action: "operate", ObjectType: "device", ObjectID: deviceID,
	})
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return fmt.Errorf("%w: the caller may not operate this printer", errNotAuthorized)
	}
	return nil
}

func validateRelativePath(value string, limit int) error {
	if value == "" {
		return errors.New("path must not be empty")
	}
	if len(value) > limit || path.IsAbs(value) {
		return errors.New("path is outside the published limit")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return errors.New("path must not leave its storage")
		}
	}
	return nil
}

func decodeJSON(r *http.Request, into any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"code": code, "message": message})
}

func writeMappedError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, deviceapi.ErrInvalidQuery), errors.Is(err, storageapi.ErrInvalidPath):
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", err.Error())
	case errors.Is(err, errQueueItemNotFound), errors.Is(err, deviceapi.ErrNoDevice), errors.Is(err, deviceapi.ErrNoFile), errors.Is(err, storageapi.ErrNoFile):
		writeError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, errQueueItemNotPending):
		writeError(w, http.StatusConflict, "NOT_PENDING", err.Error())
	case errors.Is(err, errNotAuthorized), errors.Is(err, deviceapi.ErrDenied), errors.Is(err, storageapi.ErrDenied), errors.Is(err, accessapi.ErrDenied), errors.Is(err, accessapi.ErrUnknownSubject):
		writeError(w, http.StatusForbidden, "DENIED", err.Error())
	case errors.Is(err, deviceapi.ErrNotReady), errors.Is(err, deviceapi.ErrBusy), errors.Is(err, deviceapi.ErrPrintRefused), errors.Is(err, deviceapi.ErrPrintFailed), errors.Is(err, deviceapi.ErrNameTaken):
		writeError(w, http.StatusConflict, "NOT_READY", err.Error())
	default:
		writeError(w, http.StatusBadGateway, "HOST_ERROR", err.Error())
	}
}

func stateValue(device deviceapi.Device) string {
	var value string
	if raw := device.State["/state"]; len(raw) != 0 {
		if json.Unmarshal(raw, &value) != nil {
			value = strings.Trim(string(raw), `"`)
		}
	}
	return value
}

func (m *module) eventLoop(ctx context.Context, store *queueStore) {
	params := busapi.SubscribeParams{
		Types: []string{
			event.TypeDeviceTransferStarted,
			event.TypeDeviceTransferSettled,
			event.TypeDeviceCommandSettled,
			event.TypeDeviceJobStarted,
			event.TypeDeviceJobSettled,
			event.TypeDevicePresenceChanged,
			event.TypeDeviceTelemetrySampled,
		},
		IntervalMS: 250,
	}
	for ctx.Err() == nil {
		err := busapi.Subscribe(ctx, m.conn, params, func(window busapi.Window) error {
			if window.Resync {
				m.reconcile(ctx, store)
			}
			for _, raw := range window.Telemetry {
				if err := m.handleTelemetry(ctx, store, raw); err != nil {
					fmt.Fprintf(os.Stderr, "%s: telemetry: %v\n", name, err)
				}
			}
			for _, raw := range window.Events {
				if err := m.handleEvent(ctx, store, raw); err != nil {
					fmt.Fprintf(os.Stderr, "%s: event: %v\n", name, err)
				}
			}
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: event subscription: %v\n", name, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (m *module) handleTelemetry(ctx context.Context, store *queueStore, raw json.RawMessage) error {
	var envelope event.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode telemetry envelope: %w", err)
	}
	var sample struct {
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(envelope.Data, &sample); err != nil {
		return fmt.Errorf("decode telemetry sample: %w", err)
	}
	if sample.Path == "/state" {
		// The dispatcher polls as its source of truth, while this wake-up
		// makes a READY transition start the next item promptly.
		m.dispatchOnce(ctx, store)
	}
	return nil
}

func (m *module) handleEvent(ctx context.Context, store *queueStore, raw json.RawMessage) error {
	var envelope event.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	_, origin, err := event.ParseSourceURN(envelope.Source)
	if err != nil {
		return err
	}
	deviceID := origin.ID()
	switch envelope.Type {
	case event.TypeDeviceTransferStarted:
		var payload event.TransferStarted
		if err = json.Unmarshal(envelope.Data, &payload); err != nil {
			return err
		}
		item, findErr := store.findByTransferStart(ctx, payload.Storage, payload.Source, payload.Destination)
		if errors.Is(findErr, errQueueItemNotFound) {
			return nil
		}
		if findErr != nil {
			return findErr
		}
		return store.setDispatch(ctx, item.ID, payload.TransferID, item.CommandID)
	case event.TypeDeviceTransferSettled:
		var payload event.TransferSettled
		if err = json.Unmarshal(envelope.Data, &payload); err != nil {
			return err
		}
		item, findErr := store.findByIDField(ctx, "transfer_id", payload.TransferID)
		if errors.Is(findErr, errQueueItemNotFound) {
			return nil
		}
		if findErr != nil {
			return findErr
		}
		if payload.State != event.TransferDone {
			return store.setState(ctx, item.ID, stateFailed, payload.Reason)
		}
	case event.TypeDeviceCommandSettled:
		var payload event.CommandSettled
		if err = json.Unmarshal(envelope.Data, &payload); err != nil {
			return err
		}
		item, findErr := store.findByIDField(ctx, "command_id", payload.CommandID)
		if errors.Is(findErr, errQueueItemNotFound) {
			return nil
		}
		if findErr != nil {
			return findErr
		}
		if payload.OutcomeUnknown {
			return store.setState(ctx, item.ID, stateStarting, "command result unknown")
		}
		if payload.Status != event.CommandSucceeded {
			return store.setState(ctx, item.ID, stateFailed, "printer command "+payload.Status)
		}
	case event.TypeDeviceJobStarted:
		var payload event.JobStarted
		if err = json.Unmarshal(envelope.Data, &payload); err != nil {
			return err
		}
		var item queueItem
		findErr := errQueueItemNotFound
		if payload.CommandID != "" {
			item, findErr = store.findByIDField(ctx, "command_id", payload.CommandID)
		}
		if errors.Is(findErr, errQueueItemNotFound) && payload.Path != "" {
			item, findErr = store.findByJobPath(ctx, deviceID, payload.Path)
		}
		if errors.Is(findErr, errQueueItemNotFound) {
			return nil
		}
		if findErr != nil {
			return findErr
		}
		return store.setPrinting(ctx, item.ID, payload.JobID)
	case event.TypeDeviceJobSettled:
		var payload event.JobSettled
		if err = json.Unmarshal(envelope.Data, &payload); err != nil {
			return err
		}
		item, findErr := store.findByIDField(ctx, "job_id", payload.JobID)
		if errors.Is(findErr, errQueueItemNotFound) {
			return nil
		}
		if findErr != nil {
			return findErr
		}
		switch payload.Outcome {
		case event.JobFinished:
			return store.setState(ctx, item.ID, stateDone, "")
		case event.JobCancelled:
			return store.setState(ctx, item.ID, stateCancelled, "print cancelled")
		default:
			return store.setState(ctx, item.ID, stateFailed, "print ended with outcome "+payload.Outcome)
		}
	}
	return nil
}

func (m *module) sourceDigest(ctx context.Context, principal, storage, source string, info storageapi.FileInfo) (string, error) {
	if info.SHA256 != "" {
		return info.SHA256, nil
	}

	digest := sha256.New()
	var offset int64
	for offset < info.Size {
		length := storageapi.MaxReadBytes
		remaining := info.Size - offset
		if remaining < int64(length) {
			length = int(remaining)
		}
		chunk, err := m.files.Read(ctx, storageapi.ReadParams{
			Principal: principal, Storage: storage, Path: source, Offset: offset, Length: length,
		})
		if err != nil {
			return "", err
		}
		if len(chunk.Data) == 0 {
			return "", errors.New("storage returned an empty chunk before end of file")
		}
		if _, err = digest.Write(chunk.Data); err != nil {
			return "", fmt.Errorf("hash source file: %w", err)
		}
		offset += int64(len(chunk.Data))
		if chunk.EOF && offset != info.Size {
			return "", errors.New("storage ended before the recorded file size")
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func (m *module) dispatchLoop(ctx context.Context, store *queueStore) {
	m.reconcile(ctx, store)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcile(ctx, store)
			m.dispatchOnce(ctx, store)
		}
	}
}

func (m *module) dispatchOnce(ctx context.Context, store *queueStore) {
	page, err := m.fleet.List(ctx, deviceapi.ListParams{Limit: deviceapi.MaxPageLimit})
	if err != nil {
		return
	}
	for _, device := range page.Items {
		if !device.Connected || stateValue(device) != "READY" {
			continue
		}
		active, err := store.getMany(ctx, "WHERE device_id = ? AND state IN ('starting', 'printing')", device.ID)
		if err != nil || len(active) != 0 {
			continue
		}
		item, claimed, err := store.claimNext(ctx, device.ID)
		if err != nil || !claimed {
			continue
		}
		decision, err := m.access.Check(ctx, accessapi.CheckParams{
			Subject: item.SubmittedBy, Action: "operate", ObjectType: "device", ObjectID: item.DeviceID,
		})
		if err != nil || !decision.Allowed {
			_ = store.setState(ctx, item.ID, stateFailed, "submitter no longer has printer operate permission")
			continue
		}
		file, err := m.files.Stat(ctx, storageapi.StatParams{Storage: item.Storage, Path: item.Source})
		if err != nil {
			_ = store.setState(ctx, item.ID, stateFailed, "source file is no longer available: "+err.Error())
			continue
		}
		if file.IsDir || file.Irregular || file.Size != item.SourceSize || file.MTime != item.SourceMTime {
			_ = store.setState(ctx, item.ID, stateFailed, "source file changed since it was queued")
			continue
		}
		digest, err := m.sourceDigest(ctx, "", item.Storage, item.Source, file)
		if err != nil {
			_ = store.setState(ctx, item.ID, stateFailed, "source file could not be verified: "+err.Error())
			continue
		}
		if item.SourceSHA256 != "" && digest != item.SourceSHA256 {
			_ = store.setState(ctx, item.ID, stateFailed, "source file changed since it was queued")
			continue
		}
		started, err := m.fleet.Print(ctx, deviceapi.PrintParams{
			ID: device.ID, Storage: item.Storage, Source: item.Source, Path: item.Destination,
		})
		if err != nil {
			switch {
			case errors.Is(err, deviceapi.ErrNotReady), errors.Is(err, deviceapi.ErrBusy):
				_ = store.setState(ctx, item.ID, stateQueued, "")
			case errors.Is(err, deviceapi.ErrPrintRefused), errors.Is(err, deviceapi.ErrNoFile), errors.Is(err, deviceapi.ErrNameTaken):
				_ = store.setState(ctx, item.ID, stateFailed, err.Error())
			default:
				// The call may have reached the printer even though this process
				// did not receive a result. Keep the claim active and reconcile;
				// never retry an ambiguous print automatically.
				_ = store.setState(ctx, item.ID, stateStarting, "print result unknown: "+err.Error())
			}
			continue
		}
		if started.TransferID == "" && started.CommandID == "" {
			_ = store.setState(ctx, item.ID, stateFailed, "printer accepted no transfer or command id")
			continue
		}
		_ = store.setDispatch(ctx, item.ID, started.TransferID, started.CommandID)
	}
}

func (m *module) reconcile(ctx context.Context, store *queueStore) {
	items, err := store.active(ctx)
	if err != nil {
		return
	}
	for _, item := range items {
		jobs, err := m.fleet.Jobs(ctx, deviceapi.JobsParams{ID: item.DeviceID})
		if err != nil {
			continue
		}
		if jobs.Current != nil && samePrint(jobs.Current.Path, item.Destination) &&
			(item.StartedAt == nil || !jobs.Current.Started.Before(*item.StartedAt)) {
			_ = store.setPrinting(ctx, item.ID, jobs.Current.ID)
			continue
		}
		lastMatches := jobs.Last != nil && item.JobID != "" && jobs.Last.ID == item.JobID
		if !lastMatches && jobs.Last != nil && item.JobID == "" && samePrint(jobs.Last.Path, item.Destination) &&
			(item.StartedAt == nil || !jobs.Last.Started.Before(*item.StartedAt)) {
			lastMatches = true
		}
		if lastMatches {
			if jobs.Last.Outcome == "finished" {
				_ = store.setState(ctx, item.ID, stateDone, "")
			} else {
				_ = store.setState(ctx, item.ID, stateFailed, "print ended with outcome "+jobs.Last.Outcome)
			}
			continue
		}
		if item.State == stateStarting && item.StartedAt != nil && time.Since(*item.StartedAt) > 30*time.Second {
			_ = store.setState(ctx, item.ID, stateFailed, "dispatch outcome unknown; no matching printer job was found")
		}
	}
}

func samePrint(a, b string) bool {
	return a == b || (a != "" && b != "" && path.Base(a) == path.Base(b))
}

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error                { return os.Stdin.Close() }
