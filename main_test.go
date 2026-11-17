package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prusa3d/prusa-connect/sdk/accessapi"
	"github.com/prusa3d/prusa-connect/sdk/deviceapi"
	"github.com/prusa3d/prusa-connect/sdk/event"
	"github.com/prusa3d/prusa-connect/sdk/routesapi"
	"github.com/prusa3d/prusa-connect/sdk/storageapi"
	"github.com/prusa3d/prusa-connect/sdk/wirerpc"
)

type queueTestCaller struct {
	mu            sync.Mutex
	device        deviceapi.Device
	printerFile   deviceapi.File
	storageFile   storageapi.FileInfo
	history       []deviceapi.Job
	allowStorage  bool
	printRequests int
}

func (f *queueTestCaller) Call(_ context.Context, method wirerpc.Method, params any) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch method {
	case deviceapi.MethodList:
		return json.Marshal(deviceapi.Page{Items: []deviceapi.Device{f.device}, Total: 1, Limit: deviceapi.MaxPageLimit})
	case deviceapi.MethodGet:
		return json.Marshal(f.device)
	case deviceapi.MethodFiles:
		return json.Marshal(deviceapi.Listing{Entry: &f.printerFile})
	case deviceapi.MethodPrint:
		f.printRequests++
		return json.Marshal(deviceapi.Started{TransferID: "transfer-1"})
	case deviceapi.MethodJobs:
		return json.Marshal(deviceapi.Jobs{})
	case deviceapi.MethodJobHistory:
		return json.Marshal(deviceapi.JobPage{Items: f.history, Limit: deviceapi.MaxPageLimit})
	case storageapi.MethodStat:
		return json.Marshal(f.storageFile)
	case storageapi.MethodRead:
		return json.Marshal(storageapi.Chunk{Data: []byte("source"), Size: f.storageFile.Size, EOF: true})
	case accessapi.MethodCheck:
		var question accessapi.CheckParams
		body, _ := json.Marshal(params)
		if err := json.Unmarshal(body, &question); err != nil {
			return nil, err
		}
		allowed := true
		if question.Action == "read_storage" {
			allowed = f.allowStorage
		}
		return json.Marshal(accessapi.Decision{Allowed: allowed})
	default:
		return json.Marshal(struct{}{})
	}
}

func newQueueTestModule(t *testing.T, caller *queueTestCaller) (*module, *queueStore) {
	t.Helper()
	store, _ := openTestStore(t)
	return &module{
		fleet:  deviceapi.NewClient(caller),
		access: accessapi.NewClient(caller),
		files:  storageapi.NewClient(caller),
		store:  store,
	}, store
}

func moduleRequest(t *testing.T, method, path, principal string, body any) *http.Request {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := (routesapi.Request{
		Method: method, Path: path, Body: raw, Principal: principal,
	}).HTTP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestEnqueuePrinterFileStoresAndPrintsTheValidatedPath(t *testing.T) {
	caller := &queueTestCaller{
		device:       deviceapi.Device{ID: "printer-1", Kind: "printer", Connected: true},
		printerFile:  deviceapi.File{Path: "usb/part.gcode", Size: 10, Modified: 7},
		allowStorage: true,
	}
	module, store := newQueueTestModule(t, caller)

	recorder := httptest.NewRecorder()
	module.ServeHTTP(recorder, moduleRequest(t, http.MethodPost, "/queues/printer-1/items", "user:alice", map[string]string{
		"printer_path": "usb/part.gcode",
	}))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("enqueue status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	items, err := store.list(context.Background(), "printer-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Source != "usb/part.gcode" || items[0].Destination != "usb/part.gcode" {
		t.Fatalf("stored printer file = %#v", items)
	}
}

func TestEnqueuePrinterFileRejectsDestinationOverride(t *testing.T) {
	caller := &queueTestCaller{
		device:      deviceapi.Device{ID: "printer-1", Kind: "printer", Connected: true},
		printerFile: deviceapi.File{Path: "usb/part.gcode", Size: 10, Modified: 7},
	}
	module, _ := newQueueTestModule(t, caller)

	recorder := httptest.NewRecorder()
	module.ServeHTTP(recorder, moduleRequest(t, http.MethodPost, "/queues/printer-1/items", "user:alice", map[string]string{
		"printer_path": "usb/part.gcode",
		"destination":  "usb/other.gcode",
	}))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("override status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestConcurrentDispatchKeepsOneActiveItemPerPrinter(t *testing.T) {
	caller := &queueTestCaller{
		device:       deviceapi.Device{ID: "printer-1", Kind: "printer", Connected: true, State: map[string]json.RawMessage{"/state": json.RawMessage(`"READY"`)}},
		storageFile:  storageapi.FileInfo{Path: "part.gcode", Size: 10, MTime: 7, SHA256: "digest"},
		allowStorage: true,
	}
	module, store := newQueueTestModule(t, caller)
	for _, name := range []string{"one.gcode", "two.gcode"} {
		if _, err := store.enqueue(context.Background(), newQueueItem{
			DeviceID: "printer-1", Storage: "local", Source: name, SourceSize: 10,
			SourceMTime: 7, SourceSHA256: "digest", Destination: "usb/" + name,
			DisplayName: name, SubmittedBy: "user:alice",
		}); err != nil {
			t.Fatal(err)
		}
	}

	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			module.dispatchOnce(context.Background(), store)
		}()
	}
	wait.Wait()

	items, err := store.list(context.Background(), "printer-1")
	if err != nil {
		t.Fatal(err)
	}
	if caller.printRequests != 1 {
		t.Fatalf("print requests = %d, want 1", caller.printRequests)
	}
	var queued, active int
	for _, item := range items {
		if item.State == stateQueued {
			queued++
		}
		if item.State == stateStarting || item.State == statePrinting {
			active++
		}
	}
	if queued != 1 || active != 1 {
		t.Fatalf("queue states = %#v, want one queued and one active", items)
	}
}

func TestReconcileRecoversCancelledJobFromHistory(t *testing.T) {
	ended := time.Now()
	caller := &queueTestCaller{
		device:  deviceapi.Device{ID: "printer-1", Kind: "printer", Connected: true},
		history: []deviceapi.Job{{ID: "job-1", Path: "usb/part.gcode", Started: ended.Add(-time.Minute), Ended: &ended, Outcome: event.JobCancelled}},
	}
	module, store := newQueueTestModule(t, caller)
	item, err := store.enqueue(context.Background(), newQueueItem{
		DeviceID: "printer-1", Storage: "", Source: "usb/part.gcode", SourceSize: 10,
		SourceMTime: 7, Destination: "usb/part.gcode", DisplayName: "part.gcode", SubmittedBy: "user:alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.claimNext(context.Background(), item.DeviceID)
	if err != nil || !ok {
		t.Fatalf("claim = %#v, %v, want claimed item", claimed, err)
	}
	if err := store.setPrinting(context.Background(), claimed.ID, "job-1"); err != nil {
		t.Fatal(err)
	}

	module.reconcile(context.Background(), store)
	items, err := store.list(context.Background(), item.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].State != stateCancelled {
		t.Fatalf("reconciled item = %#v", items)
	}
}

func TestDispatchRechecksStoragePermission(t *testing.T) {
	caller := &queueTestCaller{
		device:       deviceapi.Device{ID: "printer-1", Kind: "printer", Connected: true, State: map[string]json.RawMessage{"/state": json.RawMessage(`"READY"`)}},
		storageFile:  storageapi.FileInfo{Path: "part.gcode", Size: 10, MTime: 7, SHA256: "digest"},
		allowStorage: false,
	}
	module, store := newQueueTestModule(t, caller)
	if _, err := store.enqueue(context.Background(), newQueueItem{
		DeviceID: "printer-1", Storage: "local", Source: "part.gcode", SourceSize: 10,
		SourceMTime: 7, SourceSHA256: "digest", Destination: "usb/part.gcode",
		DisplayName: "part.gcode", SubmittedBy: "user:alice",
	}); err != nil {
		t.Fatal(err)
	}

	module.dispatchOnce(context.Background(), store)
	items, err := store.list(context.Background(), "printer-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].State != stateFailed {
		t.Fatalf("permission-revoked item = %#v", items)
	}
	if caller.printRequests != 0 {
		t.Fatalf("print requests = %d, want 0", caller.printRequests)
	}
}
