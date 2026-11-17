package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	stateQueued    = "queued"
	stateStarting  = "starting"
	statePrinting  = "printing"
	stateDone      = "done"
	stateFailed    = "failed"
	stateCancelled = "cancelled"
)

type queueItem struct {
	ID           string     `json:"id"`
	DeviceID     string     `json:"device_id"`
	Storage      string     `json:"storage"`
	Source       string     `json:"source"`
	SourceSize   int64      `json:"source_size"`
	SourceMTime  int64      `json:"source_mtime"`
	SourceSHA256 string     `json:"source_sha256,omitempty"`
	Destination  string     `json:"destination"`
	DisplayName  string     `json:"display_name"`
	SubmittedBy  string     `json:"submitted_by"`
	Position     int        `json:"position"`
	State        string     `json:"state"`
	TransferID   string     `json:"transfer_id,omitempty"`
	CommandID    string     `json:"command_id,omitempty"`
	JobID        string     `json:"job_id,omitempty"`
	Error        string     `json:"error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type newQueueItem struct {
	DeviceID     string
	Storage      string
	Source       string
	SourceSize   int64
	SourceMTime  int64
	SourceSHA256 string
	Destination  string
	DisplayName  string
	SubmittedBy  string
}

type queueStore struct {
	db       *sql.DB
	postgres bool
	now      func() time.Time
}

var errQueueItemNotFound = errors.New("queue item not found")
var errQueueItemNotPending = errors.New("queue item is not pending")

func newQueueStore(db *sql.DB, engine string) *queueStore {
	return &queueStore{db: db, postgres: engine == "postgres", now: time.Now}
}

func (s *queueStore) query(query string) string {
	if !s.postgres {
		return query
	}

	var out strings.Builder
	argument := 1
	for index, part := range strings.Split(query, "?") {
		if index > 0 {
			fmt.Fprintf(&out, "$%d", argument)
			argument++
		}
		out.WriteString(part)
	}

	return out.String()
}

func (s *queueStore) enqueue(ctx context.Context, input newQueueItem) (queueItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queueItem{}, fmt.Errorf("begin enqueue: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var position int
	if err = tx.QueryRowContext(ctx, s.query(`
		SELECT COALESCE(MAX(position), -1) + 1
		FROM queue_items
		WHERE device_id = ? AND state = 'queued'`), input.DeviceID).Scan(&position); err != nil {
		return queueItem{}, fmt.Errorf("find queue position: %w", err)
	}

	now := s.now().UTC()
	item := queueItem{
		ID:           uuid.NewString(),
		DeviceID:     input.DeviceID,
		Storage:      input.Storage,
		Source:       input.Source,
		SourceSize:   input.SourceSize,
		SourceMTime:  input.SourceMTime,
		SourceSHA256: input.SourceSHA256,
		Destination:  input.Destination,
		DisplayName:  input.DisplayName,
		SubmittedBy:  input.SubmittedBy,
		Position:     position,
		State:        stateQueued,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	_, err = tx.ExecContext(ctx, s.query(`
		INSERT INTO queue_items
		(id, device_id, storage, source, source_size, source_mtime, source_sha256,
		 destination, display_name, submitted_by, position, state, transfer_id,
		 command_id, job_id, error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', '', ?, ?)`),
		item.ID, item.DeviceID, item.Storage, item.Source, item.SourceSize,
		item.SourceMTime, item.SourceSHA256, item.Destination, item.DisplayName,
		item.SubmittedBy, item.Position, item.State, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		return queueItem{}, fmt.Errorf("insert queue item: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return queueItem{}, fmt.Errorf("commit enqueue: %w", err)
	}

	return item, nil
}

func (s *queueStore) list(ctx context.Context, deviceID string) ([]queueItem, error) {
	query := `
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error,
		       created_at, started_at, finished_at, updated_at
		FROM queue_items`
	args := []any{}
	if deviceID != "" {
		query += " WHERE device_id = ?"
		args = append(args, deviceID)
	}
	query += " ORDER BY device_id, CASE state WHEN 'queued' THEN 0 WHEN 'starting' THEN 1 WHEN 'printing' THEN 2 ELSE 3 END, position, created_at, id"

	rows, err := s.db.QueryContext(ctx, s.query(query), args...)
	if err != nil {
		return nil, fmt.Errorf("list queue: %w", err)
	}
	defer rows.Close()

	var items []queueItem
	for rows.Next() {
		item, scanErr := scanQueueItem(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("read queue item: %w", scanErr)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list queue rows: %w", err)
	}

	return items, nil
}

func (s *queueStore) get(ctx context.Context, id string) (queueItem, error) {
	row := s.db.QueryRowContext(ctx, s.query(`
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error,
		       created_at, started_at, finished_at, updated_at
		FROM queue_items WHERE id = ?`), id)

	item, err := scanQueueItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return queueItem{}, errQueueItemNotFound
	}
	if err != nil {
		return queueItem{}, fmt.Errorf("get queue item: %w", err)
	}

	return item, nil
}

func (s *queueStore) claimNext(ctx context.Context, deviceID string) (queueItem, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queueItem{}, false, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, s.query(`
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error, created_at, started_at, finished_at, updated_at
		FROM queue_items
		WHERE device_id = ? AND state = 'queued'
		ORDER BY position, created_at, id
		LIMIT 1`), deviceID)
	item, err := scanQueueItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return queueItem{}, false, nil
	}
	if err != nil {
		return queueItem{}, false, fmt.Errorf("find next queue item: %w", err)
	}

	now := s.now().UTC()
	result, err := tx.ExecContext(ctx, s.query(`
		UPDATE queue_items
		SET state = 'starting', started_at = ?, updated_at = ?
		WHERE id = ? AND state = 'queued'`), now, now, item.ID)
	if err != nil {
		return queueItem{}, false, fmt.Errorf("claim queue item: %w", err)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		if affectedErr != nil {
			return queueItem{}, false, fmt.Errorf("check queue claim: %w", affectedErr)
		}
		return queueItem{}, false, nil
	}
	if err = tx.Commit(); err != nil {
		return queueItem{}, false, fmt.Errorf("commit queue claim: %w", err)
	}

	item.State = stateStarting
	item.StartedAt = &now
	item.UpdatedAt = now

	return item, true, nil
}

func (s *queueStore) setDispatch(ctx context.Context, id, transferID, commandID string) error {
	_, err := s.db.ExecContext(ctx, s.query(`
		UPDATE queue_items
		SET transfer_id = ?, command_id = ?, updated_at = ?
		WHERE id = ? AND state = 'starting'`), transferID, commandID, s.now().UTC(), id)
	if err != nil {
		return fmt.Errorf("save dispatch ids: %w", err)
	}

	return nil
}

func (s *queueStore) setState(ctx context.Context, id, state, message string) error {
	now := s.now().UTC()
	finished := any(nil)
	if state == stateDone || state == stateFailed || state == stateCancelled {
		finished = now
	}

	_, err := s.db.ExecContext(ctx, s.query(`
		UPDATE queue_items
		SET state = ?, error = ?, finished_at = COALESCE(?, finished_at), updated_at = ?
		WHERE id = ?`), state, message, finished, now, id)
	if err != nil {
		return fmt.Errorf("set queue state: %w", err)
	}

	return nil
}

func (s *queueStore) setPrinting(ctx context.Context, id, jobID string) error {
	_, err := s.db.ExecContext(ctx, s.query(`
		UPDATE queue_items
		SET state = 'printing', job_id = ?, updated_at = ?
		WHERE id = ? AND state = 'starting'`), jobID, s.now().UTC(), id)
	if err != nil {
		return fmt.Errorf("mark queue item printing: %w", err)
	}

	return nil
}

func (s *queueStore) findByIDField(ctx context.Context, field, value string) (queueItem, error) {
	if field != "transfer_id" && field != "command_id" && field != "job_id" {
		return queueItem{}, errors.New("invalid queue lookup field")
	}
	return s.getByQuery(ctx, fmt.Sprintf("WHERE %s = ? AND state IN ('starting', 'printing')", field), value)
}

func (s *queueStore) findByJobPath(ctx context.Context, deviceID, destination string) (queueItem, error) {
	items, err := s.getMany(ctx, "WHERE device_id = ? AND state IN ('starting', 'printing') ORDER BY created_at, id", deviceID)
	if err != nil {
		return queueItem{}, err
	}
	for _, item := range items {
		if item.Destination == destination || path.Base(item.Destination) == path.Base(destination) {
			return item, nil
		}
	}
	return queueItem{}, errQueueItemNotFound
}

func (s *queueStore) findByTransferStart(ctx context.Context, storage, source, destination string) (queueItem, error) {
	items, err := s.getMany(ctx, "WHERE storage = ? AND source = ? AND state = 'starting' ORDER BY created_at, id", storage, source)
	if err != nil {
		return queueItem{}, err
	}
	for _, item := range items {
		if item.Destination == destination || path.Base(item.Destination) == path.Base(destination) {
			return item, nil
		}
	}
	return queueItem{}, errQueueItemNotFound
}

func (s *queueStore) getByQuery(ctx context.Context, suffix string, args ...any) (queueItem, error) {
	row := s.db.QueryRowContext(ctx, s.query(`
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error,
		       created_at, started_at, finished_at, updated_at
		FROM queue_items `+suffix+` ORDER BY created_at, id LIMIT 1`), args...)
	item, err := scanQueueItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return queueItem{}, errQueueItemNotFound
	}
	if err != nil {
		return queueItem{}, fmt.Errorf("find queue item: %w", err)
	}
	return item, nil
}

func (s *queueStore) deletePending(ctx context.Context, id string) (queueItem, error) {
	item, err := s.get(ctx, id)
	if err != nil {
		return queueItem{}, err
	}
	if item.State != stateQueued {
		return queueItem{}, errQueueItemNotPending
	}
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, s.query(`
		UPDATE queue_items
		SET state = 'cancelled', error = 'cancelled by user', finished_at = ?, updated_at = ?
		WHERE id = ? AND state = 'queued'`), now, now, id)
	if err != nil {
		return queueItem{}, fmt.Errorf("cancel queue item: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return queueItem{}, fmt.Errorf("check queue cancellation: %w", err)
	}
	if affected != 1 {
		return queueItem{}, errQueueItemNotPending
	}
	item.State = stateCancelled
	item.Error = "cancelled by user"
	item.FinishedAt = &now
	item.UpdatedAt = now
	return item, nil
}

func (s *queueStore) move(ctx context.Context, id string, position int) (queueItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queueItem{}, fmt.Errorf("begin move: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, s.query(`
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error, created_at, started_at, finished_at, updated_at
		FROM queue_items WHERE id = ?`), id)
	item, err := scanQueueItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return queueItem{}, errQueueItemNotFound
	}
	if err != nil {
		return queueItem{}, fmt.Errorf("read item to move: %w", err)
	}
	if item.State != stateQueued {
		return queueItem{}, errQueueItemNotPending
	}

	rows, err := tx.QueryContext(ctx, s.query(`
		SELECT id FROM queue_items
		WHERE device_id = ? AND state = 'queued'
		ORDER BY position, created_at, id`), item.DeviceID)
	if err != nil {
		return queueItem{}, fmt.Errorf("list items to move: %w", err)
	}
	var ids []string
	for rows.Next() {
		var one string
		if err = rows.Scan(&one); err != nil {
			_ = rows.Close()
			return queueItem{}, fmt.Errorf("read item ids: %w", err)
		}
		ids = append(ids, one)
	}
	if err = rows.Close(); err != nil {
		return queueItem{}, fmt.Errorf("close item ids: %w", err)
	}
	if err = rows.Err(); err != nil {
		return queueItem{}, fmt.Errorf("read item ids: %w", err)
	}

	current := 0
	for index, one := range ids {
		if one == id {
			current = index
			break
		}
	}
	ids = append(ids[:current], ids[current+1:]...)
	if position < 0 {
		position = 0
	}
	if position > len(ids) {
		position = len(ids)
	}
	ids = append(ids, "")
	copy(ids[position+1:], ids[position:])
	ids[position] = id

	now := s.now().UTC()
	for index, one := range ids {
		result, execErr := tx.ExecContext(ctx, s.query(`UPDATE queue_items SET position = ?, updated_at = ? WHERE id = ? AND state = 'queued'`), index, now, one)
		if execErr != nil {
			return queueItem{}, fmt.Errorf("write queue order: %w", execErr)
		}
		if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
			if affectedErr != nil {
				return queueItem{}, fmt.Errorf("check queue order: %w", affectedErr)
			}
			return queueItem{}, errQueueItemNotPending
		}
	}
	if err = tx.Commit(); err != nil {
		return queueItem{}, fmt.Errorf("commit move: %w", err)
	}

	item.Position = position
	item.UpdatedAt = now
	return item, nil
}

func (s *queueStore) active(ctx context.Context) ([]queueItem, error) {
	return s.getMany(ctx, `WHERE state IN ('starting', 'printing') ORDER BY device_id, position, created_at`)
}

func (s *queueStore) getMany(ctx context.Context, suffix string, args ...any) ([]queueItem, error) {
	rows, err := s.db.QueryContext(ctx, s.query(`
		SELECT id, device_id, storage, source, source_size, source_mtime, source_sha256,
		       destination, display_name, submitted_by, position, state, transfer_id,
		       command_id, job_id, error,
		       created_at, started_at, finished_at, updated_at
		FROM queue_items `+suffix), args...)
	if err != nil {
		return nil, fmt.Errorf("query queue items: %w", err)
	}
	defer rows.Close()

	var items []queueItem
	for rows.Next() {
		item, scanErr := scanQueueItem(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan queue items: %w", scanErr)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read queue items: %w", err)
	}
	return items, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanQueueItem(row rowScanner) (queueItem, error) {
	var item queueItem
	var started, finished sql.NullTime
	if err := row.Scan(
		&item.ID, &item.DeviceID, &item.Storage, &item.Source, &item.SourceSize,
		&item.SourceMTime, &item.SourceSHA256, &item.Destination, &item.DisplayName,
		&item.SubmittedBy, &item.Position, &item.State, &item.TransferID,
		&item.CommandID, &item.JobID, &item.Error,
		&item.CreatedAt, &started, &finished, &item.UpdatedAt,
	); err != nil {
		return queueItem{}, err
	}
	if started.Valid {
		item.StartedAt = &started.Time
	}
	if finished.Valid {
		item.FinishedAt = &finished.Time
	}
	return item, nil
}
