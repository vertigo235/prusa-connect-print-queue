package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/prusa3d/prusa-connect/sdk/moduledb"
	"github.com/prusa3d/prusa-connect/sdk/wirerpc"
)

func openTestStore(t *testing.T) (*queueStore, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db, _, err := moduledb.Open(ctx, wirerpc.DatabaseGrant{
		Engine: wirerpc.DatabaseSQLite,
		DSN:    t.TempDir() + "/queue.db",
	}, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newQueueStore(db, string(wirerpc.DatabaseSQLite)), db
}

func TestQueuePersistsOrderAndStateAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	grant := wirerpc.DatabaseGrant{Engine: wirerpc.DatabaseSQLite, DSN: dir + "/queue.db"}
	ctx := context.Background()

	db, _, err := moduledb.Open(ctx, grant, schema)
	if err != nil {
		t.Fatal(err)
	}
	store := newQueueStore(db, string(wirerpc.DatabaseSQLite))
	first, err := store.enqueue(ctx, newQueueItem{DeviceID: "printer-1", Storage: "local", Source: "a.gcode", SourceSize: 10, SourceMTime: 1, Destination: "a.gcode", DisplayName: "A", SubmittedBy: "user:a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.enqueue(ctx, newQueueItem{DeviceID: "printer-1", Storage: "local", Source: "b.gcode", SourceSize: 20, SourceMTime: 2, Destination: "b.gcode", DisplayName: "B", SubmittedBy: "user:b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.move(ctx, second.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err = store.setState(ctx, first.ID, stateDone, "finished"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	db, _, err = moduledb.Open(ctx, grant, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = newQueueStore(db, string(wirerpc.DatabaseSQLite))
	items, err := store.list(ctx, "printer-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ID != second.ID || items[1].ID != first.ID {
		t.Fatalf("restart lost order: %#v", items)
	}
	if items[1].State != stateDone || items[1].Error != "finished" {
		t.Fatalf("restart lost state: %#v", items[1])
	}
}

func TestQueueClaimsOneItemOnce(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()
	if _, err := store.enqueue(ctx, newQueueItem{DeviceID: "printer-1", Storage: "local", Source: "a.gcode", Destination: "a.gcode", DisplayName: "A", SubmittedBy: "user:a"}); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	claimed := make(chan string, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			item, ok, err := store.claimNext(ctx, "printer-1")
			if err != nil {
				errs <- err
				return
			}
			if ok {
				claimed <- item.ID
			}
		}()
	}
	wait.Wait()
	close(claimed)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var ids []string
	for id := range claimed {
		ids = append(ids, id)
	}
	if len(ids) != 1 {
		t.Fatalf("%d workers claimed one item: %v", len(ids), ids)
	}
}

func TestQueueConcurrentEnqueueHasUniquePositions(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()
	const count = 32
	items := make(chan queueItem, count)
	errs := make(chan error, count)
	var wait sync.WaitGroup
	wait.Add(count)
	for index := range count {
		go func(index int) {
			defer wait.Done()
			item, err := store.enqueue(ctx, newQueueItem{
				DeviceID: "printer-1", Storage: "local", Source: fmt.Sprintf("%d.gcode", index),
				Destination: fmt.Sprintf("%d.gcode", index), DisplayName: fmt.Sprintf("%d", index), SubmittedBy: "user:a",
			})
			if err != nil {
				errs <- err
				return
			}
			items <- item
		}(index)
	}
	wait.Wait()
	close(items)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	positions := make([]int, 0, count)
	for item := range items {
		positions = append(positions, item.Position)
	}
	sort.Ints(positions)
	for index, position := range positions {
		if index != position {
			t.Fatalf("position %d was claimed by multiple enqueues: %v", index, positions)
		}
	}
}
