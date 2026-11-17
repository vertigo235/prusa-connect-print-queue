# Changelog

## 0.3.0 - 2026-11-17

- Queue files already held on printer storage from printer Files tabs.
- Bound queue history and display names, paginate fleet reads, and recover job history after missed events.
- Harden dispatch serialization, transfer matching, permission rechecks, and cancellation recovery.
- Add HTTP, dispatch, reconciliation, printer-file, and concurrent queue tests.

## 0.2.0 - 2026-11-17

- Add a durable per-printer queue backed by the module database.
- Add authenticated queue inspection, enqueue, reorder, and pending-item deletion.
- Dispatch only connected printers explicitly reporting `READY`.
- Reconcile transfer, command, and job events without retrying ambiguous prints.
- Add the Print Queue UI and repeatable multi-platform release packaging.

## 0.1.0 - 2026-11-17

- Start the print queue module with a service health endpoint.
