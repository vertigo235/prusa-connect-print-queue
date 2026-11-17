# Prusa Connect Print Queue

An independent Prusa Connect Local module that adds one durable, ordered queue
per printer. It queues existing G-code files from Prusa Connect storage; it
does not upload files or cancel an active print.

## Behavior

- Items persist in the module database and are ordered per printer.
- A printer must be connected and explicitly report `READY`; the module never
  marks a printer ready or clears the bed.
- Only one queue item can be active on a printer.
- Missing or changed source files, revoked permissions, failed transfers and
  failed prints remain visible as queue errors.
- An uncertain `device.print` result is reconciled against printer jobs and is
  never automatically retried.

The module API is described in `openapi.json` and is mounted by the host under
`/api/v1/modules/com.github.aux.vertigo235.prusa-connect-print-queue/`.

## Install into the local Docker deployment

This repository is developed beside `/apps/prusa-connect` and uses its local Go
SDK replacement. Build a release and install it into the Docker deployment:

```sh
make release
docker cp dist/. prusa_connect:/tmp/print-queue-release/
docker compose -f /apps/prusa-connect-local/docker-compose.yml exec -T prusa-connect \
  /prusa_connect module add github.com/vertigo235/prusa-connect-print-queue@0.2.0 \
  -from /tmp/print-queue-release -approve
docker compose -f /apps/prusa-connect-local/docker-compose.yml restart prusa-connect
```

An administrator must grant the module `operate` on each printer (the host
intentionally does not grant the organization operator role to a sink):

```sh
curl -b cookies.txt -X POST http://localhost:8090/api/v1/grants/device/<device-id> \
  -H 'Content-Type: application/json' \
  -d '{"relation":"operator","subject":"sink:print-queue"}'
```

After installing, refresh the web app and open **Printers → a printer →
Queue**. The queue is a tab on that printer's page; it is no longer intended
as a fleet-wide Extensions screen. From a storage file's menu, **Add to queue**
opens the queue form with that file prefilled.

The module requests `observe`, `operate`, and `read_storage`, and checks the
submitting principal's `operate` permission before enqueue, move, or delete.
Its worker uses the granted module permissions to inspect storage and start
prints.

## Development

```sh
make check
make release
```

The release contains the UI bundle, all supported platform binaries, and
`checksums.txt`. Planning artifacts under `artifacts/` are local and ignored.
