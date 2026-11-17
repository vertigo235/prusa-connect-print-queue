# Prusa Connect Print Queue

An independent Prusa Connect module that adds a durable, per-printer print queue.

This repository is developed against the Prusa Connect SDK. The local reference
checkout is kept outside this repository.

## Planned behavior

- one queue per printer
- add, reorder and remove pending prints
- start only when a printer is explicitly ready
- persist queue state across restarts
- track transfers and print outcomes
- provide a module API and web page

## Development

The module will use the Prusa Connect service-module contract and the SDK under
`prusa-connect/sdk`. The SDK path is configured in `go.mod` for local
checkout-based development.
