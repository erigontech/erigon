Turbo-API
---

Turbo-API is a set of tools for building applications containing turbo-geth node.

Our own binary [`tg`](../cmd/tg) is built using it.

## Modules

* [`cli`](./cli) - turbo-cli, methods & helpers to run a CLI app with turbo-geth node.

* [`node`](./node) - represents an Ethereum node, running devp2p and sync and writing state to the database.

* [`stagedsync`](../eth/stagedsync) - staged sync algorithm.

## Examples

* [`tg`](../cmd/tg/main.go) - our binary is using turbo-api with all defaults

* [`tgcustom`](../cmd/tgcustom/main.go) - a very simple example of adding a custom stage, a custom bucket and a custom command-line parameter

* [turbo-api-examples](https://github.com/mandrigin/turbo-api-examples) - a series of examples for turbo-geth api
