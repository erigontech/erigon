Turbo-API
---

Turbo-API is a set of tools for building applications containing turbo-geth node.

Our own binary [`tg`](../cmd/tg) is built using it.

## Modules

* [`cli`](./cli) - turbo-cli, methods & helpers to run a CLI app with turbo-geth node.

* [`node`](./node) - represents an Ethereum node, running devp2p and sync and writing state to the database.

* [`stagedsync`](../eth/stagedsync) - staged sync algorithm.
