Erigon-API
---

Erigon-API is a set of tools for building applications containing Erigon node.

Our own binary [`erigon`](../cmd/erigon) is built using it.

## Modules

* [`cli`](./cli) - erigon-cli, methods & helpers to run a CLI app with Erigon node.

* [`node`](./node) - represents an Ethereum node, running devp2p and sync and writing state to the database.

* [`stagedsync`](../eth/stagedsync) - staged sync algorithm.

## Examples

* [`erigon`](../cmd/erigon/main.go) - our binary is using erigon-api with all defaults

* [`erigoncustom`](../cmd/erigoncustom/main.go) - a very simple example of adding a custom stage, a custom bucket and a custom command-line parameter

* [erigon-examples](https://github.com/mandrigin/turbo-api-examples) - a series of examples for Erigon api
