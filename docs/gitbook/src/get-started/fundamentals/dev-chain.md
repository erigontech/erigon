---
description: How to run a local Erigon development chain for testing and development.
---

# Dev Chain

A dev chain is a local, private network useful for smart contract development, testing RPC calls, and experimenting with Erigon configuration without connecting to a public network.

## Quick Start

```bash
./erigon \
  --datadir=dev \
  --chain=dev \
  --mine \
  --http.api=eth,erigon,web3,net,debug,trace,txpool,parity,admin \
  --http.corsdomain="*"
```

Key flags:

| Flag | Purpose |
|------|---------|
| `--chain=dev` | Run a local dev chain |
| `--datadir=dev` | Store chain data in the `dev/` folder |
| `--mine` | Enable block production |
| `--dev.period <seconds>` | Block interval in seconds. Must be > 0 to mine empty blocks (default: 0, mines only when there are transactions) |

## Running Multiple Nodes

To connect two local nodes, save the `enode://` address printed at startup of node 1 and pass it to node 2 via `--staticpeers`.

```bash
# Node 1
./erigon --datadir=dev1 --chain=dev --mine --port=30303

# Node 2 (uses node 1's enode as static peer)
./erigon --datadir=dev2 --chain=dev --port=30304 \
  --staticpeers="enode://<NODE1_PUBKEY>@127.0.0.1:30303"
```

## Full Tutorial

For a complete step-by-step walkthrough including RPC daemon setup and multi-node configuration, see [DEV\_CHAIN.md](https://github.com/erigontech/erigon/blob/main/docs/DEV_CHAIN.md) in the repository.
