---
title: "EEZ Follower"
description: "Run Erigon as a follower of an EEZ chain, deriving it from its settlements on L1."
sidebar_position: 10
---

# EEZ Follower

An EEZ chain settles its blocks on an L1 through the EEZ registry. In follower mode Erigon rebuilds the EEZ chain from that L1 evidence alone: it reads the batches the registry settled, re-executes the blocks they carry, and checks every result against the state root L1 committed to. The safe block is the latest settled block; the finalized block is the latest one settled in a finalized L1 block. A block that contradicts canonical L1 stops the node.

EEZ support is off unless `--eez.enabled` is set; without it Erigon behaves as it always does.

## Running a follower

The follower keeps the EEZ chain under `<datadir>/eez`, with its own database and lock. It needs an L1 source: the JSON-RPC endpoint of any L1 node, or the private API of an Erigon L1 node.

```bash
./build/bin/erigon --eez.enabled --eez.follower --chain=eez-devnet \
  --datadir=/data/eez-devnet \
  --eez.l1.rpc=http://l1-node:8545 \
  --http.api=eth,debug,eez
```

In follower mode the node runs no consensus layer, no engine API, no transaction pool, no snapshot downloader and no P2P networking: the follower is the only source and the only driver of the chain.

### A chain from its genesis file

For a chain that is not built in, such as a Kurtosis devnet enclave, initialise its genesis first. The `l2` section of the chain config either comes with the genesis file or from the flags below, which `init` writes into it:

```bash
./build/bin/erigon init --datadir=/data/eez --eez.enabled \
  --eez.l1.chainid=7331 \
  --eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3 \
  --eez.registry.deployblock=12 \
  --eez.rollupid=1 \
  --eez.l1.blocktime=12s --eez.l2.blocktime=2s \
  genesis.json

./build/bin/erigon --eez.enabled --eez.follower --datadir=/data/eez --eez.l1.rpc=http://l1-node:8545
```

A follower checks these flags against the stored `l2` section and refuses to start when they differ.

### Reading an Erigon L1 directly

Next to an Erigon L1 node, read its database over the private API instead of JSON-RPC. The follower then reacts to the L1 node's new heads as they are executed:

```bash
./build/bin/erigon --eez.enabled --eez.follower --chain=eez-devnet \
  --datadir=/data/eez-devnet --eez.l1.kv=127.0.0.1:9090
```

### Following the sequencer

With `--eez.sequencer.rpc` the follower also takes the sequencer's latest block as its head, ahead of settlement, as long as the sequencer's chain descends from the safe block. The sequencer must serve `debug_getRawBlock`. Settlement later confirms these blocks without rebuilding them.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--eez.enabled` | | Enable EEZ support: the EEZ networks for `--chain`, the `l2` config section, transaction type `0x76` and the EEZ rules. Every other `--eez.*` flag requires it. |
| `--eez.follower` | | Derive the EEZ chain from L1. |
| `--eez.l1.rpc` | | JSON-RPC endpoint of the L1 node. |
| `--eez.l1.kv` | | Private API address of an Erigon L1 node. |
| `--eez.l1.poll` | `2s` | Interval between reads of the L1 head. |
| `--eez.l1.logchunk` | `100000` | Widest block range of one L1 log query; halved when the node refuses it. |
| `--eez.l1.reorgdepth` | `62` | Deepest L1 reorg the follower survives. |
| `--eez.sequencer.rpc` | | Sequencer endpoint for the unsafe head. |
| `--eez.l1.chainid`, `--eez.registry`, `--eez.registry.deployblock`, `--eez.rollupid`, `--eez.l1.blocktime`, `--eez.l2.blocktime` | | The `l2` section of the chain config, written by `init`. |

## Restarts and L1 reorgs

The follower records its latest settlement in `<datadir>/eez/eez-reconcile-checkpoint`. On restart it resumes there when L1 and the local chain still hold that settlement; otherwise it resumes at the newest settlement whose block the local chain holds, and failing that from the registry's deploy block.

An L1 reorg that drops settlements moves the head and the safe block back to what the remaining settlements settled, and the replacement branch is derived. A reorg deeper than `--eez.l1.reorgdepth`, or one that drops a finalized settlement, stops the node.

## Halting

The node exits with status `3` when a settled block contradicts canonical L1: the block it derives does not hash to the root L1 committed to, the DA payload cannot be read, or an L2-to-L1 call L1 settled was never made on L2. The same evidence on an L1 block that has since been reorged is not a divergence; the follower rewinds and continues. Other failures, such as an unreachable L1 node, are retried.

## RPC

The `eez` namespace, enabled with `--http.api=...,eez`:

- `eez_followerStatus()`: the last L1 block scanned, the latest finalized L1 block, the cursor, safe and finalized EEZ blocks, and the latest settlement.
- `eez_settlement(blockNumber)`: the L1 settlement that settled an EEZ block, or `null` while none has, or when it predates the follower's start or the latest finalized settlement.
- `eez_decodeBatch(input)`: a `postAndVerifyBatch` input decoded, with the DA payload it carries.

EEZ system transactions (type `0x76`) render with the fixed gas of `2000000`, a zero gas price and zero signature values, from the system address.

## Metrics and logs

Prometheus metrics: `eez_l1_head`, `eez_l1_scanned`, `eez_l1_finalized`, `eez_l1_lag_blocks`, `eez_l2_cursor`, `eez_l2_safe`, `eez_l2_finalized`, `eez_l2_blocks_derived_total`, `eez_l1_reorgs_total`, `eez_divergences_total` and `eez_sync_seconds`.

Log lines that mark derivation events carry an `event` field named like the reference client's, for example `eez.deriver.l1.reorg.retreated`, `eez.deriver.state.diverged_post` or `eez.l1_watcher.reorg.detected`.

## The `eez` tool

`make eez` builds `build/bin/eez`:

- `eez decode <hex | @file | ->` decodes a `postAndVerifyBatch` input.
- `eez diff --a <url> --b <url> --from <n> --to <n>` finds the first block two EEZ nodes disagree on and the header fields that differ.
- `eez capture --l1.rpc <url> --registry <address> --from <n> --to <n> --out <dir>` records the L1 blocks, registry logs and batch postings a follower reads, for replay in tests.

## Docker

The `erigon` binary of the standard image includes follower mode. To add the `eez` tool:

```bash
docker build --build-arg BINARIES="erigon eez" -t erigon-eez .
docker run -v /data/eez:/home/erigon/data erigon-eez \
  --eez.enabled --eez.follower --chain=eez-devnet --datadir=/home/erigon/data \
  --eez.l1.rpc=http://l1-node:8545
```

## Networks

| Network | `--chain` | Notes |
|---|---|---|
| EEZ devnet | `eez-devnet` | The genesis of the reference Kurtosis devnet, registry at `0x5fbdb2315678afecb367f032d93f642f64180aa3`, rollup 1 on L1 chain 7331. |
| Kurtosis enclave | | Initialise the enclave's `genesis.json` with `init`; deployments rewrite the genesis timestamp. |
| EEZ Chiado | | Not supported yet: it posts its DA payload in blobs, which the follower does not read. |
