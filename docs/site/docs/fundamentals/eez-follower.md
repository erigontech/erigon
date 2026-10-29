---
title: "EEZ Follower"
description: "Run Erigon as a follower of an EEZ chain, deriving it from its settlements on L1."
sidebar_position: 10
---

# EEZ Follower

An EEZ chain settles its blocks on an L1 through the EEZ registry. With `--eez.enabled` Erigon runs an EEZ chain as its follower, rebuilding it from that L1 evidence alone: it reads the batches the registry settled, re-executes the blocks they carry, and checks every result against the state root L1 committed to. The safe block is the latest settled block; the finalized block is the latest one settled in a finalized L1 block. A block that contradicts canonical L1 stops the node.

EEZ support is off unless `--eez.enabled` is set; without it Erigon behaves as it always does.

## Running a follower

An EEZ node keeps its chain's database in `<datadir>/eez/chaindata` and its snapshots in `<datadir>/snapshots/eez`, as Caplin keeps its own; the rest of the datadir is the node's. The follower needs an L1 source: the JSON-RPC endpoint of any L1 node, or the private API of an Erigon L1 node.

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet \
  --datadir=/data/eez-devnet \
  --eez.l1.rpc=http://l1-node:8545 \
  --http.api=eth,debug,eez
```

An EEZ node runs no consensus layer, no engine API, no transaction pool, no snapshot downloader and no P2P networking: the follower is the only source and the only driver of the chain.

### A chain from its genesis file

For a chain that is not built in, such as a Kurtosis devnet enclave, initialise its genesis first. The `l2` section of the chain config either comes with the genesis file or from the flags below, which `init` writes into it; `init --eez.enabled` refuses a genesis left without one:

```bash
./build/bin/erigon init --datadir=/data/eez --eez.enabled \
  --eez.l1.chainid=7331 \
  --eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3 \
  --eez.registry.deployblock=12 \
  --eez.rollupid=1 \
  --eez.l1.blocktime=12s --eez.l2.blocktime=2s \
  genesis.json

./build/bin/erigon --eez.enabled --datadir=/data/eez --eez.l1.rpc=http://l1-node:8545
```

A follower checks these flags against the stored `l2` section and refuses to start when they differ.

### Reading an Erigon L1 directly

Next to an Erigon L1 node, read its database over the private API instead of JSON-RPC. The follower then reacts to the L1 node's new heads as they are executed:

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet \
  --datadir=/data/eez-devnet --eez.l1.kv=127.0.0.1:9090
```

### Following the sequencer

With `--eez.sequencer.rpc` the follower also takes the sequencer's latest block as its head, ahead of settlement, as long as the sequencer's chain descends from the safe block. The sequencer must serve `debug_getRawBlock`. Settlement later confirms these blocks without rebuilding them.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--eez.enabled` | | Run an EEZ chain, derived from L1 by the follower: the EEZ networks for `--chain`, the `l2` config section, transaction type `0x76` and the EEZ rules. Every other `--eez.*` flag requires it. |
| `--eez.l1.rpc` | | JSON-RPC endpoint of the L1 node. A running node needs this or `--eez.l1.kv`. |
| `--eez.l1.kv` | | Private API address of an Erigon L1 node. |
| `--eez.l1.beacon` | | Beacon API of the L1, which serves the blobs of rollupX batches. |
| `--eez.l1.blobs` | | Directory of blobs named by versioned hash, read before the beacon API; blobs the beacon API serves are stored in it. |
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

EEZ system transactions (type `0x76`) render with the gas limit of their rule set, a zero gas price and zero signature values, from the system address. The gas limit is `2000000` on rollup0 networks. On rollupX v0.0.1 networks it is the cap of `16777216`: a rollupX system transaction runs with the gas its block has left, up to that cap.

On rollupX v0.1 networks, native transactions (type `0x45`) and L1 history updates (type `0x46`) render the same way, from `0xfffffffffffffffffffffffffffffffffffffffe`, with `yParity` and, for type `0x45`, an empty access list. Their `sourceHash`, `version` and `blocks` fields are not rendered.

## Metrics and logs

Prometheus metrics: `eez_l1_head`, `eez_l1_scanned`, `eez_l1_finalized`, `eez_l1_lag_blocks`, `eez_l2_cursor`, `eez_l2_safe`, `eez_l2_finalized`, `eez_l2_blocks_derived_total`, `eez_l1_reorgs_total`, `eez_divergences_total` and `eez_sync_seconds`.

Every 30 seconds the follower logs `[eez] following` with the last L1 block it scanned (`l1Scanned`), the L1 head it scans to (`l1Head`), the finalized L1 block, the safe and finalized EEZ blocks, the batches derived since the previous line and the blocks they carry. Each derived batch also logs `[eez] batch derived` with its L1 block, transaction and block range.

Log lines that mark derivation events carry an `event` field named like the reference client's, for example `eez.deriver.l1.reorg.retreated`, `eez.deriver.state.diverged_post` or `eez.l1_watcher.reorg.detected`.

## The `eez` tool

`make eez` builds `build/bin/eez`:

- `eez decode <hex | @file | ->` decodes a `postAndVerifyBatch` input.
- `eez diff --a <url> --b <url> --from <n> --to <n>` finds the first block two EEZ nodes disagree on and the header fields that differ.
- `eez capture --l1.rpc <url> --registry <address> --from <n> --to <n> --out <dir>` records the L1 blocks, registry logs and batch postings a follower reads, for replay in tests.
- `eez mirror --chain <network> --l1.rpc <url> --beacon <url> --dir <dir>` indexes the batches of a network and stores their blobs; see [Blob custody](#blob-custody).

## Docker

The `erigon` binary of the standard image runs EEZ chains. To add the `eez` tool:

```bash
docker build --build-arg BINARIES="erigon eez" -t erigon-eez .
docker run -v /data/eez:/home/erigon/data erigon-eez \
  --eez.enabled --chain=eez-devnet --datadir=/home/erigon/data \
  --eez.l1.rpc=http://l1-node:8545
```

## Networks

| Network | `--chain` | Notes |
|---|---|---|
| EEZ devnet | `eez-devnet` | The genesis of the reference Kurtosis devnet, registry at `0x5fbdb2315678afecb367f032d93f642f64180aa3`, rollup 1 on L1 chain 7331. |
| Kurtosis enclave | | Initialise the enclave's `genesis.json` with `init`; deployments rewrite the genesis timestamp. |
| eez.dev | `eez-chiado` | RollupX v0.1, chain 699169 on Chiado, rollup 2 of registry `0x6258189d3a04eb31a196a8931077dfe001a9f6fd`. |
| eez.asuscomm.com | `eez-chiado-asus` | RollupX v0.1, chain 916969 on Chiado, rollup 4 of the same registry. |
| eez.dev until 2026-10-28 | `eez-chiado-699069` | RollupX v0.0.1, chain 699069 on Chiado; last batch in L1 block 23,723,620. |
| eez.asuscomm.com until 2026-10-28 | `eez-chiado-906969` | RollupX v0.0.1, chain 906969 on Chiado; last batch in L1 block 23,691,477. |
| mainnet.eez.dev | `eez-mainnet` | RollupX v0.0.1, chain 696990 on Ethereum. |

## RollupX networks

The Gnosis-hosted networks run eez-rollupX, a rule set derived from eez-rollup0. An `l2` section with a `rollupX` schedule selects it. Its blocks have:

- a 30,000,000 gas limit;
- a base fee that changes by at most 1/50 per block and is burned;
- a prevRandao that comes from L1;
- the EEZL2 manager at `0x4B2760F84B83d6b3829E30A7573B0172B6d6402C`.

Batches carry their payload in EIP-4844 blobs only.

Under v0.0.1 the base fee is burned, and the balances of EEZL2 and the system address are burned at the start of every block.

Under v0.1, selected by `feeCollector` and `historyUpdateOffset` in the `rollupX` section:

- the base fee of ordinary transactions goes to the fee collector, `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0078` on Chiado;
- cross-chain calls execute as native transactions (type `0x45`) to EEZL2, which mint their value and run with the gas their block has left, up to `16777216`;
- an L1 history update (type `0x46`) may open the block `historyUpdateOffset + 1` after each Sync position, importing up to 64 L1 block hashes into `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0077`; the latest imported L1 block is the origin of later blocks' prevRandao;
- the balances of EEZL2 and `0xfffffffffffffffffffffffffffffffffffffffe` are burned after each cross-chain group and at the end of every block.

A follower of these networks needs:

- An L1 archive node that serves logs from the registry's deploy block on.
- A beacon API, set with `--eez.l1.beacon`, that serves the blobs of every batch the follower derives. The follower recomputes each blob's KZG commitment and accepts only blobs that match the posting transaction's versioned hashes. A blob the beacon node does not serve stops derivation at that batch until it does.

Beacon nodes prune blobs after about 15 days on Chiado and 18 on Ethereum. Deriving a network from its genesis therefore needs a beacon node that kept every blob since the network started, an archive beacon API, or a blob mirror (see [Blob custody](#blob-custody)). On Chiado, `https://rpc-gbc.chiadochain.net` serves blobs older than that.

For a Chiado L1, run Erigon in archive mode with Caplin and its beacon API. Since Fulu, Caplin serves blobs only with `--caplin.blobs-archive`. Keep blobs from the start of the network:

```bash
./build/bin/erigon --chain=chiado --datadir=/data/chiado --prune.mode=archive \
  --beacon.api=beacon,config --beacon.api.addr=127.0.0.1 --beacon.api.port=5555 \
  --caplin.blobs-archive --caplin.blobs-no-pruning \
  --http.api=eth --http.port=8545

./build/bin/erigon --eez.enabled --chain=eez-chiado --datadir=/data/eez-chiado \
  --eez.l1.rpc=http://127.0.0.1:8545 --eez.l1.beacon=http://127.0.0.1:5555 \
  --http.api=eth,debug,eez --http.port=8546 --authrpc.port=8552 --private.api.addr=127.0.0.1:9091 \
  --port=30304 --torrent.port=42070
```

At boot the follower checks that L1 finalized the genesis's L1 reference block with the hash, timestamp and mixHash the genesis records.

The follower rebuilds cross-chain transactions of any shape: calls from L1, L2-to-L1 calls, nested calls in either direction, static reads and reverted regions. It rebuilds them from the batch's execution table and checks the result against the action hashes L1 settled.

### Blob custody

A follower that may have to derive a network from its genesis again needs every batch blob, also after beacon nodes pruned it. `eez mirror` keeps a copy:

```bash
./build/bin/eez mirror --chain=eez-chiado --dir=/data/blobs/chiado \
  --l1.rpc=http://127.0.0.1:8545 --beacon=https://rpc-gbc.chiadochain.net
```

It appends the batches the network settled up to the finalized L1 block to `<dir>/<chain id>.jsonl`, one JSON line per batch with its L1 block, posting transaction and blob versioned hashes, and stores each blob it lacks in `<dir>/blobs/<versioned hash>.blob`, after checking its KZG commitment. Each run resumes after the last indexed batch. A blob no longer served is reported, and the command exits non-zero. Run it more often than the beacon API prunes, for example daily; without `--beacon` it only checks the stored blobs.

The follower reads such a directory with `--eez.l1.blobs`, before the beacon API, and stores there the blobs it reads from the beacon API:

```bash
./build/bin/erigon --eez.enabled --chain=eez-chiado --datadir=/data/eez-chiado \
  --eez.l1.rpc=http://127.0.0.1:8545 --eez.l1.beacon=http://127.0.0.1:5555 \
  --eez.l1.blobs=/data/blobs/chiado/blobs
```

Several networks can share one directory. The two Chiado v0.1 networks post about 1,700 blobs a day between them, 210 MiB.

A batch whose blobs no source serves stops derivation there: the safe block stays at the previous settlement and the follower retries, logging a warning each time. The retries log at error level once a blob has been missing for a minute, or at once when it is lost: past the beacon node's blob retention (`MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS`, about 15 days on Chiado) without being served, or missing from a `--eez.l1.blobs` directory with no beacon API set. A blob copied into the directory as `<versioned hash>.blob` is read on the next retry.
