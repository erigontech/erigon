---
title: "EEZ Follower"
description: "Run Erigon as a follower of an EEZ chain, deriving it from its settlements on L1."
sidebar_position: 10
---

# EEZ Follower

An EEZ chain settles its blocks on an L1 through the EEZ registry. With `--eez.enabled` Erigon runs an EEZ chain as its follower, rebuilding it from that L1 evidence alone: it reads the batches the registry settled, re-executes the blocks they carry, and checks every result against the state root L1 committed to. The safe block is the latest settled block; the finalized block is the latest one settled in a finalized L1 block. The follower reads the finalized L1 block at startup and after each L1 range it scans, so the finalized block also advances during a catch-up. A block that contradicts canonical L1 stops the node.

EEZ support is off unless `--eez.enabled` is set; without it Erigon behaves as it always does.

## Running a follower

An EEZ node keeps its chain's database in `<datadir>/eez/chaindata` and its snapshots in `<datadir>/snapshots/eez`, as Caplin keeps its own; the rest of the datadir is the node's. The follower needs an L1 source: the JSON-RPC endpoint of any L1 node, or the private API of an Erigon L1 node.

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet \
  --datadir=/data/eez-devnet \
  --eez.l1.rpc=http://l1-node:8545 \
  --http.api=eth,debug,eez --nodiscover --eez.sync.l1only
```

An EEZ node runs no consensus layer, no engine API, no transaction pool and no snapshot downloader: the follower is the only source and the only driver of the chain. On rollup0 networks it takes the history below L1's finalized root from [L2 peers](#l2-peers) unless `--eez.sync.l1only` is set, as in the examples here, and serves them its blocks.

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

./build/bin/erigon --eez.enabled --datadir=/data/eez --eez.l1.rpc=http://l1-node:8545 --nodiscover --eez.sync.l1only
```

A follower checks these flags against the stored `l2` section and refuses to start when they differ.

### Reading an Erigon L1 directly

Next to an Erigon L1 node, read its database over the private API instead of JSON-RPC. The follower then reacts to the L1 node's new heads as they are executed:

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet \
  --datadir=/data/eez-devnet --eez.l1.kv=127.0.0.1:9090 --nodiscover --eez.sync.l1only
```

### Running beside its L1 in one process

With `--eez.chain` one Erigon process runs both chains: the L1 of `--chain`, as any Erigon node runs it, and beside it the EEZ network of `--eez.chain`, which must settle on that L1. The follower reads the L1 chain's database in place and derives each L1 block as soon as it is executed; it needs no L1 endpoint. It reads the blobs of rollupX batches from `--eez.l1.blobs`, then from the stores of the L1's own consensus layer, then from `--eez.l1.beacon`; each is optional. Since Fulu the consensus layer keeps full blobs only with `--caplin.blobs-immediate-backfill`, for `MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS` epochs (about 15 days on Chiado), with `--caplin.blobs-archive`, or as a supernode; without either flag the node warns at start. Even then it lacks the blobs of a block whose columns its peers do not serve, so keep `--eez.l1.beacon` set.

```bash
./build/bin/erigon --chain=<l1-network> --datadir=/data/node \
  --eez.enabled --eez.chain=<eez-network> \
  --eez.http.port=8645 --eez.sync.l1only
```

The L1 keeps the datadir's usual layout and every component of a node: P2P, snapshot downloader, consensus layer, transaction pool, engine API, private API and the JSON-RPC of `--http.*`. The EEZ chain keeps the layout above and adds its own lock, temporary files and migration records under `<datadir>/eez`; it serves JSON-RPC on `--eez.http.port` with the namespaces of `--eez.http.api`, and none of the L1's components. On rollup0 networks it runs its own P2P, set by the `--eez.*` P2P flags of [Peers](#peers). Its log lines carry `chain=<eez-network>`. Stopping the node stops the EEZ chain before the L1, and a follower that halts stops both.

Erigon's execution, staged-sync, database and P2P metrics are process-wide: in this mode they mix both chains. The `eez_*` metrics below are the EEZ chain's own.

### Following the sequencer

With `--eez.sequencer.rpc` the follower also takes the sequencer's latest block as its head, ahead of settlement, as long as the sequencer's chain descends from the safe block. The sequencer must serve `debug_getRawBlock`. Settlement later confirms these blocks without rebuilding them. On rollup0 networks [signed heads](#signed-heads) are the other source of the head; the two exclude each other.

## L2 peers

On rollup0 networks a follower takes L2 peers. Over devp2p it fetches blocks by hash from them, the history below L1 finality and the blocks of signed heads, and serves them its own; over the signed-head overlay it receives the heads the sequencer signs. L1 stays the only trust anchor: a block from a peer reaches the chain only through the follower, as an ancestor of the root L1 finalized or of a signed head, and derivation replaces whatever L1 settles differently. RollupX networks run none of this.

### Sync modes

`--eez.sync.l1only` selects where boot takes the history below L1's finalized root from:

| Mode | Default on | History below L1's finalized root |
|---|---|---|
| peers | rollup0 networks | Fetched from L2 peers; derivation starts at L1's finalized block. Set with `--eez.sync.l1only=false`. |
| L1-only | rollupX networks | Derived from L1, from the reconcile checkpoint or the registry's deploy block. Set with `--eez.sync.l1only`. |

A node that must not depend on how far L1 has finalized, such as a test or devnet node, sets the mode explicitly.

### Peers

The node takes L2 peers over eth/68 and eth/69 and serves them its blocks. A block reaches the chain only through the follower, never from devp2p on its own, and the node takes no transactions from its peers. Peers come from Erigon's P2P flags: `--bootnodes` with discovery, or `--staticpeers` and `--trustedpeers` with `--nodiscover`; trusted peers are dialled like static ones. EEZ networks list no bootnodes, so the node refuses to start with discovery on and neither `--bootnodes` nor `--discovery.dns`. A chain initialised from a genesis file never takes the network id, bootnodes or DNS list of the default `--chain`: its network id is its chain id unless `--networkid` is set. On rollupX networks the node stays off the P2P network whatever the flags.

Erigon's P2P flags belong to the L1 of a node [running beside its L1](#running-beside-its-l1-in-one-process). Its EEZ chain joins the P2P network once one of `--eez.port`, `--eez.bootnodes`, `--eez.staticpeers`, `--eez.trustedpeers` or `--eez.p2p.listen` is set. It then listens on `--eez.port`, which must differ from the L1's `--port`, with a node key of its own, generated at its first start in `<datadir>/eez/nodekey`, and its node database under `<datadir>/eez`. It discovers peers only from `--eez.bootnodes`, and dials `--eez.staticpeers` and `--eez.trustedpeers`. It shares the L1's node name, `--nat`, `--netrestrict` and `--metrics`, and takes Erigon's defaults for the other P2P settings, such as `--maxpeers`. A chain initialised from a genesis file takes its chain id as network id. The signed-head overlay of `--eez.p2p.listen` runs on the EEZ chain's node key, on a port other than `--eez.port`. Peers mode, the default on rollup0 networks, refuses to start without `--eez.bootnodes`, `--eez.staticpeers` or `--eez.trustedpeers`.

```bash
./build/bin/erigon --chain=<l1-network> --datadir=/data/node \
  --eez.enabled --eez.chain=<eez-network> \
  --eez.port=30403 --eez.staticpeers=enode://<node-a>@<host>:30303
```

### History at L1 finality

In peers mode the follower takes the history below the root that L1's finalized block F records from its L2 peers, and derives from L1 only the settlements after F. At boot it reads F and the registry's root at F. When the local chain lacks that root, it rewinds to its own finalized block, fetches the missing blocks by hash from its peers, executing them `--sync.loop.block.limit` blocks at a time, and starts derivation at F. It waits, saying why every 30 seconds, while L1 has no finalized root or no peer serves the blocks, and never derives that history from L1. This mode keeps no reconcile checkpoint.

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet --datadir=/data/eez-b \
  --eez.l1.rpc=http://l1-node:8545 \
  --nodiscover --trustedpeers=enode://<node-a>@<host>:30303
```

An L1-only boot on a rollup0 network checks the same root: it stops with "L1 history incomplete" when L1 no longer serves a block more than 64 below its head that the scan needs, or when the scan ends without the root L1 had finalized before it.

### Signed heads

The sequencer signs every change of its head: an EIP-712 `SignedHead` of the rollup id, the block's number, hash and parent hash, the start time of the signing process in milliseconds (its epoch) and a counter, in the domain `EEZ Sequencer`, version `0`, of the L2 chain id and the registry. It gossips each head, with its block, on a libp2p gossipsub overlay. With `--eez.p2p.listen` and `--eez.sequencer.address` the follower joins the overlay and takes as its head the highest-version head, latest epoch first, then highest counter, among those that descend from the safe block:

```bash
./build/bin/erigon --eez.enabled --chain=eez-devnet --datadir=/data/eez-b \
  --eez.l1.rpc=http://l1-node:8545 \
  --bootnodes=enode://<node-a>@<host>:30303 \
  --eez.p2p.listen=/ip4/0.0.0.0/tcp/9100 \
  --eez.sequencer.address=<sequencer address>
```

- **Transport.** TCP with Noise and Yamux, on the topic `/eez/<L2 chain id>/<rollup id>/heads/0`. The overlay identity is the node's devp2p key; the node logs its peer id in `[eez] overlay listening`. `--eez.p2p.listen` needs a fixed port.
- **Overlay peers.** Peers discovered over discv5 whose node record carries the overlay port, in an `eez` entry, and the chain's fork id are dialled every 5 seconds while fewer than 16 are connected. The static peers of `--eez.p2p.peers`, IP multiaddrs ending in `/p2p/<peer id>`, are dialled whenever they are not connected. A node accepts at most 64 inbound connections.
- **Relay.** A node relays a head only after verifying it: signed by `--eez.sequencer.address` for the chain, registry and rollup, carrying its own block if any, and above the latest head of its source, the signer and epoch. It ignores a repeat, an older head and a head whose epoch is more than 10 seconds ahead of its clock, and rejects anything else, including a second head with the same version.
- **Missing blocks** come by hash from L2 devp2p peers, at most 1,024 blocks back, each request bounded to 2 seconds, and only while a peer is connected. A head more than 1,024 blocks ahead of the local head waits for derivation. While derivation has indexed a batch above the safe block, the head holds.
- **Retries.** A head not followed is tried again every L2 block interval. Safe and finalized stay derivation's: L1's settlement of a different block replaces the blocks of a signed head.

### Refusals

The node refuses to start with:

- `--eez.p2p.listen` without `--eez.sequencer.address` or the reverse, and `--eez.p2p.peers` without `--eez.p2p.listen`;
- `--eez.p2p.listen` and `--eez.sequencer.rpc` together: both set the head;
- the overlay flags on rollupX networks, whose sequencer signs no heads;
- `--eez.sync.l1only=false` on rollupX networks;
- peers mode beside an L1 without `--eez.bootnodes`, `--eez.staticpeers` or `--eez.trustedpeers`;
- discovery on a rollup0 network without `--bootnodes` or `--discovery.dns`;
- `--eez.port` out of range, or equal to the L1's `--port` or to the port of `--eez.p2p.listen`;
- a listen address that is not TCP or has port 0, and a static overlay peer given by DNS name or without `/p2p/<peer id>`.

Peers mode stops the node when the root's chain leaves the local finalized block ("datadir is not on L1's chain"), when an L1 reorg drops F ("L1 reorg below the seed at L1 finality"), and when the local chain refuses the blocks its peers served ("the local chain refuses the history from L2 peers").

### Status

`eez_followerStatus` reports where the follower takes its history and its head from, beside how far it derived:

| Field | Value |
|---|---|
| `l1Scanned`, `l1Finalized` | The last L1 block whose settlements are derived; the latest finalized L1 block. |
| `cursor`, `safe`, `finalized` | The L2 block the latest settlement ended at; the safe and finalized L2 blocks. |
| `lastSettlement` | The latest derived settlement: `l1Block`, `l1BlockHash`, `txHash`, `l2Block`. |
| `syncMode` | `peers` or `l1_only`. |
| `phase` | `booting`; in peers mode `waiting_for_finality` while L1 names no finalized root, `syncing` while the history comes from peers, `waiting_for_peers` while no peer serves it; then `deriving` while boot derives up to the L1 head, and `following`. |
| `anchor` | F and the root there, as boot read them: `l1Block`, `l1BlockHash`, `root`, null when the rollup is not registered at F, and `l2Block`, null until the local chain holds the root. Null before boot reads them and on rollupX networks; in L1-only mode, also null when L1 serves no state reads or names no finalized root at boot. |
| `peers` | Connected L2 devp2p peers, counted every 5 seconds. |
| `overlayPeers` | Connected overlay peers; null without the overlay. |
| `signedHeads` | The latest head of each signing source, highest version first: `signer`, `epochMs`, `counter`, `number`, `hash`. Null without the overlay. |
| `headSource` | What moved the head last: `derivation`, `history_sync`, `signed_head` or `sequencer_rpc`. Null while nothing has since boot. |

## Flags

| Flag | Default | Description |
|---|---|---|
| `--eez.enabled` | | Run an EEZ chain, derived from L1 by the follower: the EEZ networks for `--chain`, the `l2` config section, transaction type `0x76` and the EEZ rules. Every other `--eez.*` flag requires it. |
| `--eez.l1.rpc` | | JSON-RPC endpoint of the L1 node. A running node needs this, `--eez.l1.kv` or `--eez.chain`. |
| `--eez.l1.kv` | | Private API address of an Erigon L1 node. |
| `--eez.l1.beacon` | | Beacon API of the L1, which serves the blobs of rollupX batches. |
| `--eez.l1.blobs` | | Directory of blobs named by versioned hash, read before the beacon API; blobs the beacon API serves are stored in it. |
| `--eez.chain` | | EEZ network to run in the same process as the L1 of `--chain`. |
| `--eez.http.port` | `8645` | JSON-RPC port of the EEZ chain of `--eez.chain`. |
| `--eez.http.api` | `eth,debug,eez` | JSON-RPC namespaces of the EEZ chain of `--eez.chain`. |
| `--eez.port` | `30403` | P2P listening port of the EEZ chain of `--eez.chain`. |
| `--eez.bootnodes` | | Enode URLs from which the EEZ chain of `--eez.chain` discovers its L2 peers; unset, it runs no discovery. |
| `--eez.staticpeers` | | Enode URLs of L2 peers the EEZ chain of `--eez.chain` keeps connected. |
| `--eez.trustedpeers` | | Enode URLs of L2 peers the EEZ chain of `--eez.chain` keeps connected, even above its peer limit. |
| `--eez.l1.poll` | `2s` | Interval between reads of the L1 head. |
| `--eez.l1.logchunk` | `100000` | Widest block range of one L1 log query; halved when the node refuses it. |
| `--eez.l1.reorgdepth` | `62` | Deepest L1 reorg the follower survives. |
| `--eez.sequencer.rpc` | | Sequencer endpoint for the unsafe head. |
| `--eez.sync.l1only` | rule set | Derive the whole history from L1; see [Sync modes](#sync-modes). Unset, true on rollupX networks and false on rollup0 networks. |
| `--eez.p2p.listen` | | TCP multiaddr with a fixed port on which the node joins the [signed-head overlay](#signed-heads). Requires `--eez.sequencer.address`. |
| `--eez.p2p.peers` | | Static overlay peers, comma-separated IP multiaddrs ending in `/p2p/<peer id>`. |
| `--eez.sequencer.address` | | Address of the sequencer, whose signed heads the overlay accepts. Requires `--eez.p2p.listen`. |
| `--eez.l1.chainid`, `--eez.registry`, `--eez.registry.deployblock`, `--eez.rollupid`, `--eez.l1.blocktime`, `--eez.l2.blocktime` | | The `l2` section of the chain config, written by `init`. |

## Restarts and L1 reorgs

The follower records its latest settlement in `<datadir>/eez/eez-reconcile-checkpoint`. On restart it resumes there when L1 and the local chain still hold that settlement; otherwise it resumes at the newest settlement whose block the local chain holds, and failing that from the registry's deploy block. The finalized block stays finalized across the restart, up to the settlement it resumes at.

An L1 reorg that drops settlements moves the head and the safe block back to what the remaining settlements settled, and the replacement branch is derived. One that drops the settlement the follower resumed at makes it look for a resume point again, as on restart. A reorg deeper than `--eez.l1.reorgdepth`, or one that drops a finalized settlement, stops the node.

## Halting

The node exits with status `3` when a settled block contradicts canonical L1: the block it derives does not hash to the root L1 committed to, the DA payload cannot be read, or an L2-to-L1 call L1 settled was never made on L2. It logs `[eez] local chain diverged from L1` at critical level (`CRIT`), with an `event` naming the failed check. The same evidence on an L1 block that has since been reorged is not a divergence; the follower logs a warning, rewinds and continues. Every condition that stops the node, including the reorgs above, logs `[eez] follower halted, stopping the node` at critical level. Other failures, such as an unreachable L1 node, are retried. A failed L1 poll logs `[eez] L1 poll failed, retrying` (`eez.l1_watcher.poll.failed`) as a warning, with `poll` naming the `head` or `finalized` read, at error level once that read has failed for a minute, and its first read that succeeds logs `[eez] L1 poll recovered`.

## RPC

The `eez` namespace, enabled with `--http.api=...,eez`:

- `eez_followerStatus()`: the last L1 block scanned, the latest finalized L1 block, the cursor, safe and finalized EEZ blocks, the latest settlement, the sync mode and boot phase, L1's finalized block and root as boot read them, the L2 and overlay peers, the tracked signed heads and the source of the head; see [Status](#status).
- `eez_settlement(blockNumber)`: the L1 settlement that settled an EEZ block, or `null` while none has, or when it predates the follower's start or the latest finalized settlement.
- `eez_decodeBatch(input)`: a `postAndVerifyBatch` input decoded, with the DA payload it carries.

EEZ system transactions (type `0x76`) render with the gas limit of their rule set, a zero gas price and zero signature values, from the system address. The gas limit is `2000000` on rollup0 networks. On rollupX v0.0.1 networks it is the cap of `16777216`: a rollupX system transaction runs with the gas its block has left, up to that cap.

On rollupX v0.1 networks, native transactions (type `0x45`) and L1 history updates (type `0x46`) render the same way, from `0xfffffffffffffffffffffffffffffffffffffffe`, with `yParity` and, for type `0x45`, an empty access list. Their `sourceHash`, `version` and `blocks` fields are not rendered.

## Metrics and logs

Prometheus metrics of derivation: `eez_l1_head`, `eez_l1_scanned`, `eez_l1_finalized`, `eez_l1_lag_blocks`, `eez_l2_cursor`, `eez_l2_safe`, `eez_l2_finalized`, `eez_l2_blocks_derived_total`, `eez_l1_reorgs_total`, `eez_divergences_total` and `eez_sync_seconds`.

Of boot and [L2 peers](#l2-peers):

- `eez_sync_phase{phase="..."}`: 1 for the follower's [phase](#status), 0 for the others.
- `eez_l2_peers`: connected L2 devp2p peers, counted every 5 seconds.
- `eez_head_adopted_total` and `eez_head_rewound_total`: moves of the head to a signed head that keep the previous head canonical, and that drop it, to an ancestor or another branch.
- `eez_head_held_total` and `eez_head_ahead_total`: decisions on signed heads that hold the head while derivation has indexed a batch above safe, and that wait for derivation to come within 1,024 blocks.
- `eez_head_fetch_failures_total`: fetches of the blocks of a signed head that failed.
- `eez_head_rejected_total`: signed heads not followed, once per head and reason.
- `eez_p2p_peers`: connected overlay peers.
- `eez_p2p_message_received_total`, and of those `eez_p2p_message_accepted_total`, `eez_p2p_message_ignored_total` and `eez_p2p_message_rejected_total`: overlay messages from peers, by the verdict of the relay rule.
- `eez_p2p_inbound_dropped_total`: accepted heads dropped because the follower had not taken the previous 64.

Every 30 seconds the follower logs `[eez] following` with the last L1 block it scanned (`l1Scanned`), the L1 head it scans to (`l1Head`), the finalized L1 block, the safe and finalized EEZ blocks, the batches derived since the previous line and the blocks they carry. Each derived batch also logs `[eez] batch derived` with its L1 block, transaction and block range.

Log lines that mark derivation events carry an `event` field named like the reference client's, for example `eez.deriver.l1.reorg.retreated`, `eez.deriver.state.diverged_post` or `eez.l1_watcher.reorg.detected`. Boot logs `eez.node.sync.mode`, `eez.node.sync.waiting`, `eez.node.sync.waiting_for_peers`, `eez.node.sync.rewound` and `eez.node.sync.seeded`; the signed-head fork choice `eez.node.follower.head.advanced`, `.rejected`, `.held` and `.ahead`; the overlay `eez.p2p.listening`, and every 5 seconds at most one `eez.p2p.message.rejects` and one `eez.p2p.inbound.full` warning with the count since the previous one.

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
  --eez.l1.rpc=http://l1-node:8545 --nodiscover --eez.sync.l1only
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

A batch whose blobs no source serves stops derivation there: the safe block stays at the previous settlement and the follower retries. A beacon node serves the blobs of a slot seconds after the L1 has its block, so the retries log at debug level for the first 30 seconds, then as warnings, and at error level once a blob has been missing for a minute, or at once when it is lost: past the beacon node's blob retention (`MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS`, about 15 days on Chiado) without being served, or missing from a `--eez.l1.blobs` directory with no beacon API set. A blob copied into the directory as `<versioned hash>.blob` is read on the next retry.
