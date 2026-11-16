---
title: "Caplin"
description: "Erigon's built-in consensus layer — run a full node without an external CL client."
sidebar_position: 6
---

# Caplin

Caplin is Erigon's embedded Consensus Layer. It runs inside the Erigon process and keeps all of its data under the same `--datadir` as the execution layer, so a full node needs neither a second client process nor a second datadir to operate.

Caplin does write to disk. It maintains an indexing database at `<datadir>/caplin/indexing`, its own snapshots under `<datadir>/snapshots/caplin`, and further subdirectories for blobs, PeerDAS column data and beacon state. What Caplin removes is the separate process and separate datadir of an external consensus client — not the storage itself.

## Caplin Usage

Caplin is enabled by default, at which point an external consensus layer is no longer needed.

```bash
./build/bin/erigon
```

Caplin also has an archive mode for historical states, blocks, and blobs. These can be enabled with the following flags:

* `--caplin.states-archive`: Enables the storage and retrieval of historical state data, allowing access to past states of the blockchain for debugging, analytics, or other use cases.
* `--caplin.blocks-archive`: Enables the storage of historical block data, making it possible to query or analyze full block history.
* `--caplin.blobs-archive`: Enables the storage of historical blobs, ensuring access to additional off-chain data that might be required for specific applications.
* `--caplin.blobs-archive-from-slot <slot>`: Bounds the blob archive to this beacon slot and later, for rollup nodes (Arbitrum Nitro, OP) that started from a snapshot and need blobs only from that point. It also enables block archival: every beacon block since genesis is downloaded and kept; only blob sidecars are bounded. The spec serving window (about 18 days) is always kept, even when the slot is newer. On first sync only blob snapshots that reach the slot or that window are downloaded; with `--caplin.custom-config` or `--caplin.custom-genesis` every blob snapshot is downloaded and the older ones are pruned afterwards. The newest blob snapshot is always kept. A rollup derives the slot from an L1 block timestamp as `(timestamp - genesis_time) / 12`. Raising it later drops the older blob snapshots; lowering it does not fetch them again. Changing an existing datadir from this bounded mode to plain `--caplin.blobs-archive`, or removing the bound, does not fetch dropped history: backfill stops at `FrozenBlobs` and `SyncSnapshots` is skipped once snapshots are local, so the range from Deneb to the first local blob segment remains unavailable.

In addition, Caplin can backfill recent blobs for an op-node or other uses with the new flag:

* `--caplin.blobs-immediate-backfill`: Backfills the last 18 days' worth of blobs to quickly populate historical blob data for operational needs or analytics.

The blob backfill can also ask a beacon API endpoint first, which is much faster than peers and covers slots that peers no longer serve:

* `--caplin.blobs-backfill-endpoint <url>`: Beacon API endpoint asked for a block's blobs before peers; repeat it for fallback endpoints. The blobs are verified against the block before they are stored, so the endpoint does not need to be trusted. Without `--caplin.blobs-archive` the backfill covers only the blob retention window (4096 epochs, about 18 days on mainnet); with it, slots older than that need an archive-capable endpoint. An endpoint that does not serve several slots in a row is not asked for older slots until the next backfill pass, and one that keeps failing (down, timing out or rate-limiting) is paused for 5 minutes; peers fill what the endpoints do not. From Fulu on, a beacon node builds blob sidecars from data columns and can serve them only when it keeps at least half of the columns (a supernode or semi-supernode); Nimbus does not serve the `blob_sidecars` API.
* `--caplin.blobs-backfill-endpoint-workers <n>`: CPUs the backfill uses for the fetched blobs, mostly to compute KZG proofs the endpoint leaves out; up to twice as many blocks are fetched at a time. The default picks a quarter of the CPUs the process may use, at least 1 (4 on a 16-core machine). In a container this follows the CPU limit (`--cpus` or a Kubernetes CPU limit), as `GOMAXPROCS` does; set `GOMAXPROCS` or this flag where the limit is not detected, for example under Windows job objects. Each worker keeps about one CPU busy, so a node with validators can set a lower value to leave the CPU to attestation and block processing, and a dedicated archive node can set a higher one, for example `8` on a 16-core machine. At startup the node logs the endpoints (host only), the worker count and the number of CPU cores, and warns when there are more workers than cores.

With state archival enabled, Caplin's indexing database (`<datadir>/caplin/indexing`) is pruned automatically: state rows already frozen into snapshots are deleted on the antiquary cadence, so the database holds only the un-frozen tail. The `mdbx.dat` file does not shrink when rows are pruned — freed pages are reused, bounding future growth. Set `CAPLIN_STATE_PRUNE_DISABLE=true` to turn pruning off.

### PeerDAS Data Column Retention

For nodes participating in PeerDAS (EIP-7594), Caplin retains data column sidecars for a configurable window:

* `--caplin.columns-keep-slots` (default: `0`): Number of slots to retain PeerDAS data column sidecars. `0` uses the chain's own spec window, `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS × SLOTS_PER_EPOCH`, so the retained slot count and the wall-clock duration it covers both follow the chain (on Ethereum mainnet, 131072 slots, ~18 days). Increase this value for data availability (DA) oracle or rollup nodes that require a longer column history.

Caplin can also be used for [block production](../staking/caplin), aka **staking**.

## Beacon API Configuration

When Caplin is running, it exposes a Beacon API that external tools can query. The following flags control the Beacon API server:

| Flag | Default | Description |
|------|---------|-------------|
| `--beacon.api.addr` | `localhost` | Listening address for the Beacon API |
| `--beacon.api.port` | `5555` | Listening port for the Beacon API |
| `--beacon.api.cors.allow-origins` | (empty) | Cross-Origin Resource Sharing (CORS) allowed origins |
| `--beacon.api.cors.allow-methods` | `GET, POST, PUT, DELETE, OPTIONS` | CORS allowed methods |
| `--beacon.api.cors.allow-credentials` | `false` | Allow credentials in CORS requests |
| `--beacon.api.protocol` | `tcp` | Network protocol (`tcp` or `tcp4` or `tcp6`) |
| `--beacon.api.read.timeout` | `5` | HTTP server read timeout, in seconds |
| `--beacon.api.write.timeout` | `31536000` | HTTP server write timeout, in seconds (~1 year) |
| `--beacon.api.idle.timeout` | `25` | HTTP server idle timeout, in seconds |
| `--beacon.api.preserve-graffiti` | `false` | Use validator-supplied graffiti exactly as given, without prefixing the EL+CL client identification segment |

The API is not served until you enable it with `--beacon.api=<namespaces>`; see [Caplin for staking](../staking/caplin) for the full namespace list.

By default, block-production requests that include a `graffiti` value have the [client-version graffiti standard](https://github.com/ethereum/execution-apis/blob/main/src/engine/identification.md)'s identification segment (execution and consensus client codes and commit prefixes, e.g. `EGab48CN5c2c`) prefixed to them, truncating the supplied graffiti to fit the 32-byte field if needed. Set `--beacon.api.preserve-graffiti` to use the supplied graffiti as decoded instead, with no prefix or truncation. A supplied `graffiti` value shorter than 32 bytes is right-padded (its bytes come first, zero bytes fill the rest) under both settings.

:::note
Enabling the Beacon API increases RAM usage by roughly **6 GB**. Account for it when sizing your host — see [Hardware Requirements](../get-started/hardware-requirements).
:::
