---
description: Erigon's embedded Consensus Layer
metaLinks:
  alternates:
    - https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/fundamentals/caplin
---

# Caplin

Caplin, the innovative Erigon's embedded Consensus Layer, significantly enhances the performance, efficiency, and reliability of Ethereum infrastructure. Its groundbreaking design minimizes disk usage, facilitating faster transaction processing and bolstering network security. By integrating the consensus layer directly into the EVM-node, Caplin eliminates the need for separate disk storage, thereby reducing system complexity and enhancing overall efficiency.

## Caplin Usage

Caplin is enabled by default, at which point an external consensus layer is no longer needed.

```bash
./build/bin/erigon
```

Caplin also has an archive mode for historical states, blocks, and blobs. These can be enabled with the following flags:

* `--caplin.states-archive`: Enables the storage and retrieval of historical state data, allowing access to past states of the blockchain for debugging, analytics, or other use cases.
* `--caplin.blocks-archive`: Enables the storage of historical block data, making it possible to query or analyze full block history.
* `--caplin.blobs-archive`: Enables the storage of historical blobs, ensuring access to additional off-chain data that might be required for specific applications.

In addition, Caplin can backfill recent blobs for an op-node or other uses with the new flag:

* `--caplin.blobs-immediate-backfill`: Backfills the last 18 days' worth of blobs to quickly populate historical blob data for operational needs or analytics.

### PeerDAS Data Column Retention

For nodes participating in PeerDAS (EIP-7594), Caplin retains data column sidecars for a configurable window:

* `--caplin.columns-keep-slots` (default: `131072`, ~18 days): Number of slots to retain PeerDAS data column sidecars. The default matches `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS × SLOTS_PER_EPOCH`. Increase this value for DA oracle or rollup nodes that require a longer column history.

Caplin can also be used for [block production](../staking/caplin.md), aka **staking**.

## Beacon API Configuration

When Caplin is running, it exposes a Beacon API that external tools can query. The following flags control the Beacon API server:

| Flag | Default | Description |
|------|---------|-------------|
| `--beacon.api.addr` | `localhost` | Listening address for the Beacon API |
| `--beacon.api.port` | `5555` | Listening port for the Beacon API |
| `--beacon.api.cors.allow-origins` | (empty) | CORS allowed origins |
| `--beacon.api.cors.allow-methods` | `GET, POST, PUT, DELETE, OPTIONS` | CORS allowed methods |
| `--beacon.api.cors.allow-credentials` | `false` | Allow credentials in CORS requests |
| `--beacon.api.protocol` | `tcp` | Network protocol (`tcp` or `tcp4` or `tcp6`) |
| `--beacon.api.read.timeout` | `5s` | HTTP server read timeout |
| `--beacon.api.write.timeout` | `31536000s` (~1 year) | HTTP server write timeout |
| `--beacon.api.ide.timeout` | `25s` | HTTP server idle timeout (note: flag name is `ide` not `idle` — typo in source) |
