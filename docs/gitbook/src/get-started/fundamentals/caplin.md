---
description: Erigon's embedded Consensus Layer
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

Caplin can also be used for [block production](staking/caplin.md), aka **staking**.

## Beacon API Configuration

Caplin exposes a Beacon API on port `5555` by default. The following flags control its behaviour:

| Flag | Default | Description |
|------|---------|-------------|
| `--beacon.api.port` | `5555` | Port to listen for Beacon API requests |
| `--beacon.api.addr` | `localhost` | Bind address for the Beacon API. Set to `0.0.0.0` to expose externally (use with care — protect with a firewall or proxy). |
| `--beacon.api` | (all groups) | Comma-separated list of Beacon API endpoint groups to enable: `beacon`, `builder`, `config`, `debug`, `events`, `node`, `validator`, `lighthouse`. Omit groups you do not need to reduce attack surface. |
| `--beacon.api.cors.allow-origins` | `` (deny all) | Comma-separated allowed CORS origins for the Beacon API. Required when a browser-based validator UI connects to Caplin. |
| `--beacon.api.cors.allow-methods` | `GET,POST,PUT,DELETE,OPTIONS` | Allowed HTTP methods for CORS. |
| `--beacon.api.cors.allow-credentials` | `false` | Whether to allow credentials in CORS requests. |
| `--beacon.api.protocol` | `tcp` | Network protocol for the Beacon API listener. |
| `--beacon.api.read.timeout` | `5` | Read timeout in seconds. |
| `--beacon.api.write.timeout` | `31536000` | Write timeout in seconds (default is 1 year to support long-lived SSE event streams). |
| `--beacon.api.ide.timeout` | `25` | Idle connection timeout in seconds. |
