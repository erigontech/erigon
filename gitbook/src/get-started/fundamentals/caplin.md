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
