# Caplin

Caplin, the novel embedded Consensus Layer, brings unparalleled performance, efficiency, and reliability to Ethereum infrastructure. Its innovative design minimize disk usage, enabling faster transaction processing and a more secure network. By **integrating the consensus layer into the EVM-node**, Caplin eliminates the need for separate disk storage, reducing overall system complexity and improving overall efficiency.

**OtterSync**, a new syncing algorithm, further enhances performance by shifting 98% of the computation to network bandwidth, reducing sync times and improving chain tip performance, disk footprint, and decentralization.

# Caplin Usage

Caplin is enabled by default, at which point an external consensus layer is no longer needed.

```bash
./build/bin/erigon
```

Caplin also has an archive mode for historical states, blocks, and blobs. These can be enabled with the following flags:

- `--caplin.states-archive`: Enables the storage and retrieval of historical state data, allowing access to past states of the blockchain for debugging, analytics, or other use cases.
- `--caplin.blocks-archive`: Enables the storage of historical block data, making it possible to query or analyze full block history.
- `--caplin.blobs-archive`: Enables the storage of historical blobs, ensuring access to additional off-chain data that might be required for specific applications.

In addition, Caplin can backfill recent blobs for an op-node or other uses with the new flag:

- `--caplin.blobs-immediate-backfill`: Backfills the last 18 days' worth of blobs to quickly populate historical blob data for operational needs or analytics.

Caplin can also be used for [block production](/staking/caplin.md), aka **staking**.