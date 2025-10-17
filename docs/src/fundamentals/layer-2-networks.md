# Layer 2 Networks

## Running an Op-Node alongside Erigon

To run an op-node alongside Erigon, follow these steps:

1. **Start Erigon with Caplin Enabled**:
    If Caplin is running as the consensus layer (CL), use the `--caplin.blobs-immediate-backfill` flag to ensure the last 18 days of blobs are backfilled, which is critical for proper synchronization with the op-node, assuming you start from a snapshot.
    ```bash
    ./build/bin/erigon --caplin.blobs-immediate-backfill
    ```
2. **Run the Op-Node**:
Configure the op-node with the `--l1.trustrpc` flag to trust the Erigon RPC layer as the L1 node. This setup ensures smooth communication and synchronization.

This configuration enables the op-node to function effectively with Erigon serving as both the L1 node and the CL.