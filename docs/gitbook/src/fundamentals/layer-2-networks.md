# Layer 2 Networks

### Running an Op-Node alongside Erigon

To run an op-node alongside Erigon, follow these steps:

1.  **Start Erigon with Caplin Enabled**: If Caplin is running as the consensus layer (CL), use the `--caplin.blobs-immediate-backfill` flag to ensure the last 18 days of blobs are backfilled, which is critical for proper synchronization with the op-node, assuming you start from a snapshot.

    ```bash
    ./build/bin/erigon --caplin.blobs-immediate-backfill
    ```
2. **Run the Op-Node**: Configure the op-node with the `--l1.trustrpc` flag to trust the Erigon RPC layer as the L1 node. This setup ensures smooth communication and synchronization.

This configuration enables the op-node to function effectively with Erigon serving as both the L1 node and the CL.

### How to run Erigon Nitro for Sepolia (experimental)

To start an Erigon Nitro node, simply use this Docker command:

```shell
mkdir /disk/datadir && \
docker run -d -v /disk/datadir/:/home/user/erigon-data/ erigontech/nitro-erigon:main-fe4c973 --torrent.download.rate=10G --prune.mode=archive --l2rpc="http://rpcserver:port --sync.loop.block.limit 100000
```

* **Arbitrum Sequencer**: We're currently working on adding support for the Arbitrum Sequencer. Until that work is complete, you must point Erigon to an external L2 Arbitrum RPC to get the most recent transactions and blocks. This can be either a pruned Nitro node you operate or a publicly available RPC. Any data beyond that is queried via the configured `l2rpc` during setup and synchronization, so keep this in mind when bringing a node online.
* **RPC compatibility**: Before declaring full Arbitrum One support, we are targeting complete RPC compatibility with the Nitro node. Please note that the `timeboosted` field will remain unavailable until Sequencer integration is complete.

For more recent versions, please check [https://hub.docker.com/r/erigontech/nitro-erigon/tags](https://hub.docker.com/r/erigontech/nitro-erigon/tags).
