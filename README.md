# cdk-erigon

cdk-erigon is a fork of Erigon, currently in Alpha, optimized for syncing with the Polygon Hermez zkEVM network.

***
## Release Roadmap
- **v1.1.x**: RPC (full support)
- **v2.x.x**: Sequencer (full support)
- **v3.x.x**: Erigon 3 based (snapshot support)

***

## Hardware requirements

* A Linux-based OS (e.g., Ubuntu Server 22.04 LTS).
* At least 32GB RAM with a 4-core CPU.
* Both Apple Silicon and AMD64 are supported.

## Chain/Fork Support
Current status of cdk-erigon's support for running various chains and fork ids:

- zkEVM Cardona testnet — full support
- zkEVM mainnet — beta support
- CDK Chains - beta support (forkid.9 and above)

## Dynamic Chain Configuration
To use chains other than the defaults above, a set of configuration files can be supplied to run any chain.

1. Ensure your chain name starts with the word `dynamic` e.g. `dynamic-mynetwork`
3. Create 3 files for dynamic configs (examples for Cardona in `zk/examples/dynamic-configs`, edit as required)
   - `dynamic-{network}-allocs.json` - the allocs file
   - `dynamic-{network}-chainspec.json` - the chainspec file
   - `dynamic-{network}-conf.json` - an additional configuration file
   - `dynamic-{network}.yaml` - the run config file for erigon.  You can use any of the example yaml files at the root of the repo as a base and edit as required, but ensure the `chain` field is in the format `dynamic-{network}` and matches the names of the config files above.
4. Place the erigon config file along with the other files in the directory of your choice, for example `dynamic-mynetwork`.

**Tip**: if you have allocs in the format from Polygon when originally launching the network you can save this file to the root of the cdk-erigon code
base and run `go run cmd/hack/allocs/main.go [your-file-name]` to convert it to the format needed by erigon, this will form the `dynamic-{network}-allocs.json` file.

**Tip**: the contract addresses in the `dynamic-{network}.yaml` can be found in the files output when launching the network:
- zkevm.address-sequencer => create_rollup_output.json => `sequencer`
- zkevm.address-zkevm => create_rollup_output.json => `rollupAddress`
- zkevm.address-rollup => deploy_output.json => `polygonRollupManagerAddress`
- zkevm.address-ger-manager => deploy_output.json => `polygonZkEVMGlobalExitRootAddress`

Mount the directory containing the config files on docker container: `/dynamic-mynetwork` for example

To use the new config when starting erigon use the `--cfg` flag with the path to the config file e.g. `--cfg="/dynamic-mynetwork/dynamic-mynetwork.yaml"`

## Prereqs
In order to use the optimal vectorized poseidon hashing for the Sparse Merkle Tree, on x86 the following packages are required (for Apple silicon it will fall back to the iden3 library and as such these dependencies are not required in that case.

Please install: 
- Linux: `libgtest-dev` `libomp-dev` `libgmp-dev`
- MacOS: `brew install libomp` `brew install gmp`

Using the Makefile command: `make build-libs` will install these for the relevant architecture.

Due to dependency requirements Go 1.19 is required to build.

## L1 Interaction
In order to retrieve data from the L1, the L1 syncer must be configured to know how to request the highest block, this can be configured by flag:

- `zkevm.l1-highest-block-type` which defaults to retrieving the 'finalized' block, however there are cases where you may wish to pass 'safe' or 'latest'.

### L1 Cache
The node can cache the L1 requests/responses to speed up the sync and enable quicker responses to RPC requests requiring for example OldAccInputHash from the L1. This is enabled by default,
but can be controlled via the following flags:

- `zkevm.l1-cache-enabled` - defaults to true, set to false to disable the cache
- `zkevm.l1-cache-port` - the port the cache server will run on, defaults to 6969

To transplant the cache between datadirs, the `l1cache` dir can be copied. To use an upstream cdk-erigon node's L1 cache, the zkevm.l1-cache-enabled can be set to false, and the node provided the endpoint of the cache,
instead of a regular L1 URL. e.g. `zkevm.l1-rpc-url=http://myerigonnode:6969?endpoint=http%3A%2F%2Fsepolia-rpc.com&chainid=2440`. NB: this node must be syncing the same network for any benefit!

## Sequencer (WIP)

Enable Sequencer: `CDK_ERIGON_SEQUENCER=1 ./build/bin/cdk-erigon <flags>`

### Special mode - L1 recovery
The sequencer supports a special recovery mode which allows it to continue the chain using data from the L1.  To enable
this add the flag `zkevm.l1-sync-start-block: [first l1 block with sequencer data]`.  It is important to find the first block
on the L1 from the sequencer contract that contains the `sequenceBatches` event.  When the node starts up it will pull of
the L1 data into the cdk-erigon database and use this during execution rather than waiting for transactions from the txpool, effectively
rebuilding the chain from the L1 data.  This can be used in tandem with unwinding the chain, or using the `zkevm.sync-limit` flag
to limit the chain to a certain block height before starting the L1 recovery (useful if you have an RPC node available to speed up the process).

**Important Note:**
**If using the `zkevm.sync-limit` flag you need to go to the boundary of a batch+1 block so if batch 41 ends at block 99
then set the sync limit flag to 100.**

## zkEVM-specific API Support

In order to enable the zkevm_ namespace, please add 'zkevm' to the http.api flag (see the example config below).

### Supported
- `zkevm_batchNumber`
- `zkevm_batchNumberByBlockNumber`
- `zkevm_consolidatedBlockNumber`
- `zkevm_isBlockConsolidated`
- `zkevm_verifiedBatchNumber`
- `zkevm_isBlockVirtualized`
- `zkevm_virtualBatchNumber`
- `zkevm_getFullBlockByHash`
- `zkevm_getFullBlockByNumber`
- `zkevm_virtualCounters`
- `zkevm_traceTransactionCounters`
- `zkevm_getVersionHistory` - returns cdk-erigon versions and timestamps of their deployment (stored in datadir)

### Supported (remote)
- `zkevm_getBatchByNumber`

### Configurable
- `zkevm_getBatchWitness` - concurrency can be limited with `zkevm.rpc-get-batch-witness-concurrency-limit` flag which defaults to 1. Use 0 for no limit.

### Not yet supported
- `zkevm_getNativeBlockHashesInRange`

### Deprecated
- `zkevm_getBroadcastURI` - it was removed by zkEvm
***

## Limitations/Warnings/Performance

- The golden poseidon hashing will be much faster on x86, so developers on Mac may experience slowness on Apple silicone
- Falling behind the network significantly will cause a SMT rebuild - which will take some time for longer chains

Initial SMT build performance can be increased if machine has enough RAM:
- `zkevm.smt-regenerate-in-memory` - setting this to true will use RAM to build the SMT rather than disk which is faster, but requires enough RAM (OOM kill potential)

***

## Configuration Files
Config files are the easiest way to configure cdk-erigon, there are examples in the repository for each network e.g. `hermezconfig-mainnet.yaml.example`.
***

## Running CDK-Erigon
- Build using  `make cdk-erigon`
- Set up your config file (copy one of the examples found in the repository root directory, and edit as required)
- run `./build/bin/cdk-erigon --config="./hermezconfig-{network}.yaml"` (complete the name of your config file as required)

NB: `--externalcl` flag is removed in upstream erigon so beware of re-using commands/config

### Run modes
cdk-erigon can be run as an RPC node which will use the data stream to fetch new block/batch information and track a 
remote sequencer (the default behaviour).  It can also run as a sequencer. To enable the sequencer, set the `CDK_ERIGON_SEQUENCER` environment variable to `1` and start the node.
cdk-erigon supports migrating a node from being an RPC node to a sequencer and vice versa.  To do this, stop the node, set the `CDK_ERIGON_SEQUENCER` environment variable to the desired value and restart the node.
Please ensure that you do include the sequencer specific flags found below when running as a sequencer.  You can include these flags when running as an RPC to keep a consistent configuration between the two run modes.

### Docker ([DockerHub](https://hub.docker.com/r/hermeznetwork/cdk-erigon))
The image comes with 3 preinstalled default configs which you may wish to edit according to the config section below, otherwise you can mount your own config to the container as necessary.

A datadir must be mounted to the container to persist the chain data between runs.

Example commands:
- Mainnet 
```
docker run -d -p 8545:8545 -v ./cdk-erigon-data/:/home/erigon/.local/share/erigon hermeznetwork/cdk-erigon  --config="./mainnet.yaml" --zkevm.l1-rpc-url=https://rpc.eth.gateway.fm
```
- Cardona
```
docker run -d -p 8545:8545 -v ./cdk-erigon-data/:/home/erigon/.local/share/erigon hermeznetwork/cdk-erigon  --config="./cardona.yaml" --zkevm.l1-rpc-url=https://rpc.sepolia.org
```
docker-compose example:

- Mainnet:
```
NETWORK=mainnet L1_RPC_URL=https://rpc.eth.gateway.fm docker-compose -f docker-compose-example.yml up -d
```
- Cardona:
```
NETWORK=cardona L1_RPC_URL=https://rpc.sepolia.org docker-compose -f docker-compose-example.yml up -d
```

### Config
The examples are comprehensive but there are some key fields which will need setting e.g. `datadir`, and others you may wish to change
to increase performance, e.g. `zkevm.l1-rpc-url` as the provided RPCs may have restrictive rate limits.

For a full explanation of the config options, see below:
- `datadir`: Path to your node's data directory.
- `chain`: Specifies the L2 network to connect with, e.g., hermez-mainnet.  For dynamic configs this should always be in the format `dynamic-{network}`
- `http`: Enables HTTP RPC server (set to true).
- `private.api.addr`: Address for the private API, typically localhost:9091, change this to run multiple instances on the same machine
- `zkevm.l2-chain-id`: Chain ID for the L2 network, e.g., 1101.
- `zkevm.l2-sequencer-rpc-url`: URL for the L2 sequencer RPC.
- `zkevm.l2-datastreamer-url`: URL for the L2 data streamer.
- `zkevm.l1-chain-id`: Chain ID for the L1 network.
- `zkevm.l1-rpc-url`: L1 Ethereum RPC URL.
- `zkevm.l1-first-block`: The first block on L1 from which we begin syncing (where the rollup begins on the L1). NB: for AggLayer networks this must be the L1 block where the GER Manager contract was deployed.
- `zkevm.address-sequencer`: The contract address for the sequencer
- `zkevm.address-zkevm`: The address for the zkevm contract
- `zkevm.address-rollup`: The address for the rollup contract
- `zkevm.address-ger-manager`: The address for the GER manager contract
- `zkevm.data-stream-port`: Port for the data stream.  This needs to be set to enable the datastream server
- `zkevm.data-stream-host`: The host for the data stream i.e. `localhost`.  This must be set to enable the datastream server
- `zkevm.datastream-version:` Version of the data stream protocol.
- `externalcl`: External consensus layer flag.
- `http.api`: List of enabled HTTP API modules.

Sequencer specific config:
- `zkevm.executor-urls`: A csv list of the executor URLs.  These will be used in a round robbin fashion by the sequencer
- `zkevm.executor-strict`: Defaulted to true, but can be set to false when running the sequencer without verifications (use with extreme caution)
- `zkevm.witness-full`: Defaulted to true.  Controls whether the full or partial witness is used with the executor.
- `zkevm.reject-smart-contract-deployments`: Defaulted to false.  Controls whether smart contract deployments are rejected by the TxPool.

Resource Utilisation config:
- `zkevm.smt-regenerate-in-memory`: As documented above, allows SMT regeneration in memory if machine has enough RAM, for a speedup in initial sync.

Useful config entries:
- `zkevm.sync-limit`: This will ensure the network only syncs to a given block height.
- `debug.timers`: This will enable debug timers in the logs to help with performance tuning. Recording timings of witness generation, etc. at INFO level.

***


## Networks

| Network Name  | Chain ID | ForkID | Genesis File | RPC URL                                          | Rootchain        | Rollup Address                               |
|---------------|----------|--------|--------------|--------------------------------------------------|------------------|----------------------------------------------|
| zkEVM Mainnet | 1101     | 9      | [Link](https://hackmd.io/bpmxb5QaSFafV0nB4i-KZA) | [Mainnet RPC](https://zkevm-rpc.com/)            | Ethereum Mainnet | `0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2` |
| zkEVM Cardona | 2442     | 9      | [Link](https://hackmd.io/Ug9pB613SvevJgnXRC4YJA) | [Cardona RPC](https://rpc.cardona.zkevm-rpc.com/) | Sepolia          | `0x32d33D5137a7cFFb54c5Bf8371172bcEc5f310ff` |

***

## Health Checks

- Node version: `curl -X POST --data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}' {{node url}}` - returns cdk-erigon version
- Node syncing status: `curl -X POST --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' {{node url}}` - returns stages process or false
- Health check: GET request with header `X-ERIGON-HEALTHCHECK: synced` - returns 200 response if OK


## Additional Resources

- Block Explorers:
  - Mainnet: [PolygonScan Mainnet](https://zkevm.polygonscan.com/)
  - Cardona: [PolygonScan Cardona](https://cardona-zkevm.polygonscan.com/) 

***

_Supported by [Gateway.fm](https://gateway.fm) and [Limechain](https://limechain.tech/)._
