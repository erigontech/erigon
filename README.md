# cdk-erigon

cdk-erigon is a fork of Erigon, optimized for syncing with the Polygon Hermez zkEVM network.


## Gateway Documentation

Please visit https://docs.gateway.fm/cdk-erigon/what-is-cdk-erigon/ for comprehensive documentation of the cdk-erigon stack.

***

## Hardware requirements

* A Linux-based OS (e.g., Ubuntu Server 22.04 LTS), with g++ 12+ being necessary, meaning Ubuntu 22.04 LTS or a more recent version is required.
* At least 32GB RAM with a 4-core CPU.
* Both Apple Silicon and AMD64 are supported.

## Chain/Fork Support
Current status of cdk-erigon's support for running various chains and fork ids:

- zkEVM Cardona testnet — full support
- zkEVM mainnet — beta support
- CDK Chains - beta support (forkid.9 and above)

## Dynamic Chain Configuration
To use chains other than the defaults above, a set of configuration files can be supplied to run any chain. There are 2 ways to do this:

### Method 1:
1. Ensure your chain name starts with the word `dynamic` e.g. `dynamic-mynetwork`
2. Create 3 files for dynamic configs (examples for Cardona in `zk/examples/dynamic-configs`, edit as required)
   - `dynamic-{network}-allocs.json` - the allocs file
   - `dynamic-{network}-chainspec.json` - the chainspec file
   - `dynamic-{network}-conf.json` - an additional configuration file
   - `dynamic-{network}.yaml` - the run config file for erigon.  You can use any of the example yaml files at the root of the repo as a base and edit as required, but ensure the `chain` field is in the format `dynamic-{network}` and matches the names of the config files above.
3. Place the erigon config file along with the other files in the directory of your choice, for example `dynamic-mynetwork`.

### Method 2:
1. Ensure your chain name starts with the word `dynamic` e.g. `dynamic-mynetwork`
2. Create 1 dynamic config file (example file in `zk/examples/dynamic-configs/union-dynamic-config.json`, edit as required)
3. Use `zkevm.genesis-config-path` to set the path to the config file e.g. `--zkevm.genesis-config-path="/dynamic-mynetwork/dynamic-mynetwork.json"`

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

Due to dependency requirements Go 1.24 is required to build.

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

### Witness Cache
The node can cache witnesses for batches to speed up the RPC endpoint `zkevm_getBatchWitness`. To read more on how to enable this feature, see the [Witness Cache Flags](docs/witness-cache.md) section.

## Sequencer

Enable Sequencer: `CDK_ERIGON_SEQUENCER=1 ./build/bin/cdk-erigon <flags>`
[Golang version >= 1.24](https://golang.org/doc/install); GCC 10+ or Clang; On Linux: kernel > v4

### Special mode - L1 recovery
The sequencer supports a special recovery mode which allows it to continue the chain using data from the L1.  To enable
this add the flag `zkevm.l1-sync-start-block: [first l1 block with sequencer data]`.  It is important to find the first block
on the L1 from the sequencer contract that contains the `sequenceBatches` event.  When the node starts up it will pull of
the L1 data into the cdk-erigon database and use this during execution rather than waiting for transactions from the txpool, effectively
rebuilding the chain from the L1 data.  This can be used in tandem with unwinding the chain, or using the `zkevm.sync-limit` flag
to limit the chain to a certain block height before starting the L1 recovery (useful if you have an RPC node available to speed up the process).

**Important Note:**
**This mode is not supported for pre-forkid8 networks. In their case, the node should be synced up to forkid8 and then switch to sequencer recover mode.**


**Important Note:**
**If using the `zkevm.sync-limit` flag you need to go to the boundary of a batch+1 block so if batch 41 ends at block 99
then set the sync limit flag to 100.**

## zkEVM-specific API Support

In order to enable the zkevm_ namespace, please add 'zkevm' to the http.api flag (see the example config below).

### Supported
- `zkevm_batchNumber`
- `zkevm_batchNumberByBlockNumber`
- `zkevm_consolidatedBlockNumber`
- `zkevm_estimateCounters`
- `zkevm_getBatchByNumber`
- `zkevm_getBatchCountersByNumber`
- `zkevm_getBlockRangeWitness`
- `zkevm_getExitRootTable`
- `zkevm_getExitRootsByGER`
- `zkevm_getForkById`
- `zkevm_getForkId`
- `zkevm_getForkIdByBatchNumber`
- `zkevm_getForks`
- `zkevm_getFullBlockByHash`
- `zkevm_getFullBlockByNumber`
- `zkevm_getL2BlockInfoTree`
- `zkevm_getLatestDataStreamBlock`
- `zkevm_getLatestGlobalExitRoot`
- `zkevm_getProverInput`
- `zkevm_getRollupAddress`
- `zkevm_getRollupManagerAddress`
- `zkevm_getVersionHistory`
- `zkevm_getWitness`
- `zkevm_isBlockConsolidated`
- `zkevm_isBlockVirtualized`
- `zkevm_verifiedBatchNumber`
- `zkevm_virtualBatchNumber`

### Configurable
- `zkevm_getBatchWitness` - concurrency can be limited with `zkevm.rpc-get-batch-witness-concurrency-limit` flag which defaults to 1. Use 0 for no limit.

### Not yet supported
- `zkevm_getNativeBlockHashesInRange`

### Deprecated
- `zkevm_getBroadcastURI` - it was removed by zkEvm
- `zkevm.l1-cache-enabled` - the l1 cache is removed from erigon
- `zkevm.l1-cache-port` - removed as the cache is no longer supported
- `zkevm_virtualCounters`
- `zkevm_traceTransactionCounters`

For a more information on available API methods and how to configure them see [docs.gateway.fm](https://docs.gateway.fm/cdk-erigon/json-rpc/zkevm/polygon-zkevm-node-api/).

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
- `http.api`: List of enabled HTTP API modules.
- `zkevm.initial-commitment`: The initial commitment method for hashing the state. Either `smt` (Sparse Merkle Tree) or `pmt` (Patricia Merkle Trie). Default is `smt`. This must be enabled from genesis and modifying the value after will not change the commitment method.

Sequencer specific config:
- `zkevm.executor-urls`: A csv list of the executor URLs.  These will be used in a round robbin fashion by the sequencer
- `zkevm.executor-strict`: Defaulted to true, but can be set to false when running the sequencer without verifications (use with extreme caution)
- `zkevm.witness-full`: Defaulted to false.  Controls whether the full or partial witness is used with the executor.
- `zkevm.reject-smart-contract-deployments`: Defaulted to false.  Controls whether smart contract deployments are rejected by the TxPool.
- `zkevm.ignore-bad-batches-check`: Defaulted to false.  Controls whether the sequencer will ignore bad batches and continue to the next batch. <strong style='color:red'>WARNING: this is a very specific and dangerous mode of operation!</strong>

Resource Utilisation config:
- `zkevm.smt-regenerate-in-memory`: As documented above, allows SMT regeneration in memory if machine has enough RAM, for a speedup in initial sync.
- `zkevm.shadow-sequencer`: Defaulted to false. Allows the sequencer to lag behind the latest L1 batch. Used for local testing.

Useful config entries:
- `zkevm.sync-limit`: This will ensure the network only syncs to a given block height.
- `debug.timers`: This will enable debug timers in the logs to help with performance tuning. Recording timings of witness generation, etc. at INFO level.
- `zkevm.panic-on-reorg`: Useful when the state should be preserved on history reorg

Metrics and pprof configuration flags:

- `metrics:` Enables or disables the metrics collection. Set to true to enable.
- `metrics.addr`: The address on which the metrics server will listen. Default is "0.0.0.0".
- `metrics.port`: The port on which the metrics server will listen. Default is 6060.
- `pprof`: Enables or disables the pprof profiling. Set to true to enable.
- `pprof.addr`: The address on which the pprof server will listen. Default is "0.0.0.0".
- `pprof.port`: The port on which the pprof server will listen. Default is 6061.

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

## Nix Development Environment

If you're not using [NixOS](https://nixos.org/), start by installing the [nix package manager](https://nixos.org/download/).

Once that's set up:
- If you have [direnv](https://github.com/direnv/direnv) installed, it will automatically load the required development packages for you.
- If not, you can manually run `nix develop` in the project directory to achieve the same effect.

## Additional Resources

- Block Explorers:
  - Mainnet: [PolygonScan Mainnet](https://zkevm.polygonscan.com/)
  - Cardona: [PolygonScan Cardona](https://cardona-zkevm.polygonscan.com/) 

***

_Supported by [Gateway.fm](https://gateway.fm) and [Limechain](https://limechain.tech/)._
