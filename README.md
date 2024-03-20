# cdk-erigon

cdk-erigon is a fork of Erigon, currently in Alpha, optimized for syncing with the Polygon Hermez zkEVM network.

***
## Release Roadmap
- **v0.9.x**: Support for Cardona testnet
- **v1.x.x**: Support for Mainnet
- **v3.x.x**: Erigon 3 based (snapshot support)

***

## Chain/Fork Support
Current status of cdk-erigon's support for running various chains and fork ids:

- zkEVM Cardona testnet — beta support
- zkEVM mainnet — alpha support
- CDK Chains - experimental support (forkid.8 and above)

## Prereqs
In order to use the optimal vectorized poseidon hashing for the Sparse Merkle Tree, on x86 the following packages are required (for Apple silicone it will fall back to the iden3 library and as such these dependencies are not required in that case.

Please install: 
- Linux: `libgtest-dev` `libomp-dev` `libgmp-dev`
- MacOS: `brew install libomp` `brew install gmp`

Using the Makefile command: `make build-libs` will install these for the relevant architecture.

## sequencer (WIP)

Enable Sequencer: `CDK_ERIGON_SEQUENCER=1 ./build/bin/cdk-erigon <flags>`

## zkevm-specific API Support

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

### Supported (remote)
- `zkevm_getBatchByNumber`

### Not yet supported
- `zkevm_getNativeBlockHashesInRange`

### Deprecated
- `zkevm_getBroadcastURI` - it was removed by zkEvm
***

## Limitations/Warnings

- The golden poseidon hashing will be much faster on x86, so developers on Mac may experience slowness on Apple silicone
- Falling behind the network significantly will cause a SMT rebuild - which will take some time for longer chains

***

## Configuration Files
Config files are the easiest way to configure cdk-erigon, there are examples in the repository for each network e.g. `hermezconfig-mainnet.yaml.example`.

Depending on the RPC provider you are using, you may wish to alter `zkevm.rpc-ratelimit`.

***

## Running CDK-Erigon
- Build using  `make cdk-erigon`
- Set up your config file (copy one of the examples found in the repository root directory, and edit as required)
- run `./build/bin/cdk-erigon --config="./hermezconfig-{network}.yaml"` (complete the name of your config file as required)

NB: `--externalcl` flag is removed in upstream erigon so beware of re-using commands/config

### Docker ([DockerHub](https://hub.docker.com/r/hermeznetwork/cdk-erigon))
The image comes with 3 preinstalled default configs which you may wish to edit according to the config section below, otherwise you can mount your own config to the container as necessary.

A datadir must be mounted to the container to persist the chain data between runs.

Example commands:
- Mainnet `docker run -p 8545:8545 -v ./cdk-erigon-data/:~/.cdk-erigon/data hermeznetwork/cdk-erigon  --config="./mainnet.yaml"`
- Cardona `docker run -p 8545:8545 -v ./cdk-erigon-data/:~/.cdk-erigon/data hermeznetwork/cdk-erigon  --config="./cardona.yaml"`

### Config
The examples are comprehensive but there are some key fields which will need setting e.g. `datadir`, and others you may wish to change
to increase performance, e.g. `zkevm.l1-rpc-url` as the provided RPCs may have restrictive rate limits.

For a full explanation of the config options, see below:
- `datadir`: Path to your node's data directory.
- `chain`: Specifies the L2 network to connect with, e.g., hermez-mainnet.
- `http`: Enables HTTP RPC server (set to true).
- `private.api.addr`: Address for the private API, typically localhost:9091, change this to run multiple instances on the same machine
- `zkevm.l2-chain-id`: Chain ID for the L2 network, e.g., 1101.
- `zkevm.l2-sequencer-rpc-url`: URL for the L2 sequencer RPC.
- `zkevm.l2-datastreamer-url`: URL for the L2 data streamer.
- `zkevm.l1-chain-id`: Chain ID for the L1 network.
- `zkevm.l1-rpc-url`: L1 Ethereum RPC URL.
- `zkevm.l1-polygon-rollup-manager`, `zkevm.l1-rollup`, `zkevm.l1-matic-contract-address`: Addresses and topics for smart contracts and event listening.
- `zkevm.rpc-ratelimit`: Rate limit for RPC calls.
- `zkevm.datastream-version:` Version of the data stream protocol.
- `externalcl`: External consensus layer flag.
- `http.api`: List of enabled HTTP API modules.

***


## Networks

| Network Name  | Chain ID | ForkID | Genesis File | RPC URL                                          | Rootchain        | Rollup Address                               |
|---------------|----------|--------|--------------|--------------------------------------------------|------------------|----------------------------------------------|
| zkEVM Mainnet | 1101     | 7      | [Link](https://hackmd.io/bpmxb5QaSFafV0nB4i-KZA) | [Mainnet RPC](https://zkevm-rpc.com/)            | Ethereum Mainnet | `0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2` |
| zkEVM Cardona | 2442     | 7      | [Link](https://hackmd.io/Ug9pB613SvevJgnXRC4YJA) | [Cardona RPC](https://rpc.cardona.zkevm-rpc.com/) | Sepolia          | `0x32d33D5137a7cFFb54c5Bf8371172bcEc5f310ff` |

***

## Additional Resources

- Block Explorers:
  - Mainnet: [PolygonScan Mainnet](https://zkevm.polygonscan.com/)
  - Cardona: [PolygonScan Cardona](https://cardona-zkevm.polygonscan.com/) 

***

_Supported by [Gateway.fm](https://gateway.fm) and [Limechain](https://limechain.tech/)._
