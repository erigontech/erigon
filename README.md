# cdk-erigon

cdk-erigon is a fork of Erigon, currently in Alpha, optimized for syncing with the Polygon Hermez zkEVM network.

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

## Running zKEVM Erigon
- Build using  `make cdk-erigon`
- Set up your config file (copy an example and edit as required)
- run `./build/bin/cdk-erigon --config="./hermezconfig-{network}.yaml"` (complete the name of your config file as required)

NB: `--externalcl` flag is removed in upstream erigon so beware of re-using commands/config

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
