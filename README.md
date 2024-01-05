# cdk-erigon

cdk-erigon is a fork of Erigon, currently in Alpha, optimized for syncing with the Polygon Hermez zkEVM network.

***

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
- `zkevm_getBatchByNumber`
- `zkevm_isBlockConsolidated`
- `zkevm_verifiedBatchNumber`
- `zkevm_isBlockVirtualized`
- `zkevm_virtualBatchNumber`
- `zkevm_getFullBlockByHash`
- `zkevm_getFullBlockByNumber`

### Not yet supported
- `zkevm_getNativeBlockHashesInRange`

***

## Limitations/Warnings

- The golden poseidon hashing will be much faster on x86, so developers on Mac may experience slowness on Apple silicone
- Falling behind by > 500 blocks (at some point this will be configurable) will cause a SMT rebuild - which will take some time for longer chains

***

## Chain Configs
- Testnet - this is the formal testnet
  - chain: `hermez-testnet`
- Mainnet - this runs against L1 Ethereum Mainnet
  - chain: `hermez-mainnet`

***

## Configuration Files
Config files are the easiest way to configure cdk-erigon, there are examples in the repository for each network e.g. `hermezconfig-testnet.yaml.example`.

Depending on the RPC provider you are using, you may wish to alter `zkevm.rpc-ratelimit`.

Here is an example of the testnet config `hermezconfig-testnet.yaml`:

```yaml
datadir : '/Path/to/your/datadirs/hermez-testnet'
chain : "hermez-testnet"
http : true
private.api.addr : "localhost:9091"
zkevm.l2-chain-id: 1442
zkevm.l2-sequencer-rpc-url: "https://rpc.public.zkevm-test.net"
zkevm.l2-datastreamer-url: "stream.zkevm-test.net:6900"
zkevm.l1-chain-id: 5
zkevm.l1-rpc-url: "https://rpc.goerli.eth.gateway.fm"
zkevm.l1-contract-address: "0xa997cfD539E703921fD1e3Cf25b4c241a27a4c7A"
zkevm.l1-matic-contract-address: "0x1319D23c2F7034F52Eb07399702B040bA278Ca49"
zkevm.l1-ger-manager-contract-address: "0x4d9427DCA0406358445bC0a8F88C26b704004f74"
zkevm.l1-first-block: 8577775
zkevm.rpc-ratelimit: 250

externalcl: true
http.api : ["eth","debug","net","trace","web3","erigon", "zkevm"]
```

***

## Running zKEVM Erigon
- Build using  `make cdk-erigon`
- Set up your config file (copy an example and edit as required)
- run `./build/bin/cdk-erigon --config="./hermezconfig-{network}.yaml"` (complete the name of your config file as required)

NB: `--externalcl` flag is removed in upstream erigon so beware of re-using commands/config

***

## Networks

| Network Name        | Chain ID | ForkID | Genesis File | RPC URL | Rootchain  | Smart Contract Address |
|---------------------|----------|--------|--------------|---------|------------|------------------------|
| zkEVM Mainnet       | 1101     | 5      | [Link](https://hackmd.io/bpmxb5QaSFafV0nB4i-KZA) | [Mainnet RPC](https://zkevm-rpc.com/) | Ethereum Mainnet | `0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2` |
| zkEVM Public Testnet| 1442     | 5      | [Link](https://hackmd.io/Ug9pB613SvevJgnXRC4YJA) | [Testnet RPC](https://rpc.public.zkevm-test.net/) | Goerli | `0xa997cfD539E703921fD1e3Cf25b4c241a27a4c7A` |

***

## Additional Resources

- Block Explorers:
  - Testnet: [PolygonScan Testnet](https://testnet-zkevm.polygonscan.com/)
  - Mainnet: [PolygonScan Mainnet](https://zkevm.polygonscan.com/)

***
## Funding for Testing

Use the Goerli faucet to obtain ETH for testing: [Goerli Faucet](https://goerlifaucet.com). Transfer ETH to L2 using the bridge contract page. ENS is not supported on the L2.
<br><br>

***

_Supported by [Gateway.fm](https://gateway.fm) and [Limechain](https://limechain.tech/)._
