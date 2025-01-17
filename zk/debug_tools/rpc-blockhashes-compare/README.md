# RPC Blockhashes Compare Tool

This tool compares block hashes between two Ethereum nodes (local and remote) and performs a binary search to find the first block where they mismatch. It then prints the block number and the field differences.

## How to Run

It also needs debugToolsConfig, here an example of it: [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)

Run the command with the appropriate configuration:
```sh
go run zk/debug_tools/rpc-blockhashes-compare/main.go
```
