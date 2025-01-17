# RPC Trace Compare from File Tool

This tool compares transaction traces between a local JSON file and a remote Ethereum node to ensure consistency and detect discrepancies.

## How to Run

It also needs debugToolsConfig, here is an example of it: [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)

Another requirement is a folder called traces, it's going to parse through each of them.

Run the command with the appropriate configuration:
```sh
go run zk/debug_tools/rpc-trace-compare-from-file/main.go
```
