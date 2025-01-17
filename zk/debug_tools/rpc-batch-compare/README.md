# RPC Batch Compare Tool

This tool compares batches from two different Ethereum nodes (Erigon and Legacy) to ensure consistency and detect discrepancies.

## How to Run

Run the command with the appropriate flags:
```sh
go run zk/debug_tools/rpc-batch-compare/main.go --erigon http://erigon-node:8545 --legacy http://legacy-node:8546 --skip 1 --batches 1000 --offset 0 --override 0
```

The flags used in the rpc-batch-compare tool are:

- `erigon`: RPC URL of the Erigon node (default: http://localhost:8545)
- `legacy`: RPC URL of the Legacy node (default: http://localhost:8546)
- `skip`: Number of batches to skip between each check (default: 1)
- `batches`: Number of batches to check (default: 1000)
- `offset`: Offset from the highest batch number (default: 0)
- `override`: Override start batch number (default: 0)