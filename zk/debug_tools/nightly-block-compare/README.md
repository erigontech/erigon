# Nightly Block Comparison Tool

This tool compares blocks from three different Ethereum nodes (Erigon, zkEVM, and Sequencer) to ensure consistency and detect discrepancies.

## How to Run

Run the command with the appropriate flags:
```sh
go run main.go --erigon http://erigon-node:8545 --zkevm http://zkevm-node:8546 --sequencer http://sequencer-node:8547 --blocks 500 --diff 5 --mode root_and_hash
```

The flags used in the block comparison tool are:

- erigon: RPC URL of the Erigon node (default: http://localhost:8545)
- zkevm: RPC URL of the zkEVM node (default: http://localhost:8546)
- sequencer: RPC URL of the Sequencer node (default: http://localhost:8547)
- blocks: Number of blocks to check (default: 1000)
- diff: Allowed block height difference between nodes (default: 10)
- mode: Comparison mode: full or root_and_hash (default: full)
