# L1 info tree builder

This Go program retrieves Ethereum logs and block headers from a specified endpoint, processes them to build a Merkle tree of L1 information, and prints the new root hash for each added leaf.

## How to Run

To run the program, use the following command:

```sh
go run main.go --start 0 --endpoint http://localhost:8545 --address 0x...
```

you can see another example [here](run.sh)

Required Flags:

- start: The starting block number (e.g., 0).
- endpoint: The endpoint URL to query (e.g., http://localhost:8545).
- address: The contract address to filter logs (e.g., 0x...).