# L1 Block Finder

This Go command finds the block number for a specific batch number by querying an Ethereum node for logs.

## How to Run

Run the command with the required flags:

```sh
go run zk/debug_tools/l1-block-finder/main.go -batch=<batch_number> -endpoint=<node_endpoint> -contracts=<contract_addresses> -start=<start_block>
```

Input:

- batch: The batch number to find.
- endpoint: The Ethereum node endpoint to query.
- contracts: Comma-separated contract addresses to filter logs.
- start: The starting block number for the search.