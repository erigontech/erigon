# Unwind Compare Tool

This tool compares batches and blocks between two Ethereum nodes to ensure consistency and detect discrepancies.

## How to Run

Run the command with the appropriate flags:
```sh
go run unwind-compare.go --node1 http://node1-url --node2 http://node2-url --stop 100
```

The flags used in the unwind-compare tool are:

- `node1`: URL of the first node.
- `node2`: URL of the second node.
- `stop`: Batch number to stop at (optional).