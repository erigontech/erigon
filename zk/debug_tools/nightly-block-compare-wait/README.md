# Nightly Block Comparison Wait Tool

This tool compares blocks from three different Ethereum nodes (Erigon, Compare, and Compare2) to ensure consistency and detect discrepancies.

nightly-block-compare-wait continuously checks block heights and compares blocks at specified intervals over a given duration, with an option to exit after the first successful check.

## How to Run

Run the command with the appropriate flags:
```sh
go run main.go --erigon http://erigon:8123 --compare http://other-node:8545 --compare2 http://other-node2:8545 --interval 2m --duration 20m --max-block-diff 1000 --exit-on-first-check
```

The flags used in the block comparison wait tool are:

- erigon: URL for the Erigon node (default: http://erigon:8123)
- compare: URL for the compare node (default: http://other-node:8545)
- compare2: URL for the second compare node (default: http://other-node2:8545)
- interval: Interval to check the block height (default: 2m)
- duration: Total duration to run the checks (default: 20m)
- max-block-diff: Maximum allowed block difference (default: 1000)
- exit-on-first-check: Exit after the first check (default: true)

