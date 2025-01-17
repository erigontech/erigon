# Cast Scripts

This directory contains scripts for interacting with the blockchain using `cast`.

- [balance-after-tx.sh](cast-scripts/balance-after-tx.sh): Checks the balance after a transaction.
- [estimate-gas-infinite-loop.sh](cast-scripts/estimate-gas-infinite-loop.sh): Estimates gas for a transaction that runs an infinite loop.
- [nonce-with-infinite-loop-sc.sh](cast-scripts/nonce-with-infinite-loop-sc.sh): Sends a transaction with an infinite loop smart contract and checks the nonce.

### How to Run

1. Copy `.env.example` to `.env` and fill in the required values.
2. Run the desired script using `bash`.

Example:

```sh
    # considering you are at the root of the project
    # fill the .env before running, but all the scripts above are easily runnable:
    ./zk/debug_tools/cast-scripts/balance-after-tx.sh
    ./zk/debug_tools/cast-scripts/estimate-gas-infinite-loop.sh
    ./zk/debug_tools/cast-scripts/nonce-with-infinite-loop-sc.sh
```
