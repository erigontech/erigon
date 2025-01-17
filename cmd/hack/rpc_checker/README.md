# RPC Checker

The RPC Checker compares block hashes and transaction receipts between two Ethereum RPC services to find discrepancies.

## Usage

1. Clone the repository:
    ```sh
    git clone <repository-url>
    cd /Users/afa/Code/gateway/cdk-erigon/cmd/hack/rpc_checker
    ```

2. Run the tool:
    ```sh
    go run main.go -node1=http://your-node1-url -node2=http://your-node2-url -fromBlock=3000000 -step=1000 -compare-receipts=true
    ```

3. Flags:
    - `-node1`: First RPC service URL.
    - `-node2`: Second RPC service URL.
    - `-fromBlock`: Starting block number.
    - `-step`: Block increment.
    - `-compare-receipts`: Compare transaction receipts.

## Example

```sh
go run main.go -node1=http://0.0.0.0:8123 -node2=https://rpc.cardona.zkevm-rpc.com -fromBlock=3816916 -step=1 -compare-receipts=true
```

This starts comparing from block 3816916, incrementing by 1, and compares receipts if there's a hash mismatch.

## Notes

- Ensure nodes are accessible.
- The tool reports the first block with a mismatch.

## Workflow

RPC Cache is used by github workflow in the docker-composes of zk/tests/nightly-l1-recovery