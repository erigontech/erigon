# Executor Send Tool

This tool sends a JSON payload to a specified gRPC endpoint for processing.

## How to Run

Run the command with the appropriate flags:
```sh
go run main.go --file=path/to/payload.json --endpoint=grpc-endpoint
```

The flags used in the executor-send tool are:

- `file`: Path to the JSON file to send.
- `endpoint`: gRPC endpoint to send the payload to.