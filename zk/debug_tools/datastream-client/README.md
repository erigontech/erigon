# Datastream Client Debug Tool

A simple tool for debugging and inspecting datastream entries from a datastream server.

## Usage

```bash
go run main.go -server localhost:6900
```

### Options
- `-server`: Address of the datastream server (default: "localhost:6900")

## Features
- Connects to a datastream server
- Prints basic information about each entry type received:
  - Batch Start entries (number and fork ID)
  - Batch End entries (number and state root)
  - L2 Blocks (number, hash, and batch number)
  - GER Updates (batch number and global exit root)
- Graceful shutdown on Ctrl+C

## Example Output
```
Batch Start: Number=123, ForkId=7
Block: Number=456, Hash=0x..., Batch=123
GER Update: Batch=123, GER=0x...
Batch End: Number=123, StateRoot=0x...
```

## Testing
1. Start the datastream server:
```bash
go run ../datastream-host/main.go -file /path/to/datastream
```

2. In another terminal, run the relay:
```bash
go run main.go -server localhost:6900
``` 