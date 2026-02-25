# Erigon MCP Server

The Erigon MCP (Model Context Protocol) server exposes Ethereum blockchain data
to AI assistants like Claude Desktop via the MCP standard.

## Two Server Variants

### 1. Embedded (inside Erigon)

The MCP server runs inside the Erigon process with direct access to APIs.
Enabled with `--mcp.addr` and `--mcp.port` flags.

```bash
./build/bin/erigon --datadir=./data --mcp.addr=127.0.0.1 --mcp.port=8553
```

This mode uses SSE transport and provides full access to all tools including
in-process Prometheus metrics.

### 2. Standalone (`cmd/mcp`)

A separate binary that connects to Erigon via JSON-RPC or direct DB access.
Supports stdio transport (for Claude Desktop) and SSE.

```bash
make mcp
./build/bin/mcp --help
```

## Connection Modes

### Mode 1: JSON-RPC Proxy (recommended)

Connect to a running Erigon node via its HTTP JSON-RPC endpoint:

```bash
# Explicit URL:
./build/bin/mcp --rpc.url http://127.0.0.1:8545

# Shorthand for localhost:
./build/bin/mcp --port 8545

# With log access:
./build/bin/mcp --port 8545 --log.dir /data/erigon/logs
```

### Mode 2: Direct Datadir Access

Open Erigon's MDBX database directly (like an external rpcdaemon).
Requires either a running Erigon with gRPC private API, or read-only DB access.

```bash
# Connect to Erigon's gRPC API + read DB directly:
./build/bin/mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

# Default private API address is 127.0.0.1:9090
./build/bin/mcp --datadir /data/erigon
```

### Mode 3: Auto-Discovery

When no flags are provided, the server probes localhost ports 8545-8547
for a running JSON-RPC endpoint:

```bash
./build/bin/mcp
```

## Transport Modes

### stdio (default)

Reads MCP protocol messages from stdin and writes to stdout.
This is the standard transport for Claude Desktop and similar tools.

```bash
./build/bin/mcp --port 8545
```

### SSE (Server-Sent Events)

Serves MCP over HTTP with SSE transport:

```bash
./build/bin/mcp --port 8545 --transport sse --sse.addr 127.0.0.1:8553
```

## Claude Desktop Configuration

Add to `~/.config/claude-desktop/config.json`:

```json
{
  "mcpServers": {
    "erigon": {
      "command": "/path/to/build/bin/mcp",
      "args": ["--port", "8545", "--log.dir", "/data/erigon/logs"]
    }
  }
}
```

## Available Tools

### Ethereum Standard (eth_*)
`eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash`,
`eth_getBalance`, `eth_getTransactionByHash`, `eth_getTransactionReceipt`,
`eth_getBlockReceipts`, `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`,
`eth_getTransactionCount`, `eth_call`, `eth_estimateGas`, `eth_gasPrice`,
`eth_chainId`, `eth_syncing`, `eth_getProof`, and more.

### Erigon-Specific (erigon_*)
`erigon_forks`, `erigon_blockNumber`, `erigon_getHeaderByNumber`,
`erigon_getHeaderByHash`, `erigon_getBlockByTimestamp`,
`erigon_getBalanceChangesInBlock`, `erigon_getLogsByHash`,
`erigon_getLogs`, `erigon_getBlockReceiptsByBlockHash`, `erigon_nodeInfo`.

### Otterscan (ots_*)
`ots_getApiLevel`, `ots_getInternalOperations`,
`ots_searchTransactionsBefore`, `ots_searchTransactionsAfter`,
`ots_getBlockDetails`, `ots_getBlockTransactions`, `ots_hasCode`,
`ots_traceTransaction`, `ots_getTransactionError`,
`ots_getTransactionBySenderAndNonce`, `ots_getContractCreator`.

### Log Analysis
`logs_tail`, `logs_head`, `logs_grep`, `logs_stats` — requires `--log.dir`
or `--datadir` to locate Erigon/torrent log files.

### Metrics
`metrics_list`, `metrics_get` — only available in embedded mode (inside Erigon).
In standalone mode, these return an informational message.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc.url` | `http://127.0.0.1:8545` | Erigon JSON-RPC endpoint URL |
| `--port` | 0 | JSON-RPC port shorthand |
| `--datadir` | | Erigon data directory (enables direct DB mode) |
| `--private.api.addr` | `127.0.0.1:9090` | gRPC private API (with --datadir) |
| `--transport` | `stdio` | Transport: `stdio` or `sse` |
| `--sse.addr` | `127.0.0.1:8553` | SSE listen address |
| `--log.dir` | | Log directory (overrides datadir detection) |
