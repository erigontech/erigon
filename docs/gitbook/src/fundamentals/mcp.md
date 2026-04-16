---
description: Using AI assistants with Erigon via the Model Context Protocol
---

# MCP Server

Erigon includes a built-in [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server that exposes blockchain data and node metrics to AI assistants like Claude Desktop. It allows you to query blocks, transactions, logs, balances, and internal node state using natural language.

## Two Server Variants

### Embedded (inside Erigon)

The MCP server is **enabled by default** and starts automatically with Erigon on `127.0.0.1:8553` (TCP, SSE transport). No extra binary is needed.

```bash
# MCP server starts automatically — no extra flags required
./build/bin/erigon --datadir=./data

# Disable the embedded MCP server
./build/bin/erigon --datadir=./data --mcp.disable

# Custom address/port
# ⚠️ Using 0.0.0.0 exposes the MCP server to all interfaces — only use on trusted networks
./build/bin/erigon --datadir=./data --mcp.addr=0.0.0.0 --mcp.port=9000
```

This mode has direct access to all Erigon APIs including Prometheus metrics.

### Standalone (`mcp` binary)

A separate binary that connects to Erigon via JSON-RPC or directly via its data directory. Supports `stdio` transport (required for Claude Desktop) and SSE.

```bash
make mcp
./build/bin/mcp --help
```

## Connection Modes (standalone binary)

### Mode 1: JSON-RPC Proxy (recommended)

```bash
# Connect to a running Erigon node via HTTP JSON-RPC
./build/bin/mcp --rpc.url http://127.0.0.1:8545

# Shorthand for localhost
./build/bin/mcp --port 8545

# With log access
./build/bin/mcp --port 8545 --log.dir /data/erigon/logs
```

### Mode 2: Direct Datadir Access

Opens Erigon's MDBX database directly, similar to an external rpcdaemon.

```bash
# Connect via gRPC private API and read the database directly
./build/bin/mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

# Default private API address is 127.0.0.1:9090
./build/bin/mcp --datadir /data/erigon
```

### Mode 3: Auto-Discovery

When no flags are provided, the binary probes `localhost` ports 8545–8547 for a running JSON-RPC endpoint.

```bash
./build/bin/mcp
```

## Transport Modes

| Transport | Flag | Use case |
|-----------|------|----------|
| `stdio` (default) | — | Claude Desktop and MCP-compatible tools |
| `sse` | `--transport sse` | Browser-based tools or remote access |

```bash
# SSE transport
./build/bin/mcp --port 8545 --transport sse --sse.addr 127.0.0.1:8553
```

## Claude Desktop Configuration

Add to `~/.config/claude-desktop/config.json`:

{% code title="~/.config/claude-desktop/config.json" %}
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
{% endcode %}

Or using datadir mode:

{% code title="~/.config/claude-desktop/config.json" %}
```json
{
  "mcpServers": {
    "erigon": {
      "command": "/path/to/build/bin/mcp",
      "args": ["--datadir", "/data/erigon"]
    }
  }
}
```
{% endcode %}

## Available Tools

### Ethereum Standard (`eth_*`)

`eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_getBalance`, `eth_getTransactionByHash`, `eth_getTransactionReceipt`, `eth_getBlockReceipts`, `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_call`, `eth_estimateGas`, `eth_gasPrice`, `eth_chainId`, `eth_syncing`, `eth_getProof`, and more.

### Erigon-Specific (`erigon_*`)

`erigon_forks`, `erigon_blockNumber`, `erigon_getHeaderByNumber`, `erigon_getHeaderByHash`, `erigon_getBlockByTimestamp`, `erigon_getBalanceChangesInBlock`, `erigon_getLogsByHash`, `erigon_getLogs`, `erigon_getBlockReceiptsByBlockHash`, `erigon_nodeInfo`.

### Otterscan (`ots_*`)

`ots_getApiLevel`, `ots_getInternalOperations`, `ots_searchTransactionsBefore`, `ots_searchTransactionsAfter`, `ots_getBlockDetails`, `ots_getBlockTransactions`, `ots_hasCode`, `ots_traceTransaction`, `ots_getTransactionError`, `ots_getTransactionBySenderAndNonce`, `ots_getContractCreator`.

### Log Analysis

`logs_tail`, `logs_head`, `logs_grep`, `logs_stats` — requires `--log.dir` or `--datadir` to locate Erigon and torrent log files.

### Metrics

`metrics_list`, `metrics_get` — available in **embedded mode only**. In standalone mode, these return an informational message.

## Flags (standalone binary)

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc.url` | `http://127.0.0.1:8545` | Erigon JSON-RPC endpoint URL |
| `--port` | `0` | JSON-RPC port shorthand |
| `--datadir` | — | Erigon data directory (enables direct DB mode) |
| `--private.api.addr` | `127.0.0.1:9090` | gRPC private API address (used with `--datadir`) |
| `--transport` | `stdio` | Transport mode: `stdio` or `sse` |
| `--sse.addr` | `127.0.0.1:8553` | SSE listen address |
| `--log.dir` | — | Log directory for log analysis tools |

For embedded MCP server flags (`--mcp.disable`, `--mcp.addr`, `--mcp.port`), see [Default Ports](default-ports.md).
