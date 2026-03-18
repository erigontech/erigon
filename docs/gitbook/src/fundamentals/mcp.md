---
description: >-
  Using Erigon's Model Context Protocol (MCP) server to interact with
  blockchain data through AI assistants
metaLinks:
  alternates:
    - https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/fundamentals/mcp
---

# MCP Server

The **Model Context Protocol (MCP)** is an open standard developed by Anthropic that allows AI assistants (such as Claude Desktop) to securely connect to external data sources and tools. MCP defines a uniform interface through which an AI model can discover, read, and invoke capabilities provided by a server — without needing any custom integration code.

Erigon ships a full MCP server implementation. Once connected, an AI assistant gains structured, read-only access to Ethereum blockchain data, Erigon logs, and internal metrics — turning natural-language questions into precise on-chain lookups.

{% hint style="success" %}
MCP is a read-only interface: the server exposes query tools only. No write operations (sending transactions, changing state) are available.
{% endhint %}

## Two Server Variants

Erigon provides two ways to run the MCP server:

### Embedded (inside Erigon)

The MCP server runs inside the main `erigon` process, with direct access to internal APIs and Prometheus metrics. It is
**enabled by default** on `127.0.0.1:8553`:

```bash
# MCP server starts automatically on 127.0.0.1:8553
./build/bin/erigon --datadir=./data
```

To disable the embedded MCP server entirely, pass `--mcp.disable`:

```bash
./build/bin/erigon --datadir=./data --mcp.disable
```

This mode uses **SSE (Server-Sent Events)** transport over HTTP and is the simplest option when you are already running a full Erigon node.

### Standalone (`mcp` binary)

A separate `mcp` binary that connects to an existing Erigon node either via its JSON-RPC endpoint or directly via the MDBX database. Supports both **stdio** (for Claude Desktop) and **SSE** transports.

The `mcp` binary is available in the following ways:

* **Official GitHub Releases**: The `mcp` binary is bundled with every official Erigon release on [GitHub Releases](https://github.com/erigontech/erigon/releases). Download the archive for your platform and find the binary alongside `erigon`.
* **Docker image**: The binary is included in the official Erigon Docker image at `/usr/local/bin/mcp`.
* **Build from source**:

```bash
make mcp
./build/bin/mcp --help
```

## Configuration

### Flags (embedded mode)

| Flag            | Default     | Description                                   |
|-----------------|-------------|-----------------------------------------------|
| `--mcp.disable` | `false`     | Disable the embedded MCP server entirely      |
| `--mcp.addr`    | `127.0.0.1` | Listening address for the embedded MCP server |
| `--mcp.port`    | `8553`      | Listening port for the embedded MCP server    |

{% hint style="info" %}
The embedded MCP server is **enabled by default** and listens on `127.0.0.1:8553`. Because it binds to localhost only,
it is not reachable from external networks. If you do not need AI-assistant integration, pass `--mcp.disable` to avoid
opening the port. If you are running multiple Erigon instances on the same machine, assign distinct ports with
`--mcp.port` to avoid conflicts.
{% endhint %}

### Flags (standalone `mcp` binary)

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc.url` | `http://127.0.0.1:8545` | Erigon JSON-RPC endpoint to proxy |
| `--port` | — | Shorthand for `--rpc.url http://127.0.0.1:<port>` |
| `--datadir` | — | Erigon data directory (enables direct DB access mode) |
| `--private.api.addr` | `127.0.0.1:9090` | gRPC private API address (used with `--datadir`) |
| `--transport` | `stdio` | Transport type: `stdio` or `sse` |
| `--sse.addr` | `127.0.0.1:8553` | SSE listen address (when `--transport sse`) |
| `--log.dir` | — | Path to Erigon log directory (enables log analysis tools) |

## Connection Modes (standalone)

The standalone `mcp` binary supports three connection modes, tried in priority order:

### 1. JSON-RPC Proxy (recommended)

Forwards tool calls to a running Erigon node's HTTP JSON-RPC endpoint. Works with any Erigon instance that has the RPC server enabled.

```bash
# Explicit URL
./build/bin/mcp --rpc.url http://127.0.0.1:8545

# Port shorthand
./build/bin/mcp --port 8545

# With log analysis enabled
./build/bin/mcp --port 8545 --log.dir /data/erigon/logs
```

### 2. Direct Datadir Access

Opens Erigon's MDBX database directly — similar to an external `rpcdaemon`. Requires either a running Erigon instance with the gRPC private API, or read-only local disk access.

```bash
# gRPC + direct DB
./build/bin/mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

# Default private.api.addr (127.0.0.1:9090)
./build/bin/mcp --datadir /data/erigon
```

### 3. Auto-Discovery

When started without flags, the binary probes `localhost:8545`, `8546`, and `8547` for a running JSON-RPC endpoint:

```bash
./build/bin/mcp
```

## Use Cases

### Interactive Blockchain Analysis

Connect your AI assistant to a running Erigon node and ask natural-language questions about on-chain data without writing a single line of code.

> **"What is the ETH balance of vitalik.eth right now, and how many transactions has it sent?"**
>
> The assistant calls `eth_getBalance` and `eth_getTransactionCount` on the address, formats the results in human-readable form, and summarises the answer.

> **"Show me all ERC-20 Transfer events emitted in the last 100 blocks from contract 0xA0b…"**
>
> The assistant calls `eth_getLogs` with the appropriate filter, decodes the ABI-encoded topics, and presents a table of transfers with amounts and counterparties.

> **"Which transactions in block 21,000,000 consumed the most gas?"**
>
> The assistant calls `eth_getBlockByNumber` and `eth_getBlockReceipts`, sorts by gas used, and returns the top offenders with links to the relevant hashes.

### Node Debugging and Monitoring

When something looks wrong with your node, ask the assistant to sift through logs and metrics for you.

> **"My node seems stuck. Check the sync status and the last 50 log lines for any errors."**
>
> The assistant calls `eth_syncing` to get the current sync progress, then uses `logs_tail` and `logs_grep` to search for `ERROR` or `WARN` entries — surfacing actionable information without requiring you to SSH into the server.

> **"Are there any unusual patterns in the torrent download stats over the last hour?"**
>
> Using the `torrent_status` prompt and `logs_stats`, the assistant analyses download throughput, stall events, and peer connectivity in a single conversational turn.

## Setting Up with Claude Desktop

Add the following to your Claude Desktop configuration file (`~/.config/claude-desktop/config.json` on Linux/macOS, `%APPDATA%\claude-desktop\config.json` on Windows):

{% tabs %}
{% tab title="JSON-RPC mode" %}
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
{% endtab %}
{% tab title="Datadir mode" %}
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
{% endtab %}
{% tab title="Embedded (SSE)" %}
```json
{
  "mcpServers": {
    "erigon": {
      "type": "sse",
      "url": "http://127.0.0.1:8553/sse"
    }
  }
}
```

The embedded MCP server starts by default on `127.0.0.1:8553`. To use a different address or port, pass `--mcp.addr` and
`--mcp.port`. To disable it, pass `--mcp.disable`.
{% endtab %}
{% endtabs %}

After saving, restart Claude Desktop. The Erigon tools will appear in the tool panel under **erigon**.

## Available Tools

The MCP server exposes over 40 tools grouped by namespace:

### Ethereum Standard (`eth_*`)

Standard JSON-RPC methods exposed as MCP tools: `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_getBalance`, `eth_getTransactionByHash`, `eth_getTransactionReceipt`, `eth_getBlockReceipts`, `eth_getLogs`, `eth_getCode`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_call`, `eth_estimateGas`, `eth_gasPrice`, `eth_chainId`, `eth_syncing`, `eth_getProof`, and more.

### Erigon-Specific (`erigon_*`)

`erigon_nodeInfo`, `erigon_forks`, `erigon_getBlockByTimestamp`, `erigon_getBalanceChangesInBlock`, `erigon_getLogsByHash`, `erigon_getBlockReceiptsByBlockHash`, and more.

### Otterscan (`ots_*`)

`ots_getInternalOperations`, `ots_searchTransactionsBefore`, `ots_searchTransactionsAfter`, `ots_getBlockDetails`, `ots_traceTransaction`, `ots_getTransactionError`, `ots_getContractCreator`, and more.

### Log Analysis (`logs_*`)

`logs_tail`, `logs_head`, `logs_grep`, `logs_stats` — require `--log.dir` or `--datadir` to locate Erigon log files.

### Metrics (`metrics_*`)

`metrics_list`, `metrics_get` — available in **embedded mode only**. In standalone mode these return a descriptive message.

## Resources and Prompts

In addition to tools, the MCP server exposes structured **resources** and **prompts**:

**Resources** (addressable by URI):

| Resource | Description |
|----------|-------------|
| `erigon://node/info` | Node version, chain, capabilities |
| `erigon://chain/config` | Full chain configuration |
| `erigon://blocks/recent` | Last 10 blocks summary |
| `erigon://network/status` | Sync state and peer count |
| `erigon://gas/current` | Current gas price |
| `erigon://address/{address}/summary` | Balance, nonce, contract status |
| `erigon://block/{number}/summary` | Block summary |
| `erigon://transaction/{hash}/analysis` | Transaction analysis |

**Prompts** (pre-built analysis templates):

| Prompt | Description |
|--------|-------------|
| `analyze_transaction` | Deep-dive into a transaction |
| `investigate_address` | Profile an address (balance, history, code) |
| `analyze_block` | Summarise a block and its transactions |
| `gas_analysis` | Current and historical gas price analysis |
| `debug_logs` | Triage node issues from recent log output |
| `torrent_status` | Snapshot download health check |
| `sync_analysis` | Node sync progress and performance |
