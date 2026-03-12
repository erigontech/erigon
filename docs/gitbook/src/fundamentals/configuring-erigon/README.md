---
description: Customizing your Erigon node
metaLinks:
  alternates:
    - >-
      https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/fundamentals/configuring-erigon
---

# CLI Reference

The Erigon CLI has a wide range of flags that can be used to customize its behavior. There are 3 ways to configure Erigon, listed by priority:

* [Command line options](./#command-line-options) (flags)
* [Configuration file](./#configuration-file)
* [Environment variables](./#environment-variables)

## Command line options

Here's a breakdown of the most important flags, see [options](./#options) for the full list:

### General Options

These flags cover the general behavior and configuration of the Erigon client.

* `--datadir value`: Specifies the data directory for the databases.
  * Default: `/home/usr/.local/share/erigon`
* `--ethash.dagdir value`: Sets the directory to store the ethash mining DAGs.
  * Default: `/home/usr/.local/share/erigon-ethash`
* `--config value`: Sets Erigon flags using a YAML/TOML file.
* `--version, -v`: Prints the version information.
* `--help, -h`: Displays help information.
* `--chain value`: Sets the name of the [network](../supported-networks.md) to join.
  * Default: `mainnet`
* `--networkid value`: Explicitly sets the network ID.
  * Default: `1`
* `--identity value`: Sets a custom node name.
* `--externalcl`: Enables the external consensus layer.
  * Default: `false`, means that Caplin is enabled.
* `--override.osaka value`: Manually specifies the Osaka fork time.
  * Default: `0`
* `--override.amsterdam value`: Manually specifies the Amsterdam fork time.
  * Default: `0`
* `--vmdebug`: Records information for VM and contract debugging.
  * Default: `false`
* `--gdbme`: Restarts Erigon under gdb for debugging.
  * Default: `false`
* `--ethstats value`: The reporting URL for an ethstats service.
* `--trusted-setup-file value`: Absolute path to a `trusted_setup.json` file.
* `--persist.receipts, --experiment.persist.receipts.v2`: Downloads historical receipts.
  * Default: `true` for minimal and full nodes, `false` for archive nodes

### Database and Caching

These flags control database performance and memory usage.

* `--db.pagesize value`: Sets the fixed page size for the database.
  * Default: `16KB`
* `--db.size.limit value`: Sets a runtime limit on the chaindata database size.
  * Default: `1TB`
* `--db.writemap`: Enables `WRITE_MAP` for fast database writes.
  * Default: `true`
* `--db.read.concurrency value`: Limits the number of parallel database reads. Default: equal to GOMAXPROCS (or number of CPU)
  * Default: `1408`
* `--database.verbosity value`: Enables internal database logs.
  * Default: `2`
* `--batchSize value`: Sets the batch size for the execution stage.
  * Default: `512M`
* `--bodies.cache value`: Sets the size limit for the block bodies cache.
  * Default: `268435456`
* `--state.cache value`: Sets the amount of data to store in the StateCache.
  * Default: `0MB`
* `--sync.parallel-state-flushing`: Enables parallel state flushing.
  * Default: `true`

### Pruning and Snapshots

Flags for managing how old chain data is handled and stored.

* `--prune.mode value`: Selects a pruning preset (`full`, `archive`, `minimal`, `blocks`). See also [Sync Modes](../sync-modes.md)
  * Default: `"full"`
* `--prune.distance value`: Keeps state history for the latest `N` blocks.
  * Default: `0`
* `--prune.distance.blocks value`: Keeps block history for the latest `N` blocks.
  * Default: `0`
* `--prune.include-commitment-history, --prune.experimental.include-commitment-history, --experimental.commitment-history`: Enables blazing fast `eth_getProof` for executed blocks by storing commitment history.
  * Default: `false`
* `--snap.keepblocks`: Keeps ancient blocks in the database for debugging.
  * Default: `false`
* `--snap.stop`: Stops generating new snapshots.
  * Default: `false`
* `--snap.state.stop`: Stops generating new state files.
  * Default: `false`
* `--snap.skip-state-snapshot-download`: Skips state download and starts from genesis.
  * Default: `false`

### Transaction Pool (TxPool)

Options for configuring the transaction pool.

* `--txpool.api.addr value`: The TxPool API network address.
  * Default: Uses the value of `--private.api.addr`
* `--txpool.disable`: Disables the internal transaction pool.
  * Default: `false`
* `--txpool.pricelimit value`: Sets the minimum gas price for acceptance into the pool.
  * Default: `1`
* `--txpool.pricebump value`: Sets the price bump percentage to replace a transaction.
  * Default: `10`
* `--txpool.blobpricebump value`: Sets the price bump percentage for replacing a type-3 blob transaction.
  * Default: `100`
* `--txpool.accountslots value`: Sets the number of executable transaction slots per account.
  * Default: `16`
* `--txpool.blobslots value`: Sets the maximum number of blobs per account.
  * Default: `540`
* `--txpool.totalblobpoollimit value`: Sets the total limit on the number of all blobs in the pool.
  * Default: `5400`
* `--txpool.globalslots value`: Sets the maximum number of executable transaction slots for all accounts.
  * Default: `10000`
* `--txpool.globalbasefeeslots value`: Sets the maximum number of non-executable transactions with insufficient base fees.
  * Default: `30000`
* `--txpool.globalqueue value`: Sets the maximum number of non-executable transaction slots for all accounts.
  * Default: `30000`
* `--txpool.trace.senders value`: A comma-separated list of addresses whose transactions will be traced.
* `--txpool.commit.every value`: Sets how often transactions are committed to storage.
  * Default: `15s`
* `--txpool.gossip.disable`: Disables P2P gossip of transactions.
  * Default: `false`

### Network and Peers

These flags manage network connectivity, peer discovery, and traffic control.

* `--port value`: The main network listening port.
  * Default: `30303`
* `--p2p.protocol value`: The version of the `eth` P2P protocol.
  * Default: `68`, `69`
* `--p2p.allowed-ports value`: A comma-separated list of allowed ports for different P2P protocols.
  * Default: `30303, 30304, 30305, 30306, 30307`
* `--nat value`: The NAT port mapping mechanism (See [here](nat.md) for more details).
* `--nodiscover`: Disables peer discovery.
  * Default: `false`
* `--discovery.v4`, `--discv4`: Enables the Node Discovery Protocol v4 (Discv4) for managed ENRs and topic discovery.
  * Default: `false`
* `--discovery.v5`, `--discv5`, `--v5disc`: Enables the Node Discovery Protocol v5 (Discv5) for managed ENRs and topic discovery.
  * Default: `true`
* `--netrestrict value`: Restricts network communication to specific IP networks.
* `--nodekey value`: The P2P node key file.
* `--nodekeyhex value`: The P2P node key as a hexadecimal string.
* `--discovery.dns value`: Sets DNS discovery entry points.
* `--bootnodes value`: Comma-separated enode URLs for P2P discovery bootstrap.
* `--staticpeers value`: Comma-separated enode URLs to connect to.
* `--trustedpeers value`: Comma-separated enode URLs for trusted peers.
* `--maxpeers value`: The maximum number of network peers.
  * Default: `32`

### RPC & API

Flags for configuring various RPC servers and their behavior.

* `--private.api.addr value`: The internal gRPC API address for Erigon's components.
  * Default: `127.0.0.1:9090`
* `--private.api.ratelimit value`: Limits the number of simultaneous internal API requests.
  * Default: `31872`
* `--ipcdisable`: Disables the IPC-RPC server.
  * Default: `false`
* `--ipcpath value`: Filename for the IPC socket/pipe within the datadir (explicit paths escape it).
* `--http`: Enables the JSON-RPC HTTP server.
  * Default: `true`
* `--http.enabled`: An alternative flag to enable the HTTP server.
  * Default: `true`
* `--graphql`: Enables the GraphQL endpoint.
  * Default: `false`
* `--http.addr value`: The HTTP-RPC server listening interface.
  * Default: `localhost`
* `--http.port value`: The HTTP-RPC server listening port.
  * Default: `8545`
* `--authrpc.addr value`: The HTTP-RPC server listening interface for the Engine API.
  * Default: `localhost`
* `--authrpc.port value`: The HTTP-RPC server listening port for the Engine API.
  * Default: `8551`
* `--authrpc.jwtsecret value`: The path to the JWT secret file for the consensus layer.
* `--http.compression`: Enables compression over HTTP-RPC.
  * Default: `true`
* `--http.corsdomain value`: A comma-separated list of domains for cross-origin requests.
* `--http.vhosts value`: A comma-separated list of virtual hostnames.
  * Default: `localhost`
* `--authrpc.vhosts value`: A comma-separated list of virtual hostnames for the Engine API.
  * Default: `localhost`
* `--http.api value`: The APIs offered over the HTTP-RPC interface.
  * Default: `eth,erigon,engine`
* `--ws`: Enables the WS-RPC server.
  * Default: `false`
* `--ws.addr value`: The WS-RPC server listening interface.
  * Default: `localhost`
* `--ws.port value`: The WS-RPC server listening port.
  * Default: `8546`
* `--ws.api value`: The APIs offered over the WS-RPC interface.
* `--ws.origins value`: Origins from which to accept WebSocket requests.
* `--ws.compression`: Enables compression over WebSocket.
  * Default: `true`
* `--rpc.gethcompat`: Enables Geth-compatible storage iteration order for `debug_storageRangeAt` (sorted by keccak256 hash). Disabled by default for performance.
  * Default: `false`
* `--rpc.txsync.defaulttimeout value`: Default timeout for `eth_sendRawTransactionSync`.
  * Default: `25s`
* `--rpc.txsync.maxtimeout value`: Maximum allowed timeout for `eth_sendRawTransactionSync`.
  * Default: `1m0s`
* `--rpc.batch.concurrency value`: Limits the number of goroutines for batch requests.
  * Default: `2`
* `--rpc.streaming.disable`: Disables JSON streaming for heavy endpoints.
  * Default: `false`
* `--rpc.accessList value`: Specifies a granular API allowlist.
* `--rpc.gascap value`: Sets a cap on gas usage for `eth_call`/`estimateGas`.
  * Default: `50000000`
* `--rpc.batch.limit value`: Sets the maximum number of requests in a batch.
  * Default: `100`
* `--rpc.blockrange.limit value`: Sets a hardware cap on the number of blocks scanned per RPC request. Protects the rpcdaemon from resource exhaustion (CPU/Memory) and hangs caused by "heavy" queries. A value of `0` means unlimited.
  * Default: `0`
* `--rpc.returndata.limit value`: Sets the maximum return data size for `eth_call`.
  * Default: `100000`
* `--rpc.allow-unprotected-txs`: Allows unprotected transactions via RPC.
  * Default: `false`
* `--rpc.txfeecap value`: Sets a cap on transaction fees in ether.
  * Default: `1`
* `--rpc.slow value`: Logs RPC requests slower than the specified threshold.
  * Default: `0s`
* `--rpc.evmtimeout value`: The maximum time to wait for an EVM call.
  * Default: `5m0s`
* `--rpc.overlay.getlogstimeout value`: The maximum time to wait for `overlay_getLogs`.
  * Default: `5m0s`
* `--rpc.overlay.replayblocktimeout value`: The maximum time to wait to replay a single block.
  * Default: `10s`
* `--rpc.subscription.filters.maxlogs value`: Maximum logs to store per subscription.
  * Default: `0`
* `--rpc.subscription.filters.maxheaders value`: Maximum block headers to store per subscription.
  * Default: `0`
* `--rpc.subscription.filters.maxtxs value`: Maximum transactions to store per subscription.
  * Default: `0`
* `--rpc.subscription.filters.maxaddresses value`: Maximum addresses per subscription to filter logs by.
  * Default: `0`
* `--rpc.subscription.filters.maxtopics value`: Maximum topics per subscription to filter logs by.
  * Default: `0`
* `--http.timeouts.read value`: Maximum duration for reading a request.
  * Default: `30s`
* `--http.timeouts.write value`: Maximum duration before timing out a response write.
  * Default: `30m0s`
* `--http.timeouts.idle value`: Maximum idle time for a connection with keep-alives enabled.
  * Default: `2m0s`
* `--authrpc.timeouts.read value`: Maximum read duration for an Engine API request.
  * Default: `30s`
* `--authrpc.timeouts.write value`: Maximum write duration for an Engine API response.
  * Default: `30m0s`
* `--authrpc.timeouts.idle value`: Maximum idle time for an Engine API connection.
  * Default: `2m0s`
* `--healthcheck`: Enables gRPC health checks.
  * Default: `false`

### MCP Server

Flags for configuring the Model Context Protocol (MCP) server. The embedded MCP server is **enabled by default** on
`127.0.0.1:8553`. Pass `--mcp.disable` to turn it off.

* `--mcp.disable`: Disables the embedded MCP server.
    * Default: `false`
* `--mcp.addr value`: The MCP server listening address.
  * Default: `127.0.0.1`
* `--mcp.port value`: The MCP server listening port.
  * Default: `8553`

### Logging and Profiling

Flags for controlling logging and performance profiling.

* `--log.json`: Formats console logs with JSON.
  * Default: `false`
* `--log.console.json`: Formats console logs with JSON.
  * Default: `false`
* `--log.dir.json`: Formats file logs with JSON.
  * Default: `false`
* `--verbosity value`: Sets the log level for console logs.
  * Default: `info`
* `--log.console.verbosity value`: Sets the log level for console logs.
  * Default: `info`
* `--log.dir.disable`: Disables disk logging.
  * Default: `false`
* `--log.dir.path value`: The path to store user and error logs.
* `--log.dir.prefix value`: The file name prefix for logs stored on disk.
* `--log.dir.verbosity value`: Sets the log verbosity for disk logs.
  * Default: `dbug`
* `--log.delays`: Enables block delay logging.
  * Default: `false`
* `--pprof`: Enables the pprof HTTP server.
  * Default: `false`
* `--pprof.addr value`: The pprof HTTP server listening interface.
  * Default: `127.0.0.1`
* `--pprof.port value`: The pprof HTTP server listening port.
  * Default: `6060`
* `--pprof.cpuprofile value`: Writes a CPU profile to a file.
* `--trace value`: Writes an execution trace to a file.
* `--vmtrace value`: Sets the provider tracer.
* `--vmtrace.jsonconfig value`: Sets the tracer's configuration.
* `--metrics`: Enables metrics collection and reporting.
  * Default: `false`
* `--metrics.addr value`: The stand-alone metrics HTTP server listening interface.
  * Default: `127.0.0.1`
* `--metrics.port value`: The metrics HTTP server listening port.
  * Default: `6061`

### Consensus and Forks

Flags related to consensus mechanisms and network forks.

* `--clique.checkpoint value`: The number of blocks after which to save the vote snapshot.
  * Default: `10`
* `--clique.snapshots value`: The number of recent vote snapshots to keep in memory.
  * Default: `1024`
* `--clique.signatures value`: The number of recent block signatures to keep in memory.
  * Default: `16384`
* `--clique.datadir value`: The path to the clique database folder.
* `--fakepow`: Disables proof-of-work verification.
  * Default: `false`
* `--gpo.blocks value`: The number of recent blocks to check for gas prices.
  * Default: `20`
* `--gpo.percentile value`: The percentile of recent transaction gas prices to use for a suggested gas price.
  * Default: `60`
* `--gpo.maxprice value`: The maximum gas price recommended by the gas price oracle.
  * Default: `500000000000` (500 GWei)
* `--proposer.disable`: Disables the PoS proposer.
  * Default: `false`
* `--builder.maxblobs value`: Cap the number of blob transactions included in a built block.
* `--bor.heimdall value`: The URL of the Heimdall service.
  * Default: `http://localhost:1317`
* `--bor.withoutheimdall`: Runs without the Heimdall service.
  * Default: `false`
* `--bor.period`: Overrides the bor block period.
  * Default: `false`
* `--bor.minblocksize`: Ignores the bor block period and waits for `blocksize` transactions.
  * Default: `false`
* `--polygon.pos.ssf`: Enables Polygon PoS Single Slot Finality.
  * Default: `false`
* `--polygon.pos.ssf.block value`: Enables Polygon PoS Single Slot Finality from a specific block.
  * Default: `0`

### Sentry

Flags for configuring the Sentry component.

* `--sentry.api.addr value`: A comma-separated list of sentry addresses.
* `--sentry.log-peer-info`: Logs detailed peer info when a peer connects or disconnects.
  * Default: `false`
* `--sentinel.addr value`: The address for the sentinel component.
  * Default: `localhost`
* `--sentinel.port value`: The port for the sentinel component.
  * Default: `7777`
* `--sentinel.bootnodes value`: Comma-separated enode URLs for P2P discovery bootstrap for the sentinel.
* `--sentinel.staticpeers value`: Connects to comma-separated consensus static peers.

Here is the rewritten and merged Downloader section, combining the two original sections and ensuring consistent formatting:

### Downloader and Synchronization

These flags control the block synchronization and data downloading process, including the BitTorrent protocol settings.

* `--downloader.api.addr value`: The downloader address.
* `--downloader.disable.ipv4`: Disables IPv4 for the downloader.
  * Default: `false`
* `--downloader.disable.ipv6`: Disables IPv6 for the downloader.
  * Default: `false`
* `--no-downloader`: Disables the downloader component.
  * Default: `false`
* `--downloader.verify`: Verifies snapshots on startup.
  * Default: `false`
* `--sync.loop.throttle value`: Sets the minimum time between sync loop starts.
* `--sync.loop.block.limit value`: Sets the maximum number of blocks to process per loop iteration.
  * Default: `5000`
* `--sync.loop.break.after value`: Sets the last stage of the sync loop to run.
* `--bad.block value`: Marks a block as bad and forces a reorg.
* `--webseed value`: Comma-separated URLs for network support infrastructure.

#### BitTorrent Options

* `--torrent.port value`: The port to listen for the BitTorrent protocol.
  * Default: `42069`
* `--torrent.maxpeers value`: An unused parameter.
  * Default: `100`
* `--torrent.conns.perfile value`: The number of connections per file.
  * Default: `10`
* `--torrent.trackers.disable`: Disables conventional BitTorrent trackers.
  * Default: `false`
* `--torrent.upload.rate value`: The upload rate in bytes per second.
  * Default: `16mb`
* `--torrent.download.rate value`: The download rate in bytes per second.
  * Default: `512mb`
* `--torrent.webseed.download.rate value`: The download rate for webseeds. If not set, rate limit is shared with torrent.
* `--torrent.verbosity value`: Sets the verbosity level for BitTorrent logs. 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set `--verbosity` to equal or higher level)
  * Default: `1`

### Fork Choice Update (FCU)

Flags for configuring Fork Choice Update behavior.

* `--fcu.timeout value`: FCU timeout before switching to async processing (use `0` to disable).
  * Default: `1s`
* `--fcu.background.prune`: Enables background pruning after FCU.
  * Default: `true`
* `--fcu.background.commit`: Enables background flush and commit after FCU.
  * Default: `false`

### Caplin (Consensus Layer)

Flags for configuring the Caplin consensus layer.

* `--caplin.discovery.addr value`: The address for the Caplin DISCV5 protocol.
  * Default: `0.0.0.0`
* `--caplin.discovery.port value`: The port for the Caplin DISCV5 protocol.
  * Default: `4000`
* `--caplin.discovery.tcpport value`: The TCP port for the Caplin DISCV5 protocol.
  * Default: `4001`
* `--caplin.checkpoint-sync-url value`: The checkpoint sync endpoint.
* `--caplin.subscribe-all-topics`: Subscribes to all gossip topics.
  * Default: `false`
* `--caplin.max-peer-count value`: The maximum number of peers to connect to.
  * Default: `128`
* `--caplin.enable-upnp`: Enables NAT porting for Caplin.
  * Default: `false`
* `--caplin.max-inbound-traffic-per-peer value`: The maximum inbound traffic per second per peer.
  * Default: `1MB`
* `--caplin.max-outbound-traffic-per-peer value`: The maximum outbound traffic per second per peer.
  * Default: `1MB`
* `--caplin.adaptable-maximum-traffic-requirements`: Makes the node adaptable to traffic based on the number of validators.
  * Default: `true`
* `--caplin.blocks-archive`: Enables backfilling for blocks.
  * Default: `false`
* `--caplin.blobs-archive`: Enables backfilling for blobs.
  * Default: `false`
* `--caplin.states-archive`: Enables the archival node for historical states.
  * Default: `false`
* `--caplin.blobs-immediate-backfill`: Tells Caplin to immediately backfill blobs.
  * Default: `false`
* `--caplin.blobs-no-pruning`: Disables blob pruning.
  * Default: `false`
* `--caplin.checkpoint-sync.disable`: Disables checkpoint sync.
  * Default: `false`
* `--caplin.snapgen`: Enables snapshot generation.
  * Default: `false`
* `--caplin.mev-relay-url value`: The MEV relay endpoint.
* `--caplin.validator-monitor`: Enables Caplin validator monitoring metrics.
  * Default: `false`
* `--caplin.custom-config value`: Sets a custom config for Caplin.
* `--caplin.custom-genesis value`: Sets a custom genesis for Caplin.
* `--caplin.use-engine-api`: Uses the Engine API for internal Caplin.
  * Default: `false`

### Shutter Network Encrypted Transactions

Flags for configuring the Shutter Network encrypted transactions mempool.

* `--shutter`: Enables the Shutter encrypted transactions mempool.
  * Default: `false`
* `--shutter.p2p.bootstrap.nodes value`: Overrides the default P2P bootstrap nodes.
* `--shutter.p2p.listen.port value`: Overrides the default P2P listen port.
  * Default: `0`

## Configuration file

You can configure Erigon using a YAML or TOML configuration file by specifying its path with the `--config` flag.

{% hint style="success" %}
**Note**: flags specified in the configuration file can be overridden by directly setting them in the Erigon command line.
{% endhint %}

Both in YAML and TOML files boolean operators (`false`, `true`) and strings without spaces can be specified without quotes, while strings with spaces must be included in `""` or `''` quotes.

### YAML

Use the `--config` flag to point at the YAML configuration file.

```bash
./build/bin/erigon --config ./config.yaml --chain=holesky
```

Example of a YAML config file:

```yaml
datadir : 'your datadir'
chain : "mainnet"
http : true
http.api : ["eth","debug","net"]
```

In this case the `--chain` flag in the command line will override the value in the YAML file and Erigon will run on the Holesky testnet.

### TOML

Use the `--config` flag to point at the TOML configuration file.

```bash
./build/bin/erigon --config ./config.toml
```

Example of a TOML config file:

```toml
datadir = 'your datadir'
chain = "mainnet"
http = true
"http.api" = ["eth","debug","net"]
```

## Environment variables

Erigon supports configuration through environment variables, primarily for experimental features and advanced settings.

### Core Environment Variables

**Database and Performance:**

* `MDBX_LOCK_IN_RAM` - Locks MDBX database in RAM for better performance
* `MDBX_DIRTY_SPACE_MB` - Sets dirty space limit for MDBX database
* `SNAPSHOT_MADV_RND` - Controls snapshot memory advice randomization (default: `true`)

**Synchronization and Pruning:**

* `NO_PRUNE` - Disables pruning when set to true [5](./#0-4)
* `NO_MERGE` - Disables merging operations [6](./#0-5)
* `PRUNE_TOTAL_DIFFICULTY` - Controls total difficulty pruning (default: `true`)
* `MAX_REORG_DEPTH` - Sets maximum reorganization depth (default: `512`)

**Execution and Processing:**

* `EXEC3_PARALLEL` - Enables parallel execution in version 3
* `EXEC3_WORKERS` - Sets number of execution workers
* `STAGES_ONLY_BLOCKS` - Limits stages to blocks only

### Memory and Debugging Variables

**Memory Management:**

* `NO_MEMSTAT` - Disables memory statistics collection
* `SAVE_HEAP_PROFILE` - Enables automatic heap profiling
* `HEAP_PROFILE_THRESHOLD` - Memory usage percentage threshold for heap profiling (default: 35%)

**Tracing and Debugging:**

* `TRACE_ACCOUNTS` - Comma-separated list of accounts to trace
* `TRACE_BLOCKS` - Comma-separated list of block numbers to trace
* `TRACE_INSTRUCTIONS` - Enables instruction-level tracing

### Docker Environment Variables

When running Erigon in Docker, you can configure user permissions and data directories:

* `DOCKER_UID` - The UID of the docker user
* `DOCKER_GID` - The GID of the docker user
* `XDG_DATA_HOME` - The data directory mounted to containers

### Usage Examples

**Basic Performance Tuning:**

```bash
export MDBX_LOCK_IN_RAM=true
export SNAPSHOT_MADV_RND=false
./build/bin/erigon --datadir=/path/to/data
```

**Memory Debugging:**

```bash
export SAVE_HEAP_PROFILE=true
export HEAP_PROFILE_THRESHOLD=45
./build/bin/erigon --datadir=/path/to/data
```

**Docker Deployment:**

```bash
export DOCKER_UID=$(id -u)
export DOCKER_GID=$(id -g)
export XDG_DATA_HOME=/preferred/data/folder
make docker-compose
```

## Options

_All available options_

In order to see all the available options (flags) you must run the command:

```bash
./build/bin/erigon --help
```

The flag listing is reproduced below for your convenience (version 3.3.7-9a898cf7):

**Commands:**
- `init` — Bootstrap and initialize a new genesis block
- `import` — Import a blockchain file
- `snapshots, seg, snapshot, segments, segment` — Managing historical data segments (partitions)
- `support` — Connect Erigon instance to a diagnostics system for support
- `shutter-validator-reg-check` — Check if the provided validators are registered with Shutter
- `help, h` — Shows a list of commands or help for one command

**Global Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--datadir value` | `/home/user/.local/share/erigon` | Data directory for the databases |
| `--ethash.dagdir value` | `/home/user/.local/share/erigon-ethash` | Directory to store the ethash mining DAGs |
| `--externalcl` | `false` | Enables the external consensus layer |
| `--txpool.disable` | `false` | External pool and block producer. Disabling internal txpool and block producer. |
| `--txpool.pricelimit value` | `1` | Minimum gas price (fee cap) limit to enforce for acceptance into the pool |
| `--txpool.pricebump value` | `10` | Price bump percentage to replace an already existing transaction |
| `--txpool.blobpricebump value` | `100` | Price bump percentage to replace existing (type-3) blob transaction |
| `--txpool.accountslots value` | `16` | Minimum number of executable transaction slots guaranteed per account |
| `--txpool.blobslots value` | `540` | Max allowed total number of blobs (within type-3 txs) per account |
| `--txpool.totalblobpoollimit value` | `5400` | Total limit of number of all blobs in txs within the txpool |
| `--txpool.globalslots value` | `10000` | Maximum number of executable transaction slots for all accounts |
| `--txpool.globalbasefeeslots value` | `30000` | Maximum number of non-executable transactions where only not enough baseFee |
| `--txpool.globalqueue value` | `30000` | Maximum number of non-executable transaction slots for all accounts |
| `--txpool.trace.senders value` | | Comma separated list of addresses, whose transactions will traced in transaction pool with debug printing |
| `--txpool.commit.every value` | `15s` | How often transactions should be committed to the storage |
| `--prune.distance value` | `0` | Keep state history for the latest N blocks (default: everything) |
| `--prune.distance.blocks value` | `0` | Keep block history for the latest N blocks (default: everything) |
| `--prune.mode value` | `full` | Choose a pruning preset: "full", "archive", "minimal", "blocks" |
| `--prune.include-commitment-history` | `false` | Enables blazing fast eth_getProof for executed block. Aliases: `--experimental.commitment-history`, `--prune.experimental.include-commitment-history` |
| `--fcu.timeout value` | `1s` | FCU timeout before it switches to being process async (use 0 to disable) |
| `--fcu.background.prune` | `true` | Enables background pruning post fcu |
| `--fcu.background.commit` | `false` | Enables background flush and commit post fcu |
| `--batchSize value` | `512M` | Batch size for the execution stage |
| `--bodies.cache value` | `268435456` | Limit on the cache for block bodies |
| `--database.verbosity value` | `2` | Enabling internal db logs. Default: 2 (warning) |
| `--private.api.addr value` | `127.0.0.1:9090` | Internal gRPC API address |
| `--private.api.ratelimit value` | `31872` | Amount of requests server handles simultaneously |
| `--etl.bufferSize value` | `256MB` | Buffer size for ETL operations |
| `--tls` | `false` | Enable TLS handshake |
| `--tls.cert value` | | Specify certificate |
| `--tls.key value` | | Specify key file |
| `--tls.cacert value` | | Specify certificate authority |
| `--state.stream.disable` | `false` | Disable streaming of state changes from core to RPC daemon |
| `--sync.loop.throttle value` | | Sets the minimum time between sync loop starts (e.g. 1h30m, default is none) |
| `--bad.block value` | | Marks block with given hex string as bad and forces initial reorg before normal staged sync |
| `--http` | `true` | JSON-RPC server (enabled by default). Use `--http=false` to disable it |
| `--http.enabled` | `true` | JSON-RPC HTTP server (enabled by default). Use `--http.enabled=false` to disable it |
| `--graphql` | `false` | Enable the graphql endpoint |
| `--http.addr value` | `localhost` | HTTP-RPC server listening interface |
| `--http.port value` | `8545` | HTTP-RPC server listening port |
| `--authrpc.addr value` | `localhost` | HTTP-RPC server listening interface for the Engine API |
| `--authrpc.port value` | `8551` | HTTP-RPC server listening port for the Engine API |
| `--authrpc.jwtsecret value` | | Path to the token that ensures safe connection between CL and EL |
| `--http.compression` | `true` | Enable compression over HTTP-RPC |
| `--http.corsdomain value` | | Comma separated list of domains for cross origin requests |
| `--http.vhosts value` | `localhost` | Comma separated list of virtual hostnames |
| `--authrpc.vhosts value` | `localhost` | Virtual hostnames for Engine API |
| `--http.api value` | `eth,erigon,engine` | APIs offered over HTTP-RPC |
| `--ws.port value` | `8546` | WS-RPC server listening port |
| `--ws` | `false` | Enable the WS-RPC server |
| `--ws.addr value` | | WS-RPC server listening interface |
| `--ws.api value` | | APIs offered over WS-RPC |
| `--ws.origins value` | | Origins allowed for WebSocket connections |
| `--ws.compression` | `true` | Enable compression over WebSocket |
| `--http.trace` | `false` | Print all HTTP requests to logs with INFO level |
| `--http.dbg.single` | `false` | Allow pass HTTP header 'dbg: true' to print more detailed logs. Alias: `--rpc.dbg.single` |
| `--state.cache value` | `0MB` | Amount of data to store in StateCache |
| `--rpc.batch.concurrency value` | `2` | Limit goroutines to process 1 batch request |
| `--rpc.streaming.disable` | `false` | Disable json streaming for heavy endpoints like trace_* |
| `--db.read.concurrency value` | `1408` | Limit parallel db reads. Default: equal to GOMAXPROCS |
| `--rpc.accessList value` | | Specify granular (method-by-method) API allowlist |
| `--trace.compat` | `false` | Bug for bug compatibility with OE for trace_ routines |
| `--rpc.gascap value` | `50000000` | Sets a cap on gas that can be used in eth_call/estimateGas |
| `--rpc.batch.limit value` | `100` | Maximum number of requests in a batch |
| `--rpc.returndata.limit value` | `100000` | Maximum number of bytes returned from eth_call or similar invocations |
| `--rpc.allow-unprotected-txs` | `false` | Allow for unprotected (non-EIP155 signed) transactions via RPC |
| `--rpc.txfeecap value` | `1` | Cap on transaction fee (in ether) via RPC APIs (0 = no cap) |
| `--rpc.gethcompat` | `false` | Enables geth-compatible debug_storageRangeAt storage order |
| `--rpc.txsync.defaulttimeout value` | | Default timeout for eth_sendRawTransactionSync |
| `--rpc.txsync.maxtimeout value` | | Maximum timeout for eth_sendRawTransactionSync |
| `--rpc.blockrange.limit value` | | Limits the block range for eth_getLogs, erigon_getLogs, trace_filter (DoS protection) |
| `--txpool.api.addr value` | _(value of --private.api.addr)_ | TxPool api network address |
| `--trace.maxtraces value` | `200` | Sets a limit on traces returned in trace_filter |
| `--experimental.always-generate-changesets` | `false` | Allows to override changesets generation logic |
| `--http.timeouts.read value` | `30s` | Maximum duration for reading the entire request |
| `--http.timeouts.write value` | `30m0s` | Maximum duration before timing out writes of the response |
| `--http.timeouts.idle value` | `2m0s` | Maximum wait time for next request when keep-alive enabled |
| `--authrpc.timeouts.read value` | `30s` | Maximum duration for reading the Engine API request |
| `--authrpc.timeouts.write value` | `30m0s` | Maximum duration before timing out Engine API writes |
| `--authrpc.timeouts.idle value` | `2m0s` | Maximum wait time for next Engine API request |
| `--rpc.evmtimeout value` | `5m0s` | Maximum time to wait for EVM call answer |
| `--rpc.slow value` | `0s` | Print RPC requests slower than given threshold in logs |
| `--rpc.overlay.getlogstimeout value` | `5m0s` | Maximum time for overlay_getLogs call |
| `--rpc.overlay.replayblocktimeout value` | `10s` | Maximum time to replay a single block in overlay_getLogs |
| `--rpc.subscription.filters.maxlogs value` | `0` | Maximum number of logs per subscription |
| `--rpc.subscription.filters.maxheaders value` | `0` | Maximum number of block headers per subscription |
| `--rpc.subscription.filters.maxtxs value` | `0` | Maximum number of transactions per subscription |
| `--rpc.subscription.filters.maxaddresses value` | `0` | Maximum number of addresses per subscription |
| `--rpc.subscription.filters.maxtopics value` | `0` | Maximum number of topics per subscription |
| `--snap.keepblocks` | `false` | Keep ancient blocks in db |
| `--snap.stop` | `false` | Workaround to stop producing new snapshots |
| `--snap.state.stop` | `false` | Workaround to stop producing new state files |
| `--ipcdisable` | `false` | Disable the IPC-RPC server |
| `--ipcpath value` | | Filename for IPC socket/pipe within the datadir |
| `--gpo.maxprice value` | `500 GWei` | Maximum gas price recommended by oracle |
| `--builder.maxblobs value` | | Max blob transactions per built block |
| `--txpool.gossip.disable` | `false` | Disable p2p gossip of txs |
| `--sync.loop.block.limit value` | `5000` | Maximum blocks to process per loop iteration |
| `--sync.loop.break.after value` | | Last stage of the sync loop to run |
| `--sync.parallel-state-flushing` | `true` | Enables parallel state flushing |
| `--chaos.monkey` | `false` | Enable 'chaos monkey' for testing |
| `--shutter` | `false` | Enable Shutter encrypted transactions mempool |
| `--shutter.p2p.bootstrap.nodes value` | | Override default p2p bootstrap nodes for Shutter |
| `--shutter.p2p.listen.port value` | `0` | Override default p2p listen port for Shutter |
| `--polygon.pos.ssf` | `false` | Enable Polygon PoS Single Slot Finality |
| `--polygon.pos.ssf.block value` | `0` | Enable Polygon PoS SSF since block |
| `--polygon.wit-protocol` | `false` | Enable WIT protocol for stateless witness data exchange |
| `--gdbme` | `false` | Restart erigon under gdb for debug purposes |
| `--experimental.concurrent-commitment` | `false` | EXPERIMENTAL: enables concurrent trie for commitment |
| `--erigondb.override.stepsize value` | `1562500` | Override number of transactions per step |
| `--erigondb.override.stepsinfrozenfile value` | `64` | Override number of steps in frozen snapshot files |
| `--caplin.blocks-archive` | `false` | Sets whether backfilling is enabled for caplin |
| `--caplin.blobs-archive` | `false` | Sets whether blob backfilling is enabled for caplin |
| `--caplin.states-archive` | `false` | Enables archival node for historical states in caplin |
| `--caplin.blobs-immediate-backfill` | `false` | Sets whether caplin should immediately backfill blobs |
| `--caplin.blobs-no-pruning` | `false` | Disable blob pruning in caplin |
| `--caplin.checkpoint-sync.disable` | `false` | Disable checkpoint sync in caplin |
| `--caplin.snapgen` | `false` | Enables snapshot generation in caplin |
| `--caplin.mev-relay-url value` | | MEV relay endpoint (enables builder mode) |
| `--caplin.validator-monitor` | `false` | Enable caplin validator monitoring metrics |
| `--caplin.custom-config value` | | Set custom config for caplin |
| `--caplin.custom-genesis value` | | Set custom genesis for caplin |
| `--caplin.use-engine-api` | `false` | Use engine API for internal Caplin |
| `--caplin.subscribe-all-topics` | `false` | Subscribe to all gossip topics |
| `--caplin.max-peer-count value` | `128` | Max number of peers to connect |
| `--caplin.enable-upnp` | `false` | Enable NAT porting for Caplin |
| `--caplin.max-inbound-traffic-per-peer value` | `1MB` | Max inbound traffic per second per peer |
| `--caplin.max-outbound-traffic-per-peer value` | `1MB` | Max outbound traffic per second per peer |
| `--caplin.adaptable-maximum-traffic-requirements` | `true` | Adapt traffic requirements based on validator count |
| `--sentinel.addr value` | `localhost` | Address for sentinel |
| `--sentinel.port value` | `7777` | Port for sentinel |
| `--sentinel.bootnodes value` | | Enode URLs for P2P discovery bootstrap |
| `--sentinel.staticpeers value` | | Connect to Consensus static peers |
| `--ots.search.max.pagesize value` | `25` | Max page size for search methods |
| `--beacon.api value` | | Enable beacon API (available: beacon, builder, config, debug, events, node, validator, lighthouse) |
| `--beacon.api.addr value` | `localhost` | Host to listen for beacon api requests |
| `--beacon.api.cors.allow-methods value` | `GET, POST, PUT, DELETE, OPTIONS` | CORS allow methods |
| `--beacon.api.cors.allow-origins value` | | CORS allow origins |
| `--beacon.api.cors.allow-credentials` | `false` | CORS allow credentials |
| `--beacon.api.port value` | `5555` | Port for beacon api requests |
| `--beacon.api.read.timeout value` | `5` | Read timeout in seconds for beacon api |
| `--beacon.api.write.timeout value` | `31536000` | Write timeout in seconds for beacon api |
| `--beacon.api.protocol value` | `tcp` | Protocol for beacon API |
| `--beacon.api.ide.timeout value` | `25` | Idle timeout in seconds for beacon api |
| `--trusted-setup-file value` | | Absolute path to trusted_setup.json file |
| `--pprof` | `false` | Enable the pprof HTTP server |
| `--pprof.addr value` | `127.0.0.1` | pprof HTTP server listening interface |
| `--pprof.port value` | `6060` | pprof HTTP server listening port |
| `--pprof.cpuprofile value` | | Write CPU profile to the given file |
| `--trace value` | | Write execution trace to the given file |
| `--vmtrace value` | | Set the provider tracer |
| `--vmtrace.jsonconfig value` | | Set the config of the tracer |
| `--metrics` | `false` | Enable metrics collection and reporting |
| `--metrics.addr value` | `127.0.0.1` | Metrics HTTP server listening interface |
| `--metrics.port value` | `6061` | Metrics HTTP server listening port |
| `--log.json` | `false` | Format console logs with JSON |
| `--log.console.json` | `false` | Format console logs with JSON |
| `--log.dir.json` | `false` | Format file logs with JSON |
| `--verbosity value` | `info` | Set the log level for console logs |
| `--log.console.verbosity value` | `info` | Set the log level for console logs |
| `--log.dir.disable` | `false` | Disable disk logging |
| `--log.dir.path value` | | Path to store user and error logs to disk |
| `--log.dir.prefix value` | | File name prefix for logs stored to disk |
| `--log.dir.verbosity value` | `dbug` | Set the log verbosity for logs stored to disk |
| `--log.delays` | `false` | Enable block delay logging |
| `--config value` | | Sets erigon flags from YAML/TOML file |
| `--help, -h` | | Show help |
| `--version, -v` | | Print the version |
