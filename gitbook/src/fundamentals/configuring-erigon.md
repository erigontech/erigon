---
description: Customizing your Erigon node
---

# Configuring Erigon

The Erigon CLI has a wide range of flags that can be used to customize its behavior. There are 3 ways to configure Erigon, listed by priority:

* [Command line options](configuring-erigon.md#command-line-options) (flags)
* [Configuration file](configuring-erigon.md#configuration-file)
* [Environment variables](configuring-erigon.md#environment-variables)

## Command line options

Here's a breakdown of some of the flags, see [options](configuring-erigon.md#options) for the full list:

### General Options

These flags cover the general behavior and configuration of the Erigon client.

* `--datadir value`: Specifies the data directory for the databases.
  * Default: `/home/usr/.local/share/erigon`
* `--ethash.dagdir value`: Sets the directory to store the ethash mining DAGs.
  * Default: `/home/usr/.local/share/erigon-ethash`
* `--config value`: Sets Erigon flags using a YAML/TOML file.
* `--version, -v`: Prints the version information.
* `--help, -h`: Displays help information.
* `--chain value`: Sets the name of the [network](https://erigon.gitbook.io/docs/summary/fundamentals/supported-networks) to join.
  * Default: `mainnet`
* `--networkid value`: Explicitly sets the network ID.
  * Default: `1`
* `--identity value`: Sets a custom node name.
* `--externalcl`: Enables the external consensus layer.
  * Default: `false`, means that Caplin is enabled.
* `--override.osaka value`: Manually specifies the Osaka fork time.
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
* `--db.read.concurrency value`: Limits the number of parallel database reads.
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

* `--prune.mode value`: Selects a pruning preset (`full`, `archive`, `minimal`, `blocks`).
  * Default: `"full"`
* `--prune.distance value`: Keeps state history for the latest `N` blocks.
  * Default: `0`
* `--prune.distance.blocks value`: Keeps block history for the latest `N` blocks.
  * Default: `0`
* `--prune.experimental.include-commitment-history, --experimental.commitment-history`: Enables faster `eth_getProof` for executed blocks.
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
  * Default: `68`, `67`
* `--p2p.allowed-ports value`: A comma-separated list of allowed ports for different P2P protocols.
  * Default: `30303, 30304, 30305, 30306, 30307`
* `--nat value`: The NAT port mapping mechanism.
* `--nodiscover`: Disables peer discovery.
  * Default: `false`
* `--v5disc`: Enables the experimental RLPx V5 (Topic Discovery) mechanism.
  * Default: `false`
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
* `--ws.port value`: The WS-RPC server listening port.
  * Default: `8546`
* `--ws.compression`: Enables compression over WebSocket.
  * Default: `true`
* `--rpc.batch.concurrency value`: Limits the number of goroutines for batch requests.
  * Default: `2`
* `--rpc.streaming.disable`: Disables JSON streaming for heavy endpoints.
  * Default: `false`
* `--rpc.accessList value`: Specifies a granular API allowlist.
* `--rpc.gascap value`: Sets a cap on gas usage for `eth_call`/`estimateGas`.
  * Default: `50000000`
* `--rpc.batch.limit value`: Sets the maximum number of requests in a batch.
  * Default: `100`
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
  * Default: `info`
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
* `--proposer.disable`: Disables the PoS proposer.
  * Default: `false`
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

### Downloader

Flags for controlling the downloader component.

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

### Downloader

Flags for managing the Downloader for data synchronization.

* `--torrent.port value`: The port to listen for the BitTorrent protocol.
  * Default: `42069`
* `--torrent.maxpeers value`: An unused parameter.
  * Default: `100`
* `--torrent.conns.perfile value`: The number of connections per file.
  * Default: `10`
* `--torrent.trackers.disable`: Disables conventional BitTorrent trackers.
  * Default: `false`
* `--torrent.upload.rate value`: The upload rate in bytes per second.
  * Default: `32mb`
* `--torrent.download.rate value`: The download rate in bytes per second.
* `--torrent.webseed.download.rate value`: The download rate for webseeds.
* `--torrent.verbosity value`: Sets the verbosity level for BitTorrent logs.
  * Default: `1`

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

* `NO_PRUNE` - Disables pruning when set to true [5](configuring-erigon.md#0-4)
* `NO_MERGE` - Disables merging operations [6](configuring-erigon.md#0-5)
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

The `--help` flag listing is reproduced below for your convenience.

```bash
./build/bin/erigon --help
```

{% code overflow="wrap" fullWidth="true" %}
```bash
NAME:
   erigon - erigon

USAGE:
   erigon [command] [flags]

VERSION:
   3.2.1-0b0fde3a

COMMANDS:
   init                                         Bootstrap and initialize a new genesis block
   import                                       Import a blockchain file
   snapshots, seg, snapshot, segments, segment  Managing historical data segments (partitions)
   support                                      Connect Erigon instance to a diagnostics system for support
   shutter-validator-reg-check                  check if the provided validators are registered with shutter
   help, h                                      Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --datadir value                                                                     Data directory for the databases (default: /home/user/.local/share/erigon)
   --ethash.dagdir value                                                               Directory to store the ethash mining DAGs (default: /home/user/.local/share/erigon-ethash)
   --externalcl                                                                        Enables the external consensus layer (default: false)
   --txpool.disable                                                                    External pool and block producer, see ./cmd/txpool/readme.md for more info. Disabling internal txpool and block producer. (default: false)
   --txpool.pricelimit value                                                           Minimum gas price (fee cap) limit to enforce for acceptance into the pool (default: 1)
   --txpool.pricebump value                                                            Price bump percentage to replace an already existing transaction (default: 10)
   --txpool.blobpricebump value                                                        Price bump percentage to replace existing (type-3) blob transaction (default: 100)
   --txpool.accountslots value                                                         Minimum number of executable transaction slots guaranteed per account (default: 16)
   --txpool.blobslots value                                                            Max allowed total number of blobs (within type-3 txs) per account (default: 540)
   --txpool.totalblobpoollimit value                                                   Total limit of number of all blobs in txs within the txpool (default: 5400)
   --txpool.globalslots value                                                          Maximum number of executable transaction slots for all accounts (default: 10000)
   --txpool.globalbasefeeslots value                                                   Maximum number of non-executable transactions where only not enough baseFee (default: 30000)
   --txpool.globalqueue value                                                          Maximum number of non-executable transaction slots for all accounts (default: 30000)
   --txpool.trace.senders value                                                        Comma separated list of addresses, whose transactions will traced in transaction pool with debug printing
   --txpool.commit.every value                                                         How often transactions should be committed to the storage (default: 15s)
   --prune.distance value                                                              Keep state history for the latest N blocks (default: everything) (default: 0)
   --prune.distance.blocks value                                                       Keep block history for the latest N blocks (default: everything) (default: 0)
   --prune.mode value                                                                  Choose a pruning preset to run onto. Available values: "full", "archive", "minimal", "blocks".
                                                                                             full: Keep only necessary blocks and latest state,
                                                                                             blocks: Keep all blocks but not the state history,
                                                                                             archive: Keep the entire state history and all blocks,
                                                                                             minimal: Keep only latest state (default: "full")
   --prune.experimental.include-commitment-history, --experimental.commitment-history  Enables blazing fast eth_getProof for executed block (default: false)
   --batchSize value                                                                   Batch size for the execution stage (default: "512M")
   --bodies.cache value                                                                Limit on the cache for block bodies (default: "268435456")
   --database.verbosity value                                                          Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning. (default: 2)
   --private.api.addr value                                                            Erigon's components (txpool, rpcdaemon, sentry, downloader, ...) can be deployed as independent Processes on same/another server. Then components will connect to erigon by this internal grpc API. example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface (default: "127.0.0.1:9090")
   --private.api.ratelimit value                                                       Amount of requests server handle simultaneously - requests over this limit will wait. Increase it - if clients see 'request timeout' while server load is low - it means your 'hot data' is small or have much RAM.  (default: 31872)
   --etl.bufferSize value                                                              Buffer size for ETL operations. (default: "256MB")
   --tls                                                                               Enable TLS handshake (default: false)
   --tls.cert value                                                                    Specify certificate
   --tls.key value                                                                     Specify key file
   --tls.cacert value                                                                  Specify certificate authority
   --state.stream.disable                                                              Disable streaming of state changes from core to RPC daemon (default: false)
   --sync.loop.throttle value                                                          Sets the minimum time between sync loop starts (e.g. 1h30m, default is none)
   --bad.block value                                                                   Marks block with given hex string as bad and forces initial reorg before normal staged sync
   --http                                                                              JSON-RPC server (enabled by default). Use --http=false to disable it (default: true)
   --http.enabled                                                                      JSON-RPC HTTP server (enabled by default). Use --http.enabled=false to disable it (default: true)
   --graphql                                                                           Enable the graphql endpoint (default: false)
   --http.addr value                                                                   HTTP-RPC server listening interface (default: "localhost")
   --http.port value                                                                   HTTP-RPC server listening port (default: 8545)
   --authrpc.addr value                                                                HTTP-RPC server listening interface for the Engine API (default: "localhost")
   --authrpc.port value                                                                HTTP-RPC server listening port for the Engine API (default: 8551)
   --authrpc.jwtsecret value                                                           Path to the token that ensures safe connection between CL and EL
   --http.compression                                                                  Enable compression over HTTP-RPC. Use --http.compression=false to disable it (default: true)
   --http.corsdomain value                                                             Comma separated list of domains from which to accept cross origin requests (browser enforced)
   --http.vhosts value                                                                 Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts 'any' or '*' as wildcard. (default: "localhost")
   --authrpc.vhosts value                                                              Comma separated list of virtual hostnames from which to accept Engine API requests (server enforced). Accepts 'any' or '*' as wildcard. (default: "localhost")
   --http.api value                                                                    API's offered over the HTTP-RPC interface (default: "eth,erigon,engine")
   --ws.port value                                                                     WS-RPC server listening port (default: 8546)
   --ws                                                                                Enable the WS-RPC server (default: false)
   --ws.compression                                                                    Enable compression over WebSocket (enabled by default in case WS-RPC is enabled). Use --ws.enabled=false to disable it (default: true)
   --http.trace                                                                        Print all HTTP requests to logs with INFO level (default: false)
   --http.dbg.single, --rpc.dbg.single                                                 Allow pass HTTP header 'dbg: true' to printt more detailed logs - how this request was executed (default: false)
   --state.cache value                                                                 Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB (default: "0MB")
   --rpc.batch.concurrency value                                                       Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request (default: 2)
   --rpc.streaming.disable                                                             Erigon has enabled json streaming for some heavy endpoints (like trace_*). It's a trade-off: greatly reduce amount of RAM (in some cases from 30GB to 30mb), but it produce invalid json format if error happened in the middle of streaming (because json is not streaming-friendly format) (default: false)
   --db.read.concurrency value                                                         Does limit amount of parallel db reads. Default: equal to GOMAXPROCS (or number of CPU) (default: 1408)
   --rpc.accessList value                                                              Specify granular (method-by-method) API allowlist
   --trace.compat                                                                      Bug for bug compatibility with OE for trace_ routines (default: false)
   --rpc.gascap value                                                                  Sets a cap on gas that can be used in eth_call/estimateGas (default: 50000000)
   --rpc.batch.limit value                                                             Maximum number of requests in a batch (default: 100)
   --rpc.returndata.limit value                                                        Maximum number of bytes returned from eth_call or similar invocations (default: 100000)
   --rpc.allow-unprotected-txs                                                         Allow for unprotected (non-EIP155 signed) transactions to be submitted via RPC (default: false)
   --rpc.txfeecap value                                                                Sets a cap on transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap) (default: 1)
   --txpool.api.addr value                                                             TxPool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)
   --trace.maxtraces value                                                             Sets a limit on traces that can be returned in trace_filter (default: 200)
   --http.timeouts.read value                                                          Maximum duration for reading the entire request, including the body. (default: 30s)
   --http.timeouts.write value                                                         Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read. (default: 30m0s)
   --http.timeouts.idle value                                                          Maximum amount of time to wait for the next request when keep-alive connections are enabled. If http.timeouts.idle is zero, the value of http.timeouts.read is used. (default: 2m0s)
   --authrpc.timeouts.read value                                                       Maximum duration for reading the entire request, including the body. (default: 30s)
   --authrpc.timeouts.write value                                                      Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read. (default: 30m0s)
   --authrpc.timeouts.idle value                                                       Maximum amount of time to wait for the next request when keep-alive connections are enabled. If authrpc.timeouts.idle is zero, the value of authrpc.timeouts.read is used. (default: 2m0s)
   --rpc.evmtimeout value                                                              Maximum amount of time to wait for the answer from EVM call. (default: 5m0s)
   --rpc.overlay.getlogstimeout value                                                  Maximum amount of time to wait for the answer from the overlay_getLogs call. (default: 5m0s)
   --rpc.overlay.replayblocktimeout value                                              Maximum amount of time to wait for the answer to replay a single block when called from an overlay_getLogs call. (default: 10s)
   --rpc.subscription.filters.maxlogs value                                            Maximum number of logs to store per subscription. (default: 0)
   --rpc.subscription.filters.maxheaders value                                         Maximum number of block headers to store per subscription. (default: 0)
   --rpc.subscription.filters.maxtxs value                                             Maximum number of transactions to store per subscription. (default: 0)
   --rpc.subscription.filters.maxaddresses value                                       Maximum number of addresses per subscription to filter logs by. (default: 0)
   --rpc.subscription.filters.maxtopics value                                          Maximum number of topics per subscription to filter logs by. (default: 0)
   --snap.keepblocks                                                                   Keep ancient blocks in db (useful for debug) (default: false)
   --snap.stop                                                                         Workaround to stop producing new snapshots, if you meet some snapshots-related critical bug. It will stop move historical data from DB to new immutable snapshots. DB will grow and may slightly slow-down - and removing this flag in future will not fix this effect (db size will not greatly reduce). (default: false)
   --snap.state.stop                                                                   Workaround to stop producing new state files, if you meet some state-related critical bug. It will stop aggregate DB history in a state files. DB will grow and may slightly slow-down - and removing this flag in future will not fix this effect (db size will not greatly reduce). (default: false)
   --snap.skip-state-snapshot-download                                                 Skip state download and start from genesis block (default: false)
   --snap.download.to.block value, --shadow.fork.block value                           Download snapshots up to the given block number (exclusive). Disabled by default. Useful for testing and shadow forks. (default: 0)
   --db.pagesize value                                                                 DB is splitted to 'pages' of fixed size. Can't change DB creation. Must be power of 2 and '256b <= pagesize <= 64kb'. Default: equal to OperationSystem's pageSize. Bigger pageSize causing: 1. More writes to disk during commit 2. Smaller b-tree high 3. Less fragmentation 4. Less overhead on 'free-pages list' maintainance (a bit faster Put/Commit) 5. If expecting DB-size > 8Tb then set pageSize >= 8Kb (default: "16KB")
   --db.size.limit value                                                               Runtime limit of chaindata db size (can change at any time) (default: "1TB")
   --db.writemap                                                                       Enable WRITE_MAP feature for fast database writes and fast commit times (default: true)
   --torrent.port value                                                                Port to listen and serve BitTorrent protocol (default: 42069)
   --torrent.maxpeers value                                                            Unused parameter (reserved for future use) (default: 100)
   --torrent.conns.perfile value                                                       Number of connections per file (default: 10)
   --torrent.trackers.disable                                                          Disable conventional BitTorrent trackers (default: false)
   --torrent.upload.rate value                                                         Bytes per second, example: 32mb. Set Inf for no limit. (default: "16mb")
   --torrent.download.rate value                                                       Bytes per second, example: 32mb. Set Inf for no limit. Shared with webseeds unless that rate is set separately. (default: "512mb")
   --torrent.webseed.download.rate value                                               Bytes per second for webseeds, example: 32mb. Set Inf for no limit. If not set, rate limit is shared with torrent.download.rate
   --torrent.verbosity value                                                           0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set --verbosity to equal or higher level) (default: 1)
   --port value                                                                        Network listening port (default: 30303)
   --p2p.protocol value [ --p2p.protocol value ]                                       Version of eth p2p protocol (default: 68, 69)
   --p2p.allowed-ports value [ --p2p.allowed-ports value ]                             Allowed ports to pick for different eth p2p protocol versions as follows <porta>,<portb>,..,<porti> (default: 30303, 30304, 30305, 30306, 30307)
   --nat value                                                                         NAT port mapping mechanism (any|none|upnp|pmp|stun|extip:<IP>)
                                                                                            "" or "none"         Default - do not nat
                                                                                            "extip:77.12.33.4"   Will assume the local machine is reachable on the given IP
                                                                                            "any"                Uses the first auto-detected mechanism
                                                                                            "upnp"               Uses the Universal Plug and Play protocol
                                                                                            "pmp"                Uses NAT-PMP with an auto-detected gateway address
                                                                                            "pmp:192.168.0.1"    Uses NAT-PMP with the given gateway address
                                                                                            "stun"               Uses STUN to detect an external IP using a default server
                                                                                            "stun:<server>"      Uses STUN to detect an external IP using the given server (host:port)
   --nodiscover                                                                        Disables the peer discovery mechanism (manual peer addition) (default: false)
   --v5disc                                                                            Enables the experimental RLPx V5 (Topic Discovery) mechanism (default: false)
   --netrestrict value                                                                 Restricts network communication to the given IP networks (CIDR masks)
   --nodekey value                                                                     P2P node key file
   --nodekeyhex value                                                                  P2P node key as hex (for testing)
   --discovery.dns value                                                               Sets DNS discovery entry points (use "" to disable DNS)
   --bootnodes value                                                                   Comma separated enode URLs for P2P discovery bootstrap
   --staticpeers value                                                                 Comma separated enode URLs to connect to
   --trustedpeers value                                                                Comma separated enode URLs which are always allowed to connect, even above the peer limit
   --maxpeers value                                                                    Maximum number of network peers (network disabled if set to 0) (default: 32)
   --chain value                                                                       name of the network to join (default: "mainnet")
   --dev.period value                                                                  Block period to use in developer mode (0 = mine only if transaction pending) (default: 0)
   --vmdebug                                                                           Record information useful for VM and contract debugging (default: false)
   --networkid value                                                                   Explicitly set network id (integer)(For testnets: use --chain <testnet_name> instead) (default: 1)
   --persist.receipts, --experiment.persist.receipts.v2                                Download historical Receipts. If disabled: using state-history to re-exec transactions and generate Receipts - all RPC: eth_getLogs, eth_getBlockReceipts will work (just higher latency) (default: false)
   --fakepow                                                                           Disables proof-of-work verification (default: false)
   --gpo.blocks value                                                                  Number of recent blocks to check for gas prices (default: 20)
   --gpo.percentile value                                                              Suggested gas price is the given percentile of a set of recent transaction gas prices (default: 60)
   --allow-insecure-unlock                                                             Allow insecure account unlocking when account-related RPCs are exposed by http (default: false)
   --identity value                                                                    Custom node name
   --clique.checkpoint value                                                           Number of blocks after which to save the vote snapshot to the database (default: 10)
   --clique.snapshots value                                                            Number of recent vote snapshots to keep in memory (default: 1024)
   --clique.signatures value                                                           Number of recent block signatures to keep in memory (default: 16384)
   --clique.datadir value                                                              Path to clique db folder
   --mine                                                                              Enable mining (default: false)
   --proposer.disable                                                                  Disables PoS proposer (default: false)
   --miner.notify value                                                                Comma separated HTTP URL list to notify of new work packages
   --miner.gaslimit value                                                              Target gas limit for mined blocks (default: 0)
   --miner.etherbase value                                                             Public address for block mining rewards (default: "0")
   --miner.gasprice value                                                              Minimum gas price for mining a transaction (default: 0)
   --miner.extradata value                                                             Block extra data set by the miner (default = client version)
   --miner.noverify                                                                    Disable remote sealing verification (default: false)
   --miner.sigfile value                                                               Private key to sign blocks with
   --miner.recommit value                                                              Time interval to recreate the block being mined (default: 3s)
   --sentry.api.addr value                                                             Comma separated sentry addresses '<host>:<port>,<host>:<port>'
   --sentry.log-peer-info                                                              Log detailed peer info when a peer connects or disconnects. Enable to integrate with observer. (default: false)
   --downloader.api.addr value                                                         downloader address '<host>:<port>'
   --downloader.disable.ipv4                                                           Turns off ipv4 for the downloader (default: false)
   --downloader.disable.ipv6                                                           Turns off ipv6 for the downloader (default: false)
   --no-downloader                                                                     Disables downloader component (default: false)
   --downloader.verify                                                                 Verify snapshots on startup. It will not report problems found, but re-download broken pieces. (default: false)
   --healthcheck                                                                       Enable grpc health check (default: false)
   --bor.heimdall value                                                                URL of Heimdall service (default: "http://localhost:1317")
   --webseed value                                                                     Comma-separated URL's, holding metadata about network-support infrastructure (like S3 buckets with snapshots, bootnodes, etc...)
   --bor.withoutheimdall                                                               Run without Heimdall service (for testing purposes) (default: false)
   --bor.period                                                                        Override the bor block period (for testing purposes) (default: false)
   --bor.minblocksize                                                                  Ignore the bor block period and wait for 'blocksize' transactions (for testing purposes) (default: false)
   --aa                                                                                Enable AA transactions (default: false)
   --ethstats value                                                                    Reporting URL of a ethstats service (nodename:secret@host:port)
   --override.osaka value                                                              Manually specify the Osaka fork time, overriding the bundled setting (default: 0)
   --keep.stored.chain.config                                                          Avoid overriding chain config already stored in the DB (default: false)
   --caplin.discovery.addr value                                                       Address for Caplin DISCV5 protocol (default: "0.0.0.0")
   --caplin.discovery.port value                                                       Port for Caplin DISCV5 protocol (default: 4000)
   --caplin.discovery.tcpport value                                                    TCP Port for Caplin DISCV5 protocol (default: 4001)
   --caplin.checkpoint-sync-url value [ --caplin.checkpoint-sync-url value ]           checkpoint sync endpoint
   --caplin.subscribe-all-topics                                                       Subscribe to all gossip topics (default: false)
   --caplin.max-peer-count value                                                       Max number of peers to connect (default: 128)
   --caplin.enable-upnp                                                                Enable NAT porting for Caplin (default: false)
   --caplin.max-inbound-traffic-per-peer value                                         Max inbound traffic per second per peer (default: "1MB")
   --caplin.max-outbound-traffic-per-peer value                                        Max outbound traffic per second per peer (default: "1MB")
   --caplin.adaptable-maximum-traffic-requirements                                     Make the node adaptable to the maximum traffic requirement based on how many validators are being ran (default: true)
   --sentinel.addr value                                                               Address for sentinel (default: "localhost")
   --sentinel.port value                                                               Port for sentinel (default: 7777)
   --sentinel.bootnodes value [ --sentinel.bootnodes value ]                           Comma separated enode URLs for P2P discovery bootstrap
   --sentinel.staticpeers value [ --sentinel.staticpeers value ]                       connect to comma-separated Consensus static peers
   --ots.search.max.pagesize value                                                     Max allowed page size for search methods (default: 25)
   --silkworm.exec                                                                     Enable Silkworm block execution (default: false)
   --silkworm.rpc                                                                      Enable embedded Silkworm RPC service (default: false)
   --silkworm.sentry                                                                   Enable embedded Silkworm Sentry service (default: false)
   --silkworm.verbosity value                                                          Set the log level for Silkworm console logs (default: "info")
   --silkworm.contexts value                                                           Number of I/O contexts used in embedded Silkworm RPC and Sentry services (zero means use default in Silkworm) (default: 0)
   --silkworm.rpc.log                                                                  Enable interface log for embedded Silkworm RPC service (default: false)
   --silkworm.rpc.log.maxsize value                                                    Max interface log file size in MB for embedded Silkworm RPC service (default: 1)
   --silkworm.rpc.log.maxfiles value                                                   Max interface log files for embedded Silkworm RPC service (default: 100)
   --silkworm.rpc.log.response                                                         Dump responses in interface logs for embedded Silkworm RPC service (default: false)
   --silkworm.rpc.workers value                                                        Number of worker threads used in embedded Silkworm RPC service (zero means use default in Silkworm) (default: 0)
   --silkworm.rpc.compatibility                                                        Preserve JSON-RPC compatibility using embedded Silkworm RPC service (default: true)
   --beacon.api value [ --beacon.api value ]                                           Enable beacon API (available endpoints: beacon, builder, config, debug, events, node, validator, lighthouse)
   --beacon.api.addr value                                                             sets the host to listen for beacon api requests (default: "localhost")
   --beacon.api.cors.allow-methods value [ --beacon.api.cors.allow-methods value ]     set the cors' allow methods (default: "GET", "POST", "PUT", "DELETE", "OPTIONS")
   --beacon.api.cors.allow-origins value [ --beacon.api.cors.allow-origins value ]     set the cors' allow origins
   --beacon.api.cors.allow-credentials                                                 set the cors' allow credentials (default: false)
   --beacon.api.port value                                                             sets the port to listen for beacon api requests (default: 5555)
   --beacon.api.read.timeout value                                                     Sets the seconds for a read time out in the beacon api (default: 5)
   --beacon.api.write.timeout value                                                    Sets the seconds for a write time out in the beacon api (default: 31536000)
   --beacon.api.protocol value                                                         Protocol for beacon API (default: "tcp")
   --beacon.api.ide.timeout value                                                      Sets the seconds for a write time out in the beacon api (default: 25)
   --caplin.blocks-archive                                                             sets whether backfilling is enabled for caplin (default: false)
   --caplin.blobs-archive                                                              sets whether backfilling is enabled for caplin (default: false)
   --caplin.states-archive                                                             enables archival node for historical states in caplin (it will enable block archival as well) (default: false)
   --caplin.blobs-immediate-backfill                                                   sets whether caplin should immediatelly backfill blobs (4096 epochs) (default: false)
   --caplin.blobs-no-pruning                                                           disable blob pruning in caplin (default: false)
   --caplin.checkpoint-sync.disable                                                    disable checkpoint sync in caplin (default: false)
   --caplin.snapgen                                                                    enables snapshot generation in caplin (default: false)
   --caplin.mev-relay-url value                                                        MEV relay endpoint. Caplin runs in builder mode if this is set
   --caplin.validator-monitor                                                          Enable caplin validator monitoring metrics (default: false)
   --caplin.custom-config value                                                        set the custom config for caplin
   --caplin.custom-genesis value                                                       set the custom genesis for caplin
   --caplin.use-engine-api                                                             Use engine API for internal Caplin. useful for testing and if CL network is degraded (default: false)
   --trusted-setup-file value                                                          Absolute path to trusted_setup.json file
   --rpc.slow value                                                                    Print in logs RPC requests slower than given threshold: 100ms, 1s, 1m. Exluded methods: eth_getBlock,eth_getBlockByNumber,eth_getBlockByHash,eth_blockNumber,erigon_blockNumber,erigon_getHeaderByNumber,erigon_getHeaderByHash,erigon_getBlockByTimestamp,eth_call (default: 0s)
   --txpool.gossip.disable                                                             Disabling p2p gossip of txs. Any txs received by p2p - will be dropped. Some networks like 'Optimism execution engine'/'Optimistic Rollup' - using it to protect against MEV attacks (default: false)
   --sync.loop.block.limit value                                                       Sets the maximum number of blocks to process per loop iteration (default: 5000)
   --sync.loop.break.after value                                                       Sets the last stage of the sync loop to run
   --sync.parallel-state-flushing                                                      Enables parallel state flushing (default: true)
   --chaos.monkey                                                                      Enable 'chaos monkey' to generate spontaneous network/consensus/etc failures. Use ONLY for testing (default: false)
   --shutter                                                                           Enable the Shutter encrypted transactions mempool (defaults to false) (default: false)
   --shutter.p2p.bootstrap.nodes value [ --shutter.p2p.bootstrap.nodes value ]         Use to override the default p2p bootstrap nodes (defaults to using the values in the embedded config)
   --shutter.p2p.listen.port value                                                     Use to override the default p2p listen port (defaults to 23102) (default: 0)
   --polygon.pos.ssf                                                                   Enabling Polygon PoS Single Slot Finality (default: false)
   --polygon.pos.ssf.block value                                                       Enabling Polygon PoS Single Slot Finality since block (default: 0)
   --polygon.wit-protocol                                                              Enable WIT protocol for stateless witness data exchange (auto-enabled for Bor chains) (default: false)
   --gdbme                                                                             restart erigon under gdb for debug purposes (default: false)
   --experimental.concurrent-commitment                                                EXPERIMENTAL: enables concurrent trie for commitment (default: false)
   --el.block.downloader.v2                                                            Enables the EL engine v2 block downloader (default: true)
   --pprof                                                                             Enable the pprof HTTP server (default: false)
   --pprof.addr value                                                                  pprof HTTP server listening interface (default: "127.0.0.1")
   --pprof.port value                                                                  pprof HTTP server listening port (default: 6060)
   --pprof.cpuprofile value                                                            Write CPU profile to the given file
   --trace value                                                                       Write execution trace to the given file
   --vmtrace value                                                                     Set the provider tracer
   --vmtrace.jsonconfig value                                                          Set the config of the tracer
   --metrics                                                                           Enable metrics collection and reporting (default: false)
   --metrics.addr value                                                                Enable stand-alone metrics HTTP server listening interface (default: "127.0.0.1")
   --metrics.port value                                                                Metrics HTTP server listening port (default: 6061)
   --diagnostics.disabled                                                              Disable diagnostics (default: true)
   --diagnostics.endpoint.addr value                                                   Diagnostics HTTP server listening interface (default: "127.0.0.1")
   --diagnostics.endpoint.port value                                                   Diagnostics HTTP server listening port (default: 6062)
   --diagnostics.speedtest                                                             Enable speed test (default: false)
   --log.json                                                                          Format console logs with JSON (default: false)
   --log.console.json                                                                  Format console logs with JSON (default: false)
   --log.dir.json                                                                      Format file logs with JSON (default: false)
   --verbosity value                                                                   Set the log level for console logs (default: "info")
   --log.console.verbosity value                                                       Set the log level for console logs (default: "info")
   --log.dir.disable                                                                   disable disk logging (default: false)
   --log.dir.path value                                                                Path to store user and error logs to disk
   --log.dir.prefix value                                                              The file name prefix for logs stored to disk
   --log.dir.verbosity value                                                           Set the log verbosity for logs stored to disk (default: "info")
   --log.delays                                                                        Enable block delay logging (default: false)
   --config value                                                                      Sets erigon flags from YAML/TOML file
   --help, -h                                                                          show help
   --version, -v                                                                       print the version

```
{% endcode %}
