---
description: Remote Procedure Call Daemon
---

# RPC Daemon

The RPC daemon is a vital part of Erigon, as it implements the **RPC Service** by processing JSON remote procedure calls (RPCs). This is what allows clients to access Erigon's APIs. Its flexible architecture means it can be deployed in-process (running inside Erigon) or out-of-process (as a standalone service).

### RPC Deployment Modes

The Erigon RPC service offers three distinct deployment modes when it comes to the logical functionality, depending on whether it runs as part of the main `erigon` process or as a standalone `rpcdaemon` process.

<table data-header-hidden><thead><tr><th width="137"></th><th></th><th></th><th></th></tr></thead><tbody><tr><td><strong>Mode</strong></td><td><strong>Configuration</strong></td><td><strong>CLI Command to Run</strong></td><td><strong>Key Characteristics</strong></td></tr><tr><td>Embedded 🏡</td><td>RPC server is hosted within the <code>erigon</code> process.</td><td><code>./build/bin/erigon</code></td><td>The simplest, all-in-one solution. No separate RPC command is needed.</td></tr><tr><td>Local ⚙️</td><td><code>rpcdaemon</code> runs as a standalone process on the same machine as <code>erigon</code>. It directly accesses local data storage.</td><td><p>First (Erigon):</p><p><code>./build/bin/erigon --private.api.addr="&#x3C;host_IP>:9090"</code><br></p><p>Second (<code>rpcdaemon</code>):</p><p><code>./build/bin/rpcdaemon --datadir="&#x3C;erigon_data_path>"</code></p></td><td>Improves isolation, increases tuning options, and maintains high-performance data access. Requires two processes running on the same machine.</td></tr><tr><td>Remote 🌐</td><td><code>rpcdaemon</code> runs as a standalone process on a separate machine and accesses data storage remotely via the Erigon gRPC interface.</td><td><p>First (Erigon):</p><p><code>./build/bin/erigon --private.api.addr="&#x3C;host_IP>:9090" --grpc</code></p><p></p><p>Second (<code>rpcdaemon</code>):</p><p><code>./build/bin/rpcdaemon --private.api.addr="&#x3C;erigon_IP>:9090"</code></p></td><td>Highly scalable, leverages the same data storage for multiple service endpoints. Uses the <code>--private.api.addr</code> flag to point the daemon to the remote Erigon instance.</td></tr></tbody></table>

For a comprehensive understanding and the latest instructions, the official documentation is essential:

* **Detailed Functionality and Configuration**: The primary documentation for the `rpcdaemon` is located at [https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md).
* **Local Access**: This documentation is also contained in your locally compiled Erigon folder at `/cmd/rpcdaemon`.
* **Remote Running Instructions**: For detailed instructions on running RPC in Remote mode, refer to the documentation [here](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md#running-remotely).

{% hint style="success" %}
To interact with the **RPC Service** visit the dedicated page [Interacting with Erigon](../interacting-with-erigon/).
{% endhint %}

## Command Line Options

When running RPC daemon in Local or Remote deployment mode, use this command to display available options:

```bash
./build/bin/rpcdaemon --help
```

The `--help` flag listing is reproduced below for your convenience.

{% code overflow="wrap" %}
```
rpcdaemon is JSON RPC server that connects to Erigon node for remote DB access

Usage:
  rpcdaemon [flags]

Flags:
      --datadir string                              path to Erigon working directory
      --db.read.concurrency int                     Does limit amount of parallel db reads. Default: equal to GOMAXPROCS (or number of CPU) (default 1408)
      --diagnostics.disabled                        Disable diagnostics
      --diagnostics.endpoint.addr string            Diagnostics HTTP server listening interface (default "127.0.0.1")
      --diagnostics.endpoint.port uint              Diagnostics HTTP server listening port (default 6062)
      --diagnostics.speedtest                       Enable speed test
      --graphql                                     enables graphql endpoint (disabled by default)
      --grpc                                        Enable GRPC server
      --grpc.addr string                            GRPC server listening interface (default "localhost")
      --grpc.healthcheck                            Enable GRPC health check
      --grpc.port int                               GRPC server listening port (default 8547)
  -h, --help                                        help for rpcdaemon
      --http.addr string                            HTTP server listening interface (default "localhost")
      --http.api strings                            API's offered over the RPC interface: eth,erigon,web3,net,debug,trace,txpool,db. Supported methods: https://github.com/erigontech/erigon/tree/main/cmd/rpcdaemon (default [eth,erigon])
      --http.compression                            Disable http compression (default true)
      --http.corsdomain strings                     Comma separated list of domains from which to accept cross origin requests (browser enforced)
      --http.dbg.single                             Allow pass HTTP header 'dbg: true' to printt more detailed logs - how this request was executed
      --http.enabled                                enable http server (default true)
      --http.port int                               HTTP server listening port (default 8545)
      --http.timeouts.idle duration                 Maximum amount of time to wait for the next request when keep-alives are enabled. If http.timeouts.idle is zero, the value of http.timeouts.read is used (default 2m0s)
      --http.timeouts.read duration                 Maximum duration for reading the entire request, including the body. (default 30s)
      --http.timeouts.write duration                Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read (default 30m0s)
      --http.trace                                  Trace HTTP requests with INFO level
      --http.url string                             HTTP server listening url. will OVERRIDE http.addr and http.port. will NOT respect http paths. prefix supported are tcp, unix
      --http.vhosts strings                         Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard. (default [localhost])
      --https.addr string                           rpc HTTPS server listening interface (default "localhost")
      --https.cert string                           certificate for rpc HTTPS server
      --https.enabled                               enable http server
      --https.key string                            key file for rpc HTTPS server
      --https.port int                              rpc HTTPS server listening port. default to http+363 if not set
      --https.url string                            rpc HTTPS server listening url. will OVERRIDE https.addr and https.port. will NOT respect paths. prefix supported are tcp, unix
      --log.console.json                            Format console logs with JSON
      --log.console.verbosity string                Set the log level for console logs (default "info")
      --log.delays                                  Enable block delay logging
      --log.dir.disable                             disable disk logging
      --log.dir.json                                Format file logs with JSON
      --log.dir.path string                         Path to store user and error logs to disk
      --log.dir.prefix string                       The file name prefix for logs stored to disk
      --log.dir.verbosity string                    Set the log verbosity for logs stored to disk (default "info")
      --log.json                                    Format console logs with JSON
      --metrics                                     Enable metrics collection and reporting
      --metrics.addr string                         Enable stand-alone metrics HTTP server listening interface (default "127.0.0.1")
      --metrics.port int                            Metrics HTTP server listening port (default 6061)
      --ots.search.max.pagesize uint                Max allowed page size for search methods (default 25)
      --polygon.sync                                Enable if Erigon has been synced using the new polygon sync component
      --pprof                                       Enable the pprof HTTP server
      --pprof.addr string                           pprof HTTP server listening interface (default "127.0.0.1")
      --pprof.cpuprofile string                     Write CPU profile to the given file
      --pprof.port int                              pprof HTTP server listening port (default 6060)
      --private.api.addr string                     Erigon's components (txpool, rpcdaemon, sentry, downloader, ...) can be deployed as independent Processes on same/another server. Then components will connect to erigon by this internal grpc API. Example: 127.0.0.1:9090 (default "127.0.0.1:9090")
      --rpc.accessList string                       Specify granular (method-by-method) API allowlist
      --rpc.allow-unprotected-txs                   Allow for unprotected (non-EIP155 signed) transactions to be submitted via RPC
      --rpc.batch.concurrency uint                  Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request (default 2)
      --rpc.batch.limit int                         Maximum number of requests in a batch (default 100)
      --rpc.evmtimeout duration                     Maximum amount of time to wait for the answer from EVM call. (default 5m0s)
      --rpc.gascap uint                             Sets a cap on gas that can be used in eth_call/estimateGas (default 50000000)
      --rpc.maxgetproofrewindblockcount.limit int   Max GetProof rewind block count (default 100000)
      --rpc.overlay.getlogstimeout duration         Maximum amount of time to wait for the answer from the overlay_getLogs call. (default 5m0s)
      --rpc.overlay.replayblocktimeout duration     Maximum amount of time to wait for the answer to replay a single block when called from an overlay_getLogs call. (default 10s)
      --rpc.returndata.limit int                    Maximum number of bytes returned from eth_call or similar invocations (default 100000)
      --rpc.slow duration                           Print in logs RPC requests slower than given threshold: 100ms, 1s, 1m. Excluded methods: eth_getBlock,eth_getBlockByNumber,eth_getBlockByHash,eth_blockNumber,erigon_blockNumber,erigon_getHeaderByNumber,erigon_getHeaderByHash,erigon_getBlockByTimestamp,eth_call
      --rpc.streaming.disable                       Erigon has enabled json streaming for some heavy endpoints (like trace_*). It's a trade-off: greatly reduce amount of RAM (in some cases from 30GB to 30mb), but it produce invalid json format if error happened in the middle of streaming (because json is not streaming-friendly format)
      --rpc.subscription.filters.maxaddresses int   Maximum number of addresses per subscription to filter logs by.
      --rpc.subscription.filters.maxheaders int     Maximum number of block headers to store per subscription.
      --rpc.subscription.filters.maxlogs int        Maximum number of logs to store per subscription.
      --rpc.subscription.filters.maxtopics int      Maximum number of topics per subscription to filter logs by.
      --rpc.subscription.filters.maxtxs int         Maximum number of transactions to store per subscription.
      --rpc.txfeecap float                          Sets a cap on transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap) (default 1)
      --socket.enabled                              Enable IPC server
      --socket.url string                           IPC server listening url. prefix supported are tcp, unix (default "unix:///var/run/erigon.sock")
      --state.cache string                          Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB RAM (default "0MB")
      --tls.cacert string                           CA certificate for client side TLS handshake for GRPC
      --tls.cert string                             certificate for client side TLS handshake for GRPC
      --tls.key string                              key file for client side TLS handshake for GRPC
      --trace string                                Write execution trace to the given file
      --trace.compat                                Bug for bug compatibility with OE for trace_ routines
      --trace.maxtraces uint                        Sets a limit on traces that can be returned in trace_filter (default 200)
      --txpool.api.addr string                      txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)
      --verbosity string                            Set the log level for console logs (default "info")
      --ws                                          Enable Websockets - Same port as HTTP[S]
      --ws.api.subscribelogs.channelsize int        Size of the channel used for websocket logs subscriptions (default 8192)
      --ws.compression                              Enable Websocket compression (RFC 7692)
```
{% endcode %}
