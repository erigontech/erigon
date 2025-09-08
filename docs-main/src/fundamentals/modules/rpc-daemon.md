
# RPC Daemon
*Remote Procedure Call*

The RPC daemon is a crucial component of Erigon, enabling JSON remote procedure calls and providing access to various APIs. It is designed to operate effectively both as an internal or as an external component. For detailed instructions on running it remotely, refer to the documentation [here](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md#running-remotely).

## Erigon RPC Method Guidelines

This document provides guidelines for understanding and using the various RPC methods available in Erigon.

1. **Compatibility with `eth` namespace**  
   - Erigon aims to be compatible with all standard Ethereum JSON-RPC API methods, as described [here](https://ethereum.org/en/developers/docs/apis/json-rpc/). See also [here](https://github.com/erigontech/erigon/blob/main/docs/readthedocs/source/rpc/index.rst) for examples.

1. **Compatibility with standard Geth methods**
   - All methods featured by Geth including [WebSocket Server](https://geth.ethereum.org/docs/interacting-with-geth/rpc#websockets-server), [IPC Server](https://geth.ethereum.org/docs/interacting-with-geth/rpc#ipc-server), TLS, [GraphQL](https://geth.ethereum.org/docs/interacting-with-geth/rpc/graphql), etc..., are supported by Erigon.

1. **Otterscan Methods (`ots_`)**  
   - In addition to the standard Geth methods, Erigon includes RPC methods prefixed with `ots_` for **Otterscan**. These are specific to the Otterscan functionality integrated with Erigon. See more details [here](https://docs.otterscan.io/api-docs/ots-api). 

1. **Erigon Extensions (`erigon_`)**  
   - Erigon introduces some small extensions to the Geth API, denoted by the `erigon_` prefix aimed to enhance the functionality, see more details [here](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md#rpc-implementation-status) about implementation status.

1. **gRPC API**  
   - Erigon also exposes a **gRPC** API for lower-level data access. This is primarily used by Erigon’s components when they are deployed separately as independent processes (either on the same or different servers). 
   - This gRPC API is also accessible to users. For more information, visit the [Erigon Interfaces GitHub repository](https://github.com/erigontech/interfaces).

1. **Trace Module (`trace_`)**  
   - Erigon includes **[the `trace_` module](JSONRPC-trace-module.md)**, which originates from OpenEthereum. This module provides additional functionality related to tracing transactions and state changes, which is valuable for advanced debugging and analysis.

## More info

For a comprehensive understanding of the RPC daemon's functionality, configuration, and usage, please refer to <https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md> (also contained in your locally compiled Erigon folder at `/cmd/rpcdaemon`) which covers the following key topics:

1. **Introduction**: An overview of the RPC daemon, its benefits, and how it integrates with Erigon.
2. **Getting Started**: Step-by-step guides for running the RPC daemon locally and remotely, including configuration options and command-line flags.
3. **Healthcheck**: Information on performing health checks using POST requests or GET requests with custom headers.
4. **Testing and debugging**: Examples of testing the RPC daemon using `curl` commands and Postman, debugging.
5. **FAQ**: Frequently asked questions and answers covering topics such as prune options, RPC implementation status, and securing communication between the RPC daemon and Erigon instance.
6. **For Developers**: Resources for developers, including code generation and information on working with the RPC daemon.
7. **Relations between prune options and RPC methods**: Explains how prune options affect RPC methods.
8. **RPC Implementation Status**: Provides a table showing the current implementation status of Erigon's RPC daemon.
9. **Securing the communication between RPC daemon and Erigon instance via TLS and authentication**: Outlines the steps to secure communication between the RPC daemon and Erigon instance.
10. **Ethstats**: Describes how to run ethstats with the RPC daemon.
11. **Allowing only specific methods (Allowlist)**: Explains how to restrict access to specific RPC methods.

## Command Line Options

To display available options for RPCdaemon digit:

```bash
./build/bin/rpcdaemon --help
```

The `--help` flag listing is reproduced below for your convenience.

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