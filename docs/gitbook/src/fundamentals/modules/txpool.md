---
description: Erigon's Transaction Pool
---

# TxPool

The transaction pool, also known as the mempool, is a dynamic storage area where pending transactions are held before being confirmed and added to the blockchain. Each node on the Ethereum network maintains its own local transaction pool, which is combined with others to form the global pool.

In Erigon, the txpool is a dedicated API namespace that stores pending and queued transactions in local memory. Its primary function is to manage transactions waiting to be processed by miners.

The TxPool component is typically run as an internal Erigon component, but it can also be operated as a separate process, providing flexibility in transaction management.

## Txpool as an internal Erigon component

Txpool is run as an internal Erigon component by default.

## Txpool as a separate process (experimental)

Running an external txpool can provide a more secure, scalable, and flexible transaction management solution, which can be particularly beneficial in high-performance or high-availability Ethereum node deployments.

1. Before TxPool can be using as a separate process the executable must be built:

```bash
cd erigon
make txpool
```

2. Together with external TxPool also [Sentry](sentry.md) and [RPCDaemon](rpc-daemon.md) must be compiled and run separately.

```bash
make sentry
```

```bash
make rpcdaemon
```

3. Now Erigon and other services can be started as separate processes

```sh
./build/bin/erigon --txpool.disable --private.api.addr=localhost:9090 --datadir=<your datadir> --http=false
```

If Erigon is on a different device, add the flags `--pprof --pprof.addr 0.0.0.0` or TxPool will listen on localhost by default.

```sh
./build/bin/sentry --sentry.api.addr=localhost:9091 --datadir=<your datadir>
```

```sh
./build/bin/txpool --private.api.addr=localhost:9090 --sentry.api.addr=localhost:9091 --txpool.api.addr=localhost:9094 --datadir=<your datadir>
```

```sh
./build/bin/rpcdaemon --private.api.addr=localhost:9090 --datadir=<your datadir> --txpool.api.addr=localhost:9094
```

## Flags explanation

* `--txpool.disable`: This flag disables the internal transaction pool (txpool) and block producer (default: `false`). When running the txpool as a separate process, this flag is used to prevent the internal txpool from interfering with the external one.
* `--private.api.addr=localhost:9090`: This flag sets the address and port for the private API. The private API is used for internal communication between Erigon components (default: `127.0.0.1:9090`).
* `--datadir=<your datadir>`: This flag specifies the data directory for Erigon. This is where Erigon stores its databases and other data.
* `--http=false`: This flag disables the HTTP API server in Erigon (default: `true)`. When running the txpool as a separate process, this flag is used to prevent the internal HTTP server from interfering with the external txpool.
* `--sentry.api.addr=localhost:9091`: This flag sets the address and port for the sentry API. The sentry API is used for communication between the txpool and the sentry.
* `--txpool.api.addr=localhost:9094`: This flag sets the address and port for the txpool API (default: use value of `--private.api.addr`). The txpool API is used for communication between the txpool and other Erigon components.
* `--pprof`: Enable the pprof HTTP server (default: `false`)
* `--pprof.addr 0.0.0.0`: This flag sets the address for the pprof HTTP server (default: `127.0.0.1`). The pprof server is used for profiling and debugging Erigon. By setting this flag to `0.0.0.0`, the pprof server is made accessible from outside the local machine.

## Command Line Options

To display available options for Txpool digit:

```bash
./build/bin/txpool --help
```

The `--help` flag listing is reproduced below for your convenience.

```
./build/bin/txpool --help
Launch external Transaction Pool instance - same as built-into Erigon, but as independent Process

Usage:
  txpool [flags]

Flags:
      --datadir string                     Data directory for the databases (default "/home/user/.local/share/erigon")
      --db.writemap                        Enable WRITE_MAP feature for fast database writes and fast commit times (default true)
      --diagnostics.disabled               Disable diagnostics
      --diagnostics.endpoint.addr string   Diagnostics HTTP server listening interface (default "127.0.0.1")
      --diagnostics.endpoint.port uint     Diagnostics HTTP server listening port (default 6062)
      --diagnostics.speedtest              Enable speed test
  -h, --help                               help for txpool
      --log.console.json                   Format console logs with JSON
      --log.console.verbosity string       Set the log level for console logs (default "info")
      --log.delays                         Enable block delay logging
      --log.dir.disable                    disable disk logging
      --log.dir.json                       Format file logs with JSON
      --log.dir.path string                Path to store user and error logs to disk
      --log.dir.prefix string              The file name prefix for logs stored to disk
      --log.dir.verbosity string           Set the log verbosity for logs stored to disk (default "info")
      --log.json                           Format console logs with JSON
      --metrics                            Enable metrics collection and reporting
      --metrics.addr string                Enable stand-alone metrics HTTP server listening interface (default "127.0.0.1")
      --metrics.port int                   Metrics HTTP server listening port (default 6061)
      --pprof                              Enable the pprof HTTP server
      --pprof.addr string                  pprof HTTP server listening interface (default "127.0.0.1")
      --pprof.cpuprofile string            Write CPU profile to the given file
      --pprof.port int                     pprof HTTP server listening port (default 6060)
      --private.api.addr string            execution service <host>:<port> (default "localhost:9090")
      --sentry.api.addr strings            comma separated sentry addresses '<host>:<port>,<host>:<port>' (default [localhost:9091])
      --tls.cacert string                  CA certificate for client side TLS handshake
      --tls.cert string                    certificate for client side TLS handshake
      --tls.key string                     key file for client side TLS handshake
      --trace string                       Write execution trace to the given file
      --txpool.accountslots uint           Minimum number of executable transaction slots guaranteed per account (default 16)
      --txpool.api.addr string             txpool service <host>:<port> (default "localhost:9094")
      --txpool.blobpricebump uint          Price bump percentage to replace an existing blob (type-3) transaction (default 100)
      --txpool.blobslots uint              Max allowed total number of blobs (within type-3 txs) per account (default 48)
      --txpool.commit.every duration       How often transactions should be committed to the storage (default 15s)
      --txpool.globalbasefeeslots int      Maximum number of non-executable transactions where only not enough baseFee (default 30000)
      --txpool.globalqueue int             Maximum number of non-executable transaction slots for all accounts (default 30000)
      --txpool.globalslots int             Maximum number of executable transaction slots for all accounts (default 10000)
      --txpool.gossip.disable              Disabling p2p gossip of txs. Any txs received by p2p - will be dropped. Some networks like 'Optimism execution engine'/'Optimistic Rollup' - using it to protect against MEV attacks
      --txpool.pricebump uint              Price bump percentage to replace an already existing transaction (default 10)
      --txpool.pricelimit uint             Minimum gas price (fee cap) limit to enforce for acceptance into the pool (default 1)
      --txpool.totalblobpoollimit uint     Total limit of number of all blobs in txs within the txpool (default 480)
      --txpool.trace.senders strings       Comma separated list of addresses, whose transactions will traced in transaction pool with debug printing
      --verbosity string                   Set the log level for console logs (default "info")
```
