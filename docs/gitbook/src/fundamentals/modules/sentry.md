---
description: P2P network management
---

# Sentry

Sentry connects Erigon to the Ethereum P2P network, enabling the discovery of other participants across the Internet and secure communication with them. It performs these main functions:

* Peer discovery via the following:
  * Kademlia DHT
  * DNS lookup
  * Configured static peers
  * Node info saved in the database
  * Boot nodes pre-configured in the source code
* Peer management:
  * handshakes
  * holding p2p connection even if Erigon is restarted

The ETH core interacts with the Ethereum p2p network through the Sentry component. Sentry provides a simple interface to the core, with functions to download data, receive notifications about gossip messages, upload data on request from peers, and broadcast gossip messages either to a selected set of peers or to all peers.

## Running with an external Sentry or multiple Sentries

It is possible to have multiple Sentry to increase connectivity to the network or to obscure the location of the core computer. In this case it is necessary to define address and port of each Sentry that should be connected to the Core.

Before using the Sentry component the executable must be built:

```bash
cd erigon
make sentry
```

Then it can be launched as an independent component with the command:

```bash
./build/bin/sentry
```

### Example

In this example we will run an instance of Erigon and Sentry on the same machine.

Following is the Sentry client running separately:

```bash
./build/bin/sentry --datadir=~/.local/share/erigon
```

And Erigon attaching to it:

```bash
./build/bin/erigon --sentry.api.addr=127.0.0.1:9091
```

Erigon might be attached to several Sentry instances running across different machines. As per Erigon help:

```bash
--sentry.api.addr value
```

Where `value` is comma separated sentry addresses `<host>:<port>,<host>:<port>`.

## More info

For other information regarding Sentry functionality, configuration, and usage, please refer to the embedded file you can find in your compiled Erigon folder at `./cmd/sentry/README.md`.

### Command Line Options

To display available options for Sentry digit:

```bash
./build/bin/sentry --help
```

The `--help` flag listing is reproduced below for your convenience.

```
Run p2p sentry

Usage:
  sentry [flags]

Flags:
      --datadir string                     Data directory for the databases (default "/home/user/.local/share/erigon")
      --diagnostics.disabled               Disable diagnostics
      --diagnostics.endpoint.addr string   Diagnostics HTTP server listening interface (default "127.0.0.1")
      --diagnostics.endpoint.port uint     Diagnostics HTTP server listening port (default 6062)
      --diagnostics.speedtest              Enable speed test
      --discovery.dns strings              Sets DNS discovery entry points (use "" to disable DNS)
      --healthcheck                        Enabling grpc health check
  -h, --help                               help for sentry
      --log.console.json                   Format console logs with JSON
      --log.console.verbosity string       Set the log level for console logs (default "info")
      --log.delays                         Enable block delay logging
      --log.dir.disable                    disable disk logging
      --log.dir.json                       Format file logs with JSON
      --log.dir.path string                Path to store user and error logs to disk
      --log.dir.prefix string              The file name prefix for logs stored to disk
      --log.dir.verbosity string           Set the log verbosity for logs stored to disk (default "info")
      --log.json                           Format console logs with JSON
      --maxpeers int                       Maximum number of network peers (network disabled if set to 0) (default 32)
      --maxpendpeers int                   Maximum number of TCP connections pending to become connected peers (default 1000)
      --metrics                            Enable metrics collection and reporting
      --metrics.addr string                Enable stand-alone metrics HTTP server listening interface (default "127.0.0.1")
      --metrics.port int                   Metrics HTTP server listening port (default 6061)
      --nat string                         NAT port mapping mechanism (any|none|upnp|pmp|stun|extip:<IP>)
                                           			 "" or "none"         Default - do not nat
                                           			 "extip:77.12.33.4"   Will assume the local machine is reachable on the given IP
                                           			 "any"                Uses the first auto-detected mechanism
                                           			 "upnp"               Uses the Universal Plug and Play protocol
                                           			 "pmp"                Uses NAT-PMP with an auto-detected gateway address
                                           			 "pmp:192.168.0.1"    Uses NAT-PMP with the given gateway address
                                           			 "stun"               Uses STUN to detect an external IP using a default server
                                           			 "stun:<server>"      Uses STUN to detect an external IP using the given server (host:port)
                                           
      --netrestrict string                 Restricts network communication to the given IP networks (CIDR masks)
      --nodiscover                         Disables the peer discovery mechanism (manual peer addition)
      --p2p.allowed-ports uints            Allowed ports to pick for different eth p2p protocol versions as follows <porta>,<portb>,..,<porti> (default [30303,30304,30305,30306,30307])
      --p2p.protocol uint                  Version of eth p2p protocol (default 68)
      --port int                           Network listening port (default 30303)
      --pprof                              Enable the pprof HTTP server
      --pprof.addr string                  pprof HTTP server listening interface (default "127.0.0.1")
      --pprof.cpuprofile string            Write CPU profile to the given file
      --pprof.port int                     pprof HTTP server listening port (default 6060)
      --sentry.api.addr string             grpc addresses (default "localhost:9091")
      --staticpeers strings                Comma separated enode URLs to connect to
      --trace string                       Write execution trace to the given file
      --trustedpeers strings               Comma separated enode URLs which are always allowed to connect, even above the peer limit
      --verbosity string                   Set the log level for console logs (default "info")
```
