# Erigon

Erigon is an implementation of Ethereum (aka "Ethereum client"), on the efficiency frontier, written in Go.

![Build status](https://github.com/ledgerwatch/erigon/actions/workflows/ci.yml/badge.svg)

<!--ts-->

- [System Requirements](#system-requirements)
- [Usage](#usage)
    + [Getting Started](#getting-started)
    + [Testnets](#testnets)
    + [Mining](#mining)
    + [Windows](#windows)
    + [GoDoc](https://godoc.org/github.com/ledgerwatch/erigon)
    + [Beacon Chain](#beacon-chain)
    + [Dev Chain](#dev-chain)

- [Key features](#key-features)
    + [More Efficient State Storage](#more-efficient-state-storage)
    + [Faster Initial Sync](#faster-initial-sync)
    + [JSON-RPC daemon](#json-rpc-daemon)
    + [Run all components by docker-compose](#run-all-components-by-docker-compose)
    + [Grafana dashboard](#grafana-dashboard)
- [FAQ](#faq)
- [Getting in touch](#getting-in-touch)
    + [Erigon Discord Server](#erigon-discord-server)
    + [Reporting security issues/concerns](#reporting-security-issues-concerns)
    + [Team](#team)
- [Known issues](#known-issues)
    + [`htop` shows incorrect memory usage](#htop-shows-incorrect-memory-usage)

<!--te-->


NB! <code>In-depth links are marked by the microscope sign (🔬) </code>

**Disclaimer: this software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.**


System Requirements
===================

For a full archive node we recommend >=3TB storage space on a single partition: 1.8TB state (as of March 2022),
200GB temp files (can symlink or mount folder `<datadir>/etl-tmp` to another disk).
SSD or NVMe. Do not recommend HDD - on HDD Erigon will always stay N blocks behind chain tip, but not fall behind. 
Bear in mind that SSD performance deteriorates when close to capacity.

RAM: >=16GB, 64-bit architecture, [Golang version >= 1.16](https://golang.org/doc/install), GCC 10+

<code>🔬 more info on disk storage is [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space).</code>

Usage
=====

### Getting Started

```sh
git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
cd erigon
make erigon
./build/bin/erigon
```

Default `--syncmode=snap` for `mainnet`, `goerli`, `bsc`. Other networks now have default `--syncmode=fast`. Increase download speed by flag `--torrent.download.rate=20mb`. <code>🔬 See [Downloader docs](./cmd/downloader/readme.md)</code> 

Use `--datadir` to choose where to store data.

### Optional stages

There is an optional stage that can be enabled through flags:

* `--watch-the-burn`, Enable WatchTheBurn stage which keeps track of ETH issuance and is required to use `erigon_watchTheBurn`.

### Testnets

If you would like to give Erigon a try, but do not have spare 2TB on your drive, a good option is to start syncing one
of the public testnets, Görli. It syncs much quicker, and does not take so much disk space:

```sh
git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
cd erigon
make erigon
./build/bin/erigon --datadir goerli --chain goerli
```

Please note the `--datadir` option that allows you to store Erigon files in a non-default location, in this example,
in `goerli` subdirectory of the current directory. Name of the directory `--datadir` does not have to match the name of
the chain in `--chain`.

### Mining

Support only remote-miners.

* To enable, add `--mine --miner.etherbase=...` or `--mine --miner.miner.sigkey=...` flags.
* Other supported options: `--miner.extradata`, `--miner.notify`, `--miner.gaslimit`, `--miner.gasprice`
  , `--miner.gastarget`
* RPCDaemon supports methods: eth_coinbase , eth_hashrate, eth_mining, eth_getWork, eth_submitWork, eth_submitHashrate
* RPCDaemon supports websocket methods: newPendingTransaction
* TODO:
    + we don't broadcast mined blocks to p2p-network
      yet, [but it's easy to accomplish](https://github.com/ledgerwatch/erigon/blob/9b8cdc0f2289a7cef78218a15043de5bdff4465e/eth/downloader/downloader.go#L673)
    + eth_newPendingTransactionFilter
    + eth_newBlockFilter
    + eth_newFilter
    + websocket Logs

<code> 🔬 Detailed mining explanation is [here](/docs/mining.md).</code>

### Windows

Windows users may run erigon in 3 possible ways:

* Build executable binaries natively for Windows using provided `wmake.ps1` PowerShell script. Usage syntax is the same
  as `make` command so you have to run `.\wmake.ps1 [-target] <targetname>`. Example: `.\wmake.ps1 erigon` builds erigon
  executable. All binaries are placed in `.\build\bin\` subfolder. There are some requirements for a successful native
  build on windows :
    * [Git](https://git-scm.com/downloads) for Windows must be installed. If you're cloning this repository is very
      likely you already have it
    * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.16
    * GNU CC Compiler at least version 10 (is highly suggested that you install `chocolatey` package manager - see
      following point)
    * If you need to build MDBX tools (i.e. `.\wmake.ps1 db-tools`)
      then [Chocolatey package manager](https://chocolatey.org/) for Windows must be installed. By Chocolatey you need
      to install the following components : `cmake`, `make`, `mingw` by `choco install cmake make mingw`.

  **Important note about Anti-Viruses**
  During MinGW's compiler detection phase some temporary executables are generated to test compiler capabilities. It's
  been reported some anti-virus programs detect those files as possibly infected by `Win64/Kryptic.CIS` trojan horse (or
  a variant of it). Although those are false positives we have no control over 100+ vendors of security products for
  Windows and their respective detection algorythms and we understand this might make your experience with Windows
  builds uncomfortable. To workaround the issue you might either set exclusions for your antivirus specifically
  for `build\bin\mdbx\CMakeFiles` sub-folder of the cloned repo or you can run erigon using the following other two
  options

* Use Docker :  see [docker-compose.yml](./docker-compose.yml)

* Use WSL (Windows Subsystem for Linux) **strictly on version 2**. Under this option you can build Erigon just as you
  would on a regular Linux distribution. You can point your data also to any of the mounted Windows partitions (
  eg. `/mnt/c/[...]`, `/mnt/d/[...]` etc) but in such case be advised performance is impacted: this is due to the fact
  those mount points use `DrvFS` which is a [network file system](#blocks-execution-is-slow-on-cloud-network-drives)
  and, additionally, MDBX locks the db for exclusive access which implies only one process at a time can access data.
  This has consequences on the running of `rpcdaemon` which has to be configured as [Remote DB](#for-remote-db) even if
  it is executed on the very same computer. If instead your data is hosted on the native Linux filesystem non
  limitations apply.
  **Please also note the default WSL2 environment has its own IP address which does not match the one of the network
  interface of Windows host: take this into account when configuring NAT for port 30303 on your router.**

### Beacon Chain

Erigon can be used as an execution-layer for beacon chain consensus clients (Eth2). Default configuration is ok. Eth2
relies on availability of receipts - don't prune them: don't add character `r` to `--prune` flag. However, old receipts
 are not needed for Eth2 and you can safely prune them with `--prune.r.before=11184524` in combination with `--prune htc`.

You must run the [JSON-RPC daemon](#json-rpc-daemon) in addition to the Erigon.

If beacon chain client on a different device: add `--http.addr 0.0.0.0` (JSON-RPC daemon listen on localhost by default)
.

Once the JSON-RPC daemon is running, all you need to do is point your beacon chain client to `<ip address>:8545`,
where `<ip address>` is either localhost or the IP address of the device running the JSON-RPC daemon.

Erigon has been tested with Lighthouse however all other clients that support JSON-RPC should also work.

### Authentication API

In order to establish a secure connection between the Consensus Layer and the Execution Layer, a JWT secret key is automatically generated.

The JWT secret key will be present in the datadir by default under the name of `jwt.hex` and its path can be specified with the flag `--authrpc.jwtsecret`.

This piece of info needs to be specified in the Consensus Layer as well in order to establish connection successfully. More information can be found [here](https://github.com/ethereum/execution-apis/blob/main/src/engine/authentication.md)
    
### Multiple Instances / One Machine

Define 5 flags to avoid conflicts: `--datadir --port --http.port --torrent.port --private.api.addr`. Example of multiple chains on the same machine:

```
# mainnet
./build/bin/erigon --datadir="<your_mainnet_data_path>" --chain=mainnet --port=30303 --http.port=8545 --torrent.port=42069 --private.api.addr=127.0.0.1:9090 --http --ws --http.api=eth,debug,net,trace,web3,erigon 


# rinkeby
./build/bin/erigon --datadir="<your_rinkeby_data_path>" --chain=rinkeby --port=30304 --http.port=8546 --torrent.port=42068 --private.api.addr=127.0.0.1:9091 --http --ws --http.api=eth,debug,net,trace,web3,erigon 
```

Quote your path if it has spaces.

### Dev Chain
<code> 🔬 Detailed explanation is [DEV_CHAIN](/DEV_CHAIN.md).</code>

Key features
============ 

<code>🔬 See more
detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html). It
is being updated on recurring basis.</code>

### More Efficient State Storage

**Flat KV storage.** Erigon uses a key-value database and storing accounts and storage in a simple way.

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, Erigon uses temporary files to preprocess data before inserting it into the main
DB. That reduces write amplification and DB inserts are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](https://github.com/ledgerwatch/erigon-lib/blob/main/etl/README.md).</code>

**Plain state**.

**Single accounts/state trie**. Erigon uses a single Merkle trie for both accounts and the storage.

### Faster Initial Sync

Erigon uses a rearchitected full sync algorithm from
[Go-Ethereum](https://github.com/ethereum/go-ethereum) that is split into
"stages".

<code>🔬 See more detailed explanation in the [Staged Sync Readme](/eth/stagedsync/README.md)</code>

It uses the same network primitives and is compatible with regular go-ethereum nodes that are using full sync, you do
not need any special sync capabilities for Erigon to sync.

When reimagining the full sync, with focus on batching data together and minimize DB overwrites. That makes it possible
to sync Ethereum mainnet in under 2 days if you have a fast enough network connection and an SSD drive.

Examples of stages are:

* Downloading headers;

* Downloading block bodies;

* Recovering senders' addresses;

* Executing blocks;

* Validating root hashes and building intermediate hashes for the state Merkle trie;

* [...]

### JSON-RPC daemon

Most of Erigon's components can work inside Erigon and as independent process. 

To enable built-in RPC server: `--http` and `--ws` (sharing same port with http)

Run RPCDaemon as separated process: this daemon can use local DB (with running Erigon or on snapshot of a database) or remote DB (run on another server). <code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code> 

#### **For remote DB**

This works regardless of whether RPC daemon is on the same computer with Erigon, or on a different one. They use TPC
socket connection to pass data between them. To use this mode, run Erigon in one terminal window

```sh
make erigon
./build/bin/erigon --private.api.addr=localhost:9090
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool
```

#### **gRPC ports**

`9090` erigon, `9091` sentry, `9092` consensus engine, `9093` torrent downloader, `9094` transactions pool

Supported JSON-RPC calls ([eth](./cmd/rpcdaemon/commands/eth_api.go), [debug](./cmd/rpcdaemon/commands/debug_api.go)
, [net](./cmd/rpcdaemon/commands/net_api.go), [web3](./cmd/rpcdaemon/commands/web3_api.go)):

For a details on the implementation status of each
command, [see this table](./cmd/rpcdaemon/README.md#rpc-implementation-status).

### Run all components by docker-compose

Next command starts: Erigon on port 30303, rpcdaemon 8545, prometheus 9090, grafana 3000

```sh
make docker-compose
# or
XDG_DATA_HOME=/preferred/data/folder make docker-compose
```

Makefile creates the initial directories for erigon, prometheus and grafana. The PID namespace is shared between erigon
and rpcdaemon which is required to open Erigon's DB from another process (RPCDaemon local-mode).
See: https://github.com/ledgerwatch/erigon/pull/2392/files

Windows support for docker-compose is not ready yet. Please help us with .ps1 port

### Grafana dashboard

`docker-compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

### Prune old data

Disabled by default. To enable see `./build/bin/erigon --help` for flags `--prune`

FAQ
================

### How much RAM do I need

- Baseline (ext4 SSD): 16Gb RAM sync takes 6 days, 32Gb - 5 days, 64Gb - 4 days
- +1 day on "zfs compression=off". +2 days on "zfs compression=on" (2x compression ratio). +3 days on btrfs.
- -1 day on NVMe

Detailed explanation: [./docs/programmers_guide/db_faq.md](./docs/programmers_guide/db_faq.md)

### Default Ports and Protocols / Firewalls?

#### `erigon` ports

|  Port |  Protocol |      Purpose           |  Expose |
|:-----:|:---------:|:----------------------:|:-------:|
| 30303 | TCP & UDP | eth/66 peering         |  Public |
|  9090 |    TCP    | gRPC Connections       | Private |
| 42069 | TCP & UDP | Snap sync (Bittorrent) |  Public |
|  6060 |    TCP    | Metrics or Pprof       | Private |

Typically 30303 and 30304 are exposed to the internet to allow incoming peering connections. 9090 is exposed only
internally for rpcdaemon or other connections, (e.g. rpcdaemon -> erigon)

#### `RPC` ports

|  Port |  Protocol |      Purpose       |  Expose |
|:-----:|:---------:|:------------------:|:-------:|
|  8545 |    TCP    | HTTP & WebSockets  | Private |
|  8550 |    TCP    |       HTTP         | Private |
|  8551 |    TCP    | HTTP with JWS auth | Private |

Typically 8545 is exposed only internally for JSON-RPC queries. Both HTTP and WebSocket connections are on the same port.
Typically 8550 (unauthenticated) and 8551 (authenticated) are exposed only internally for the Engine API JSON-RPC queries.

#### `sentry` ports

|  Port |  Protocol |      Purpose     |  Expose |
|:-----:|:---------:|:----------------:|:-------:|
| 30303 | TCP & UDP |      Peering     |  Public |
|  9091 |    TCP    | gRPC Connections | Private |

Typically a sentry process will run one eth/xx protocl (e.g. eth/66) and will be exposed to the internet on 30303. Port
9091 is for internal gRCP connections (e.g erigon -> sentry)

#### Other ports

| Port | Protocol | Purpose |  Expose |
|:----:|:--------:|:-------:|:-------:|
| 6060 |    TCP   |  pprof  | Private |
| 6060 |    TCP   | metrics | Private |

Optional flags can be enabled that enable pprof or metrics (or both) - however, they both run on 6060 by default, so
you'll have to change one if you want to run both at the same time. use `--help` with the binary for more info.

Reserved for future use: **gRPC ports**: `9092` consensus engine, `9093` snapshot downloader, `9094` TxPool

### How to get diagnostic for bug report?

- Get stack trace: `kill -SIGUSR1 <pid>`, get trace and stop: `kill -6 <pid>`
- Get CPU profiling: add `--pprof flag`
  run `go tool pprof -png  http://127.0.0.1:6060/debug/pprof/profile\?seconds\=20 > cpu.png`
- Get RAM profiling: add `--pprof flag`
  run `go tool pprof -inuse_space -png  http://127.0.0.1:6060/debug/pprof/heap > mem.png`
    
### How to run local devnet?
<code> 🔬 Detailed explanation is [here](/DEV_CHAIN.md).</code>

### Docker permissions error

Docker uses user erigon with UID/GID 1000 (for security reasons). You can see this user being created in the Dockerfile.
Can fix by giving a host's user ownership of the folder, where the host's user UID/GID is the same as the docker's user UID/GID (1000).
More details in [post](https://www.fullstaq.com/knowledge-hub/blogs/docker-and-the-host-filesystem-owner-matching-problem)

Getting in touch
================

### Erigon Discord Server

The main discussions are happening on our Discord server. To get an invite, send an email to `tg [at] torquem.ch` with
your name, occupation, a brief explanation of why you want to join the Discord, and how you heard about Erigon.

### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

### Team

Core contributors (in alpabetical order of first names):

* Alex Sharov ([AskAlexSharov](https://twitter.com/AskAlexSharov))

* Alexey Akhunov ([@realLedgerwatch](https://twitter.com/realLedgerwatch))

* Andrea Lanfranchi([@AndreaLanfranchi](https://github.com/AndreaLanfranchi))

* Andrew Ashikhmin ([yperbasis](https://github.com/yperbasis))

* Artem Vorotnikov ([vorot93](https://github.com/vorot93))

* Boris Petrov ([b00ris](https://github.com/b00ris))

* Eugene Danilenko ([JekaMas](https://github.com/JekaMas))

* Igor Mandrigin ([@mandrigin](https://twitter.com/mandrigin))

* Giulio Rebuffo ([Giulio2002](https://github.com/Giulio2002))

* Thomas Jay Rush ([@tjayrush](https://twitter.com/tjayrush))

Thanks to:

* All contributors of Erigon

* All contributors of Go-Ethereum

* Our special respect and graditude is to the core team of [Go-Ethereum](https://github.com/ethereum/go-ethereum). Keep
  up the great job!

Happy testing! 🥤

Known issues
============

### `htop` shows incorrect memory usage

Erigon's internal DB (MDBX) using `MemoryMap` - when OS does manage all `read, write, cache` operations instead of
Application
([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html)
, [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping))

`htop` on column `res` shows memory of "App + OS used to hold page cache for given App", but it's not informative,
because if `htop` says that app using 90% of memory you still can run 3 more instances of app on the same machine -
because most of that `90%` is "OS pages cache".  
OS automatically free this cache any time it needs memory. Smaller "page cache size" may not impact performance of
Erigon at all.

Next tools show correct memory usage of Erigon:

- `vmmap -summary PID | grep -i "Physical footprint"`. Without `grep` you can see details
    - `section MALLOC ZONE column Resident Size` shows App memory usage, `section REGION TYPE column Resident Size`
      shows OS pages cache size.
- `Prometheus` dashboard shows memory of Go app without OS pages cache (`make prometheus`, open in
  browser `localhost:3000`, credentials `admin/admin`)
- `cat /proc/<PID>/smaps`

Erigon uses ~4Gb of RAM during genesis sync and ~1Gb during normal work. OS pages cache can utilize unlimited amount of
memory.

**Warning:** Multiple instances of Erigon on same machine will touch Disk concurrently, it impacts performance - one of
main Erigon optimisations: "reduce Disk random access".
"Blocks Execution stage" still does much random reads - this is reason why it's slowest stage. We do not recommend run
multiple genesis syncs on same Disk. If genesis sync passed, then it's fine to run multiple Erigon on same Disk.

### Blocks Execution is slow on cloud-network-drives

Please read https://github.com/ledgerwatch/erigon/issues/1516#issuecomment-811958891
In short: network-disks are bad for blocks execution - because blocks execution reading data from db non-parallel
non-batched way.

### Filesystem's background features are expensive

For example: btrfs's autodefrag option - may increase write IO 100x times

### Gnome Tracker can kill Erigon

[Gnome Tracker](https://wiki.gnome.org/Projects/Tracker) - detecting miners and kill them.
