# Turbo-Geth

Turbo-Geth is a fork of [Go-Ethereum](https://github.com/ethereum/go-ethereum) with focus on performance. [![CircleCI](https://circleci.com/gh/ledgerwatch/turbo-geth.svg?style=svg)](https://circleci.com/gh/ledgerwatch/turbo-geth)

<!--ts-->
- [System Requirements](#system-requirements)
- [Usage](#usage)
    + [Getting Started](#getting-started)
    + [Testnets](#testnets)
    + [Mining](#mining)
    + [Windows](#windows)
    + [GoDoc](https://godoc.org/github.com/ledgerwatch/turbo-geth)
- [Key features](#key-features)
    + [More Efficient State Storage](#more-efficient-state-storage)
    + [Faster Initial Sync](#faster-initial-sync)
    + [JSON-RPC daemon](#json-rpc-daemon)
    + [Run all components by docker-compose](#run-all-components-by-docker-compose)
    + [Grafana dashboard](#grafana-dashboard)
- [Getting in touch](#getting-in-touch)
    + [Turbo-Geth Discord Server](#turbo-geth-discord-server)
    + [Reporting security issues/concerns](#reporting-security-issues-concerns)
    + [Team](#team)
- [Known issues](#known-issues)
    + [`htop` shows incorrect memory usage](#-htop--shows-incorrect-memory-usage)
<!--te-->


NB! <code>In-depth links are marked by the microscope sign (🔬) </code>

**Disclaimer: this software is currenly a tech preview. We will do our best to
keep it stable and make no breaking changes but we don't guarantee anything.
Things can and will break.**



The current version is currently based on Go-Ethereum 1.10.1

System Requirements
===================

Recommend 2Tb storage space on a single partition: 1Tb state, 200GB temp files (can symlink or mount folder `<datadir>/etl-tmp` to another disk). 

RAM: 16GB, 64-bit architecture, [Golang version >= 1.15.6](https://golang.org/doc/install)

<code>🔬 more info on disk storage is [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)) </code>

Usage
=====

### Getting Started

```sh
> git clone --recurse-submodules -j8 https://github.com/ledgerwatch/turbo-geth.git
> cd turbo-geth
> make tg
> ./build/bin/tg
```

### Testnets

If you would like to give turbo-geth a try, but do not have spare 2Tb on your driver, a good option is to start syncing one of the public testnets, Görli. It syncs much quicker, and does not take so much disk space:
```sh
> git clone --recurse-submodules -j8 https://github.com/ledgerwatch/turbo-geth.git
> cd turbo-geth
> make tg
> ./build/bin/tg --datadir goerli --chain goerli
```

Please note the `--datadir` option that allows you to store turbo-geth files in a non-default location, in this example, in `goerli` subdirectory of the current directory. Name of the directory `--datadir` does not have to match the name if the chain in `--chain`.

### Mining

Support only remote-miners.

* To enable, add `--mine --miner.etherbase=...` or `--mine --miner.miner.sigkey=...` flags.
* Other supported options: `--miner.extradata`, `--miner.notify`, `--miner.gaslimit`, `--miner.gasprice`
  , `--miner.gastarget`
* RPCDaemon supports methods: eth_coinbase , eth_hashrate, eth_mining, eth_getWork, eth_submitWork, eth_submitHashrate
* RPCDaemon supports websocket methods: newPendingTransaction
* TODO:
    + we don't broadcast mined blocks to p2p-network yet, [but it's easy to accomplish](https://github.com/ledgerwatch/turbo-geth/blob/9b8cdc0f2289a7cef78218a15043de5bdff4465e/eth/downloader/downloader.go#L673)
    + eth_newPendingTransactionFilter
    + eth_newBlockFilter
    + eth_newFilter
    + websocket Logs

<code> 🔬 Detailed mining explanation is [here](/docs/mining.md).</code>

### Windows

Windows users may run turbo-geth in 3 possible ways:

* Build tg binaries natively for Windows : while this method is possible we still lack a fully automated build process thus, at the moment, is not to be preferred. Besides there's also a caveat which might cause your experience with TG as native on Windows uncomfortable: data file allocation is fixed so you need to know in advance how much space you want to allocate for database file using the option `--lmdb.mapSize`

* Use Docker :  see [docker-compose.yml](./docker-compose.yml)

* Use WSL (Windows Subsystem for Linux) : You can easily install WSL following [this quickstart guide](https://docs.microsoft.com/en-us/windows/wsl/install-win10). Is also suggested the reading of [interoperability amongst Windows and Linux](https://docs.microsoft.com/en-us/windows/wsl/interop) work. Once your WSL environment is ready with your preferred Kernel distribution (for this document we assume you've choosen Ubuntu) proceed to install (in the linux subsystem) the required components:

```sh
> sudo apt install build-essential git golang golang-go
```

Once this last step is completed you can run tg as if you were on Linux as described the [Usage](#usage) section.

**Note** : WSL native filesystem is set to reside in the same partition of Windows' system partition (usually C:). Unless this is the only partition of your system is advisable to have TG store its data in a different partition. Say your Windows system has a secondary partition D: WSL environment _sees_ this partition as `/mnt/d`so to have TG store its data there you will haave to launch TG as 

```sh
> ./tg --datadir /mnt/d/[<optional-subfolder>/]
```


Key features
============ 

<code>🔬 See more detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html). It is being updated on recurring basis.</code>

### More Efficient State Storage

**Flat KV storage.** Turbo-Geth uses a key-value database and storing accounts and storage in
a simple way. 

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, turbo-geth uses temporary files to preprocess data before
inserting it into the main DB. That reduces write amplification and 
DB inserts are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](/common/etl/).</code>

**Plain state**.

**Single accounts/state trie**. Turbo-Geth uses a single Merkle trie for both
accounts and the storage.


### Faster Initial Sync

Turbo-Geth uses a rearchitected full sync algorithm from
[Go-Ethereum](https://github.com/ethereum/go-ethereum) that is split into
"stages".

<code>🔬 See more detailed explanation in the [Staged Sync Readme](/eth/stagedsync/)</code>

It uses the same network primitives and is compatible with regular go-ethereum
nodes that are using full sync, you do not need any special sync capabilities
for turbo-geth to sync.

When reimagining the full sync, we focused on batching data together and minimize DB overwrites.
That makes it possible to sync Ethereum mainnet in under 2 days if you have a fast enough network connection
and an SSD drive.

Examples of stages are:

* Downloading headers;

* Downloading block bodies;

* Executing blocks;

* Validating root hashes and building intermediate hashes for the state Merkle trie;

* And more...

### JSON-RPC daemon

In turbo-geth RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main turbo-geth binary or
it can run from a snapshot of a database for read-only calls. 

<code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code>

**For local DB**

This is only possible if RPC daemon runs on the same computer as turbo-geth. This mode of operation uses shared memory access to the database of turbo-geth, which is reported to have better performance than accessing via TPC socket (see "For remote DB" section below)
```
> make rpcdaemon
> ./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net
```

In this mode, some RPC API methods do not work. Please see "For dual mode" section below on how to fix that.

**For remote DB**

This works regardless of whether RPC daemon is on the same computer with turbo-geth, or on a different one. They use TPC socket connection to pass data between them. To use this mode, run turbo-geth in one terminal window

```
> ./build/bin/tg --private.api.addr=localhost:9090
```

Run RPC daemon
```
> ./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,debug,net
```

**For dual mode**

If both `--chaindata` and `--private.api.addr` options are used for RPC daemon, it works in a "dual" mode. This only works when RPC daemon is on the same computer as turbo-geth. In this mode, most data transfer from turbo-geth to RPC daemon happens via shared memory, only certain things (like new header notifications) happen via TPC socket.

Supported JSON-RPC calls ([eth](./cmd/rpcdaemon/commands/eth_api.go), [debug](./cmd/rpcdaemon/commands/debug_api.go), [net](./cmd/rpcdaemon/commands/net_api.go), [web3](./cmd/rpcdaemon/commands/web3_api.go)):

For a details on the implementation status of each command, [see this table](./cmd/rpcdaemon/README.md#rpc-implementation-status).

### Run all components by docker-compose

Next command starts: turbo-geth on port 30303, rpcdaemon 8545, prometheus 9090, grafana 3000

```
docker-compose build
XDG_DATA_HOME=/preferred/data/folder docker-compose up
```

### Grafana dashboard

`docker-compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

Getting in touch
================

### Turbo-Geth Discord Server

The main discussions are happening on our Discord server. 
To get an invite, send an email to `tg [at] torquem.ch` with your name, occupation, 
a brief explanation of why you want to join the Discord, and how you heard about Turbo-Geth.

### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

### Team

Core contributors:

* Alexey Akhunov ([@realLedgerwatch](https://twitter.com/realLedgerwatch))

* Alex Sharov ([AskAlexSharov](https://twitter.com/AskAlexSharov))

* Andrew Ashikhmin ([yperbasis](https://github.com/yperbasis))

* Boris Petrov ([b00ris](https://github.com/b00ris))

* Eugene Danilenko ([JekaMas](https://github.com/JekaMas))

* Igor Mandrigin ([@mandrigin](https://twitter.com/mandrigin))

* Giulio Rebuffo

* Thomas Jay Rush ([@tjayrush](https://twitter.com/tjayrush))

Thanks to:

* All contributors of Turbo-Geth

* All contributors of Go-Ethereum

* Our special respect and graditude is to the core team of [Go-Ethereum](https://github.com/ethereum/go-ethereum). Keep up the great job!

Happy testing! 🥤

Known issues
============

### `htop` shows incorrect memory usage

TurboGeth's internal DB (LMDB) using `MemoryMap` - when OS does manage all `read, write, cache` operations instead of Application
([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html), [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping))

`htop` on column `res` shows memory of "App + OS used to hold page cache for given App", 
but it's not informative, because if `htop` says that app using 90% of memory you still 
can run 3 more instances of app on the same machine - because most of that `90%` is "OS pages cache".  
OS automatically free this cache any time it needs memory. 
Smaller "page cache size" may not impact performance of TurboGeth at all. 

Next tools show correct memory usage of TurboGeth: 
- `vmmap -summary PID | grep -i "Physical footprint"`. 
Without `grep` you can see details - `section MALLOC ZONE column Resident Size` shows App memory usage, `section REGION TYPE column Resident Size` shows OS pages cache size. 
- `Prometheus` dashboard shows memory of Go app without OS pages cache (`make prometheus`, open in browser `localhost:3000`, credentials `admin/admin`)
- `cat /proc/<PID>/smaps`

TurboGeth uses ~4Gb of RAM during genesis sync and < 1Gb during normal work. OS pages cache can utilize unlimited amount of memory. 

**Warning:** Multiple instances of TG on same machine will touch Disk concurrently, 
it impacts performance - one of main TG optimisations: "reduce Disk random access". 
"Blocks Execution stage" still does much random reads - this is reason why it's slowest stage.
We do not recommend run multiple genesis syncs on same Disk. 
If genesis sync passed, then it's fine to run multiple TG on same Disk.

