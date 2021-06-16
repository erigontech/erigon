
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

**Disclaimer: this software is currenly a tech preview. We will do our best to
keep it stable and make no breaking changes but we don't guarantee anything.
Things can and will break.**



The current version is currently based on Go-Ethereum 1.10.1

System Requirements
===================

Recommend 2Tb storage space on a single partition: 1Tb state, 200GB temp files (can symlink or mount folder `<datadir>/etl-tmp` to another disk).

RAM: 16GB, 64-bit architecture, [Golang version >= 1.16](https://golang.org/doc/install)

<code>🔬 more info on disk storage is [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)) </code>

Usage
=====

### Getting Started

```sh
> git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
> cd erigon
> make erigon
> ./build/bin/erigon
```

### Testnets

If you would like to give Erigon a try, but do not have spare 2Tb on your driver, a good option is to start syncing one of the public testnets, Görli. It syncs much quicker, and does not take so much disk space:
```sh
> git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
> cd erigon
> make erigon
> ./build/bin/erigon --datadir goerli --chain goerli
```

Please note the `--datadir` option that allows you to store Erigon files in a non-default location, in this example, in `goerli` subdirectory of the current directory. Name of the directory `--datadir` does not have to match the name if the chain in `--chain`.

### Mining

Support only remote-miners.

* To enable, add `--mine --miner.etherbase=...` or `--mine --miner.miner.sigkey=...` flags.
* Other supported options: `--miner.extradata`, `--miner.notify`, `--miner.gaslimit`, `--miner.gasprice`
  , `--miner.gastarget`
* RPCDaemon supports methods: eth_coinbase , eth_hashrate, eth_mining, eth_getWork, eth_submitWork, eth_submitHashrate
* RPCDaemon supports websocket methods: newPendingTransaction
* TODO:
  + we don't broadcast mined blocks to p2p-network yet, [but it's easy to accomplish](https://github.com/ledgerwatch/erigon/blob/9b8cdc0f2289a7cef78218a15043de5bdff4465e/eth/downloader/downloader.go#L673)
  + eth_newPendingTransactionFilter
  + eth_newBlockFilter
  + eth_newFilter
  + websocket Logs

<code> 🔬 Detailed mining explanation is [here](/docs/mining.md).</code>

### Windows

Windows users may run erigon in 3 possible ways:

* Build executable binaries natively for Windows using provided `win-build.ps1` PowerShell script which has to be run with local Administrator privileges.
  The script creates `libmdbx.dll` (MDBX is current default database for Erigon) and copies it into Windows's `system32` folder (generally `C:\Windows\system32`).
  There are some requirements for a successful native build on windows :
  * [Git](https://git-scm.com/downloads) for Windows must be installed. If you're cloning this repository is very likely you already have it
  * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.16
  * [Chocolatey package manager](https://chocolatey.org/) for Windows must be installed. By Chocolatey you need to install the following components : `cmake`, `make`, `mingw` by `choco install cmake make mingw`.

  **Important note about Anti-Viruses**
  During MinGW's compiler detection phase some temporary executables are generated to test compiler capabilities. It's been reported some anti-virus programs detect
  those files as possibly infected by `Win64/Kryptic.CIS` trojan horse (or a variant of it). Although those are false positives we have no control over 100+ vendors of
  security products for Windows and their respective detection algorythms and we understand this might make your experience with Windows builds uncomfortable. To
  workaround the issue you might either set exlusions for your antivirus specifically for `ethdb\mdbx\dist\CMakeFiles` folder or you can run erigon on Docker or WSL

* Use Docker :  see [docker-compose.yml](./docker-compose.yml)

* Use WSL (Windows Subsystem for Linux) **strictly on version 2**. Under this option you can build Erigon just as you would on a regular Linux distribution. You can point your data also to any of the mounted Windows partitions (eg. `/mnt/c/[...]`, `/mnt/d/[...]` etc) but in such case be advised performance is impacted: this is due to the fact those mount points use `DrvFS` which is a [network file system](#blocks-execution-is-slow-on-cloud-network-drives) and, additionally, MDBX locks the db for exclusive access which implies only one process at a time can access data. This has consequences on the running of `rpcdaemon` which has to be configured as [Remote DB](#for-remote-db) even if it is executed on the very same computer.
If instead your data is hosted on the native Linux filesystem non limitations apply.
**Please also note the default WSL2 environment has its own IP address which does not match the one of the network interface of Windows host: take this into account when configuring NAT for port 30303 on your router.**

Key features
============ 

<code>🔬 See more detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html). It is being updated on recurring basis.</code>

### More Efficient State Storage

**Flat KV storage.** Erigon uses a key-value database and storing accounts and storage in
a simple way.

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, Erigon uses temporary files to preprocess data before
inserting it into the main DB. That reduces write amplification and
DB inserts are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](/common/etl/README.md).</code>

**Plain state**.

**Single accounts/state trie**. Erigon uses a single Merkle trie for both accounts and the storage.

### Faster Initial Sync

Erigon uses a rearchitected full sync algorithm from
[Go-Ethereum](https://github.com/ethereum/go-ethereum) that is split into
"stages".

<code>🔬 See more detailed explanation in the [Staged Sync Readme](/eth/stagedsync/README.md)</code>

It uses the same network primitives and is compatible with regular go-ethereum
nodes that are using full sync, you do not need any special sync capabilities
for Erigon to sync.

When reimagining the full sync, we focused on batching data together and minimize DB overwrites.
That makes it possible to sync Ethereum mainnet in under 2 days if you have a fast enough network connection
and an SSD drive.

Examples of stages are:

* Downloading headers;

* Downloading block bodies;

* Recovering senders' addresses;

* Executing blocks;

* Validating root hashes and building intermediate hashes for the state Merkle trie;

* [...]

### JSON-RPC daemon

In Erigon RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main Erigon binary or
it can run from a snapshot of a database for read-only calls.

<code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code>

#### **For local DB**

This is only possible if RPC daemon runs on the same computer as Erigon. This mode of operation uses shared memory access to the database of Erigon, which is reported to have better performance than accessing via TPC socket (see "For remote DB" section below)
```
> make rpcdaemon
> ./build/bin/rpcdaemon --datadir ~/Library/Erigon/ --http.api=eth,debug,net
```

In this mode, some RPC API methods do not work. Please see "For dual mode" section below on how to fix that.

#### **For remote DB**

This works regardless of whether RPC daemon is on the same computer with Erigon, or on a different one. They use TPC socket connection to pass data between them. To use this mode, run Erigon in one terminal window

```
> ./build/bin/erigon --private.api.addr=localhost:9090
```

Run RPC daemon
```
> ./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,debug,net
```

**gRPC ports**: `9090` erigon, `9091` sentry, `9092` consensus engine, `9093` snapshot downloader, `9094` TxPool

**For dual mode**

If both `--datadir` and `--private.api.addr` options are used for RPC daemon, it works in a "dual" mode. This only works when RPC daemon is on the same computer as Erigon. In this mode, most data transfer from Erigon to RPC daemon happens via shared memory, only certain things (like new header notifications) happen via TPC socket.

Supported JSON-RPC calls ([eth](./cmd/rpcdaemon/commands/eth_api.go), [debug](./cmd/rpcdaemon/commands/debug_api.go), [net](./cmd/rpcdaemon/commands/net_api.go), [web3](./cmd/rpcdaemon/commands/web3_api.go)):

For a details on the implementation status of each command, [see this table](./cmd/rpcdaemon/README.md#rpc-implementation-status).

### Run all components by docker-compose

Next command starts: Erigon on port 30303, rpcdaemon 8545, prometheus 9090, grafana 3000

```
docker-compose build
XDG_DATA_HOME=/preferred/data/folder docker-compose up
```

### Grafana dashboard

`docker-compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

FAQ
================

### How much RAM do I need

- Baseline (ext4 SSD): 16Gb RAM sync takes 5 days, 32Gb - 4 days, 64Gb - 3 days
- +1 day on "zfs compression=off". +2 days on "zfs compression=on" (2x compression ratio). +3 days on btrfs.
- -1 day on NVMe 

Detailed explanation: [./docs/programmers_guide/db_faq.md](./docs/programmers_guide/db_faq.md)

Getting in touch
================

### Erigon Discord Server

The main discussions are happening on our Discord server.
To get an invite, send an email to `tg [at] torquem.ch` with your name, occupation,
a brief explanation of why you want to join the Discord, and how you heard about Erigon.

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

* Our special respect and graditude is to the core team of [Go-Ethereum](https://github.com/ethereum/go-ethereum). Keep up the great job!

Happy testing! 🥤

Known issues
============

### `htop` shows incorrect memory usage

Erigon's internal DB (MDBX) using `MemoryMap` - when OS does manage all `read, write, cache` operations instead of Application
([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html), [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping))

`htop` on column `res` shows memory of "App + OS used to hold page cache for given App",
but it's not informative, because if `htop` says that app using 90% of memory you still
can run 3 more instances of app on the same machine - because most of that `90%` is "OS pages cache".  
OS automatically free this cache any time it needs memory.
Smaller "page cache size" may not impact performance of Erigon at all.

Next tools show correct memory usage of Erigon:
- `vmmap -summary PID | grep -i "Physical footprint"`.
  Without `grep` you can see details - `section MALLOC ZONE column Resident Size` shows App memory usage, `section REGION TYPE column Resident Size` shows OS pages cache size.
- `Prometheus` dashboard shows memory of Go app without OS pages cache (`make prometheus`, open in browser `localhost:3000`, credentials `admin/admin`)
- `cat /proc/<PID>/smaps`

Erigon uses ~4Gb of RAM during genesis sync and ~1Gb during normal work. OS pages cache can utilize unlimited amount of memory.

**Warning:** Multiple instances of Erigon on same machine will touch Disk concurrently,
it impacts performance - one of main Erigon optimisations: "reduce Disk random access".
"Blocks Execution stage" still does much random reads - this is reason why it's slowest stage.
We do not recommend run multiple genesis syncs on same Disk.
If genesis sync passed, then it's fine to run multiple Erigon on same Disk.

### Blocks Execution is slow on cloud-network-drives

Please read https://github.com/ledgerwatch/erigon/issues/1516#issuecomment-811958891
In short: network-disks are bad for blocks execution - because blocks execution reading data from db non-parallel non-batched way.

### rpcdaemon "Dual-Mode" does not work with Docker Container

Running rpcdaemon in "Dual-Mode" (including the `--datadir` flag) generally results in better performance for RPC calls, however, this does not work when running erigon and rpcdaemon in separate containers. For the absolute best performance bare metal is recommended at this time.
