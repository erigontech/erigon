# Turbo-Geth

Turbo-Geth is a fork of [Go-Ethereum](https://github.com/ethereum/go-ethereum) with focus on performance. [![CircleCI](https://circleci.com/gh/ledgerwatch/turbo-geth.svg?style=svg)](https://circleci.com/gh/ledgerwatch/turbo-geth)

Table of contents
=================

<!--ts-->
   * [System Requirements](#system-requirements)
   * [Usage](#usage)
   * [Key features](#key-features)
   * [Getting in touch](#getting-in-touch)
   * [Team](#team)
   * [Known issues](#known-issues)
   * [GoDoc](https://godoc.org/github.com/ledgerwatch/turbo-geth)
<!--te-->



NB! <code>In-depth links are marked by the microscope sign (🔬) </code>

**Disclaimer: this software is currenly a tech preview. We will do our best to
keep it stable and make no breaking changes but we don't guarantee anything.
Things can and will break.**



The current version is currently based on Go-Ethereum 1.9.15.

System Requirements
===================

About 830 GB of free disk storage (630 GB state storage, 200GB temp files)

16 or 32 GB of RAM is recommended

<code>🔬 more info on disk storage is here [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)) </code>

Usage
=====

```sh
> git clone --recurse-submodules -j8 https://github.com/ledgerwatch/turbo-geth.git && cd turbo-geth
> make tg
> ./build/bin/tg
```

Key features
============ 

<code>🔬 See more detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html). It is being updated on recurring basis.</code>

#### More Efficient State Storage

**Flat KV storage.** Turbo-Geth uses a key-value database and storing accounts and storage in
a simple way. 

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, turbo-geth uses temporary files to preprocess data before
inserting it into the main DB. That reduces write amplification and 
DB inserts sometimes are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](/common/etl/).</code>

**Plain state**.

**Single accounts/state trie**. Turbo-Geth uses a single Merkle trie for both
accounts and the storage.


#### Faster Initial Sync

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

#### JSON-RPC daemon

In turbo-geth RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main turbo-geth binary or
it can run from a snapshot of a database for read-only calls. 

<code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code>

**For local DB**

```
> make rpcdaemon
> ./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net
```
**For remote DB**

Run turbo-geth in one terminal window

```
> ./build/bin/tg --private.api.addr=localhost:9090
```

Run RPC daemon
```
> ./build/bin/rpcdaemon --private.api.addr=localhost:9090
```

Supported JSON-RPC calls ([eth](./cmd/rpcdaemon/commands/eth_api.go), [debug](./cmd/rpcdaemon/commands/debug_api.go), [net](./cmd/rpcdaemon/commands/net_api.go), [web3](./cmd/rpcdaemon/commands/web3_api.go)):

For a more detailed status, [see this table](./cmd/rpcdaemon/README.md#rpc-implementation-status).

```
eth_coinbase
eth_blockNumber
eth_call
eth_estimateGas
eth_getBlockByNumber
eth_getBlockByHash
eth_getBlockTransactionCountByHash
eth_getBlockTransactionCountByNumber
eth_getBalance
eth_getCode
eth_GetTransactionCount
eth_GetUncleByBlockNumberAndIndex
eth_GetUncleByBlockHashAndIndex
eth_GetUncleCountByBlockNumber
eth_GetUncleCountByBlockHash
eth_getLogs
eth_getStorageAt
eth_getTransactionReceipt
eth_getTransactionByHash
eth_getTransactionByBlockHashAndIndex
eth_getTransactionByBlockNumberAndIndex
eth_sendRawTransaction
eth_syncing
debug_accountRange
debug_getModifiedAccountsByNumber
debug_getModifiedAccountsByHash
debug_storageRangeAt
debug_traceTransaction
net_peerCount*
net_version
web3_clientVersion
web3_sha3
trace_filter
```

\* net_peerCount currently always returns a count of 25 as work continues on Sentry.

#### Grafana dashboard:

`docker-compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

#### Or run all components by docker-compose

Next command starts: turbo-geth on port 30303, rpcdaemon 8545, prometheus 9090, grafana 3000

```
docker-compose build
XDG_DATA_HOME=/preferred/data/folder docker-compose up
```

Getting in touch
================

#### Turbo-Geth Discord Server

The main discussions are happening on our Discord server. 
To get an invite, send an email to `tg [at] torquem.ch` with your name, occupation, 
a brief explanation of why you want to join the Discord, and how you heard about Turbo-Geth.

#### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

Team
=======

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

#### `htop` shows incorrect memory usage

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
