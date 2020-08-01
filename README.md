# Turbo-Geth

[GoDoc](https://godoc.org/github.com/ledgerwatch/turbo-geth)

[![CircleCI](https://circleci.com/gh/ledgerwatch/turbo-geth.svg?style=svg)](https://circleci.com/gh/ledgerwatch/turbo-geth)

**Disclaimer: this software is currenly a tech preview. We will do our best to
keep it stable and make no breaking changes but we don't guarantee anything.
Things can and will break.**

Turbo-Geth is a fork of [Go-Ethereum](https://github.com/ethereum/go-ethereum) with focus on performance.

The current version is currently based on Go-Ethereum 1.9.15.

The current version requires about 350 GB of free disk space for the initial sync with default settings (for storing the state and temporary files).

Usage:

```sh
> git clone --recurse-submodules -j8 git@github.com:ledgerwatch/turbo-geth.git && cd turbo-geth
> make tg
> ./build/bin/tg
```

## Key features 

See more detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html).
It is being updated on recurring basis.

#### 1. More Efficient State Storage

**Flat KV storage.** Turbo-Geth uses a key-value database and storing accounts and storage in
a simple way. **See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).**

**Preprocessing**. For some operations, turbo-geth uses temporary files to preprocess data before
inserting it into the main DB. That reduces write amplification and 
DB inserts sometimes are orders of magnitude quicker.

**Plain state**.

**Single accounts/state trie**. Turbo-Geth uses a single Merkle trie for both
accounts and the storage.


#### 2. Faster Initial Sync

Turbo-Geth uses a rearchitected full sync algorithm from
[Go-Ethereum](https://github.com/ethereum/go-ethereum) that is split into
"stages".

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

#### 3. JSON-RPC daemon

In turbo-geth RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main turbo-geth binary or
it can run from a snapshot of a database for read-only calls. [Docs](./cmd/rpcdaemon/Readme.md)

**For local DB**

```
> make rpcdaemon
> ./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug
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

---

Currently supported JSON-RPC calls ([eth](./cmd/rpcdaemon/eth_api.go), [debug](./cmd/rpcdaemon/debug_api.go)):

```
eth_call
eth_getBlockByHash
eth_getBlock
eth_blockNumber
eth_getBalance
eth_getLogs
eth_estimateGas
debug_storageRangeAt
debug_traceTransaction
debug_accountRange
debug_getModifiedAccountsByNumber
debug_getModifiedAccountsByHash
```

#### 4. REST API Daemon

Apart from JSON-RPC daemon, Turbo-Geth also contains REST API daemon. It uses
turbo-geth remote DB functionality. [Docs](./cmd/rpcdaemon/Readme.md)

Run turbo-geth in one terminal window

```
> ./build/bin/tg --private.api.addr=localhost:9090
```

Run REST daemon
```
> make restapi
> ./build/bin/restapi --private.api.addr=localhost:9090
```

This API is very limited at the moment too:

```
GET /api/v1/accounts/<accountAddress>
GET /api/v1/storage/?prefix=PREFIX
```

#### 5. Run all components by docker-compose

Next command starts: turbo-geth on port 30303, rpcdaemon 8545, restapi 8080, debug-web-ui 3001, prometheus 9090, grafana 3000

```
docker-compose build
XDG_DATA_HOME=/preferred/data/folder docker-compose up
```

## Getting in touch

#### Turbo-Geth Discord Server

The main discussions are happening on our Discord server. 
To get an invite, send an email to `tg [at] torquem.ch` with your name, occupation, 
a brief explanation of why you want to join the Discord, and how you heard about Turbo-Geth.

#### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

## Team

Core contributors:

* Alexey Akhunov ([@realLedgerwatch](https://twitter.com/realLedgerwatch))

* Alex Sharov ([AskAlexSharov](https://github.com/AskAlexSharov))

* Andrew Ashikhmin ([yperbasis](https://github.com/yperbasis))

* Boris Petrov ([b00ris](https://github.com/b00ris))

* Eugene Danilenko ([JekaMas](https://github.com/JekaMas))

* Igor Mandrigin ([@mandrigin](https://twitter.com/mandrigin))

* Giulio Rebuffo

Thanks to:

* All contributors of Turbo-Geth

* All contributors of Go-Ethereum

* Our special respect and graditude is to the core team of [Go-Ethereum](https://github.com/ethereum/go-ethereum). Keep up the great job!

---

Happy testing! 🥤
