# Turbo-Geth

[GoDoc](https://godoc.org/github.com/ledgerwatch/turbo-geth)

[![CircleCI](https://circleci.com/gh/ledgerwatch/turbo-geth.svg?style=svg)](https://circleci.com/gh/ledgerwatch/turbo-geth)

**Disclaimer: this software is currenly a tech preview. We will do our best to
keep it stable and make no breaking changes but we don't guarantee anything.
Things can and will break.**

Turbo-Geth is a fork of [Go-Ethereum](https://github.com/ethereum/go-ethereum) with focus on performance.

The current version is currently based on Go-Ethereum 1.9.15.

Usage:

```sh
> make tg
> ./build/bin/tg
```

## Key features 

#### 1. More Efficient State Storage

**Plain KV storage.** Turbo-Geth uses a key-value database and storing accounts and storage in
a simple way.

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
it can run from a snapshot of a database for read-only calls.


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

Currently supported JSON-RPC calls:

* `eth_call`

* `eth_getBlockByHash`

* `eth_getBlock`

* `eth_blockNumber`

* `eth_getBalance`

* `eth_getLogs`

* `eth_estimateGas`

* `debug_storageRangeAt`

* `debug_traceTransaction`

* `debug_accountRange`

* `debug_getModifiedAccountsByNumber`

* `debug_getModifiedAccountsByHash`


#### 4. REST API Daemon

Apart from JSON-RPC daemon, Turbo-Geth also contains REST API daemon. It uses
turbo-geth remote DB functionality.

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
