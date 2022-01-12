# TurboBor
![Forks](https://img.shields.io/github/forks/maticnetwork/turbo-bor?style=social)
![Stars](https://img.shields.io/github/stars/maticnetwork/turbo-bor?style=social)
![Languages](https://img.shields.io/github/languages/count/maticnetwork/turbo-bor) 
![Issues](https://img.shields.io/github/issues/maticnetwork/turbo-bor) 
![PRs](https://img.shields.io/github/issues-pr-raw/maticnetwork/turbo-bor) 
![MIT License](https://img.shields.io/github/license/maticnetwork/turbo-bor)
![contributors](https://img.shields.io/github/contributors-anon/maticnetwork/turbo-bor) 
![size](https://img.shields.io/github/languages/code-size/maticnetwork/turbo-bor) 
![lines](https://img.shields.io/tokei/lines/github/maticnetwork/turbo-bor)
[![Discord](https://img.shields.io/discord/714888181740339261?color=1C1CE1&label=Polygon%20%7C%20Discord%20%F0%9F%91%8B%20&style=flat-square)](https://discord.gg/zdwkdvMNY2)
[![Twitter Follow](https://img.shields.io/twitter/follow/0xPolygon.svg?style=social)](https://twitter.com/0xPolygon)

TurboBor is the Official Golang implementation of the Matic protocol. It is a fork of [Erigon](https://github.com/ledgerwatch/erigon/) and EVM compabile.

Disclaimer: **This software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.**


## System Requirements

Recommend 2Tb storage space on a single partition: 1.3Tb state, 200GB temp files.

RAM: 16GB, 64-bit architecture, [Golang version >= 1.16](https://golang.org/doc/install)

## Building the source

Turbor is only available on Testnet(Mumbai) right now! (Mainnet work is in process)

```sh
git clone https://github.com/maticnetwork/turbo-bor
cd turbo-bor/
make turbo
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today
```

If you want to store TurboBor files in a non-default location 
```sh
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today --datadir=<your_data_dir>
```

## JSON-RPC daemon

In TurboBor unlike Bor the RPC calls are extracted out of the main binary into a separate daemon. This daemon can use both local or
remote DBs. That means, that this RPC daemon doesn't have to be running on the same machine as the main TurboBor binary or
it can run from a snapshot of a database for read-only calls.

#### **For local DB**

This is only possible if RPC daemon runs on the same computer as TurboBor. This mode uses shared memory access to the
database of TurboBor, which has better performance than accessing via TPC socket.
Provide both `--datadir` and `--private.api.addr` options:

```sh
make turbo
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today --datadir=<your_data_dir> --private.api.addr=localhost:9090
make rpcdaemon
./build/bin/rpcdaemon --datadir=<your_data_dir> --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool,bor
```

#### **For remote DB**

This works regardless of whether RPC daemon is on the same computer with TurboBor, or on a different one. They use TPC
socket connection to pass data between them. To use this mode, run TurboBor tminal window

```sh
make turbo
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today --datadir=<your_data_dir> --private.api.addr=<private_ip>:9090
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=<public_ip>:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool,bor
```


## Faster Initial Sync

TurbBor uses a rearchitected full sync algorithm from
[Bor](https://github.com/maticnetwork/bor) that is split into
"stages".

It uses the same network primitives and is compatible with regular bor nodes that are using full sync, you do
not need any special sync capabilities for TurboBor to sync.

Examples of stages are:

* Downloading headers;

* Downloading block bodies;

* Recovering senders' addresses;

* Executing blocks;

* Validating root hashes and building intermediate hashes for the state Merkle trie;

* [...]
