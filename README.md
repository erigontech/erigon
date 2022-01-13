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

TurboBor is a Golang implementation of the Matic protocol. It is a fork of [Erigon](https://github.com/ledgerwatch/erigon/) and EVM compabile. For now it only supports Syncing and RPC Calls, Mining is not supported yet!

TurboBor is only available on Testnet (Mumbai) right now (Mainnet testing is in process).


- [Setup](#build-from-source)
- [Usage](#usage)
    + [How to Start](#how-to-start)
    + [Configurations](#how-to-config)
- [RPC Calls](#json-rpc-daemon)
    + [Build](#build)
    + [Local DB](#for-local-db)
    + [Remote DB](#for-remote-db)
    + [Request](#rpc-request)
- [Maintenance](#maintenance)
    + [Rewinding chain](#rewinding-chain)
    + [Sync Status](#checking-sync-status)
- [Reporting issues/concerns](#report-issues)

**Disclaimer**: This software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.


## Build from source
Building TurboBor requires both a Go (version 1.16 or later) and a C compiler (GCC 10+). You can install them using your favourite package manager. Once the dependencies are installed, run

```sh
git clone https://github.com/maticnetwork/turbo-bor
cd turbo-bor/
make turbo
```
This will build utilities in `.build/bin/turbo` to run TurboBor

## Usage

### How to Start

To start TurboBor, run

```sh
turbo-bor --chain=mumbai 
```
- Use `chain=mumbai` for Mumbai testnet (Working)
- Use `chain=bor-mainnet` for Mainnet (Not properly tested yet)

### How to Config

- If you want to store TurboBor files in a non-default location use `--datadir`

  ```sh 
  turbo-bor --chain=mumbai --datadir=<your_data_dir>
  ```
- If you are not using local **hiemdall** use `--bor.heimdall=<your heimdall url>` (else by default it will try to connect to `localhost:1317`)

  ```sh
  turbo-bor --chain=mumbai --bor.heimdall=<your heimdall url> --datadir=<your_data_dir>
  ```
  Example if you want to connect to Mumbai use `--bor.heimdall=https://heimdall.api.matic.today`


## JSON-RPC daemon

In TurboBor unlike Bor the RPC calls are extracted out of the main binary into a separate daemon. This daemon can use both local or
remote DBs. That means, that this RPC daemon doesn't have to be running on the same machine as the main TurboBor binary or
it can run from a snapshot of a database for read-only calls.

### Build
To build RPC daemon, run

```sh
make rpcdaemon
```
### For local DB

This is only possible if RPC daemon runs on the same computer as TurboBor. This mode uses shared memory access to the
database of TurboBor, which has better performance than accessing via TPC socket.
Provide both `--datadir` and `--private.api.addr` options:

```sh
./build/bin/rpcdaemon --datadir=<your_data_dir> --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool,bor
```

### **For remote DB**

This works regardless of whether RPC daemon is on the same computer with TurboBor, or on a different one. They use TPC
socket connection to pass data between them. 

To use this mode, you have to give `--private.api.addr=<private_ip>:9090` while starting TurboBor where `private_ip` is the IP Address of system in which the TurboBor is running.

Run TurboBor in one terminal window

```sh
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today --datadir=<your_data_dir> --private.api.addr=<private_ip>:9090
```
On other Terminal, run

```sh
./build/bin/rpcdaemon --private.api.addr=<turbo_bor_ip>:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool,bor
```

The daemon should respond with something like:

`INFO [date-time] HTTP endpoint opened url=localhost:8545...`

### RPC Request

You can now make RPC request using the following curl command:

```sh
curl localhost:8545 -X POST --data '{"jsonrpc":"2.0","method":"bor_getSnapshot","params":["0x400"],"id":1}' -H "Content-Type: application/json"
```

## Maintenance

### Rewinding Chain

### Checking Sync Status

## Report Issues
- Feel free to Report any issues, Create an issue on [GitHub](https://github.com/maticnetwork/turbo-bor/issues/new/choose)
- Join Polygon community [on Discord](https://discord.gg/zdwkdvMNY2)
