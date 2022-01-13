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
- In case of any bad block or header the chain will rewind itself to the last known good state and will start syncing from there.
- Still if you want to rewind a specific stage  of the chain, You can use the [Integration](https://github.com/maticnetwork/turbo-bor/tree/master/cmd/integration) tool.

Build Integration tool using:
```sh
make integration
```

To check current state of the chain
```sh
./build/bin/integration print_stages --datadir=<your_datadir>
```

To rewind state stages N block backwards
```sh
/build/bin/integration state_stages --datadir=<your_datadir> --unwind=N
```

To rewind block bodies by N block backwards
```sh
./build/bin/integration stage_bodies --datadir=<your_datadir> --unwind=N 
```

To rewind block headers by N block backwards
```sh
./build/bin/integration stage_headers --datadir=<your_datadir> --unwind=N
```

You can find more examples in the [Integration](https://github.com/maticnetwork/turbo-bor/tree/master/cmd/integration) tool.

### Checking Sync Status

To check sync status you must have an RPC Daemon up and running, Then use the below command:

```sh
curl localhost:8545 -X POST --data '{"jsonrpc":"2.0","method":"eth_syncing","id":1}' -H "Content-Type: application/json"
```

If you get
```
{"jsonrpc":"2.0","id":1,"result":false}
```
It means that the chain is synced.

OR

It will give you something like this
```
{"jsonrpc":"2.0","id":1,"result":{"currentBlock":"0x0","highestBlock":"0x165b6ad","stages":[{"stage_name":"Headers","block_number":"0x165b6ad"},{"stage_name":"BlockHashes","block_number":"0x165b6ad"},{"stage_name":"Bodies","block_number":"0x165b6ad"},{"stage_name":"Senders","block_number":"0x165b6ad"},{"stage_name":"Execution","block_number":"0x112bc2c"},{"stage_name":"Translation","block_number":"0x0"},{"stage_name":"HashState","block_number":"0x0"},{"stage_name":"IntermediateHashes","block_number":"0x0"},{"stage_name":"AccountHistoryIndex","block_number":"0x0"},{"stage_name":"StorageHistoryIndex","block_number":"0x0"},{"stage_name":"LogIndex","block_number":"0x0"},{"stage_name":"CallTraces","block_number":"0x0"},{"stage_name":"TxLookup","block_number":"0x0"},{"stage_name":"TxPool","block_number":"0x0"},{"stage_name":"Finish","block_number":"0x0"}]}
```
where detail of each stage is given with the block number at which it is at.

## Report Issues
- Feel free to Report any issues, Create an issue on [GitHub](https://github.com/maticnetwork/turbo-bor/issues/new/choose)
- Join Polygon community [on Discord](https://discord.gg/zdwkdvMNY2)
