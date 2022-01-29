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
- [System Requirements](#system-requirements)
- [Usage](#usage)
  - [How to Start](#how-to-start)
  - [Configurations](#how-to-config)
- [RPC Calls](#json-rpc-daemon)
  - [Build](#build)
  - [Local DB](#for-local-db)
  - [Remote DB](#for-remote-db)
  - [Request](#rpc-request)
- [Maintenance](#maintenance)
  - [Rewinding chain](#rewinding-chain)
  - [Sync Status](#checking-sync-status)
- [Reporting issues/concerns](#report-issues)

**Disclaimer**: This software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.

NB! <code>In-depth links are marked by the microscope sign (🔬) </code>

**Disclaimer: this software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.**

# System Requirements

Recommend 2Tb storage space on a single partition: 1.6Tb state, 200GB temp files (can symlink or mount
folder `<datadir>/etl-tmp` to another disk).

RAM: 16GB, 64-bit architecture, [Golang version >= 1.16](https://golang.org/doc/install), GCC 10+

<code>🔬 more info on disk storage is [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)) </code>

# Usage

### Getting Started

```sh
git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
cd erigon
make erigon
./build/bin/erigon
```

### Optional stages

There is an optional stage that can be enabled through flags:

- `--watch-the-burn`, Enable WatchTheBurn stage which keeps track of ETH issuance and is required to use `erigon_watchTheBurn`.

### Testnets

If you would like to give Erigon a try, but do not have spare 2Tb on your drive, a good option is to start syncing one
of the public testnets, Görli. It syncs much quicker, and does not take so much disk space:

```sh
git clone https://github.com/maticnetwork/turbo-bor
cd turbo-bor/
make turbo
```

This will build utilities in `.build/bin/turbo` to run TurboBor

Please note the `--datadir` option that allows you to store Erigon files in a non-default location, in this example,
in `goerli` subdirectory of the current directory. Name of the directory `--datadir` does not have to match the name of
the chain in `--chain`.

### Mining

Support only remote-miners.

- To enable, add `--mine --miner.etherbase=...` or `--mine --miner.miner.sigkey=...` flags.
- Other supported options: `--miner.extradata`, `--miner.notify`, `--miner.gaslimit`, `--miner.gasprice`
  , `--miner.gastarget`
- RPCDaemon supports methods: eth_coinbase , eth_hashrate, eth_mining, eth_getWork, eth_submitWork, eth_submitHashrate
- RPCDaemon supports websocket methods: newPendingTransaction
- TODO:
  - we don't broadcast mined blocks to p2p-network
    yet, [but it's easy to accomplish](https://github.com/ledgerwatch/erigon/blob/9b8cdc0f2289a7cef78218a15043de5bdff4465e/eth/downloader/downloader.go#L673)
  - eth_newPendingTransactionFilter
  - eth_newBlockFilter
  - eth_newFilter
  - websocket Logs

<code> 🔬 Detailed mining explanation is [here](/docs/mining.md).</code>

### Windows

Windows users may run erigon in 3 possible ways:

- Build executable binaries natively for Windows using provided `wmake.ps1` PowerShell script. Usage syntax is the same
  as `make` command so you have to run `.\wmake.ps1 [-target] <targetname>`. Example: `.\wmake.ps1 erigon` builds erigon
  executable. All binaries are placed in `.\build\bin\` subfolder. There are some requirements for a successful native
  build on windows :

  - [Git](https://git-scm.com/downloads) for Windows must be installed. If you're cloning this repository is very
    likely you already have it
  - [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.16
  - GNU CC Compiler at least version 10 (is highly suggested that you install `chocolatey` package manager - see
    following point)
  - If you need to build MDBX tools (i.e. `.\wmake.ps1 db-tools`)
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

- Use Docker : see [docker-compose.yml](./docker-compose.yml)

- Use WSL (Windows Subsystem for Linux) **strictly on version 2**. Under this option you can build Erigon just as you
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

See [RPC-Daemon docs](./cmd/rpcdaemon/README.md) for more details.

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
./build/bin/rpcdaemon --datadir=<your_data_dir> --private.api.addr=localhost:9090 --http.api=eth,web3,net,debug,trace,txpool,bor
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

Makefile creates the initial directories for erigon, prometheus and grafana. The PID namespace is shared between erigon
and rpcdaemon which is required to open Erigon's DB from another process (RPCDaemon local-mode).
See: https://github.com/ledgerwatch/erigon/pull/2392/files

Windows support for docker-compose is not ready yet. Please help us with .ps1 port

### Grafana dashboard

`docker-compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

### Prune old data

Disabled by default. To enable see `./build/bin/erigon --help` for flags `--prune`

# FAQ

### How much RAM do I need

- Baseline (ext4 SSD): 16Gb RAM sync takes 6 days, 32Gb - 5 days, 64Gb - 4 days
- +1 day on "zfs compression=off". +2 days on "zfs compression=on" (2x compression ratio). +3 days on btrfs.
- -1 day on NVMe

Detailed explanation: [./docs/programmers_guide/db_faq.md](./docs/programmers_guide/db_faq.md)

### Default Ports and Protocols / Firewalls?

#### `erigon` ports

| Port  | Protocol  |     Purpose      | Expose  |
| :---: | :-------: | :--------------: | :-----: |
| 30303 | TCP & UDP |  eth/66 peering  | Public  |
| 9090  |    TCP    | gRPC Connections | Private |

Typically 30303 and 30304 are exposed to the internet to allow incoming peering connections. 9090 is exposed only
internally for rpcdaemon or other connections, (e.g. rpcdaemon -> erigon)

#### `rpcdaemon` ports

|  Port   |  Protocol   |       Purpose       |  Expose   |
| :-----: | :---------: | :-----------------: | :-------: |
|  8545   |     TCP     |  HTTP & WebSockets  |  Private  |
| :-----: | :---------: | :-----------------: | :-------: |
|  8550   |     TCP     |        HTTP         |  Private  |

Typically 8545 is exposed only internally for JSON-RPC queries. Both HTTP and WebSocket connections are on the same port.
Typically 8550 is exposed only internally for the engineApi JSON-RPC queries

#### `sentry` ports

| Port  | Protocol  |     Purpose      | Expose  |
| :---: | :-------: | :--------------: | :-----: |
| 30303 | TCP & UDP |     Peering      | Public  |
| 9091  |    TCP    | gRPC Connections | Private |

Typically a sentry process will run one eth/xx protocl (e.g. eth/66) and will be exposed to the internet on 30303. Port
9091 is for internal gRCP connections (e.g erigon -> sentry)

#### Other ports

| Port | Protocol | Purpose | Expose  |
| :--: | :------: | :-----: | :-----: |
| 6060 |   TCP    |  pprof  | Private |
| 6060 |   TCP    | metrics | Private |

Optional flags can be enabled that enable pprof or metrics (or both) - however, they both run on 6060 by default, so
you'll have to change one if you want to run both at the same time. use `--help` with the binary for more info.

Reserved for future use: **gRPC ports**: `9092` consensus engine, `9093` snapshot downloader, `9094` TxPool

### How to get diagnostic for bug report?

- Get stack trace: `kill -SIGUSR1 <pid>`, get trace and stop: `kill -6 <pid>`
- Get CPU profiling: add `--pprof flag`
  run `go tool pprof -png http://127.0.0.1:6060/debug/pprof/profile\?seconds\=20 > cpu.png`
- Get RAM profiling: add `--pprof flag`
  run `go tool pprof -inuse_space -png http://127.0.0.1:6060/debug/pprof/heap > mem.png`

### How to run local devnet?

<code> 🔬 Detailed explanation is [here](/DEV_CHAIN.md).</code>

# Getting in touch

### Erigon Discord Server

The main discussions are happening on our Discord server. To get an invite, send an email to `tg [at] torquem.ch` with
your name, occupation, a brief explanation of why you want to join the Discord, and how you heard about Erigon.

### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

### Team

Core contributors (in alpabetical order of first names):

- Alex Sharov ([AskAlexSharov](https://twitter.com/AskAlexSharov))

- Alexey Akhunov ([@realLedgerwatch](https://twitter.com/realLedgerwatch))

- Andrea Lanfranchi([@AndreaLanfranchi](https://github.com/AndreaLanfranchi))

- Andrew Ashikhmin ([yperbasis](https://github.com/yperbasis))

- Artem Vorotnikov ([vorot93](https://github.com/vorot93))

- Boris Petrov ([b00ris](https://github.com/b00ris))

- Eugene Danilenko ([JekaMas](https://github.com/JekaMas))

- Igor Mandrigin ([@mandrigin](https://twitter.com/mandrigin))

- Giulio Rebuffo ([Giulio2002](https://github.com/Giulio2002))

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
- Still if you want to rewind a specific stage of the chain, You can use the [Integration](https://github.com/maticnetwork/turbo-bor/tree/master/cmd/integration) tool.

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
