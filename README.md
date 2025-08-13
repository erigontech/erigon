# Erigon

[![Docs](https://img.shields.io/badge/docs-up-green)](https://docs.erigon.tech/)
[![Blog](https://img.shields.io/badge/blog-up-green)](https://erigon.tech/blog/)
[![Twitter](https://img.shields.io/twitter/follow/ErigonEth?style=social)](https://x.com/ErigonEth)
[![Build status](https://github.com/erigontech/erigon/actions/workflows/ci.yml/badge.svg)](https://github.com/erigontech/erigon/actions/workflows/ci.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=erigontech_erigon&metric=coverage)](https://sonarcloud.io/summary/new_code?id=erigontech_erigon)

Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency
frontier.

- [Erigon](#erigon)
- [System Requirements](#system-requirements)
- [Sync Times](#sync-times)
- [Usage](#usage)
    - [Getting Started](#getting-started)
    - [Datadir structure](#datadir-structure)
    - [History on cheap disk](#history-on-cheap-disk)
    - [Erigon3 datadir size](#erigon3-datadir-size)
    - [Erigon3 changes from Erigon2](#erigon3-changes-from-erigon2)
    - [Logging](#logging)
    - [Modularity](#modularity)
    - [Embedded Consensus Layer](#embedded-consensus-layer)
    - [Testnets](#testnets)
    - [Block Production (PoS Validator)](#block-production-pos-validator)
    - [Config Files TOML](#config-files-toml)
    - [Beacon Chain (Consensus Layer)](#beacon-chain-consensus-layer)
    - [Caplin](#caplin)
        - [Caplin's Usage](#caplins-usage)
    - [Multiple Instances / One Machine](#multiple-instances--one-machine)
    - [Dev Chain](#dev-chain)
- [Key features](#key-features)
    - [Faster Initial Sync](#faster-initial-sync)
    - [More Efficient State Storage](#more-efficient-state-storage)
    - [JSON-RPC daemon](#json-rpc-daemon)
    - [Grafana dashboard](#grafana-dashboard)
- [FAQ](#faq)
    - [Use as library](#use-as-library)
    - [Default Ports and Firewalls](#default-ports-and-firewalls)
        - [`erigon` ports](#erigon-ports)
        - [`caplin` ports](#caplin-ports)
        - [`beaconAPI` ports](#beaconapi-ports)
        - [`shared` ports](#shared-ports)
        - [`other` ports](#other-ports)
        - [Hetzner expecting strict firewall rules](#hetzner-expecting-strict-firewall-rules)
    - [Run as a separate user - `systemd` example](#run-as-a-separate-user---systemd-example)
    - [Grab diagnostic for bug report](#grab-diagnostic-for-bug-report)
    - [Run local devnet](#run-local-devnet)
    - [Docker permissions error](#docker-permissions-error)
    - [Public RPC](#public-rpc)
    - [RaspberryPI](#raspberrypi)
    - [Run all components by docker-compose](#run-all-components-by-docker-compose)
        - [Optional: Setup dedicated user](#optional-setup-dedicated-user)
        - [Environment Variables](#environment-variables)
        - [Run](#run)
    - [How to change db pagesize](#how-to-change-db-pagesize)
    - [Erigon3 perf tricks](#erigon3-perf-tricks)
    - [Windows](#windows)
- [Getting in touch](#getting-in-touch)
    - [Erigon Discord Server](#erigon-discord-server)
    - [Reporting security issues/concerns](#reporting-security-issuesconcerns)
- [Known issues](#known-issues)
    - [`htop` shows incorrect memory usage](#htop-shows-incorrect-memory-usage)
    - [Cloud network drives](#cloud-network-drives)
    - [Filesystem's background features are expensive](#filesystems-background-features-are-expensive)
    - [Gnome Tracker can kill Erigon](#gnome-tracker-can-kill-erigon)
    - [the --mount option requires BuildKit error](#the---mount-option-requires-buildkit-error)
    - [`cannot allocate memory` Erigon crashes due to kernel allocation limits](#erigon-crashes-due-to-kernel-allocation-limits)

<!--te-->

**Important defaults**: Erigon 3 is a Full Node by default. (Erigon 2 was an [Archive Node](https://ethereum.org/en/developers/docs/nodes-and-clients/archive-nodes/#what-is-an-archive-node) by default.)
Set `--prune.mode` to "archive" if you need an archive node or to "minimal" if you run a validator on a small disk (not allowed to change after first start).

<code>In-depth links are marked by the microscope sign (🔬) </code>

System Requirements
===================

RAM: >=32GB, [Golang >= 1.24](https://golang.org/doc/install); GCC 10+ or Clang; On Linux: kernel > v4. 64-bit
architecture.

- ArchiveNode Ethereum Mainnet: 1.6TB (May 2025). FullNode: 1.1TB (May 2025)
- ArchiveNode Gnosis: 640GB (May 2025). FullNode: 300GB (June 2024)
- ArchiveNode Polygon Mainnet: 4.1TB (April 2024). FullNode: 2Tb (April 2024)

SSD or NVMe. Do not recommend HDD - on HDD Erigon will always stay N blocks behind chain tip, but not fall behind.
Bear in mind that SSD performance deteriorates when close to capacity. CloudDrives (like
gp3): Blocks Execution is slow
on [cloud-network-drives](https://github.com/erigontech/erigon?tab=readme-ov-file#cloud-network-drives)

🔬 More details on [Erigon3 datadir size](#erigon3-datadir-size)

🔬 More details on what type of data stored [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)

Sync Times
==========

These are the  approximate sync times syncing from scratch to the tip of the chain (results may vary depending on hardware and bandwidth).


| Chain      | Archive         | Full           | Minimal        |
|------------|-----------------|----------------|----------------|
| Ethereum   | 7 Hours, 55 Minutes | 4 Hours, 23 Minutes | 1 Hour, 41 Minutes |
| Gnosis     | 2 Hours, 10 Minutes | 1 Hour, 5 Minutes  | 33 Minutes      |
| Polygon    | 1 Day, 21 Hours    | 21 Hours, 41 Minutes | 11 Hours, 54 Minutes |

Usage
=====

### Getting Started

[Release Notes and Binaries](https://github.com/erigontech/erigon/releases)

Build latest release (this will be suitable for most users just wanting to run a node):

```sh
git clone --branch release/<x.xx> --single-branch https://github.com/erigontech/erigon.git
cd erigon
make erigon
./build/bin/erigon
```

Use `--datadir` to choose where to store data.

Use `--chain=gnosis` for [Gnosis Chain](https://www.gnosis.io/), `--chain=bor-mainnet` for Polygon Mainnet,
and `--chain=amoy` for Polygon Amoy.
For Gnosis Chain you need a [Consensus Layer](#beacon-chain-consensus-layer) client alongside
Erigon (https://docs.gnosischain.com/category/step--3---run-consensus-client).

Running `make help` will list and describe the convenience commands available in the [Makefile](./Makefile).

### Upgrading from 3.0 to 3.1

1. Backup your datadir.
2. Upgrade your Erigon binary.
3. OPTIONAL: Upgrade snapshot files.
   1. Update snapshot file names. To do this either run Erigon 3.1 until the sync stage completes, or run `erigon snapshots update-to-new-ver-format --datadir /your/datadir`.
   2. Reset your datadir so that Erigon will sync to a newer snapshot. `erigon snapshots reset --datadir /your/datadir`. See [Resetting snapshots](#Resetting-snapshots) for more details.
4. Run Erigon 3.1. Your snapshots file names will be migrated automatically if you didn't do this manually. If you reset your datadir, Erigon will sync to the latest remote snapshots.

### Datadir structure

```sh
datadir        
    chaindata     # "Recently-updated Latest State", "Recent History", "Recent Blocks"
    snapshots     # contains `.seg` files - it's old blocks
        domain    # Latest State
        history   # Historical values 
        idx       # InvertedIndices: can search/filtering/union/intersect them - to find historical data. like eth_getLogs or trace_transaction
        accessor # Additional (generated) indices of history - have "random-touch" read-pattern. They can serve only `Get` requests (no search/filters).
    txpool        # pending transactions. safe to remove.
    nodes         # p2p peers. safe to remove.
    temp          # used to sort data bigger than RAM. can grow to ~100gb. cleaned at startup.
   
# There is 4 domains: account, storage, code, commitment 
```

See the [lib](db/downloader/README.md) and [cmd](cmd/downloader/README.md) READMEs for more information.

### History on cheap disk

If you can afford store datadir on 1 nvme-raid - great. If can't - it's possible to store history on cheap drive.

```sh
# place (or ln -s) `datadir` on slow disk. link some sub-folders to fast (low-latency) disk.
# Example: what need link to fast disk to speedup execution
datadir        
    chaindata   # link to fast disk
    snapshots   
        domain    # link to fast disk
        history   
        idx       
        accessor 
    temp # buffers to sort data >> RAM. sequential-buffered IO - is slow-disk-friendly   

# Example: how to speedup history access: 
#   - go step-by-step - first try store `accessor` on fast disk
#   - if speed is not good enough: `idx`
#   - if still not enough: `history` 
```

### Erigon3 datadir size

```sh
# eth-mainnet - archive - Nov 2024

du -hsc /erigon/chaindata
15G 	/erigon/chaindata

du -hsc /erigon/snapshots/* 
120G 	/erigon/snapshots/accessor
300G	/erigon/snapshots/domain
280G	/erigon/snapshots/history
430G	/erigon/snapshots/idx
2.3T	/erigon/snapshots
```

```sh
# bor-mainnet - archive - Nov 2024

du -hsc /erigon/chaindata
20G 	/erigon/chaindata

du -hsc /erigon/snapshots/* 
360G	/erigon-data/snapshots/accessor
1.1T	/erigon-data/snapshots/domain
750G	/erigon-data/snapshots/history
1.5T	/erigon-data/snapshots/idx
4.9T	/erigon/snapshots
```

### Erigon3 changes from Erigon2

- **Initial sync doesn't re-exec from 0:** downloading 99% LatestState and History
- **Per-Transaction granularity of history** (Erigon2 had per-block). Means:
    - Can execute 1 historical transaction - without executing it's block
    - If account X change V1->V2->V1 within 1 block (different transactions): `debug_getModifiedAccountsByNumber` return
      it
    - Erigon3 doesn't store Logs (aka Receipts) - it always re-executing historical txn (but it's cheaper)
- **Validator mode**: added. `--internalcl` is enabled by default. to disable use `--externalcl`.
- **Store most of data in immutable files (segments/snapshots):**
    - can symlink/mount latest state to fast drive and history to cheap drive
  - `chaindata` is less than `15gb`. It's ok to `rm -rf chaindata`. (to prevent grow: recommend `--batchSize <= 1G`)
- **`--prune` flags changed**: see `--prune.mode` (default: `full`, archive: `archive`, EIP-4444: `minimal`)
- **Other changes:**
    - ExecutionStage included many E2 stages: stage_hash_state, stage_trie, log_index, history_index, trace_index
    - Restart doesn't loose much partial progress: `--sync.loop.block.limit=5_000` enabled by default

### Logging

_Flags:_

- `verbosity`
- `log.console.verbosity` (overriding alias for `verbosity`)
- `log.json`
- `log.console.json` (alias for `log.json`)
- `log.dir.path`
- `log.dir.prefix`
- `log.dir.verbosity`
- `log.dir.json`
- `torrent.verbosity`

In order to log only to the stdout/stderr the `--verbosity` (or `log.console.verbosity`) flag can be used to supply an
int value specifying the highest output log level:

```
  LvlCrit = 0
  LvlError = 1
  LvlWarn = 2
  LvlInfo = 3
  LvlDebug = 4
  LvlTrace = 5
```

To set an output dir for logs to be collected on disk, please set `--log.dir.path` If you want to change the filename
produced from `erigon` you should also set the `--log.dir.prefix` flag to an alternate name. The
flag `--log.dir.verbosity` is
also available to control the verbosity of this logging, with the same int value as above, or the string value e.g. '
debug' or 'info'. Default verbosity is 'debug' (4), for disk logging.

Log format can be set to json by the use of the boolean flags `log.json` or `log.console.json`, or for the disk
output `--log.dir.json`.

#### Torrent client logging

The torrent client in the Downloader logs to `logs/torrent.log` at the level specified by `torrent.verbosity` or WARN, whichever is lower. Logs at `torrent.verbosity` or higher are also passed through to the top level Erigon dir and console loggers (which must have their own levels set low enough to log the messages in their respective handlers).

### Resetting snapshots

Erigon 3.1 adds the command `erigon snapshots reset`. This modifies your datadir so that Erigon will sync to the latest remote snapshots on next run. You must pass `--datadir`. If the chain cannot be inferred from the chaindata, you must pass `--chain`. `--local=false` will prevent locally generated snapshots from also being removed. Pass `--dry-run` and/or `--verbosity=5` for more information.

### Modularity

Erigon by default is "all in one binary" solution, but it's possible start TxPool as separated processes.
Same true about: JSON RPC layer (RPCDaemon), p2p layer (Sentry), history download layer (Downloader), consensus.
Don't start services as separated processes unless you have clear reason for it: resource limiting, scale, replace by
your own implementation, security.
How to start Erigon's services as separated processes, see in [docker-compose.yml](./docker-compose.yml).
Each service has own `./cmd/*/README.md` file.
[Erigon Blog](https://erigon.tech/blog/).

### Embedded Consensus Layer

Built-in consensus for Ethereum Mainnet, Sepolia, Holesky, Hoodi, Gnosis, Chiado.
To use external Consensus Layer: `--externalcl`.

### Testnets

If you would like to give Erigon a try: a good option is to start syncing one of the public testnets, Holesky (or Amoy).
It syncs much quicker, and does not take so much disk space:

```sh
git clone https://github.com/erigontech/erigon.git
cd erigon
make erigon
./build/bin/erigon --datadir=<your_datadir> --chain=holesky --prune.mode=full
```

Please note the `--datadir` option that allows you to store Erigon files in a non-default location. Name of the
directory `--datadir` does not have to match the name of the chain in `--chain`.

### Block Production (PoS Validator)

Block production is fully supported for Ethereum & Gnosis Chain. It is still experimental for Polygon.

### Config Files TOML

You can set Erigon flags through a TOML configuration file with the flag `--config`. The flags set in the
configuration file can be overwritten by writing the flags directly on Erigon command line

`./build/bin/erigon --config ./config.toml --chain=sepolia`

Assuming we have `chain : "mainnet"` in our configuration file, by adding `--chain=sepolia` allows the overwrite of the
flag inside of the toml configuration file and sets the chain to sepolia

```toml
datadir = 'your datadir'
port = 1111
chain = "mainnet"
http = true
"private.api.addr"="localhost:9090"

"http.api" = ["eth","debug","net"]
```

### Beacon Chain (Consensus Layer)

Erigon can be used as an Execution Layer (EL) for Consensus Layer clients (CL). Default configuration is OK.

If your CL client is on a different device, add `--authrpc.addr 0.0.0.0` ([Engine API] listens on localhost by default)
as well as `--authrpc.vhosts <CL host>` where `<CL host>` is your source host or `any`.

[Engine API]: https://github.com/ethereum/execution-apis/blob/main/src/engine

In order to establish a secure connection between the Consensus Layer and the Execution Layer, a JWT secret key is
automatically generated.

The JWT secret key will be present in the datadir by default under the name of `jwt.hex` and its path can be specified
with the flag `--authrpc.jwtsecret`.

This piece of info needs to be specified in the Consensus Layer as well in order to establish connection successfully.
More information can be found [here](https://github.com/ethereum/execution-apis/blob/main/src/engine/authentication.md).

Once Erigon is running, you need to point your CL client to `<erigon address>:8551`,
where `<erigon address>` is either `localhost` or the IP address of the device running Erigon, and also point to the JWT
secret path created by Erigon.

### Caplin

Caplin is a full-fledged validating Consensus Client like Prysm, Lighthouse, Teku, Nimbus and Lodestar. Its goal is:

* provide better stability
* Validation of the chain
* Stay in sync
* keep the execution of blocks on chain tip
* serve the Beacon API using a fast and compact data model alongside low CPU and memory usage.

The main reason why developed a new Consensus Layer is to experiment with the possible benefits that could come with it.
For example, The Engine API does not work well with Erigon. The Engine API sends data one block at a time, which does
not suit how Erigon works. Erigon is designed to handle many blocks simultaneously and needs to sort and process data
efficiently. Therefore, it would be better for Erigon to handle the blocks independently instead of relying on the
Engine API.

#### Caplin's Usage

Caplin is be enabled by default. to disable it and enable the Engine API, use the `--externalcl` flag. from that point
on, an external Consensus Layer will not be need
anymore.

Caplin also has an archival mode for historical states and blocks. it can be enabled through the `--caplin.archive`
flag.
In order to enable the caplin's Beacon API, the flag `--beacon.api=<namespaces>` must be added.
e.g: `--beacon.api=beacon,builder,config,debug,node,validator,lighthouse` will enable all endpoints. 
Note: enabling the Beacon API will lead to a 6 GB higher RAM usage

### Multiple Instances / One Machine

Define 6 flags to avoid conflicts: `--datadir --port --http.port --authrpc.port --torrent.port --private.api.addr`.
Example of multiple chains on the same machine:

```
# mainnet
./build/bin/erigon --datadir="<your_mainnet_data_path>" --chain=mainnet --port=30303 --http.port=8545 --authrpc.port=8551 --torrent.port=42069 --private.api.addr=127.0.0.1:9090 --http --ws --http.api=eth,debug,net,trace,web3,erigon


# sepolia
./build/bin/erigon --datadir="<your_sepolia_data_path>" --chain=sepolia --port=30304 --http.port=8546 --authrpc.port=8552 --torrent.port=42068 --private.api.addr=127.0.0.1:9091 --http --ws --http.api=eth,debug,net,trace,web3,erigon
```

Quote your path if it has spaces.

### Dev Chain

<code> 🔬 Detailed explanation is [DEV_CHAIN](/docs/DEV_CHAIN.md).</code>

Key features
============

### Faster Initial Sync

On good network bandwidth EthereumMainnet FullNode syncs in 3
hours: [OtterSync](https://erigon.substack.com/p/erigon-3-alpha-2-introducing-blazingly) can sync

### More Efficient State Storage

**Flat KV storage.** Erigon uses a key-value database and storing accounts and storage in a simple way.

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, Erigon uses temporary files to preprocess data before inserting it into the main
DB. That reduces write amplification and DB inserts are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](https://github.com/erigontech/erigon/blob/main/db/etl/README.md).</code>

**Plain state**

**Single accounts/state trie**. Erigon uses a single Merkle trie for both accounts and the storage.

<code> 🔬 [Staged Sync Readme](/docs/readthedocs/source/stagedsync.rst)</code>

### JSON-RPC daemon

Most of Erigon's components (txpool, rpcdaemon, snapshots downloader, sentry, ...) can work inside Erigon and as
independent process on same Server (or another Server). Example:

```sh
make erigon rpcdaemon
./build/bin/erigon --datadir=/my --http=false
# To run RPCDaemon as separated process: use same `--datadir` as Erigon
./build/bin/rpcdaemon --datadir=/my --http.api=eth,erigon,web3,net,debug,trace,txpool --ws
```

- Supported JSON-RPC
  calls: [eth](./rpc/jsonrpc/eth_api.go), [debug](./rpc/jsonrpc/debug_api.go), [net](./rpc/jsonrpc/net_api.go), [web3](./rpc/jsonrpc/web3_api.go)
- increase throughput by: `--rpc.batch.concurrency`, `--rpc.batch.limit`, `--db.read.concurrency`
- increase throughput by disabling: `--http.compression`, `--ws.compression`

<code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code>

### Grafana dashboard

`docker compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

FAQ
================

### Use as library

```
# please use git branch name (or commit hash). don't use git tags
go mod edit -replace github.com/erigontech/erigon-lib=github.com/erigontech/erigon/erigon-lib@5498f854e44df5c8f0804ff4f0747c0dec3caad5
go get github.com/erigontech/erigon@main
go mod tidy
```

### Default Ports and Firewalls

#### `erigon` ports

| Component | Port  | Protocol  | Purpose                     | Should Expose |
|-----------|-------|-----------|-----------------------------|---------------|
| engine    | 9090  | TCP       | gRPC Server                 | Private       |
| engine    | 42069 | TCP & UDP | Snap sync (Bittorrent)      | Public        |
| engine    | 8551  | TCP       | Engine API (JWT auth)       | Private       |
| sentry    | 30303 | TCP & UDP | eth/68 peering              | Public        |
| sentry    | 30304 | TCP & UDP | eth/67 peering              | Public        |
| sentry    | 9091  | TCP       | incoming gRPC Connections   | Private       |
| rpcdaemon | 8545  | TCP       | HTTP & WebSockets & GraphQL | Private       |
| shutter   | 23102 | TCP       | Peering                     | Public        |

Typically, 30303 and 30304 are exposed to the internet to allow incoming peering connections. 9090 is exposed only
internally for rpcdaemon or other connections, (e.g. rpcdaemon -> erigon).
Port 8551 (JWT authenticated) is exposed only internally for [Engine API] JSON-RPC queries from the Consensus Layer
node.

#### `caplin` ports

| Component | Port | Protocol | Purpose | Should Expose |
|-----------|------|----------|---------|---------------|
| sentinel  | 4000 | UDP      | Peering | Public        |
| sentinel  | 4001 | TCP      | Peering | Public        |

In order to configure the ports, use:

```
   --caplin.discovery.addr value                                                    Address for Caplin DISCV5 protocol (default: "127.0.0.1")
   --caplin.discovery.port value                                                    Port for Caplin DISCV5 protocol (default: 4000)
   --caplin.discovery.tcpport value                                                 TCP Port for Caplin DISCV5 protocol (default: 4001)
```

#### `beaconAPI` ports

| Component | Port | Protocol | Purpose | Should Expose |
|-----------|------|----------|---------|---------------|
| REST      | 5555 | TCP      | REST    | Public        |

#### `shared` ports

| Component | Port | Protocol | Purpose | Should Expose |
|-----------|------|----------|---------|---------------|
| all       | 6060 | TCP      | pprof   | Private       |
| all       | 6061 | TCP      | metrics | Private       |

Optional flags can be enabled that enable pprof or metrics (or both). Use `--help` with the binary for more info.

#### `other` ports

Reserved for future use: **gRPC ports**: `9092` consensus engine, `9093` snapshot downloader, `9094` TxPool

#### Hetzner expecting strict firewall rules

```
0.0.0.0/8             "This" Network             RFC 1122, Section 3.2.1.3
10.0.0.0/8            Private-Use Networks       RFC 1918
100.64.0.0/10         Carrier-Grade NAT (CGN)    RFC 6598, Section 7
127.16.0.0/12         Private-Use Networks       RFC 1918
169.254.0.0/16        Link Local                 RFC 3927
172.16.0.0/12         Private-Use Networks       RFC 1918
192.0.0.0/24          IETF Protocol Assignments  RFC 5736
192.0.2.0/24          TEST-NET-1                 RFC 5737
192.88.99.0/24        6to4 Relay Anycast         RFC 3068
192.168.0.0/16        Private-Use Networks       RFC 1918
198.18.0.0/15         Network Interconnect
Device Benchmark Testing   RFC 2544
198.51.100.0/24       TEST-NET-2                 RFC 5737
203.0.113.0/24        TEST-NET-3                 RFC 5737
224.0.0.0/4           Multicast                  RFC 3171
240.0.0.0/4           Reserved for Future Use    RFC 1112, Section 4
255.255.255.255/32    Limited Broadcast          RFC 919, Section 7
RFC 922, Section 7
```

Same
in [IpTables syntax](https://ethereum.stackexchange.com/questions/6386/how-to-prevent-being-blacklisted-for-running-an-ethereum-client/13068#13068)

### Run as a separate user - `systemd` example

Running erigon from `build/bin` as a separate user might produce an error:

```sh
error while loading shared libraries: libsilkworm_capi.so: cannot open shared object file: No such file or directory
```

The library needs to be *installed* for another user using `make DIST=<path> install`. You could use `$HOME/erigon`
or `/opt/erigon` as the installation path, for example:

```sh
make DIST=/opt/erigon install
```

### Grab diagnostic for bug report

- Get stack trace: `kill -SIGUSR1 <pid>`, get trace and stop: `kill -6 <pid>`
- Get CPU profiling: add `--pprof` flag and run  
  `go tool pprof -png  http://127.0.0.1:6060/debug/pprof/profile\?seconds\=20 > cpu.png`
- Get RAM profiling: add `--pprof` flag and run  
  `go tool pprof -inuse_space -png  http://127.0.0.1:6060/debug/pprof/heap > mem.png`

### Run local devnet

<code> 🔬 Detailed explanation is [here](/docs/DEV_CHAIN.md).</code>

### Docker permissions error

Docker uses user erigon with UID/GID 1000 (for security reasons). You can see this user being created in the Dockerfile.
Can fix by giving a host's user ownership of the folder, where the host's user UID/GID is the same as the docker's user
UID/GID (1000).
More details
in [post](https://www.fullstaq.com/knowledge-hub/blogs/docker-and-the-host-filesystem-owner-matching-problem)

### Public RPC

- `--txpool.nolocals=true`
- don't add `admin` in `--http.api` list
- `--http.corsdomain="*"` is bad-practice: set exact hostname or IP
- protect from DOS by reducing: `--rpc.batch.concurrency`, `--rpc.batch.limit`

### RaspberryPI

https://github.com/mathMakesArt/Erigon-on-RPi-4

### Run all components by docker-compose

Docker allows for building and running Erigon via containers. This alleviates the need for installing build dependencies
onto the host OS.

#### Optional: Setup dedicated user

User UID/GID need to be synchronized between the host OS and container so files are written with correct permission.

You may wish to setup a dedicated user/group on the host OS, in which case the following `make` targets are available.

```sh
# create "erigon" user
make user_linux
# or
make user_macos
```

#### Environment Variables

There is a `.env.example` file in the root of the repo.

* `DOCKER_UID` - The UID of the docker user
* `DOCKER_GID` - The GID of the docker user
* `XDG_DATA_HOME` - The data directory which will be mounted to the docker containers

If not specified, the UID/GID will use the current user.

A good choice for `XDG_DATA_HOME` is to use the `~erigon/.ethereum` directory created by helper
targets `make user_linux` or `make user_macos`.

#### Run

Check permissions: In all cases, `XDG_DATA_HOME` (specified or default) must be writable by the user UID/GID in docker,
which will be determined by the `DOCKER_UID` and `DOCKER_GID` at build time. If a build or service startup is failing
due to permissions, check that all the directories, UID, and GID controlled by these environment variables are correct.

Next command starts: Erigon on port 30303, rpcdaemon on port 8545, prometheus on port 9090, and grafana on port 3000.

```sh
#
# Will mount ~/.local/share/erigon to /home/erigon/.local/share/erigon inside container
#
make docker-compose

#
# or
#
# if you want to use a custom data directory
# or, if you want to use different uid/gid for a dedicated user
#
# To solve this, pass in the uid/gid parameters into the container.
#
# DOCKER_UID: the user id
# DOCKER_GID: the group id
# XDG_DATA_HOME: the data directory (default: ~/.local/share)
#
# Note: /preferred/data/folder must be read/writeable on host OS by user with UID/GID given
#       if you followed above instructions
#
# Note: uid/gid syntax below will automatically use uid/gid of running user so this syntax
#       is intended to be run via the dedicated user setup earlier
#
DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose

#
# if you want to run the docker, but you are not logged in as the $ERIGON_USER
# then you'll need to adjust the syntax above to grab the correct uid/gid
#
# To run the command via another user, use
#
ERIGON_USER=erigon
sudo -u ${ERIGON_USER} DOCKER_UID=$(id -u ${ERIGON_USER}) DOCKER_GID=$(id -g ${ERIGON_USER}) XDG_DATA_HOME=~${ERIGON_USER}/.ethereum DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```

Makefile creates the initial directories for erigon, prometheus and grafana. The PID namespace is shared between erigon
and rpcdaemon which is required to open Erigon's DB from another process (RPCDaemon local-mode).
See: https://github.com/erigontech/erigon/pull/2392/files

If your docker installation requires the docker daemon to run as root (which is by default), you will need to prefix
the command above with `sudo`. However, it is sometimes recommended running docker (and therefore its containers) as a
non-root user for security reasons. For more information about how to do this, refer to
[this article](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

### How to change db pagesize

[post](https://github.com/erigontech/erigon/blob/main/cmd/integration/Readme.md#copy-data-to-another-db)

### Erigon3 perf tricks

- on BorMainnet may help: `--sync.loop.block.limit=10_000`
- on cloud-drives (good throughput, bad latency) - can enable OS's brain to pre-fetch: `SNAPSHOT_MADV_RND=false`
- can lock latest state in RAM - to prevent from eviction (node may face high historical RPC traffic without impacting
  Chain-Tip perf):

```
vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt
ls /mnt/erigon/snapshots/domain/*.kv | parallel vmtouch -vdlw

# if it failing with "can't allocate memory", try: 
sync && sudo sysctl vm.drop_caches=3
echo 1 > /proc/sys/vm/compact_memory
```

### Windows

Windows users may run erigon in 3 possible ways:

* Build executable binaries natively for Windows using provided `wmake.ps1` PowerShell script. Usage syntax is the same
  as `make` command so you have to run `.\wmake.ps1 [-target] <targetname>`. Example: `.\wmake.ps1 erigon` builds erigon
  executable. All binaries are placed in `.\build\bin\` subfolder. There are some requirements for a successful native
  build on windows :
    * [Git](https://git-scm.com/downloads) for Windows must be installed. If you're cloning this repository is very
      likely you already have it
    * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.24
    * GNU CC Compiler at least version 13 (is highly suggested that you install `chocolatey` package manager - see
      following point)
    * If you need to build MDBX tools (i.e. `.\wmake.ps1 db-tools`)
      then [Chocolatey package manager](https://chocolatey.org/) for Windows must be installed. By Chocolatey you need
      to install the following components : `cmake`, `make`, `mingw` by `choco install cmake make mingw`. Make sure
      Windows System "Path" variable has:
      C:\ProgramData\chocolatey\lib\mingw\tools\install\mingw64\bin

  **Important note about Anti-Viruses**
  During MinGW's compiler detection phase some temporary executables are generated to test compiler capabilities. It's
  been reported some anti-virus programs detect those files as possibly infected by `Win64/Kryptic.CIS` trojan horse (or
  a variant of it). Although those are false positives we have no control over 100+ vendors of security products for
  Windows and their respective detection algorithms and we understand this might make your experience with Windows
  builds uncomfortable. To workaround the issue you might either set exclusions for your antivirus specifically
  for `build\bin\mdbx\CMakeFiles` sub-folder of the cloned repo or you can run erigon using the following other two
  options

* Use Docker :  see [docker-compose.yml](./docker-compose.yml)

* Use WSL (Windows Subsystem for Linux) **strictly on version 2**. Under this option you can build Erigon just as you
  would on a regular Linux distribution. You can point your data also to any of the mounted Windows partitions (
  eg. `/mnt/c/[...]`, `/mnt/d/[...]` etc) but in such case be advised performance is impacted: this is due to the fact
  those mount points use `DrvFS` which is
  a [network file system](https://github.com/erigontech/erigon?tab=readme-ov-file#cloud-network-drives)
  and, additionally, MDBX locks the db for exclusive access which implies only one process at a time can access data.
  This has consequences on the running of `rpcdaemon` which has to be configured as [Remote DB](#json-rpc-daemon) even if
  it is executed on the very same computer. If instead your data is hosted on the native Linux filesystem non
  limitations apply.
  **Please also note the default WSL2 environment has its own IP address which does not match the one of the network
  interface of Windows host: take this into account when configuring NAT for port 30303 on your router.**

Getting in touch
================

### Erigon Discord Server

The main discussions are happening on our Discord server. To get an invite, send an email to `bloxster [at] proton.me`
with your name, occupation, a brief explanation of why you want to join the Discord, and how you heard about Erigon.

### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

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
OS automatically frees this cache any time it needs memory. Smaller "page cache size" may not impact performance of
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
main Erigon optimizations: "reduce Disk random access".
"Blocks Execution stage" still does many random reads - this is reason why it's slowest stage. We do not recommend
running multiple genesis syncs on same Disk. If genesis sync passed, then it's fine to run multiple Erigon instances on
same Disk.

### Cloud network drives

(Like gp3)
You may read: https://github.com/erigontech/erigon/issues/1516#issuecomment-811958891
In short: network-disks are bad for blocks execution - because they are designed for parallel/batch workloads (databases
with many parallel requests). But blocks execution in Erigon is non-parallel and using blocking-io.

What can do:

- reduce disk latency (not throughput, not iops)
    - use latency-critical cloud-drives
    - or attached-NVMe (at least for initial sync)
- increase RAM
- if you throw enough RAM, then can set env variable `ERIGON_SNAPSHOT_MADV_RND=false`
- Use `--db.pagesize=64kb` (less fragmentation, more IO)
- Or use Erigon3 (it also sensitive for disk-latency - but it will download 99% of history)

### Filesystem's background features are expensive

For example: btrfs's autodefrag option - may increase write IO 100x times

### Gnome Tracker can kill Erigon

[Gnome Tracker](https://wiki.gnome.org/Attic/Tracker) - detecting miners and kill them.

### the --mount option requires BuildKit error

For anyone else that was getting the BuildKit error when trying to start Erigon the old way you can use the below...

```sh
XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```

### Erigon crashes due to kernel allocation limits

Error message: `cannot allocate memory`.

Add to `/etc/sysctl.conf` (or add .conf file in `/etc/sysctl.d/`)

```
vm.overcommit_memory = 1 
vm.max_map_count = 16777216 
```
---------
