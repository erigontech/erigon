<div align="center">

# Erigon

[![Documentation](https://img.shields.io/badge/docs-docs.erigon.tech-blue)](https://docs.erigon.tech)
[![Blog](https://img.shields.io/badge/blog-erigon.tech%2Fblog-blue)](https://erigon.tech/blog/)
[![Twitter Follow](https://img.shields.io/twitter/follow/ErigonEth?style=social)](https://twitter.com/ErigonEth)

Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency
frontier.

[![Build status](https://github.com/erigontech/erigon/actions/workflows/ci.yml/badge.svg)](https://github.com/erigontech/erigon/actions/workflows/ci.yml) 
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=erigontech_erigon&metric=coverage)](https://sonarcloud.io/summary/new_code?id=erigontech_erigon)

</div>

## Overview

Erigon is an implementation of Ethereum focused on efficiency and optimization. It provides both execution and consensus layer capabilities, designed to be highly performant while maintaining full compatibility with the Ethereum protocol.

**Important defaults**: Erigon 3 is a Full Node by default. (Erigon 2 was an [Archive Node](https://ethereum.org/en/developers/docs/nodes-and-clients/archive-nodes/#what-is-an-archive-node) by default.)
Set `--prune.mode` to "archive" if you need an archive node or to "minimal" if you run a validator on a small disk (not allowed to change after first start).

<code>In-depth links are marked by the microscope sign (🔬) </code>

## 🖥️ System Requirements

### Hardware
- **RAM**: 16GB minimum
- **Storage**:
  - **Type**: SSD or NVMe required (HDD not recommended)
- **Space Requirements** (as of April 2024)

  | Network | Archive Node | Full Node |
  |---------|-------------|------------|
  | Ethereum | 2.0 TB | 1.1 TB |
  | Gnosis | 1.7 TB | 300 GB |
  | Polygon | 4.1 TB | 2.0 TB |

### Software
- **OS**: 64-bit architecture
  - Linux: Kernel v4+
- **Compiler**: GCC 10+ or Clang
- **Go**: [Version 1.23 or higher](https://golang.org/doc/install)

> ⚠️ **Note**: SSD performance may degrade when near capacity. [Cloud network drives](https://github.com/erigontech/erigon?tab=readme-ov-file#cloud-network-drives) like GP3 may experience slower block execution.

🔬 Learn more about:
- [Erigon3 datadir size](#erigon3-datadir-size)
- [Data storage architecture](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)

## ⚡ Sync Performance

Below are approximate sync times from genesis to chain tip. Results may vary based on hardware and network conditions:

| Network  | Archive Mode | Full Mode | Minimal Mode |
|----------|-------------|-----------|--------------|
| Ethereum | 7h 55m | 4h 23m | 1h 41m |
| Gnosis   | 2h 10m | 1h 5m  | 33m |
| Polygon  | 45h | 21h 41m | 11h 54m |

## 🚀 Getting Started

### Installation

Choose one of the following installation methods:

#### Pre-built Binaries
- Download from our [Releases Page](https://github.com/erigontech/erigon/releases)

#### Build from Source
```bash
# Clone the repository
git clone --branch release/<x.xx> --single-branch https://github.com/erigontech/erigon.git
cd erigon

# Build Erigon
make erigon

# Start the node
./build/bin/erigon
```

### Basic Usage

```bash
# Start Erigon with custom data directory
erigon --datadir /path/to/data

# Increase download speed
erigon --torrent.download.rate=10G

# Run on different networks
erigon --chain=gnosis      # Gnosis Chain
erigon --chain=bor-mainnet # Polygon Mainnet
erigon --chain=amoy        # Polygon Amoy
```
> 💡 **Tip**: Run `make help` to see all available build commands

### Testnets

If you would like to give Erigon a try: a good option is to start syncing one of the public testnets, such as Sepolia.

It syncs much quicker, and does not take much disk space:

```sh
git clone https://github.com/erigontech/erigon.git
cd erigon
make erigon
./build/bin/erigon --datadir=<your_datadir> --chain=sepolia
```

🔬 For detailed configuration options, see:
- [Downloader Configuration](./cmd/downloader/readme.md)
- [Chain-specific Setup](https://docs.gnosischain.com/category/step--3---run-consensus-client)


## ✨ Key Features

### 🏃 High-Performance Sync
- **OtterSync Technology**: Full node sync in ~3 hours on good network and hardware
- **Optimized Initial Sync**: [Learn more about OtterSync](https://erigon.substack.com/p/erigon-3-alpha-2-introducing-blazingly)
- **Built-in Consensus Client**: No need for extra setup, and comes with more performance. 

### 📂 Advanced Storage Architecture

#### Efficient State Management
- **Flat KV Storage**: Simplified account and storage data structure
- **Single Merkle Trie**: Unified trie for both accounts and storage
- **Smart Preprocessing**: Reduces write amplification using temporary files

🔬 **Technical Deep Dives**:
- [Database Architecture](./docs/programmers_guide/db_walkthrough.MD)
- [ETL Process](https://github.com/erigontech/erigon/blob/main/erigon-lib/etl/README.md)
- [Staged Sync Design](/eth/stagedsync/README.md)

### 🔌 Modular RPC System

Components can run either embedded or as separate processes:

```bash
# Build components
make erigon rpcdaemon

# Run Erigon core
./build/bin/erigon --datadir=/path/to/data --http=false

# Run RPC daemon separately
./build/bin/rpcdaemon --datadir=/path/to/data \
    --http.api=eth,erigon,web3,net,debug,trace,txpool --ws
```

**Performance Tuning**:
```bash
# Increase throughput
--rpc.batch.concurrency=100
--rpc.batch.limit=100
--db.read.concurrency=100

# Optimize network
--http.compression=false
--ws.compression=false
```

## Data Directory Structure

### Core Components

```
datadir/
├── chaindata/     # Recent state and blocks
├── snapshots/     # Historical data (.seg files)
│   ├── domain/    # Latest state
│   ├── history/   # Historical values
│   ├── idx/       # Search indices (eth_getLogs, trace_transaction)
│   └── accessor/  # Generated indices for Get requests
├── txpool/        # Pending transactions (temporary)
├── nodes/         # P2P peer data (temporary)
└── temp/          # Sort buffer (~100GB, cleaned at startup)
```

> 💡 **Note**: Erigon operates across four domains: account, storage, code, and commitment

### Storage Optimization Guide

#### Tiered Storage Setup

For systems with mixed storage types (fast NVMe + slower drives):

1. **Base Configuration**
   ```
   # Place datadir on slower storage, symlink critical paths to fast storage
   datadir/
   ├── chaindata -> /fast-disk/chaindata     # Required on fast storage
   └── snapshots/
       └── domain -> /fast-disk/domain       # Required on fast storage
   ```

2. **Progressive Performance Enhancement**
   ```
   # Add these to fast storage in order of impact:
   1. datadir/snapshots/accessor/  # Immediate performance gain
   2. datadir/snapshots/idx/       # Additional search performance
   3. datadir/snapshots/history/   # Maximum historical query speed
   ```

#### Storage Requirements

Reference directory sizes for Ethereum Mainnet (Archive Node, April 2024)

| Component | Size | Description |
|-----------|------|-------------|
| chaindata | 15GB | Recent state |
| snapshots/accessor | 120GB | Access indices |
| snapshots/domain | 300GB | State data |
| snapshots/history | 280GB | Historical data |
| snapshots/idx | 430GB | Search indices |
| **Total** | **~2.3TB** | |

Reference directory sizes for Polygon PoS Mainnet (Archive Node, November 2024)

| Component | Size       | Description |
|-----------|------------|-------------|
| chaindata | 20GB       | Recent state |
| snapshots/accessor | 360GB      | Access indices |
| snapshots/domain | 1.1TB      | State data |
| snapshots/history | 750GB      | Historical data |
| snapshots/idx | 1.5TB      | Search indices |
| **Total** | **~4.9TB** | |

## Erigon 3 changes

- **No re-execution:** we download state and history
- **Per-transaction history granularity** (Erigon2 was per-block).
    - We can execute transactions in the middle of a block without executing preceding transactions.
    - If account X change V1->V2->V1 within 1 block (different transactions): `debug_getModifiedAccountsByNumber` return
      it
    - Erigon3 doesn't store Logs (aka Receipts) - it always re-executing historical txn (but it's cheaper)
- **Validator mode**: `--internalcl` is enabled by default. To disable use `--externalcl`.
- **Store most of the data in immutable files (segments/snapshots):**
    - You can symlink/mount latest state to fast drive and history to cheap drive.
    - `chaindata` is less than `15gb`. It's ok to `rm -rf chaindata`.
    - To prevent growth it is recommended to keep `--batchSize <= 1G`
- **`--prune` flags changed**: see `--prune.mode` (default: `full`, archive: `archive`, EIP-4444: `minimal`)
- **Other changes:**
    - Merge multiple E2 stages (stage_hash_state, stage_trie, log_index, history_index, trace_index) into ExecutionStage.
    - Restart during sync doesn't lose much progress: `--sync.loop.block.limit=5_000` enabled by default

## Logging

Erigon provides flexible logging options for both console and disk output.

### Console Logging Flags

- `--verbosity`
- `--log.console.verbosity` (alias for `--verbosity`)
- `--log.json`
- `--log.console.json` (alias for `--log.json`)

Use `--verbosity` (or `--log.console.verbosity`) to set the maximum log level for console output:

```
  LvlCrit = 0
  LvlError = 1
  LvlWarn = 2
  LvlInfo = 3
  LvlDebug = 4
  LvlTrace = 5
```

### Disk Logging Flags

- `--log.dir.path`
- `--log.dir.prefix`
- `--log.dir.verbosity`
- `--log.dir.json`

To enable logging to disk:

1. Set `--log.dir.path` to the desired directory.
2. (Optional) Use `--log.dir.prefix` to customize the output filename prefix (default: `erigon`).
3. Set `--log.dir.verbosity` to control log level for disk output (accepts integer `0–5` or strings like `debug`, `info`). Default is `debug` (4).
4. To use JSON format for file logs, enable `--log.dir.json`.


## Modularity

By default, Erigon runs as a single binary with all components included. However, for advanced use cases (e.g., scaling, resource isolation, or custom implementations), you can run individual services separately:
- TxPool
- RPCDaemon (JSON-RPC server)
- Sentry (P2P)
- Downloader (Ottersync)
- Consensus (Beacon client)

> **Note:** Avoid splitting services unless it is required. Separation is intended for advanced users with specific requirements (e.g., resource control, modular architecture).

To run services as individual processes, refer to each service's README under `./cmd/*/README.md`.

## Block Production (PoS Validator)

Block production is fully supported for Ethereum & Gnosis Chain. It is still experimental for Polygon.

## Config Files TOML

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
e.g: `--beacon.api=beacon,builder,config,debug,node,validator,lighthouse` will enable all endpoints. **NOTE: Caplin is
not staking-ready so aggregation endpoints are still to be implemented. Additionally enabling the Beacon API will lead
to a 6 GB higher RAM usage.

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


🔬 [Complete RPC Documentation](./cmd/rpcdaemon/README.md)

### 🕵 Monitoring
```bash
# Launch monitoring stack
docker compose up prometheus grafana
```
[🔬 Monitoring Setup Guide](./cmd/prometheus/Readme.md)

## ❓ FAQ

### 📚 Using Erigon as a Library
```go
// Use git branch/commit, not tags
go mod edit -replace github.com/erigontech/erigon-lib=github.com/erigontech/erigon/erigon-lib@5498f854e44df5c8f0804ff4f0747c0dec3caad5
go get github.com/erigontech/erigon@main
go mod tidy
```

### 🔐 Network Configuration

#### Erigon Core Ports

| Component | Port | Protocol | Purpose | Exposure |
|-----------|------|----------|----------|----------|
| Engine | 9090 | TCP | gRPC Server | 🔒 Private |
| Engine | 42069 | TCP/UDP | Snap Sync | 🔓 Public |
| Engine | 8551 | TCP | Engine API (JWT) | 🔒 Private |
| Sentry | 30303 | TCP/UDP | eth/68 Peering | 🔓 Public |
| Sentry | 30304 | TCP/UDP | eth/67 Peering | 🔓 Public |
| Sentry | 9091 | TCP | gRPC Inbound | 🔒 Private |
| RPC | 8545 | TCP | HTTP/WS/GraphQL | 🔒 Private |

> 💡 **Tip**: Only ports 30303, 30304, and 42069 need to be publicly accessible

#### Caplin Ports

| Component | Port | Protocol | Purpose | Exposure |
|-----------|------|----------|----------|----------|
| Sentinel | 4000 | UDP | Peering | 🔓 Public |
| Sentinel | 4001 | TCP | Peering | 🔓 Public |

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
    * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.23
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
- Or buy/download synced archive node from some 3-rd party Erigon2 snapshots provider
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

---------
