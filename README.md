# Erigon

Documentation: **[erigon.gitbook.io](https://erigon.gitbook.io)**

Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency
frontier. [Archive Node](https://ethereum.org/en/developers/docs/nodes-and-clients/archive-nodes/#what-is-an-archive-node)
by default.

<br>

![Build status](https://github.com/erigontech/erigon/actions/workflows/ci.yml/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=erigontech_erigon&metric=coverage)](https://sonarcloud.io/summary/new_code?id=erigontech_erigon)

<!--ts-->

- [System Requirements](#system-requirements)
- [Usage](#usage)
    + [Getting Started](#getting-started)
    + [Logging](#logging)
    + [Testnets](#testnets)
    + [Block Production](#block-production-pow-miner-or-pos-validator)
    + [Windows](#windows)
    + [GoDoc](https://godoc.org/github.com/erigontech/erigon)
    + [Beacon Chain](#beacon-chain-consensus-layer)
    + [Dev Chain](#dev-chain)
    + [Caplin (Internal Consensus Layer)](#caplin)

- [Key features](#key-features)
    + [More Efficient State Storage](#more-efficient-state-storage)
    + [Faster Initial Sync](#faster-initial-sync)
    + [JSON-RPC daemon](#json-rpc-daemon)
    + [Run all components by docker-compose](#run-all-components-by-docker-compose)
    + [Grafana dashboard](#grafana-dashboard)
    + [Internal Consensus Layer](#caplin)
- [Documentation](#documentation)
- [FAQ](#faq)
- [Getting in touch](#getting-in-touch)
    + [Erigon Discord Server](#erigon-discord-server)
    + [Reporting security issues/concerns](#reporting-security-issues/concerns)
    + [Team](#team)
- [Known issues](#known-issues)
    + [`htop` shows incorrect memory usage](#htop-shows-incorrect-memory-usage)

<!--te-->

**Disclaimer**: this software is currently a tech preview. We will do our best to keep it stable and make no breaking
changes but we don't guarantee anything. Things can and will break.

**Important defaults**: Erigon is an Archive Node by default (to remove history see: `--prune` flags
in `erigon --help`). We don't allow change this flag after first start.

<code>In-depth links are marked by the microscope sign (🔬) </code>

System Requirements
===================

* For an Archive node of Ethereum Mainnet we recommend >=3.5TB storage space: 2.3TiB state (as of March 2024),
  643GiB snapshots (can symlink or mount folder `<datadir>/snapshots` to another disk), 200GB temp files (can symlink or
  mount folder `<datadir>/temp` to another disk).
  Ethereum Mainnet Full node (see [Pruned Node][pruned_node]): 1.1TiB not including temp files (June 2024).

* Gnosis Chain Archive: 1.7TiB (March 2024).
  Gnosis Chain Full node (see [Pruned Node][pruned_node]): 300GiB (June 2024).

* Polygon Mainnet Archive: 8.5TiB (December 2023).
  Polygon Mainnet Full node (see [Pruned Node][pruned_node]) with `--prune.*.older 15768000`: 5.1Tb (September 2023).

SSD or NVMe. Do not recommend HDD - on HDD Erigon will always stay N blocks behind chain tip, but not fall behind.
Bear in mind that SSD performance deteriorates when close to capacity.

RAM: >=16GB, 64-bit architecture.

[Golang version >= 1.21](https://golang.org/doc/install); GCC 10+ or Clang; On Linux: kernel > v4

<code>🔬 more details on disk storage [here](https://erigon.substack.com/p/disk-footprint-changes-in-new-erigon?s=r)
and [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space).</code>

[pruned_node]: https://erigon.gitbook.io/erigon/basic-usage/usage/type-of-node#full-node-or-pruned-node

Usage
=====

### Getting Started

For building the latest release (this will be suitable for most users just wanting to run a node):

```sh
git clone --branch release/<x.xx> --single-branch https://github.com/erigontech/erigon.git
cd erigon
make erigon
./build/bin/erigon
```

You can check [the list of releases](https://github.com/erigontech/erigon/releases) for release notes.

For building the bleeding edge development branch:

```sh
git clone --recurse-submodules https://github.com/erigontech/erigon.git
cd erigon
git checkout main
make erigon
./build/bin/erigon
```

Default `--snapshots` for `mainnet`, `gnosis`, `chiado`. Other networks now have default `--snapshots=false`.
Increase
download speed by flag `--torrent.download.rate=20mb`. <code>🔬 See [Downloader docs](./cmd/downloader/readme.md)</code>

Use `--datadir` to choose where to store data.

Use `--chain=gnosis` for [Gnosis Chain](https://www.gnosis.io/), `--chain=bor-mainnet` for Polygon Mainnet,
and `--chain=amoy` for Polygon Amoy.
For Gnosis Chain you need a [Consensus Layer](#beacon-chain-consensus-layer) client alongside
Erigon (https://docs.gnosischain.com/node/manual/beacon).

Running `make help` will list and describe the convenience commands available in the [Makefile](./Makefile).

### Datadir structure

- chaindata: recent blocks, state, recent state history. low-latency disk recommended.
- snapshots: old blocks, old state history. can symlink/mount it to cheaper disk. mostly immutable. must have ~100gb
  free space (for merge recent files to bigger one).
- temp: can grow to ~100gb, but usually empty. can symlink/mount it to cheaper disk.
- txpool: pending transactions. safe to remove.
- nodes:  p2p peers. safe to remove.

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

### Modularity

Erigon by default is "all in one binary" solution, but it's possible start TxPool as separated processes.
Same true about: JSON RPC layer (RPCDaemon), p2p layer (Sentry), history download layer (Downloader), consensus.
Don't start services as separated processes unless you have clear reason for it: resource limiting, scale, replace by
your own implementation, security.
How to start Erigon's services as separated processes, see in [docker-compose.yml](./docker-compose.yml).

### Embedded Consensus Layer

On Ethereum Mainnet and Sepolia, the Engine API can be disabled in favour of the Erigon native Embedded
Consensus Layer.
If you want to use the internal Consensus Layer, run Erigon with flag `--internalcl`.
_Warning:_ Staking (block production) is not possible with the embedded CL.

### Testnets

If you would like to give Erigon a try, but do not have spare 2TB on your drive, a good option is to start syncing one
of the public testnets, Sepolia. It syncs much quicker, and does not take so much disk space:

```sh
git clone --recurse-submodules -j8 https://github.com/erigontech/erigon.git
cd erigon
make erigon
./build/bin/erigon --datadir=<your_datadir> --chain=sepolia
```

Please note the `--datadir` option that allows you to store Erigon files in a non-default location, in this example,
in `sepolia` subdirectory of the current directory. Name of the directory `--datadir` does not have to match the name of
the chain in `--chain`.

### Block Production (PoS Validator)

Block production is fully supported for Ethereum & Gnosis Chain. It is still experimental for Polygon.

### Windows

Windows users may run erigon in 3 possible ways:

* Build executable binaries natively for Windows using provided `wmake.ps1` PowerShell script. Usage syntax is the same
  as `make` command so you have to run `.\wmake.ps1 [-target] <targetname>`. Example: `.\wmake.ps1 erigon` builds erigon
  executable. All binaries are placed in `.\build\bin\` subfolder. There are some requirements for a successful native
  build on windows :
    * [Git](https://git-scm.com/downloads) for Windows must be installed. If you're cloning this repository is very
      likely you already have it
    * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.21
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
  those mount points use `DrvFS` which is a [network file system](#blocks-execution-is-slow-on-cloud-network-drives)
  and, additionally, MDBX locks the db for exclusive access which implies only one process at a time can access data.
  This has consequences on the running of `rpcdaemon` which has to be configured as [Remote DB](#for-remote-db) even if
  it is executed on the very same computer. If instead your data is hosted on the native Linux filesystem non
  limitations apply.
  **Please also note the default WSL2 environment has its own IP address which does not match the one of the network
  interface of Windows host: take this into account when configuring NAT for port 30303 on your router.**

### Using TOML or YAML Config Files

You can set Erigon flags through a YAML or TOML configuration file with the flag `--config`. The flags set in the
configuration
file can be overwritten by writing the flags directly on Erigon command line

### Example

`./build/bin/erigon --config ./config.yaml --chain=sepolia`

Assuming we have `chain : "mainnet"` in our configuration file, by adding `--chain=sepolia` allows the overwrite of the
flag inside
of the yaml configuration file and sets the chain to sepolia

### TOML

Example of setting up TOML config file

```
datadir = 'your datadir'
port = 1111
chain = "mainnet"
http = true
"private.api.addr"="localhost:9090"

"http.api" = ["eth","debug","net"]
```

### YAML

Example of setting up a YAML config file

```
datadir : 'your datadir'
port : 1111
chain : "mainnet"
http : true
private.api.addr : "localhost:9090"

http.api : ["eth","debug","net"]
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

#### Caplin's Usage.

Caplin is be enabled by default. to disable it and enable the Engine API, use the `--externalcl` flag. from that point
on, an external Consensus Layer will not be need
anymore.

Caplin also has an archivial mode for historical states and blocks. it can be enabled through the `--caplin.archive`
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

<code> 🔬 Detailed explanation is [DEV_CHAIN](/DEV_CHAIN.md).</code>

Key features
============

<code>🔬 See more
detailed [overview of functionality and current limitations](https://ledgerwatch.github.io/turbo_geth_release.html). It
is being updated on recurring basis.</code>

### More Efficient State Storage

**Flat KV storage.** Erigon uses a key-value database and storing accounts and storage in a simple way.

<code> 🔬 See our detailed DB walkthrough [here](./docs/programmers_guide/db_walkthrough.MD).</code>

**Preprocessing**. For some operations, Erigon uses temporary files to preprocess data before inserting it into the main
DB. That reduces write amplification and DB inserts are orders of magnitude quicker.

<code> 🔬 See our detailed ETL explanation [here](https://github.com/erigontech/erigon/blob/main/erigon-lib/etl/README.md).</code>

**Plain state**.

**Single accounts/state trie**. Erigon uses a single Merkle trie for both accounts and the storage.

### Faster Initial Sync

Erigon uses a rearchitected full sync algorithm from
[Go-Ethereum](https://github.com/ethereum/go-ethereum) that is split into
"stages".

<code>🔬 See more detailed explanation in the [Staged Sync Readme](/eth/stagedsync/README.md)</code>

It uses the same network primitives and is compatible with regular go-ethereum nodes that are using full sync, you do
not need any special sync capabilities for Erigon to sync.

When reimagining the full sync, with focus on batching data together and minimize DB overwrites. That makes it possible
to sync Ethereum mainnet in under 2 days if you have a fast enough network connection and an SSD drive.

Examples of stages are:

* Downloading headers;

* Downloading block bodies;

* Recovering senders' addresses;

* Executing blocks;

* Validating root hashes and building intermediate hashes for the state Merkle trie;

* [...]

### JSON-RPC daemon

Most of Erigon's components (txpool, rpcdaemon, snapshots downloader, sentry, ...) can work inside Erigon and as
independent process.

To enable built-in RPC server: `--http` and `--ws` (sharing same port with http)

Run RPCDaemon as separated process: this daemon can use local DB (with running Erigon or on snapshot of a database) or
remote DB (run on another server). <code>🔬 See [RPC-Daemon docs](./cmd/rpcdaemon/README.md)</code>

#### **For remote DB**

This works regardless of whether RPC daemon is on the same computer with Erigon, or on a different one. They use TPC
socket connection to pass data between them. To use this mode, run Erigon in one terminal window

```sh
make erigon
./build/bin/erigon --private.api.addr=localhost:9090 --http=false
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool
```

#### **gRPC ports**

`9090` erigon, `9091` sentry, `9092` consensus engine, `9093` torrent downloader, `9094` transactions pool

Supported JSON-RPC calls ([eth](./cmd/rpcdaemon/commands/eth_api.go), [debug](./cmd/rpcdaemon/commands/debug_api.go)
, [net](./cmd/rpcdaemon/commands/net_api.go), [web3](./cmd/rpcdaemon/commands/web3_api.go)):

For a details on the implementation status of each
command, [see this table](./cmd/rpcdaemon/README.md#rpc-implementation-status).

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

#### Check: Permissions

In all cases, `XDG_DATA_HOME` (specified or default) must be writeable by the user UID/GID in docker, which will be
determined by the `DOCKER_UID` and `DOCKER_GID` at build time.

If a build or service startup is failing due to permissions, check that all the directories, UID, and GID controlled by
these environment variables are correct.

#### Run

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

Windows support for docker-compose is not ready yet. Please help us with .ps1 port.

### Grafana dashboard

`docker compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).

###

old data

Disabled by default. To enable see `./build/bin/erigon --help` for flags `--prune`

Documentation
==============

The `./docs` directory includes a lot of useful but outdated documentation. For code located
in the `./cmd` directory, their respective documentation can be found in `./cmd/*/README.md`.
A more recent collation of developments and happenings in Erigon can be found in the
[Erigon Blog](https://erigon.substack.com/).



FAQ
================

### How much RAM do I need

- Baseline (ext4 SSD): 16Gb RAM sync takes 6 days, 32Gb - 5 days, 64Gb - 4 days
- +1 day on "zfs compression=off". +2 days on "zfs compression=on" (2x compression ratio). +3 days on btrfs.
- -1 day on NVMe

Detailed explanation: [./docs/programmers_guide/db_faq.md](./docs/programmers_guide/db_faq.md)

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
| all       | 6060 | TCP      | metrics | Private       |

Optional flags can be enabled that enable pprof or metrics (or both) - however, they both run on 6060 by default, so

you'll have to change one if you want to run both at the same time. use `--help` with the binary for more info.

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

### How to run erigon as a separate user? (e.g. as a `systemd` daemon)

Running erigon from `build/bin` as a separate user might produce an error:

    error while loading shared libraries: libsilkworm_capi.so: cannot open shared object file: No such file or directory

The library needs to be *installed* for another user using `make DIST=<path> install`. You could use `$HOME/erigon`
or `/opt/erigon` as the installation path, for example:

    make DIST=/opt/erigon install

and then run `/opt/erigon/erigon`.

### How to get diagnostic for bug report?

- Get stack trace: `kill -SIGUSR1 <pid>`, get trace and stop: `kill -6 <pid>`
- Get CPU profiling: add `--pprof flag`
  run `go tool pprof -png  http://127.0.0.1:6060/debug/pprof/profile\?seconds\=20 > cpu.png`
- Get RAM profiling: add `--pprof flag`
  run `go tool pprof -inuse_space -png  http://127.0.0.1:6060/debug/pprof/heap > mem.png`

### How to run local devnet?

<code> 🔬 Detailed explanation is [here](/DEV_CHAIN.md).</code>

### Docker permissions error

Docker uses user erigon with UID/GID 1000 (for security reasons). You can see this user being created in the Dockerfile.
Can fix by giving a host's user ownership of the folder, where the host's user UID/GID is the same as the docker's user
UID/GID (1000).
More details
in [post](https://www.fullstaq.com/knowledge-hub/blogs/docker-and-the-host-filesystem-owner-matching-problem)

### How to run public RPC api

- `--txpool.nolocals=true`
- don't add `admin` in `--http.api` list
- to increase throughput may need
  increase/decrease: `--db.read.concurrency`, `--rpc.batch.concurrency`, `--rpc.batch.limit`

### Run RaspberyPI

https://github.com/mathMakesArt/Erigon-on-RPi-4

### How to change db pagesize

[post](https://github.com/erigontech/erigon/blob/main/cmd/integration/Readme.md#copy-data-to-another-db)


Getting in touch
================

### Erigon Discord Server

The main discussions are happening on our Discord server. To get an invite, send an email to `bloxster [at] proton.me`
with
your name, occupation, a brief explanation of why you want to join the Discord, and how you heard about Erigon.

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

  Erigon uses ~4Gb of RAM during genesis sync and ~1Gb during normal work. OS pages cache can utilize unlimited amount
  of
  memory.

  **Warning:** Multiple instances of Erigon on same machine will touch Disk concurrently, it impacts performance - one
  of
  main Erigon optimisations: "reduce Disk random access".
  "Blocks Execution stage" still does many random reads - this is reason why it's slowest stage. We do not recommend
  running
  multiple genesis syncs on same Disk. If genesis sync passed, then it's fine to run multiple Erigon instances on same
  Disk.

### Blocks Execution is slow on cloud-network-drives

Please read https://github.com/erigontech/erigon/issues/1516#issuecomment-811958891
In short: network-disks are bad for blocks execution - because blocks execution reading data from db non-parallel
non-batched way.

### Filesystem's background features are expensive

For example: btrfs's autodefrag option - may increase write IO 100x times

### Gnome Tracker can kill Erigon

[Gnome Tracker](https://wiki.gnome.org/Projects/Tracker) - detecting miners and kill them.

### the --mount option requires BuildKit error

For anyone else that was getting the BuildKit error when trying to start Erigon the old way you can use the below...

```sh
XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```

---------

## Erigon3 user's guide

Git branch `main`. Just start erigon as you usually do.

RAM requirement is higher: 32gb and better 64gb. We will work on this topic a bit later.

Golang 1.21

Almost all RPC methods are implemented - if something doesn't work - just drop it on our head.

Supported networks: all.

### E3 changes from E2:

- Sync from scratch doesn't require re-exec all history. Latest state and it's history are in snapshots - can download.
- ExecutionStage - now including many E2 stages: stage_hash_state, stage_trie, stage_log_index, stage_history_index,
  stage_trace_index
- E3 can execute 1 historical transaction - without executing it's block - because history/indices have
  transaction-granularity, instead of block-granularity.
- E3 doesn't store Logs (aka Receipts) - it always re-executing historical txn (but it's cheaper then in E2 - see point
  above). Known perf issues: https://github.com/erigontech/erigon/issues/10747
- `--sync.loop.block.limit` is enabled by default. (Default: `5_000`.
  Set `--sync.loop.block.limit=10_000 --batchSize=2g` to increase sync speed on good hardware).
- datadir/chaindata is small now - to prevent it's grow: we recommend set `--batchSize <= 2G`. And it's fine
  to `rm -rf chaindata`
- can symlink/mount latest state to fast drive and history to cheap drive
- Archive Node is default. Full Node: `--prune.mode=full`, Minimal Node (EIP-4444): `--prune.mode=minimal`

### Known Problems of E3:

- don't `rm -rf downloader` - it will cause re-downloading of files: https://github.com/erigontech/erigon/issues/10976

### E3 datadir structure

```sh
datadir        
    chaindata   # "Recently-updated Latest State" and "Recent History"
    snapshots   
        domain    # Latest State: link to fast disk
        history   # Historical values 
        idx       # InvertedIndices: can search/filtering/union/intersect them - to find historical data. like eth_getLogs or trace_transaction
        accessors # Additional (generated) indices of history - have "random-touch" read-pattern. They can serve only `Get` requests (no search/filters).
    temp # buffers to sort data >> RAM. sequential-buffered IO - is slow-disk-friendly
   
# There is 4 domains: account, storage, code, commitment 
```

### E3 can store state on fast disk and history on cheap disk

If you can afford store datadir on 1 nvme-raid - great. If can't - it's possible to store history on cheap drive.

```sh
# place (or ln -s) `datadir` on slow disk. link some sub-folders to fast disk.
# Example: what need link to fast disk to speedup execution
datadir        
    chaindata   # link to fast disk
    snapshots   
        domain    # link to fast disk
        history   
        idx       
        accessors 
    temp   

# Example: how to speedup history access: 
#   - go step-by-step - first try store `accessors` on fast disk
#   - if speed is not good enough: `idx`
#   - if still not enough: `history` 
```

### E3 datadir size

```
# eth-mainnet - archive - April 2024

du -hsc /erigon/* 
6G  	/erigon/caplin
50G 	/erigon/chaindata
1.8T	/erigon/snapshots
1.9T	total

du -hsc /erigon/snapshots/* 
100G 	/erigon/snapshots/accessor
240G	/erigon/snapshots/domain
260G	/erigon/snapshots/history
410G	/erigon/snapshots/idx
1.7T	/erigon/snapshots
```

```
# bor-mainnet - archive - Jun 2024

du -hsc /erigon/* 

160M	/erigon/bor
50G 	/erigon/chaindata
3.7T	/erigon/snapshots
3.8T	total

du -hsc /erigon/snapshots/* 
260G	/erigon-data/snapshots/accessor
850G	/erigon-data/snapshots/domain
650G	/erigon-data/snapshots/history
1.4T	/erigon-data/snapshots/idx
4.1T	/erigon/snapshots
```

### E3 other perf trics

- `--sync.loop.block.limit=10_000 --batchSize=2g` - likely will help for sync speed.
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
