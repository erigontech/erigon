# Erigon

[![Docs](https://img.shields.io/badge/docs-up-green)](https://docs.erigon.tech/)
[![Blog](https://img.shields.io/badge/blog-up-green)](https://erigon.tech/blog/)
[![Twitter](https://img.shields.io/twitter/follow/ErigonEth?style=social)](https://x.com/ErigonEth)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?style=flat&logo=discord&logoColor=white)](https://dsc.gg/erigon)
[![Build status](https://github.com/erigontech/erigon/actions/workflows/ci.yml/badge.svg)](https://github.com/erigontech/erigon/actions/workflows/ci.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=erigontech_erigon&metric=coverage)](https://sonarcloud.io/summary/new_code?id=erigontech_erigon)

Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency
frontier.

**[Download](https://github.com/erigontech/erigon/releases)**
| [Documentation](https://docs.erigon.tech/)
| [Blog](https://erigon.tech/blog/)

This README is for people working on the repository. Everything about operating a node — install methods,
flags, pruning modes, ports, monitoring, staking — is on the documentation site.

Documentation
=============

The [Erigon documentation](https://docs.erigon.tech/) answers most questions. Useful starting points:

* [Installation](https://docs.erigon.tech/get-started/installation) — binaries, Docker, Linux/macOS, Windows, ARM
* [Hardware requirements](https://docs.erigon.tech/get-started/hardware-requirements) — disk, RAM, CPU, bandwidth
* [Pruning modes](https://docs.erigon.tech/fundamentals/pruning-modes) — `archive`, `full`, `blocks`, `minimal`
* [CLI reference](https://docs.erigon.tech/fundamentals/configuring-erigon) — every flag, config files, env vars
* [Default ports](https://docs.erigon.tech/fundamentals/default-ports) and [Security](https://docs.erigon.tech/fundamentals/security)
* [Interacting with Erigon](https://docs.erigon.tech/interacting-with-erigon) — JSON-RPC, GraphQL, gRPC namespaces
* [Creating a dashboard](https://docs.erigon.tech/fundamentals/creating-a-dashboard) — Prometheus and Grafana
* [Upgrading](https://docs.erigon.tech/get-started/installation/upgrading) — versions and snapshot formats

<code>In-depth links are marked by the microscope sign (🔬) </code>

System Requirements
===================

RAM: >=32GB, [Golang >= 1.25](https://golang.org/doc/install); GCC 10+ or Clang; On Linux: kernel > v4. 64-bit
architecture.

Disk space, July 2026:

| Chain            | Archive | Full   | Minimal |
|------------------|---------|--------|---------|
| Ethereum Mainnet | 2TB     | 420GB  | 380GB   |
| Gnosis           | 675GB   | 220GB  | 205GB   |
| Sepolia          | 1.1TB   | -      | -       |
| Hoodi            | 134GB   | -      | -       |
| Chiado           | 30GB    | -      | -       |

Sizes grow with the chain. Archive figures are measured on Erigon 3.6 nodes running `--prune.mode=archive` with default
pruning options; enabling receipts or commitment history adds a lot on top — see [Erigon3 datadir size](#erigon3-datadir-size).
The same figures are published, with more detail, on the
[hardware requirements](https://docs.erigon.tech/get-started/hardware-requirements) page.

SSD or NVMe. We do not recommend HDD — on HDD, Erigon will always stay a few blocks behind the chain tip but will not fall further behind.
Bear in mind that SSD performance deteriorates when close to capacity. CloudDrives (like
gp3): Blocks Execution is slow
on [cloud-network-drives](https://docs.erigon.tech/help-center/known-issues#cloud-network-drives)

🔬 More details on [Erigon3 datadir size](#erigon3-datadir-size)

🔬 More details on what type of data stored [here](https://ledgerwatch.github.io/turbo_geth_release.html#Disk-space)

Sync Times
==========

These are the approximate sync times for syncing from scratch to the tip of the chain (results may vary depending on hardware and bandwidth).


| Chain      | Archive         | Full           | Minimal        |
|------------|-----------------|----------------|----------------|
| Ethereum   | 7 Hours, 55 Minutes | 4 Hours, 23 Minutes | 1 Hour, 41 Minutes |
| Gnosis     | 2 Hours, 10 Minutes | 1 Hour, 5 Minutes  | 33 Minutes      |

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

Use `--chain=gnosis` for [Gnosis Chain](https://www.gnosis.io/).
For Gnosis Chain you need a [Consensus Layer](#beacon-chain-consensus-layer) client alongside
Erigon (https://docs.gnosischain.com/category/step--3---run-consensus-client).

Running `make help` will list and describe the convenience commands available in the [Makefile](./Makefile).

### Datadir structure

```sh
datadir        
    chaindata     # "Recently-updated Latest State", "Recent History", "Recent Blocks"
    snapshots     # contains `.seg` files - it's old blocks
        domain    # Latest State
        history   # Historical values 
        idx       # InvertedIndices: can search/filtering/union/intersect them - to find historical data. like eth_getLogs or trace_transaction
        accessor # Additional (generated) indices of history - have "random-touch" read-pattern. They can serve only `Get` requests (no search/filters).
    caplin        # embedded Consensus Layer: beacon chain db and its snapshots
    txpool        # pending transactions. safe to remove.
    nodes         # p2p peers. safe to remove.
    temp          # used to sort data bigger than RAM. can grow to ~100gb. cleaned at startup.
   
# There are 6 domains: account, storage, code, commitment, receipt, rcache. Last one only with `--prune.include-receipts`.
```

See the [lib](db/downloader/README.md) and [cmd](cmd/downloader/README.md) READMEs for more information.

### Erigon3 datadir size

Measured on Erigon 3.6 `--prune.mode=archive` nodes, July 2026. The `snapshots/*.seg` row is only the block files —
headers, bodies and transactions — that sit directly in `snapshots/`; the rows below it are its sub-folders. Each
column is one node, so totals can differ a few percent from the table above, which is measured on freshly synced
nodes.

| Path                 | eth-mainnet | gnosis    | sepolia   | hoodi     | chiado   |
|----------------------|-------------|-----------|-----------|-----------|----------|
| `chaindata`          | 22.75 GB    | 9.63 GB   | 12.87 GB  | 6.78 GB   | 2.40 GB  |
| `snapshots/*.seg`    | 996.40 GB   | 284.82 GB | 614.90 GB | 33.38 GB  | 12.91 GB |
| `snapshots/domain`   | 417.66 GB   | 227.03 GB | 237.26 GB | 52.39 GB  | 7.17 GB  |
| `snapshots/idx`      | 332.72 GB   | 136.67 GB | 115.95 GB | 25.47 GB  | 3.44 GB  |
| `snapshots/history`  | 255.60 GB   | 39.28 GB  | 68.93 GB  | 8.00 GB   | 2.33 GB  |
| `snapshots/accessor` | 141.61 GB   | 33.73 GB  | 45.85 GB  | 7.70 GB   | 1.39 GB  |
| total                | 2.17 TB     | 731.16 GB | 1.10 TB   | 133.73 GB | 29.64 GB |

Data that is off by default, measured on the same nodes:

| Flag                                 | eth-mainnet | gnosis     | sepolia    | hoodi      | chiado    |
|--------------------------------------|-------------|------------|------------|------------|-----------|
| `--prune.include-receipts`           | +441.42 GB  | +246.74 GB | +135.15 GB | +11.31 GB  | +3.38 GB  |
| `--prune.include-commitment-history` | +4.40 TB    | -          | +1.09 TB   | +182.27 GB | +57.35 GB |
| `--caplin.blocks-archive`, `--caplin.blobs-archive`, `--caplin.states-archive` | +2.48 TB | +501.00 GB | +1.40 TB | +135.28 GB | - |

Caplin numbers are the backfilled beacon blocks, blob sidecars and beacon state snapshots. They exclude the beacon
chain db that every node with the embedded Consensus Layer keeps anyway. Blob sidecars dominate: on mainnet they are
2.17 TB of the 2.48 TB total.

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
    - `chaindata` is tens of gb (22gb on an archive mainnet node). It's ok to `rm -rf chaindata`. (to prevent grow: recommend `--batchSize <= 1G`)
- **`--prune` flags changed**: see `--prune.mode` (default: `full`, archive: `archive`, EIP-4444: `minimal`)
- **Other changes:**
    - ExecutionStage included many E2 stages: stage_hash_state, stage_trie, log_index, history_index, trace_index
    - Restart doesn't loose much partial progress: `--sync.loop.block.limit=5_000` enabled by default

### Logging

Log flags, log levels and log files: [Logs](https://docs.erigon.tech/fundamentals/logs) and the
[CLI reference](https://docs.erigon.tech/fundamentals/configuring-erigon).

#### Torrent client logging

The torrent client in the Downloader logs to `logs/torrent.log` at the level specified by `torrent.verbosity` or WARN, whichever is lower. Logs at `torrent.verbosity` or higher are also passed through to the top level Erigon dir and console loggers (which must have their own levels set low enough to log the messages in their respective handlers).

### Block Production (PoS Validator)

Block production is fully supported for Ethereum & Gnosis Chain.

### Beacon Chain (Consensus Layer)

Erigon can be used as an Execution Layer for external Consensus Layer clients. See
[JWT secret](https://docs.erigon.tech/fundamentals/jwt) and
[Ethereum with an external CL](https://docs.erigon.tech/get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl).

### Caplin

Caplin is a full-fledged validating Consensus Client like Prysm, Lighthouse, Teku, Nimbus and Lodestar. Its goal is:

* provide better stability
* Validation of the chain
* Stay in sync
* keep the execution of blocks on chain tip
* serve the Beacon API using a fast and compact data model alongside low CPU and memory usage.

The main reason we developed a new Consensus Layer is to explore the potential benefits it can bring.
For example, The Engine API does not work well with Erigon. The Engine API sends data one block at a time, which does
not suit how Erigon works. Erigon is designed to handle many blocks simultaneously and needs to sort and process data
efficiently. Therefore, it would be better for Erigon to handle the blocks independently instead of relying on the
Engine API.

#### Caplin's Usage

Caplin is enabled by default. To disable it and use the Engine API instead, use the `--externalcl` flag. From that point
on, an external Consensus Layer will no longer be needed.

Caplin also has an archival mode for historical blocks, blobs and states, enabled through `--caplin.blocks-archive`,
`--caplin.blobs-archive` and `--caplin.states-archive` (the latter turns on block archival as well). All three are off
by default and cost a lot of disk — see [Erigon3 datadir size](#erigon3-datadir-size).
In order to enable the caplin's Beacon API, the flag `--beacon.api=<namespaces>` must be added.
e.g: `--beacon.api=beacon,builder,config,debug,node,validator,lighthouse` will enable all endpoints. 
Note: enabling the Beacon API will lead to a 6 GB higher RAM usage

### Dev Chain

<code> 🔬 Detailed explanation is [DEV_CHAIN](/docs/DEV_CHAIN.md).</code>

For developers
==============

### Executables

`make erigon` builds the node; `make all` builds the full suite into `./build/bin`. `make help` lists every target.

| Command      | Description                                                                                      |
|--------------|--------------------------------------------------------------------------------------------------|
| `erigon`     | The node: execution layer with the embedded consensus layer (Caplin)                             |
| `rpcdaemon`  | JSON-RPC server; runs in-process or standalone against a local or remote Erigon                  |
| `sentry`     | The p2p layer as an independent process                                                          |
| `txpool`     | The transaction pool as an independent process                                                   |
| `downloader` | Snapshot downloader, and verification of webseed metainfos against the preverified set            |
| `integration`| Sync-stage and datadir maintenance tool — see [cmd/integration/Readme.md](./cmd/integration/Readme.md) |
| `evm`        | Standalone EVM for running bytecode, disassembling and full trace logs                            |
| `mcp`        | Standalone MCP server for Erigon                                                                  |

`caplin`, `capcli`, `snapshots`, `rpctest` and `pics` are also built by `make all`.

### Testing

```sh
make test-short           # fast unit tests
make test-all             # full test suite
make test-fixtures-cl     # consensus-spec fixtures (also: test-fixtures-eest, test-fixtures-zkevm)
make lint                 # run before opening or updating a PR
```

[docs/TESTING.md](./docs/TESTING.md) covers the release-time incremental-sync verification, which is a separate
exercise from the test suite above.

### Use as library

```
# please use git branch name (or commit hash). don't use git tags
go get github.com/erigontech/erigon@main
go mod tidy
```

### Repository docs

- [docs/DEV_CHAIN.md](./docs/DEV_CHAIN.md) — dev chain / local devnet
- [docker-compose.yml](./docker-compose.yml) — how the services are wired when run as separate processes
- [db/etl/README.md](./db/etl/README.md) — the ETL framework used to preprocess data before DB inserts
- [db/downloader/README.md](./db/downloader/README.md) — snapshot downloader internals
- [cmd/rpcdaemon/README.md](./cmd/rpcdaemon/README.md) — RPC daemon, including running it remotely
- [cmd/prometheus/Readme.md](./cmd/prometheus/Readme.md) — metrics and dashboards
- [CI-GUIDELINES.md](./CI-GUIDELINES.md) — read before changing workflows

### JSON-RPC daemon

Most of Erigon's components (txpool, rpcdaemon, snapshots downloader, sentry, ...) can run inside Erigon or as
independent processes. Deployment modes, flags and remote (Remote DB) setup:
[RPC Daemon](https://docs.erigon.tech/fundamentals/modules/rpc-daemon) and
[cmd/rpcdaemon/README.md](./cmd/rpcdaemon/README.md).

### Default Ports and Firewalls

Every default port, the flags that change them, and firewalling guidance:
[Default ports](https://docs.erigon.tech/fundamentals/default-ports) and
[Security](https://docs.erigon.tech/fundamentals/security).

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

Running erigon from `build/bin` as a separate user requires the binaries to be *installed* using `make DIST=<path> install`. You could use `$HOME/erigon`
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

### How to change db pagesize

[post](https://github.com/erigontech/erigon/blob/main/cmd/integration/Readme.md#copy-data-to-another-db)

### Windows

Windows users may run erigon in 3 possible ways:

* Build executable binaries natively for Windows using `make`. Example: `make erigon` builds the erigon
  executable. All binaries are placed in `.\build\bin\` subfolder. There are some requirements for a successful native
  build on windows :
    * [Git](https://git-scm.com/downloads) for Windows must be installed (provides bash and MSYS2 environment). If
      you're cloning this repository is very likely you already have it
  * [GO Programming Language](https://golang.org/dl/) must be installed. Minimum required version is 1.25
    * [Chocolatey package manager](https://chocolatey.org/) for Windows must be installed. Then install the required
      build tools: `choco install cmake make mingw` (provides GNU CC Compiler >= 13, GNU Make, and CMake). Make sure
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
  a [network file system](https://docs.erigon.tech/help-center/known-issues#cloud-network-drives)
  and, additionally, MDBX locks the db for exclusive access which implies only one process at a time can access data.
  This has consequences on the running of `rpcdaemon` which has to be configured as [Remote DB](#json-rpc-daemon) even if
  it is executed on the very same computer. If instead your data is hosted on the native Linux filesystem non
  limitations apply.
  **Please also note the default WSL2 environment has its own IP address which does not match the one of the network
  interface of Windows host: take this into account when configuring NAT for port 30303 on your router.**

Getting in touch
================

### Reporting security issues/concerns

Send an email to `security [at] torquem.ch`.

### Getting help

* [Discord](https://dsc.gg/erigon) for community support and development chat
* [GitHub Issues](https://github.com/erigontech/erigon/issues) to report a bug — see
  [Grab diagnostic for bug report](#grab-diagnostic-for-bug-report) first
* [Release notes](https://github.com/erigontech/erigon/releases) for what changed in each version

License
=======

Erigon is licensed under the [GNU Lesser General Public License v3.0](./COPYING).