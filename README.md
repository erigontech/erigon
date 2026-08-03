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

Usage
=====

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

Caplin is Erigon's embedded Consensus Layer, enabled by default. Flags, archival modes and Beacon API configuration:
[Caplin](https://docs.erigon.tech/fundamentals/caplin) and [Caplin for staking](https://docs.erigon.tech/staking/caplin).

Why a new Consensus Layer rather than the Engine API: the Engine API delivers blocks one at a time, which does not suit
Erigon's bulk model — Erigon is built to handle many blocks simultaneously and to sort and process data in batches.
Owning the consensus layer lets Erigon drive block handling on its own terms instead of being paced by the Engine API.

### Dev Chain

<code> 🔬 Detailed explanation is [DEV_CHAIN](/docs/DEV_CHAIN.md).</code>

For developers
==============

### Building

Toolchain: [Go >= 1.25](https://golang.org/doc/install), GCC 10+ or Clang, 64-bit architecture. On Linux, kernel > v4.

```sh
git clone https://github.com/erigontech/erigon.git
cd erigon
make erigon
```

Binaries land in `./build/bin`. Use `-j<n>` to parallelise the build. Packaged binaries, Docker images and the
per-platform install steps are on the
[installation](https://docs.erigon.tech/get-started/installation) page; to build a specific release rather than `main`,
check out its tag.

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
- [cmd/downloader/readme.md](./cmd/downloader/readme.md) — snapshots overview: what they are, when they are created and pulled
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