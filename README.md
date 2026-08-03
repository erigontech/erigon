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

The torrent client in the Downloader keeps its own file, `logs/torrent.log`. It is written at whichever is **more
verbose** of `--torrent.verbosity` and `WARN`, so warnings and errors always reach it even at the default verbosity.
Messages at `--torrent.verbosity` or above are additionally forwarded to Erigon's own file and console loggers, which
emit them only if `--log.dir.verbosity` and `--verbosity` are themselves verbose enough.

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

On Windows, build natively (Chocolatey, MinGW, and the MinGW anti-virus false-positive workaround) or under WSL2:
[Native compilation](https://docs.erigon.tech/get-started/installation/#install-native) and
[WSL](https://docs.erigon.tech/get-started/installation/#install-wsl).

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

Ports to open and the reserved IPv4 ranges to block:
[Hetzner firewall note](https://docs.erigon.tech/help-center/troubleshooting#hetzner-cloud--dedicated-server-firewall-note).

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