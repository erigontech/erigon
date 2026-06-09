---
title: "Architecture"
description: "How Erigon is built — staged sync, modular processes, flat-DB on MDBX, immutable snapshots, and an embedded consensus layer."
sidebar_position: 3
---

# Architecture

Erigon is an Ethereum execution-layer client designed around three goals: **low disk footprint**, **predictable performance**, and **operational simplicity**. This page explains how those goals shape its internal design so you know what is happening when you run the binary.

## At a glance

```mermaid
flowchart TB
    subgraph erigon["erigon (one binary)"]
        direction TB
        Sentry["Sentry<br/>(p2p)"]
        Downloader["Downloader<br/>(snapshots + EL data)"]
        Execution["Execution<br/>(Staged Sync)"]
        RPC["RPC Daemon<br/>(JSON-RPC, WS,<br/>GraphQL, gRPC)"]
        TxPool["TxPool"]
        Caplin["Caplin (CL)<br/>embedded by default"]
        Datadir[("datadir<br/>MDBX chaindata<br/>+ .seg snapshots")]

        Caplin -->|new blocks<br/>(Engine API)| Execution
        Sentry -->|tx gossip| TxPool
        Execution --> RPC

        Downloader -.writes snapshots.-> Datadir
        Execution -.reads/writes.-> Datadir
        RPC -.reads.-> Datadir
        TxPool -.reads/writes.-> Datadir
        Caplin -.reads/writes.-> Datadir
    end

    classDef component fill:#fff5e6,stroke:#EF7716,stroke-width:1.5px,color:#000
    classDef storage fill:#f0f0ff,stroke:#5860EF,stroke-width:1.5px,color:#000
    class Sentry,Downloader,Execution,RPC,TxPool,Caplin component
    class Datadir storage
```

Every box above is a Go package that runs **inside the `erigon` process by default**. The same binary can also be split into independent processes — see [Modular processes](#modular-processes).

## Staged sync

Erigon processes the chain in a series of **stages** rather than the traditional "fetch block → execute block → repeat" loop. Each stage runs to completion across the whole range before the next stage starts.

A simplified pipeline:

```
1. Snapshots    → fetch immutable history files via BitTorrent (OtterSync)
2. Headers      → download and verify block headers
3. Bodies       → download block bodies
4. Senders      → recover transaction senders from signatures
5. Execution    → re-execute transactions, write state, and build the
                  state commitment (Merkle trie) — all in a single pass
6. Finalization → update the canonical chain
```

This is the order of Erigon 3's default stage list. There is no separate
`Commitment` stage — trie/state-root computation runs *inside* Execution (see
below).

Two consequences:

- **Faster initial sync.** Stages are CPU/IO-bound in different ways, so the pipeline avoids waiting on a single bottleneck per block. Snapshot download (stage 1) parallelises with execution warm-up.
- **Cheap restarts.** Each stage records its progress. Killing Erigon mid-sync and restarting resumes from the last completed checkpoint — Erigon does not re-execute from genesis.

In Erigon 3, the Execution stage absorbs many previously-separate stages (`stage_hash_state`, `stage_trie`, `log_index`, `history_index`, `trace_index`) into a single pass, which is one reason Erigon 3 syncs faster than Erigon 2.

## Modular processes

Each of Sentry, Downloader, TxPool, RPC Daemon, and Caplin can run as an **independent process** that talks to the rest over a private gRPC API.

Use the all-in-one binary by default. Split out a service when you have a concrete reason:

- **Resource isolation** — pin the RPC Daemon to its own CPUs so a heavy `eth_call` cannot stall block production.
- **Horizontal scaling** — run multiple RPC Daemons against one execution node to serve more concurrent clients.
- **Replacement** — substitute Erigon's TxPool or Sentry with your own implementation.
- **Security** — keep the p2p surface (Sentry) on a separate host from the JSON-RPC surface (RPC Daemon).

See the [Modules](modules) section for per-component documentation, and the repository's [docker-compose.yml](https://github.com/erigontech/erigon/blob/main/docker-compose.yml) for a worked split-process example.

## Storage model

Erigon stores everything under a single **datadir**. Two kinds of files live there:

| Kind | Path | Role | Mutable? |
|------|------|------|----------|
| Active state | `chaindata/` | Recent state and recent blocks, served from MDBX | Yes |
| Historical data | `snapshots/` | Older blocks, history, indices — distributed as immutable `.seg` files | No |

This split is what enables Erigon's small disk footprint relative to other archive nodes:

- **`chaindata/` is small (~15 GB on Ethereum mainnet)** because it only holds latest state and recently-updated values.
- **`snapshots/` is large but content-addressed.** Once a `.seg` file is finalised it is the *same file* on every Erigon node in the world — meaning it can be distributed via BitTorrent and verified by hash. There is no rewrite-amplification penalty for keeping history.

For the exact directory layout, file sizes on mainnet, and how to split storage across fast/slow disks, see [Database](database) and [Optimizing Storage](optimizing-storage).

## Embedded consensus (Caplin)

Erigon ships with **Caplin**, a built-in consensus-layer client. Caplin runs embedded by default, so a single `erigon` invocation runs both the execution layer and the consensus layer with no extra processes, no JWT secret to manage, and no separate binary to upgrade.

If you prefer to run an external CL (Lighthouse, Prysm, Teku, Nimbus), pass `--externalcl` and connect via the standard Engine API. The Engine-API path is identical to what Geth, Besu, or Reth exposes.

See [Caplin](caplin) for full details.

## Flat key-value state

Where most clients store account state in a Merkle Patricia Trie *inside* the database, Erigon stores it as **flat key-value pairs** and computes the trie root incrementally as part of Execution. Trade-off:

- **Read path is direct** — `getAccount(addr)` is a single key lookup, not a multi-level trie traversal.
- **Write path defers trie hashing** — updates accumulate in the KV store and the trie is rebuilt only when state-root computation requires it.

This is the single largest reason Erigon's RPC performance stays flat under load: there is no background trie compaction that can spike CPU during an `eth_call` burst.

## Pruning is a retention decision, not a sync mode

Erigon 3 has one sync pipeline. The user-facing choice is *what to retain after sync*, controlled by `--prune.mode`:

| Mode | Retained | Use case |
|---|---|---|
| `archive` | Entire state history + all blocks | Indexers, block explorers, deep historical queries |
| `full` (default) | Latest state + a rolling window of recent blocks (the default prune distance, ~262k blocks) | Most users, dApps, validators |
| `blocks` | All blocks, but no state history | Full block/receipt history without archive-size state |
| `minimal` | Latest state only (shortest block window) | Solo stakers, constrained hardware |

See [Pruning Modes](prune-modes) for the full comparison.

## Where to go next

- [Database](database) — datadir layout, MDBX internals, mainnet sizing
- [Modules](modules) — running RPC Daemon, TxPool, Sentry, Downloader as separate processes
- [Pruning Modes](prune-modes) — choosing what history to keep
- [Optimizing Storage](optimizing-storage) — splitting datadir across fast and slow disks
