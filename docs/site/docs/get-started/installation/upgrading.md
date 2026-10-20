---
title: "Upgrading from a previous version"
description: "Upgrade from a previous version of Erigon without losing chain data or your configuration."
sidebar_position: 1
---


# Upgrading from a previous version

Updating to the latest version of Erigon gives you access to the latest features and ensures optimal performance and stability.

## General Recommendations Before Upgrade

* **Read Release Notes**: Carefully review the [Release Notes](https://github.com/erigontech/erigon/releases) for breaking changes and new features relevant to your setup.
* **Terminate your Erigon**: End your current Erigon session by pressing `CTRL+C`.
* **Backup**: Always back up your `datadir` before performing major upgrades.

## Upgrading from 3.7 to 3.8

{/* llms-pinned-version: [v3.8.0 release notes](https://github.com/erigontech/erigon/releases/tag/v3.8.0) */}

A 3.7.x node upgrades to 3.8 in place: there is no database migration and no re-sync. Before restarting on the new version, check the following:

* **Standalone RPC daemon**: the private ETHBACKEND API is now version 4.2. Upgrade a standalone `rpcdaemon` together with Erigon: a 3.7 `rpcdaemon` connected to a 3.8 Erigon, or the reverse, logs `incompatible interface versions` and shuts down.
* **Database durability**: batched database syncing is now the default (`--db.safe.nosync=true`). It reduces disk-flush pauses, but a power loss can roll back the most recent commits. Set `--db.safe.nosync=false` if you need every commit flushed to disk.
* **Caplin QUIC port**: v3.7.1 used UDP `4001` for QUIC; 3.8 uses UDP `4002`. Update firewall rules and container port mappings, or set `--caplin.discovery.quicport` explicitly. If the QUIC port cannot be opened, the new `--caplin.quic.disable` flag turns QUIC off and keeps TCP. See [Default Ports](../../fundamentals/default-ports).
* **Caplin peer limits**: connections are now capped per IPv4 and IPv6 network range. If many of your peers share a range, set `--caplin.ip-block-limits.trusted-cidrs` or `--caplin.ip-block-limits.disable`.
* **Browser and GraphQL access**: browser WebSocket connections are now checked against `--http.corsdomain`. Without the flag, only pages served from `http://localhost`, `http://127.0.0.1`, `http://[::1]` or the node's own hostname can connect; once it is set, only the listed origins can. Clients that send no `Origin` header, such as scripts and backend services, are not affected. In addition, `/graphql` answers only host names listed in `--http.vhosts`. `--rpc.accessList` now also applies to WebSocket, IPC and subscriptions, so an allow list must name the subscribe methods your clients use. See [Configuring Erigon](../../fundamentals/configuring-erigon).
* **RPC resource limits**: a node now holds at most 10,000 live filters and subscriptions, and each WebSocket or IPC connection at most 1,024 subscriptions. Requests above a limit fail with error `-32005`. Raise the limits with `--rpc.subscription.filters.maxsubscriptions` and `--rpc.subscription.limit`, or set either to `0` to remove it.
* **No cleartext HTTP/2**: the plain HTTP RPC port serves HTTP/1.1 only. Clients and proxies that talk HTTP/2 to it without TLS must switch to HTTP/1.1, or enable HTTPS with `--https.enabled`.
* **Health checks**: the beacon API's `/eth/v1/node/health` returns HTTP `206` while the node is syncing. A check that requires `200` should accept `206`, or add `?syncing_status=200`.
* **Gnosis and Chiado**: Caplin now keeps data columns for 16,384 epochs (about 15 days) instead of 4,096, so these nodes need more disk.
* **Engine API and JSON-RPC**: nonstandard `engine_*` methods such as `engine_start` are no longer served, and several JSON-RPC and tracing methods changed results or error codes. Review the [v3.8.0 release notes](https://github.com/erigontech/erigon/releases/tag/v3.8.0) if you run applications or integrations against the RPC.

## Managing your Data

Erigon 3.1 introduces a new snapshot format while continuing to support the old one. This means that new releases are fully compatible with your existing data. However, users who want the latest data files and data-specific fixes can perform an **optional** manual data upgrade:

1. Backup your datadir.
2. [Upgrade your Erigon installation](#upgrading-your-erigon-installation) whether from a binary, compiled source code, or Docker.
3. Upgrade your snapshots using one of the [options below](#snapshots-upgrade-options).
4. Run Erigon; it downloads any missing snapshots and resumes syncing.

### Snapshots Upgrade Options

* `erigon snapshots update-to-new-ver-format --datadir /your/datadir`: converts your existing snapshots in place to the latest format, **keeping your data**. Quicker, but you won't get the full performance benefits of freshly built snapshots.
* `erigon snapshots reset --datadir /your/datadir`: **destructive** — deletes `chaindata/` (and any leftover `heimdall/` and `polygon-bridge/` directories from a former Polygon datadir) and removes any snapshot files **not** in the preverified set (locally generated files, and torrents with a mismatched hash). Preverified snapshots already on disk are **kept**; on the next start Erigon downloads only **missing or incorrect** snapshots and rebuilds state.

:::warning
`erigon snapshots reset` does **not** reuse your `chaindata/` — it deletes the chaindata DB (and any leftover Polygon aux directories) and any locally generated or mismatched snapshots, then rebuilds state on the next start (downloading only missing or incorrect snapshots). Back up your `--datadir` first, and prefer `update-to-new-ver-format` if you want to keep your current data.
:::

Choose `update-to-new-ver-format` to convert your data in place, or `reset` for a clean reset to the preverified snapshot set.

<details>

<summary>Why Upgrading Erigon Doesn't Always Fix Your Data</summary>

Upgrading Erigon involves a key distinction between its core software and its data files, which are managed separately. This approach is rooted in practicality and user control.

The Erigon **software** is the application that processes and interacts with blockchain data. However, the majority of the data itself, including the state of the network, exists in **data files** that you download and store locally.

If a bug is discovered, it is often in the data itself rather than a flaw in the Erigon code. In such a case, simply updating the Erigon binary won't resolve the issue because the faulty data remains on your disk. This is because Erigon upgrades do not normally alter the data files. You must reset and re-download the data snapshots to get the corrected files.

This separation prevents unnecessary and time-consuming processes. The Erigon team cannot regenerate months of data for every minor bug fix, and users shouldn't have to re-download terabytes of information with every new weekly software release.

This design also provides flexibility. A user might need a specific data fix but prefer to remain on an older software version due to a known bug or regression in the latest release. Similarly, a user might be satisfied with their current data set and only need to update the Erigon binary.

While this dual-versioning system may seem complex, it is a deliberate design choice that optimizes for both efficiency and user autonomy.

</details>

### Snapshots Downgrade Options

If upgrading snapshots(`3.0`to `3.1`) now happens automatically, you should follow these instructions for downgrading:

:::warning
**WARNING**: This algorithm will remove incompatible `3.1` snapshot files because they are not backward-compatible.
:::

1. Make sure that you're running Erigon on a 3.1.x version; check with `erigon --version`.
2. Run `erigon snapshots reset-to-old-ver-format --datadir /your/datadir` to reset your snapshots to the old format.
3. `git checkout v3.0.x` to check out the preferred `3.0` version. For example, the latest 3.0 release is: `git checkout v3.0.17`
4. Run your old version of Erigon.

## Upgrading your Erigon Installation

Follow the below instructions depending on your installation method:

* [Pre-built binaries](#pre-built-binaries-linux-only)
* [Docker](#docker)
* [Compiled source code](#compiled-from-source)

### Pre-built Binaries (Linux only)

Download the latest binary file from [https://github.com/erigontech/erigon/releases](https://github.com/erigontech/erigon/releases), verify the [checksum](/get-started/installation/#install-prebuilt), and reinstall it; no other operation is needed. Releases ship Linux binaries only (amd64 and arm64); on macOS, use [Docker](/get-started/installation/#install-docker) or [build from source](/get-started/installation/#install-source).

### Docker

If you're using Docker to run Erigon, the process to upgrade to a newer version of the software is straightforward and revolves around pulling the latest Docker image and then running it.

Follow the [Docker installation](/get-started/installation/#install-docker) instructions to pull and launch the new version.

### Compiled from source

To upgrade Erigon to a newer version when you've originally installed it via Git and manual compilation, follow the [build from source](/get-started/installation/#install-source) instructions from step 2.2, "Check Out the Desired Stable Version (Tag)".
