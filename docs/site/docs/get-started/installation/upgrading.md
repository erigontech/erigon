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

## Managing your Data

Erigon 3.1 introduces a new snapshot format while continuing to support the old one. This means that new releases are fully compatible with your existing data. However, users who want the latest data files and data-specific fixes can perform an **optional** manual data upgrade:

1. Backup your datadir.
2. [Upgrade your Erigon installation](#upgrading-your-erigon-installation) whether from a binary, compiled source code, or Docker.
3. Upgrade your snapshots using one of the [options below](#snapshots-upgrade-options).
4. Run Erigon; it downloads any missing snapshots and resumes syncing.

### Snapshots Upgrade Options

* `erigon snapshots update-to-new-ver-format --datadir /your/datadir`: converts your existing snapshots in place to the latest format, **keeping your data**. Quicker, but you won't get the full performance benefits of freshly built snapshots.
* `erigon snapshots reset --datadir /your/datadir`: **destructive** — deletes `chaindata/` (and the Heimdall / Polygon-bridge DBs) and removes any snapshot files **not** in the preverified set (locally generated files, and torrents with a mismatched hash). Preverified snapshots already on disk are **kept**; on the next start Erigon downloads only **missing or incorrect** snapshots and rebuilds state.

:::warning
`erigon snapshots reset` does **not** reuse your `chaindata/` — it deletes the chaindata DB (and the Heimdall / Polygon-bridge DBs) and any locally generated or mismatched snapshots, then rebuilds state on the next start (downloading only missing or incorrect snapshots). Back up your `--datadir` first, and prefer `update-to-new-ver-format` if you want to keep your current data.
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

1. Make sure that you're running Erigon on 3.1.x version, use `erigon --version`.
2. Run `erigon snapshots reset-to-old-ver-format --datadir /your/datadir` to reset your snapshots to old format.
3. `git checkout v3.0.x` to checkout to preferred `3.0` version. For example now latest: `git checkout v3.0.15`
4. Run your old version of Erigon.

## Upgrading your Erigon Installation

Follow the below instructions depending on your installation method:

* [Pre-built binaries](#pre-built-binaries-only-linux-and-macos)
* [Docker](#docker)
* [Compiled source code](#compiled-from-source)

### Pre-built Binaries (only Linux and macOS)

Download the latest binary file from [https://github.com/erigontech/erigon/releases](https://github.com/erigontech/erigon/releases), do the [checksum](./#install-prebuilt) and reinstall it, no other operation needed.

### Docker

If you're using Docker to run Erigon, the process to upgrade to a newer version of the software is straightforward and revolves around pulling the latest Docker image and then running it.

Simply follow the [Docker](#docker) instructions and install and launch the new version.

### Compiled from source

To upgrade Erigon to a newer version when you've originally installed it via Git and manual compilation, follow the installation instructions from step 2 "Check Out the Desired Stable Version (Tag)".
