---
title: "How to run an Ethereum node"
description: "Run a full Ethereum mainnet node using Caplin (built-in consensus layer) or connect an external CL client."
sidebar_position: 1
---


# How to run an Ethereum node

## 1. Prerequisites Check

1. Confirm your machine meets the necessary [Hardware Requirements](/get-started/hardware-requirements) based on your desired prune mode.
2. **Install Docker**:
   * For Linux, install [Docker Engine](https://docs.docker.com/engine/install).
   * For macOS or Windows, install [Docker Desktop](https://docs.docker.com/desktop/).

## 2. Configure and Launch Erigon

Follow these steps to configure and launch the All-in-One Client. Erigon uses its embedded Consensus Layer (Caplin) by default, so you don't need a separate Consensus Client (CL).

### **A. Create the Configuration File**

Create a new file named `docker-compose.yml` in a directory where you want to manage your Erigon setup, and paste the following content into it:

```sh
services:
  erigon:
    image: erigontech/erigon:v{ERIGON_VERSION}
    container_name: erigon-node
    restart: always
    command:
      # --- Basic Configuration ---
      - --chain=mainnet
      - --http.addr=0.0.0.0
      - --http.api=eth,web3,net,debug,trace,txpool
      # --- Performance Tweaks ---
      - --torrent.download.rate=512mb
      # --- Prune Mode (Optional) ---
      # To change Prune Mode, uncomment the line below:
      # - --prune.mode=archive
      # or
      # - --prune.mode=minimal
    ports:
      - "8545:8545" # Exposes the RPC port (needed for wallets/dApps)
    volumes:
      # *** IMPORTANT: CHANGE THIS PATH! ***
      # Replace the path below with an actual directory on your machine 
      # where you want the blockchain data stored (e.g., /mnt/ssd/erigon-data)
      - /path/to/erigon/data:/var/lib/erigon
```

:::warning
⚠️ **Action Required**: You must change the volume path! Replace `/path/to/erigon/data` with a valid, empty directory on your machine where you want Erigon to store its files.
:::

### **B. Launch the Node and Monitor Progress**

Open your terminal in the directory where you saved `docker-compose.yml`. To start the node and immediately see the sync process type:

```text
docker compose up
```

## Flag explanation

* `--chain=mainnet` specifies to run on Ethereum mainnet
* Add `--prune.mode=minimal` to run minimal [Pruning Mode](/fundamentals/pruning-modes) or `--prune.mode=archive` to run an archive node
* `--http.addr=0.0.0.0 --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](/fundamentals/web3-wallet)
* `--torrent.download.rate` sets the torrent download rate cap. The default is `512mb` (megabytes per second). During initial sync Erigon will use the full allowance — on a dedicated machine this is fine, but if you share the machine with other work you may want to lower it (e.g. `--torrent.download.rate=128mb`). Set `--torrent.download.rate=Inf` to remove the limit entirely.

When you get familiar with running Erigon from CLI you may also consider [staking](/staking/caplin) and/or running an Ethereum node with an [external Consensus Layer](/get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl).

:::tip
Press `Ctrl+C` in your terminal to stop Erigon.
:::

Additional flags can be added to [configure](/fundamentals/configuring-erigon/) Erigon with several options.
