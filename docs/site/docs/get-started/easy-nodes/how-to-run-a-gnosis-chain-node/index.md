---
title: "How to run a Gnosis Chain node"
description: "Run a full Gnosis Chain node with Erigon's embedded consensus layer or an external CL client."
sidebar_position: 2
---


# How to run a Gnosis Chain node

## 1. Prerequisites Check

1. Confirm your machine meets the necessary [Hardware Requirements](/get-started/hardware-requirements) based on your desired pruning mode.
2. **Install Docker**:
   * For Linux, install [Docker Engine](https://docs.docker.com/engine/install).
   * For macOS or Windows, install [Docker Desktop](https://docs.docker.com/desktop/).

## 2. Configure and Launch Erigon

Follow these steps to configure and launch the All-in-One Client. Erigon uses its embedded Consensus Layer (Caplin) by default, so you don't need a separate Consensus Client (CL).

### **A. Create the Configuration File**

Create a new file named `docker-compose.yml` in a directory where you want to manage your Erigon setup, and paste the following content into it:

```yaml
services:
  erigon:
    image: erigontech/erigon:v{ERIGON_VERSION}
    container_name: erigon-node
    restart: always
    command:
      # --- Basic Configuration ---
      - --chain=gnosis
      - --http.addr=0.0.0.0
      - --http.api=eth,web3,net,debug,trace,txpool
      # --- Pruning Mode (Optional) ---
      # To change Pruning Mode, uncomment the line below:
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

```bash
docker compose up
```

## Flag explanation

* `--chain=gnosis` specifies to run on Gnosis Chain, use `--chain=chiado` for Chiado testnet
* Add `--prune.mode=minimal` to run minimal [Pruning Mode](/fundamentals/pruning-modes) or `--prune.mode=archive` to run an archive node
* `--http.addr=0.0.0.0 --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](/fundamentals/web3-wallet)
* `--torrent.download.rate` is deliberately not set above, because its default of `512mb` (megabytes per second) is already the maximum this recipe would ask for. During initial sync Erigon uses the full allowance, which is what you want on a dedicated machine. Add the flag only to **lower** the cap if you share the machine with other work (e.g. `--torrent.download.rate=128mb`), or set `--torrent.download.rate=Inf` to remove the limit entirely.

When you get familiar with running Erigon from CLI you may also consider [staking](/staking/caplin) and/or run a Gnosis node with an [external Consensus Layer](/get-started/easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl).

:::tip
Press `Ctrl+C` in your terminal to stop Erigon.
:::

Additional flags can be added to [configure](/fundamentals/configuring-erigon/) Erigon with several options.
