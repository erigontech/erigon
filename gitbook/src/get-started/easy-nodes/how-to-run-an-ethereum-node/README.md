---
description: 'Quick Start: Running a Ethereum Node with Erigon'
---

# How to run a Ethereum node

## 1. Prerequisites Check

1. Confirm your machine meets the necessary [Hardware Requirements](../../hardware-requirements.md) based on your desired sync mode.
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
    image: erigontech/erigon:v3.2.2
    container_name: erigon-node
    restart: always
    command:
      # --- Basic Configuration ---
      - --chain=mainnet
      - --http.addr="0.0.0.0"
      - --http.api=eth,web3,net,debug,trace,txpool
      # --- Performance Tweaks ---
      - --torrent.download.rate=512mb
      # --- Sync Mode (Optional) ---
      # To change Sync Mode, uncomment the line below:
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

{% hint style="warning" %}
⚠️ **Action Required**: You must change the volume path! Replace `/path/to/erigon/data` with a valid, empty directory on your machine where you want Erigon to store its files.
{% endhint %}

### **B. Launch the Node and Monitor Progress**

Open your terminal in the directory where you saved `docker-compose.yml`. To start the node and immediately see the sync process type:

```
docker compose up
```

## Flag explanation

* `--chain=mainnet` specifies to run on Ethereum mainnet, see also other [Supported Networks](../../fundamentals/supported-networks.md)
* Add `--prune.mode=minimal` to run minimal [Sync Mode](https://erigon.gitbook.io/docs/summary/fundamentals/sync-modes) or `--prune.mode=archive` to run an archive node
* `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](../../fundamentals/web3-wallet.md)
* `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set

When you get familiar with running Erigon from CLI you may also consider [staking](../../../archive/staking.md) and/or run a Ethereum node with an [external Consensus Layer](ethereum-with-an-external-cl.md).

{% include "../../../.gitbook/includes/press-ctrl+c-in-the-termina....md" %}

Additional flags can be added to [configure](../../fundamentals/configuring-erigon.md) Erigon with several options.
