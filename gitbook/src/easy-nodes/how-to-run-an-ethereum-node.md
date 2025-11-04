---
description: 'Quick Start: Running a Ethereum Node with Erigon'
---

# How to run an Ethereum node

## First steps

1. Check the [hardware](../getting-started/hardware-requirements.md) prerequisites;
2. Install [Docker Engine](https://docs.docker.com/engine/install) if you run Linux or [Docker Desktop](https://docs.docker.com/desktop/) if you run macOS/Windows.

## Download and start Erigon​

To download Erigon and start syncing a **Ethereum full node** paste the following command in your terminal:

{% code overflow="wrap" %}
```bash
docker run -it erigontech/erigon:v3.2.2 --chain=mainnet --http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool --torrent.download.rate=512mb
```
{% endcode %}

There is no need to connect a consensus client (CL) since Erigon uses [Caplin](../fundamentals/caplin.md), its embedded CL, by default.

Now you can relax and watch your Erigon node sync!

### Flag explanation

* `-it` lets you see what's happening and interact with Erigon
* `--chain=mainnet` specifies to run on Ethereum mainnet, see also other [Supported Networks](../fundamentals/supported-networks.md)
* Add `--prune.mode=minimal` to run minimal [Sync Mode](https://erigon.gitbook.io/docs/summary/fundamentals/sync-modes) or `--prune.mode=archive` to run an archive node
* `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](../fundamentals/web3-wallet.md)
* `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set

When you get familiar with running Erigon from CLI you may also consider [staking](../staking/staking.md) and/or run a [Ethereum node with an external Consensus Layer](ethereum-with-an-external-cl.md).

{% include "../.gitbook/includes/press-ctrl+c-in-the-termina....md" %}

Additional flags can be added to [configure](../fundamentals/configuring-erigon.md) Erigon with several options.
