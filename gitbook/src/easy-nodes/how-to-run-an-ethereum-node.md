---
description: Quick start your Erigon node on Ethereum mainnet
---

# How to run an Ethereum node

## First steps

1. Check the [hardware](../getting-started/hardware-requirements.md) prerequisites;
2. Check which [sync mode](../fundamentals/sync-modes.md) you want to run and the recommended [disk space](../getting-started/hardware-requirements.md#minimal-node-requirements).
3. Install [Docker Desktop](https://app.gitbook.com/u/VThVXbGNqDg7P3yyr4K7KxnddwM2)

## Download and start Erigon​

To download Erigon and execute a Ethereum minimal node paste the following command in your terminal:

{% code overflow="wrap" %}
```bash
docker run \
-it erigontech/erigon:v3.2.2 \
--chain=mainnet \
--prune.mode=minimal \
--datadir /erigon-data \
--http.addr="0.0.0.0" \
--http.api=eth,web3,net,debug,trace,txpool \
--torrent.download.rate=512mb
```
{% endcode %}

Docker will automatically download Erigon v3.2.2 and start syncing.

{% include "../.gitbook/includes/press-ctrl+c-in-the-termina....md" %}

### Flag explanation

* `-it` lets you see what's happening and interact with Erigon;
* `--chain=mainnet` specifies to run on Ethereum mainnet;
* `--prune.mode=minimal` tells Erigon to use minimal [Sync Mode](https://erigon.gitbook.io/docs/summary/fundamentals/sync-modes); this will allow Erigon to sync in few hours.
* `--datadir` tells Erigon where to store data inside the container;
* `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](../fundamentals/web3-wallet.md);
* `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set.

When you get familiar with running Erigon from CLI you may also consider [staking](../staking/staking.md) and/or run a [Ethereum node with an external Consensus Layer](ethereum-with-an-external-cl.md).

Additional flags can be added to [configure](../fundamentals/configuring-erigon.md) Erigon with several options.
