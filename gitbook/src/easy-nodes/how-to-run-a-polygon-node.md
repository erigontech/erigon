---
description: Quick start your Erigon node on Polygon network
---

# How to run a Polygon node

{% hint style="warning" %}
The final release series of Erigon that officially supports Polygon is 3.1.\*. For the software supported by Polygon, please refer to the link: [https://github.com/0xPolygon/erigon/releases](https://github.com/0xPolygon/erigon/releases).
{% endhint %}

## First steps

1. Check the [hardware](../getting-started/hardware-requirements.md) prerequisites;
2. Check which [sync mode](../fundamentals/sync-modes.md) you want to run and the recommended [disk space](../getting-started/hardware-requirements.md#minimal-node-requirements).
3. Install [Docker Desktop](https://app.gitbook.com/u/VThVXbGNqDg7P3yyr4K7KxnddwM2)

## Start Erigon​

To execute a Ethereum full node paste the following command in your terminal:

{% code overflow="wrap" %}
```bash
docker run \
-it erigontech/erigon:v3.2.2 \
--chain=bor-mainnet \
--bor.heimdall=https://heimdall-api.polygon.technology \
--datadir /erigon-data \
--prune.mode=minimal \
--http.addr="0.0.0.0" \
--http.api=eth,web3,net,debug,trace,txpool \
--torrent.download.rate=512mb
```
{% endcode %}

Docker will automatically download Erigon version 3.2.2 and start syncing Erigon.

{% include "../.gitbook/includes/press-ctrl+c-in-the-termina....md" %}

### Flag explanation

* `-it` lets you see what's happening and interact with Erigon.
* `--chain=bor-mainnet` and `--bor.heimdall=https://heimdall-api.polygon.technologyspecifies` specify respectively the Polygon mainnet and the API endpoint for the Heimdall network; to use Amoy tesnet replace with flags `--chain=amoy --bor.heimdall=https://heimdall-api-amoy.polygon.technology`.
* `--prune.mode=minimal` tells Erigon to use minimal [Sync Mode](https://erigon.gitbook.io/docs/summary/fundamentals/sync-modes); this will allow Erigon to sync in few hours.
* `--datadir` tells Erigon where to store data inside the container;
* `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](../fundamentals/web3-wallet.md);
* `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set.
