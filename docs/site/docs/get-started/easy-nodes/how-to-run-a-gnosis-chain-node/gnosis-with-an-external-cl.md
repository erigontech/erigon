---
title: "Gnosis Chain with an external CL"
description: "Connect an external consensus layer client to Erigon for a Gnosis Chain full or validator node."
sidebar_position: 1
---

# Gnosis Chain with an external CL

Alternatively, you can also run a Gnosis Chain node as an Execution Layer (EL) and couple it with an external Consensus Layer (CL). Here is an example of configuration with **Lighthouse**.

### 1. Start Erigon:

Start Erigon adding the `--externalcl` flag.

```bash
erigon --chain=gnosis --externalcl
```

If your CL client is on a different device, add the following flags:

* `--authrpc.addr <this-host-LAN-IP>` — the Engine API listens on localhost by default, so it has to be widened for a remote CL. Read the warning below before reaching for `0.0.0.0`;
* `--authrpc.vhosts <CL_host>` where `<CL_host>` is the source host or the appropriate hostname that your CL client is using.

:::warning
The Engine API drives block processing, so anything that can reach it and holds
your JWT secret controls the node. Only widen it when the CL really is on
another machine, and protect the endpoint when you do:

* prefer the specific interface — `--authrpc.addr <this-host-LAN-IP>` — over
  `0.0.0.0`, which listens on every interface including any public one;
* restrict port `8551` at the firewall to the CL host's address, and never
  expose it to the internet;
* treat `--authrpc.vhosts` as a `Host`-header check, not a network control: it
  is not a substitute for a firewall rule;
* keep `jwt.hex` readable only by the accounts that need it, and copy it to the
  CL host over a private channel.
:::

### 2. Install Lighthouse

Install Lighthouse, following instructions at [https://lighthouse-book.sigmaprime.io/installation.html](https://lighthouse-book.sigmaprime.io/installation.html).

The official pre-built binaries already support Gnosis Chain and Chiado, so no special build is required. You can confirm this with `lighthouse --help`, which lists `gnosis` and `chiado` among the accepted `--network` values.

:::tip
Track Lighthouse releases and keep it current, the same way you would Erigon.
Beyond fixes and performance work, a consensus client carries the fork schedule
it was built with: a version released before an upcoming hard fork may not
follow the chain through it, which stalls your execution layer too. Watch the
[Lighthouse releases](https://github.com/sigp/lighthouse/releases) page and
upgrade before scheduled forks, not after.
:::

:::note
Only if you build Lighthouse **from source** do you need to enable the Gnosis Chain support explicitly, since it is not one of the default Cargo features:

```bash
env FEATURES=gnosis make
```
:::

### 3. Sync Lighthouse to a public checkpoint

Because Erigon needs a target head in order to sync, Lighthouse must be synced before Erigon can synchronize. The fastest way to synchronize Lighthouse is to use one of the many public checkpoint synchronization endpoints, for example:

1. `https://checkpoint.gnosischain.com` for Gnosis Chain;
2. `https://checkpoint.chiadochain.net` for Chiado testnet.

### 4. Set the Erigon JWT secret path in Lighthouse

To communicate with Erigon, the execution endpoint must be specified as `<erigon address>:8551`, where `<erigon address>` is either `http://localhost` or the IP address of the device running Erigon.

1.  Lighthouse must point to the [JWT secret](../../../fundamentals/jwt) automatically created by Erigon in the `--datadir` directory. In the following example the default data directory is used.

    ```bash
    lighthouse bn \
    --network gnosis \
    --datadir=data \
    --http \
    --execution-endpoint http://localhost:8551 \
    --execution-jwt /home/user/.local/share/erigon/jwt.hex \
    --checkpoint-sync-url "https://checkpoint.gnosischain.com"
    ```

    Here is an example of Lighthouse running the Chiado testnet:

    ```bash
    lighthouse bn \
    --network chiado \
    --datadir=data \
    --http \
    --execution-endpoint http://localhost:8551 \
    --execution-jwt /home/user/.local/share/erigon/jwt.hex \
    --checkpoint-sync-url "https://checkpoint.chiadochain.net"
    ```

Check the Erigon and Lighthouse logs to make sure that the EL and CL are communicating and that your node is syncing correctly.
