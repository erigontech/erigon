---
description: Run a Gnosis Chain node with Erigon and an external Consensus Layer (CL).
---

# Gnosis Chain with an external CL

Alternatively, you can also run a Ethereum node as an Execution Layer (EL) and couple it with an external Consensus Layer (CL). Here is an example of configuration with **Lighthouse**.

### 1. Start Erigon:

Start Erigon adding the `--externalcl` flag.

```bash
erigon --chain=gnosis --externalcl 
```

If your CL client is on a different device, add the following flags:

* `--authrpc.addr 0.0.0.0`, since the Engine API listens on localhost by default;
* `--authrpc.vhosts <CL_host>` where `<CL_host>` is the source host or the appropriate hostname that your CL client is using.

### 2. Install Lighthouse

Install Lighthouse, following instructions at [https://lighthouse-book.sigmaprime.io/installation.html](https://lighthouse-book.sigmaprime.io/installation.html).

* Compile it with a feature flag to enable Gnosis Chain:

```bash
env FEATURES=gnosis make
```

### 3. Sync Lighthouse to a public checkpoint

Because Erigon needs a target head in order to sync, Lighthouse must be synced before Erigon can synchronize. The fastest way to synchronize Lighthouse is to use one of the many public checkpoint synchronization endpoints, for example:

1. `https://checkpoint.gnosischain.com` for Gnosis Chain;
2. `https://checkpoint.chiadochain.net` for Chiado testnet.

### 4. Set the Erigon JWT secret path in Lighthouse

To communicate with Erigon, the execution endpoint must be specified as `<erigon address>:8551`, where `<erigon address>` is either `http://localhost` or the IP address of the device running Erigon.

1.  Lighthouse must point to the [JWT secret](https://erigon.gitbook.io/docs/summary/fundamentals/jwt) automatically created by Erigon in the `--datadir` directory. In the following example the default data directory is used.

    Copy

    ```
     lighthouse \
     --network gnosis beacon_node \
     --datadir=data \
     --http \
     --execution-endpoint http://localhost:8551 \
     --execution-jwt /home/user/.local/share/erigon/jwt.hex \
     --checkpoint-sync-url "https://checkpoint.gnosischain.com"
    ```

    Here is an example of Lighthouse running the Chiado testnet:

    Copy

    ```
     lighthouse \
     --network chiado \
     --datadir=data \
     --http \
     --execution-endpoint http://localhost:8551 \
     --execution-jwt /home/user/.local/share/erigon/jwt.hex \
     --checkpoint-sync-url "https://checkpoint.chiadochain.net"
    ```



Check the Erigon and Lightouse logs to make sure that the EL and CL are communicating and that your is syncing correctly.
