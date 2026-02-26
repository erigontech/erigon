---
description: Run an Ethereum node with Erigon and an external Consensus Layer (CL).
metaLinks:
  alternates:
    - >-
      https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl
---

# Ethereum with an external CL

You can use **Prysm**, **Lighthouse**, or any other Consensus Layer client with Erigon by including the `--externalcl` flag. This integration enables direct access to the Ethereum blockchain, allowing you to manage your keys for staking ETH and block production.

## Erigon with Prysm as the external CL

{% tabs %}
{% tab title="Prysm" %}
1.  Start Erigon adding the `--externalcl` flag:

    ```bash
    erigon --externalcl
    ```

    If your Consensus Layer (CL) client is on a different device, add the following flags:

    * `--authrpc.addr 0.0.0.0`, since the Engine API listens on localhost by default;
    * `--authrpc.vhosts <CL_host>` where \<CL\_host> is the source host or the appropriate hostname that your CL client is using.
2.  Install and run **Prysm** by following the official guide: [https://docs.prylabs.network/docs/install/install-with-script](https://docs.prylabs.network/docs/install/install-with-script).

    Prysm must fully synchronize before Erigon can start syncing, since Erigon requires an existing target head to sync to. The quickest way to get Prysm synced is to use a public checkpoint synchronization endpoint from the list at [https://eth-clients.github.io/checkpoint-sync-endpoints](https://eth-clients.github.io/checkpoint-sync-endpoints).
3. To communicate with Erigon, the `--execution-endpoint` must be specified as `<erigon address>:8551`, where `<erigon address>` is either `http://localhost` or the IP address of the device running Erigon.
4.  Prysm must point to the [JWT secret](../../../fundamentals/jwt.md) automatically created by Erigon in the `--datadir` directory.

    ```bash
    ./prysm.sh beacon-chain \
    --execution-endpoint http://localhost:8551 \
    --mainnet --jwt-secret=<your-datadir>/jwt.hex \
    --checkpoint-sync-url=https://beaconstate.info \
    --genesis-beacon-api-url=https://beaconstate.info
    ```
{% endtab %}

{% tab title="Lighthouse" %}
1.  Start Erigon adding the `--externalcl` flag:

    ```bash
    ./build/bin/erigon --externalcl
    ```
2. Install and run Lighthouse by following the official guide: [https://lighthouse-book.sigmaprime.io/installation.html](https://lighthouse-book.sigmaprime.io/installation.html)
3. Because Erigon needs a target head in order to sync, Lighthouse must be synced before Erigon can synchronize. The fastest way to synchronize Lighthouse is to use one of the many public checkpoint synchronization endpoints at [https://eth-clients.github.io/checkpoint-sync-endpoints](https://eth-clients.github.io/checkpoint-sync-endpoints).
4. To communicate with Erigon, the `--execution-endpoint` must be specified as `<erigon address>:8551`, where `<erigon address>` is either `http://localhost` or the IP address of the device running Erigon.
5.  Lighthouse must point to the [JWT secret](../../../fundamentals/jwt.md) automatically created by Erigon in the `--datadir` directory.

    ```bash
    lighthouse bn \
    --network mainnet \
    --execution-endpoint http://localhost:8551 \
    --execution-jwt <your-datadir>/jwt.hex \
    --checkpoint-sync-url https://mainnet.checkpoint.sigp.io \
    ```
{% endtab %}
{% endtabs %}

Check Erigon and your chosen CL logs to make sure that the EL and CL are communicating and that your node is syncing correctly.
