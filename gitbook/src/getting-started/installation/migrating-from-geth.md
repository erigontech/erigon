---
description: >-
  How to perform a smooth and quick transition from Geth or another Execution
  Layer (Nethermind, Reth, Besu) to Erigon.
---

# Migrating from Geth

### Migration Paths: Choosing Your Erigon Setup

When moving from another Execution Layer (EL) such as Geth, Nethermind, Reth or Besu to Erigon, node runners have a primary choice regarding their Consensus Layer (CL) setup:

* **Erigon with Caplin**: This is the highly efficient, all-in-one setup, utilizing Erigon's embedded CL client, [Caplin](../../fundamentals/caplin.md). It's an excellent choice for simplicity and performance for all node types and usage.
* **Erigon with an External Consensus Client**: This is the traditional setup, allowing to reuse an existing external CL client (like Prysm or Lighthouse).

The most secure and reliable method for all users involves running and syncing an **Erigon node alongside your existing Execution Layer (EL) client (like Geth)**. This parallel process allows for full functional verification, thorough testing, and ensures a smooth transition with **zero downtime**. Since Erigon requires significantly **less disk space** than many other clients, this parallel sync approach is practical and highly recommended for all migrations.

#### Key Recommendations

| **Node Runner Type**                                            | **Recommended Path (If Disk Space Allows)**                                                                                        | **Rationale**                                        |
| --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Validators                                                      | [Option 1](migrating-from-geth.md#option-1-sync-erigon-alongside-geth-and-its-cl): Sync Alongside Geth or any EL                   | Ensures minimal downtime and prevents slashing risk. |
| Public RPC Providers                                            | [Option 1](migrating-from-geth.md#option-1-sync-erigon-alongside-geth-and-its-cl): Sync Alongside Geth or any EL                   | Guarantees zero service interruption for users.      |
| Home Users / Standard Node Runners with no critical utilization | [Option 2](migrating-from-geth.md#option-2-remove-old-el-and-sync-erigon-downtime-accepted): Remove Geth or any EL and Sync Erigon | The fastest method if downtime is accepted.          |

#### Choosing Your Migration Strategy

* [Option 1](migrating-from-geth.md#option-1-sync-erigon-alongside-geth-and-its-cl): In case you run a highly critical process and have enough disk space, syncing Erigon alongside Geth or any other EL is the highly recommended choice for all users, and is essential for validators and RPC providers.
  * If disk space is limited and downtime is not an option, consider syncing Erigon on a separate machine.
* [Option 2](migrating-from-geth.md#option-2-remove-old-el-and-sync-erigon-downtime-accepted): If you are a home user or standard node runner (not running a validator), and the disk space is limited and downtime is acceptable, choose this option to remove your EL and the existing CL first.&#x20;

### Option 1: Sync Erigon Alongside Geth and its CL

{% tabs %}
{% tab title="Migrate to Erigon + Caplin (Internal CL)" %}
This path offers the simplest validator configuration by using Erigon's embedded consensus client, Caplin. You will no longer need your external Consensus Layer (CL) client or a JWT secret for CL-EL communication.

#### Steps for Minimal Downtime

1. **Preparation** (Installation): Install and configure Erigon.
2. **Configuration Check** (No Conflict): Ensure Erigon's standard [ports](../../fundamentals/default-ports.md) (JSON-RPC, P2P) are different from Geth's. These ports are configured via command-line options: `--http.port <port>` and `--p2p.listen-addr <IP:port>`.
3. **Synchronization**: Start syncing Erigon. Monitor the sync status using the `eth_syncing` JSON-[RPC method](../../interacting-with-erigon/interacting-with-erigon.md) or a health check.
4. **Validator Swap**: Once Erigon is fully synced, shut down Geth and the external CL client.
5. **Reconfiguration and Restart**:
   * Restart Erigon.
   * Crucially, reconfigure your validator keys (your withdrawal key setup) to point directly to the Erigon node's standard execution port (or the appropriate RPC port for staking actions), as Erigon/Caplin now handles both layers internally. See also [here](../../staking/caplin.md).
6. **Decommission Old Setup**: Verify Erigon/Caplin is proposing and attesting blocks correctly. If confirmed, safely remove Geth, the external CL client, and all their data, including the old JWT secret.
{% endtab %}

{% tab title="Migrate to Erigon + External CL" %}
This path allows you to retain your existing external Consensus Layer (CL) client (e.g., Prysm, Lighthouse) but swaps Geth for Erigon as the Execution Layer (EL). You must retain and reuse your JWT secret.

#### Steps for Minimal Downtime

1. **Preparation** (Installation): Install and configure Erigon.
2. **Configuration Check** (Ports): Ensure the network [ports](../../fundamentals/default-ports.md) for the Erigon instance are different from Geth's to avoid conflicts. This includes JSON-RPC (`--http.port`), Engine API (`--authrpc.port`), and P2P (`--p2p.listen-addr`).
3. **Configuration Check** (CL Client): Ensure your external CL client is temporarily configured to use Erigon's temporary Engine API port (e.g., 8552, if Geth was 8551) for the duration of the parallel sync.
4. **Synchronization**: Start syncing Erigon. Monitor the sync status using the `eth_syncing` JSON-RPC method or a health check.
5. **Validator Swap**: Once Erigon is fully synced, shut down both Geth and Erigon. Keep the external CL client running.
6. **Reconfiguration and Restart**:
   * Restart Erigon, ensuring it uses the same Engine API port (e.g., 8551) and JWT secret that Geth previously used.
   * Immediately shut down Geth.
   * Restart your external CL client to re-establish a connection with Erigon using the original, expected Engine API port.
7. **Decommission Old Setup**: Verify Erigon and your external CL client are proposing and attesting blocks correctly. If confirmed, safely remove Geth and its data.
{% endtab %}
{% endtabs %}

### Option 2: Remove Old EL and Sync Erigon (Downtime Accepted)

This is the simplest option as it requires no configuration adjustments. However, the node will be down until Erigon finishes syncing.

{% tabs %}
{% tab title="Erigon + Caplin (Fastest Sync)" %}
This path is the fastest way to get Erigon running. It utilizes Erigon's embedded consensus client, Caplin, requiring no external CL client or JWT secret. This is ideal for home users with limited disk space and no critical uptime requirements.

#### Steps for Quickest Start

1. **Decommission Old Setup**: Shut down and remove your old EL client (Geth, etc.) and its data. If you were using an external CL client, you can shut it down as well.
2. **Installation**: Install and configure Erigon.
3. **Configuration Ports (optional)**: Ensure Erigon uses the same standard network [ports](../../fundamentals/default-ports.md) (JSON-RPC, P2P) that your old EL client used.
4. **Synchronization**: Start syncing Erigon with the default Caplin configuration (Caplin does not use the Engine API). See [Basic Usage](../../fundamentals/basic-usage.md).
5. **Final Setup**: Once Erigon is fully synced, your node is online. If you are a validator, configure your validator keys to point directly to the Erigon node's standard execution port.
6. **Monitoring**: Monitor the sync progress using `eth_syncing` or the health check, and ensure no errors appear in Erigon's logs.
{% endtab %}

{% tab title="Erigon + External CL (Reuse Existing CL)" %}
This path retains your existing external Consensus Layer (CL) client (e.g., Prysm, Lighthouse) but swaps the old EL client for Erigon. Since downtime is accepted, this process requires simpler port management.

#### Steps for Quickest Start

1. **Decommission Old Setup**: Shut down and remove your old EL client (Geth, etc.) and its data. Keep your external CL client running.
2. **Installation**: Install and [configure](../../fundamentals/configuring-erigon.md) Erigon using `--externalcl` flag.
3. **Configuration** (JWT and Ports): Ensure Erigon uses the same Engine API [port](../../fundamentals/default-ports.md) (e.g., 8551) and [JWT secret](../../fundamentals/jwt.md) that the old EL client previously used. Otherwise, you must reconfigure your external CL client to match Erigon's new settings.
4. **Synchronization**: Start syncing Erigon. Your external CL client will automatically connect to Erigon once Erigon is running and reachable on the Engine API port.
5. **Monitoring and Verification**: Once Erigon is fully synced, check the logs of both Erigon and the external CL client to verify correct chain following and block production/attestation (if you are a validator).
{% endtab %}
{% endtabs %}
