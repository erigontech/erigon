---
description: Connecting Erigon (EL) to Consensus Clients (CL)
---

# engine

The Engine API is a standardized JSON-RPC interface defined by the Ethereum specification. Its purpose is to enable secure and structured communication between the Consensus Layer (CL) client and the Execution Layer (EL) client in the post-Merge Proof-of-Stake architecture.

The Engine API replaced older `eth_` methods for consensus communication. It is only required when running Erigon with an external consensus client like Prysm, Lighthouse, or Teku, as these external clients communicate exclusively through this interface.

#### Default Erigon Behavior (Caplin)

By default, Erigon runs its own embedded consensus layer client, Caplin. For optimized performance, Caplin bypasses the Engine API and uses direct internal calls to communicate with Erigon's execution layer.

You can optionally force Caplin to use the Engine API interface by setting the `--caplin.use-engine-api` flag. When this flag is active, Caplin connects via the same [JWT](../jwt.md)-authenticated HTTP endpoint used by external CL clients.

#### Engine API Functionality

When in use (with external CL clients), the Engine API provides essential methods for Proof-of-Stake operations:

* Payload Execution: `engine_newPayloadV1/V2/V3/V4` validates and executes blocks sent by the CL.
* Fork Choice Updates: `engine_forkchoiceUpdatedV1/V2/V3` updates the chain head and triggers block building.
* Payload Retrieval: `engine_getPayloadV1/V2/V3/V4` retrieves built blocks for the CL to propose.
* Payload Bodies: `engine_getPayloadBodiesByHashV1` and `engine_getPayloadBodiesByRangeV1` fetch historical data.

#### Configuration and Security

For security, the Engine API listens on port 8551 by default and requires JWT authentication.

* A JWT secret is automatically generated and saved in the file `jwt.hex` within your data directory.
* This secret must be shared with your external consensus layer client to establish secure communication.
* If your CL client runs on a different machine, you must expose the API using `--authrpc.addr 0.0.0.0` and configure virtual hosts appropriately with `--authrpc.vhosts`.

{% include "../../../.gitbook/includes/api-documentation-3.md" %}
