# Agent Task

This folder is being worked on by an automated agent.

## Project Context

Kurtosis devnet validation for EIP-8161.

EL erigon: /Users/monkeair/work/eip-maker/erigon  image=eip8161-el-erigon:latest
CL prysm: /Users/monkeair/work/eip-maker/prysm  image=eip8161-cl-prysm:latest

## Specification

# Kurtosis Devnet Validation for EIP-8161

Validate the EIP-8161 implementation by building Docker images,
launching a Kurtosis devnet, and spamming it with transactions.

## EIP Specification (for reference)

---
eip: 8161
title: SSZ-REST Engine API Transport
description: Defines the ssz_rest communication channel for the Engine API, replacing JSON-RPC with SSZ-encoded payloads over REST
author: Giulio Rebuffo (@Giulio2002), Ben Adams (@benaadams)
discussions-to: https://ethereum-magicians.org/t/eip-8161-ssz-rest-engine-api-transport/1
status: Draft
type: Standards Track
category: Core
created: 2026-03-01
requires: 8160
---

## Abstract

This EIP defines the `ssz_rest` communication channel advertised via `engine_getClientCommunicationChannelsV1` (EIP-8160). It specifies how every `engine_*` JSON-RPC method maps to an SSZ-encoded REST endpoint, using `application/octet-stream` for request and response bodies. This eliminates JSON serialization overhead and hex-encoding bloat, cutting payload sizes roughly in half and removing a major CPU bottleneck on the Engine API hot path.

## Motivation

EIP-8160 added the discovery mechanism — the EL can now tell the CL "I also speak ssz_rest at this URL." But it didn't define what `ssz_rest` actually means. This EIP fills that gap.

JSON-RPC is the bottleneck. Every block, the CL and EL exchange full execution payloads — all transactions, withdrawals, block headers, receipts. JSON hex-encodes every byte slice (`0x` prefix + 2 hex chars per byte), roughly doubling the wire size. Then both sides burn CPU encoding and decoding JSON. As blocks get bigger, this gets worse linearly.

SSZ (Simple Serialize) is already the consensus layer's native encoding. Execution payloads already have SSZ definitions in the consensus specs. By sending SSZ directly over HTTP REST, we:

1. **Cut wire size ~50%** — raw bytes instead of hex strings
2. **Eliminate JSON encode/decode CPU** — SSZ is trivially fast to serialize
3. **Align with the CL's native format** — the CL already thinks in SSZ, so zero conversion overhead on the CL side
4. **Provide a concrete migration path** — clients can gradually move methods to SSZ-REST while keeping JSON-RPC as fallback

## Specification

### URL Structure

All SSZ-REST endpoints live under the base URL advertised in the `engine_getClientCommunicationChannelsV1` response for `protocol: "ssz_rest"`.

Each `engine_*` method maps to a REST endpoint:

```
POST {base_url}/engine/{method_name}
```

Where `{method_name}` is the JSON-RPC method name without the `engine_` prefix and without the version suffix, but with the version as a path segment:

```
engine_newPayloadV4       → POST {base_url}/engine/v4/new_payload
engine_forkchoiceUpdatedV3 → POST {base_url}/engine/v3/forkchoice_updated
engine_getPayloadV4       → POST {base_url}/engine/v4/get_payload
engine_getClientVersionV1 → POST {base_url}/engine/v1/get_client_version
engine_exchangeCapabilitiesV1 → POST {base_url}/engine/v1/exchange_capabilities
engine_getClientCommunicationChannelsV1 → POST {base_url}/engine/v1/get_client_communication_channels
engine_getBlobsV1         → POST {base_url}/engine/v1/get_blobs
```

### Content Types

- Request: `Content-Type: application/octet-stream` (SSZ-encoded body)
- Response: `Content-Type: application/octet-stream` (SSZ-encoded body)
- Methods with no request parameters send an empty body.
- Methods with no SSZ-encodable response return an SSZ-encoded wrapper (see below).

### Authentication

The same JWT authentication as JSON-RPC MUST be used. The JWT token is passed in the `Authorization` header:

```
Authorization: Bearer <jwt_token>
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200  | Success — response body is SSZ-encoded |
| 400  | Bad request — malformed SSZ or invalid parameters |
| 401  | Unauthorized — invalid or missing JWT |
| 404  | Unknown endpoint |
| 500  | Internal server error |

### Error Responses

On non-200 responses, the body is a UTF-8 JSON error object (not SSZ) for debuggability:

```json
{"code": -32602, "message": "Invalid payload id"}
```

### SSZ Types for Engine API Methods

#### `new_payload` (v4)

**Request:** SSZ-encoded container

## Step 1: Docker Build

Build every client image. If there is no Dockerfile, look in
`Dockerfile`, `docker/Dockerfile`, or create a minimal one that
builds the Go / Rust binary.

```bash
# EL — erigon → eip8161-el-erigon:latest
cd /Users/monkeair/work/eip-maker/erigon && docker build -t eip8161-el-erigon:latest .

# CL — prysm → eip8161-cl-prysm:latest
cd /Users/monkeair/work/eip-maker/prysm && docker build -t eip8161-cl-prysm:latest .
```

ALL images MUST build successfully before proceeding.

## Step 2: Kurtosis Network

Create `network_params.yaml` and launch:

```bash
kurtosis run github.com/ethpandaops/ethereum-package --args-file network_params.yaml
```

Suggested network_params.yaml:

```yaml
participants:
  - el_type: erigon
    el_image: eip8161-el-erigon:latest
    cl_type: prysm
    cl_image: eip8161-cl-prysm:latest
    count: 1
network_params:
  network_id: "3151908"
  seconds_per_slot: 3
additional_services: []
```

Adapt `el_type` / `cl_type` to the actual client names supported by
the ethereum-package (erigon, geth, reth, nethermind, besu, prysm,
lighthouse, lodestar, teku, nimbus, etc.).

Enable the fork containing EIP-8161 at genesis or a low epoch
by adding the right `network_params` key (e.g. `electra_fork_epoch: 0`).

## Step 3: Wait for Finalization

1. Get EL RPC: `kurtosis port print <enclave> <service> rpc`
2. Poll `eth_getBlockByNumber("finalized", false)` until at least
   2 finalized epochs (finalized block > 0 and increasing)
3. Verify chain is progressing (block numbers increase)

## Step 4: Transaction Spam

1. Use `cast` (foundry) or raw `curl` JSON-RPC to send txs
2. Send at least 100 simple ETH transfers
3. If EIP-8161 introduces a new TX type, send those too
4. Verify transactions included in blocks
5. Check client logs: `kurtosis service logs <enclave> <service>`

## Step 5: Cleanup

```bash
kurtosis enclave rm -f <enclave_name>
```

## Hard Rules

- ALL Docker images MUST build before starting Kurtosis
- Network MUST reach finality (≥2 finalized epochs)
- ≥100 transactions sent and confirmed
- No panics / fatal errors / crashes in any client log
- Clean up the enclave when done


## Success Criteria (Objective)

## Success Criteria for Kurtosis Validation of EIP-8161

1. Every Docker image builds successfully
2. Kurtosis devnet launches with all custom images
3. Network reaches finality (at least 2 finalized epochs)
4. At least 100 transactions sent and confirmed in blocks
5. No panics, fatal errors, or crashes in client logs
6. Enclave is cleaned up after validation


## Important Notes

- A **strict verifier agent** will independently check your work when you are done.
- The verifier has no access to your session — it only reads the actual files.
- Claims you make that are not backed by real file changes will be caught.
- Do not leave TODOs, stubs, or placeholder code. Every criterion must be fully met.
- Run tests / build commands to confirm your work is correct before finishing.
