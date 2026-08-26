---
title: "Using Caplin as Validator"
description: "Configure Erigon's embedded consensus layer to act as a full validator node without any external CL dependency."
sidebar_position: 1
---

# Using Caplin as Validator

Caplin, the Erigon embedded Consensus Layer, is also suitable for staking. However, it is required to pair it with a **validator key manager**, such as Lighthouse or Teku, since it doesn't have a native key management system.

This guide explains how to use Erigon with its embedded Caplin consensus layer and Lighthouse as the validator client for staking on Ethereum.

## 1. Start Erigon with Caplin

The following command starts Erigon with the embedded Caplin consensus layer with the beacon API on:

```bash
erigon \
  --datadir=/data/erigon \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=engine,eth,net,web3 \
  --ws \
  --ws.port=8546 \
  --caplin.enable-upnp \
  --caplin.discovery.addr=0.0.0.0 \
  --caplin.discovery.port=4000 \
  --caplin.discovery.tcpport=4001 \
  --beacon.api=beacon,validator,builder,config,debug,events,node,lighthouse 
```

**Flags Explanation**:

* Execution Layer (Erigon):
  * `--http.api=engine,eth,net,web3`: enables the necessary APIs for external clients and Caplin.
  * `--ws`: enables WebSocket-based communication (optional).
* Consensus Layer (Caplin):
  * `--caplin.discovery.addr` and `--caplin.discovery.port`: configures Caplin's gossip and discovery layer.
  * `--beacon.api=beacon,validator,builder,config,debug,events,node,lighthouse`: enables all possible API endpoints for the validator client.

## 2. Set Up Lighthouse Validator Client

### 2.1 Install Lighthouse

Install and run Lighthouse by following the official guide at [https://lighthouse-book.sigmaprime.io/installation.html](https://lighthouse-book.sigmaprime.io/installation.html) or use Docker:

```bash
docker pull sigp/lighthouse:latest
```

### 2.2. Create Lighthouse Validator Key Directory

```bash
mkdir -p ~/.lighthouse/validators
```

### 2.3. Run Lighthouse Validator Client

Start the validator client and connect it to the Caplin CL:

```bash
lighthouse vc \
  --network mainnet \
  --beacon-nodes http://127.0.0.1:5555 \
  --suggested-fee-recipient=<your_eth_address>
```

**Flags Explanation**:

* `--network mainnet`: Specifies the Ethereum mainnet.
* `--beacon-nodes`: Points to the Caplin beacon API at `http://127.0.0.1:5555`.
* `--suggested-fee-recipient`: Specifies your Ethereum address for block rewards.

### 2.4. Import Validator Keys

If you have existing validator keys, import them:

```bash
lighthouse account validator import --directory <path_to_validator_keys>
```

## 3. Block Production Behaviour

### 3.1. Payload Preparation Ahead of the Proposer Slot

Since v3.6, Caplin primes the execution layer one slot before a slot this node is due to propose, so the execution layer has already begun assembling a payload when the proposal is requested. There is no flag for this; it is on whenever all of the following hold:

* The Beacon API is running with the `validator` namespace enabled — for example `--beacon.api=beacon,validator,...` as in the command above. Without `validator`, preparation never starts.
* A validator client has registered a fee recipient for the proposer index, which Lighthouse does through the standard `prepare_beacon_proposer` call. A node with no registered validators never does the work.
* Caplin is driving the in-process execution layer. Passing `--caplin.use-engine-api` switches Caplin onto the Engine API and **disables the Beacon API entirely** — Erigon logs `Beacon API is automatically disabled` if you also passed `--beacon.api`. Staking through Caplin is not possible in that mode: there is no `validator` namespace, so block production and duties are unavailable, not just payload preparation.

Preparation looks at most one slot ahead, and it does not alter execution-layer fork choice — it only starts the builder early. To confirm it is running, look for these lines in the Erigon log:

```
PayloadPreparation: watching for proposals
PayloadPreparation: primed execution layer
```

Preparation is skipped for the Gloas (EIP-7732) fork, where builders gossip bids instead, and before Capella.

### 3.2. Fork-Choice Head Published Before the Head-State Copy

Since v3.6, Caplin publishes the head chosen by fork choice as soon as it is selected, rather than after the head beacon state has been copied. Beacon API endpoints that only need the head block identity — such as `/eth/v1/beacon/blocks/head` and `/eth/v2/debug/beacon/heads` — therefore reflect a new head sooner. Endpoints that read the head *state* are unchanged, and a node that is still syncing continues to return `503`. There is no flag for this.

### 3.3. Default Block Graffiti

Since v3.6, when the validator client does not supply a graffiti, Caplin fills it with the execution and consensus client pair that produced the block, following the [Engine API client-identification standard](https://github.com/ethereum/execution-apis/blob/main/src/engine/identification.md). The value is a two-letter execution client code and the first four hex characters of its commit, followed by Caplin's own code `CN` and the first four hex characters of the Erigon commit — for example `EGa53eCNa53e` when Caplin is paired with Erigon. If the execution client does not answer `engine_getClientVersionV1`, or has not answered it yet, the graffiti carries the consensus half only (`CN` plus commit).

There is no CLI flag for graffiti. The only way to override it is per block, through the Beacon API: a validator client that sends a `graffiti` query parameter on the validator block-production endpoints (`GET /eth/v2`, `/eth/v3` or `/eth/v4` `/validator/blocks/{slot}`) has its value used instead of the default. The parameter is read as a 32-byte hex value, as the Beacon API specification requires. It is not validated: a malformed or empty value is not rejected and does not fall back to the default — it silently produces an all-zero graffiti. A shorter hex value is left-padded with zeros, and a longer one is cropped from the left. Note that a validator client which sets graffiti by default — including its own client string — overrides Caplin's default; to get the client-pair graffiti, leave the validator client's graffiti unset.
