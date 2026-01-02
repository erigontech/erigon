---
description: Running Erigon with Caplin as validator
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
