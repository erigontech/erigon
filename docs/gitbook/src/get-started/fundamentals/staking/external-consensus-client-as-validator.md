# Using an external consensus client as validator

To use an external Consensus Layer (CL) it is necessary to add to Erigon the flag `--externalcl`. Here are a couple of examples on how to configure Lighhouse and Prysm to run along with Erigon:

* [Ethereum](../../easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl.md)
* [Gnosis Chain](../../easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl.md)

Once you have Erigon and a CL client up and running, you can proceed to set up a Validator Client (VC). The VC is responsible for managing your keys and signing valid blocks.

## Getting Started with a Validator Client

To set up a VC, follow the instructions provided in the official documentation, such as:

[https://lighthouse-book.sigmaprime.io/mainnet-validator.html](https://lighthouse-book.sigmaprime.io/mainnet-validator.html).

This guide will walk you through the process of setting up a VC, including:

* Generating and managing your keys
* Configuring the Validator Client
* Signing valid blocks

Make sure to follow the instructions carefully and thoroughly to ensure that your VC is set up correctly. It is always recommended to start staking with a testnet.

## Example of configuration

The following example demonstrates how to configure Erigon to run with an external Consensus Layer:

```bash
erigon \
  --externalcl \
  --datadir=/data/erigon \
  --chain=sepolia \
  --authrpc.jwtsecret=/jwt
  --authrpc.addr=0.0.0.0 \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=engine,eth,net,web3 \
  --ws \
  --ws.port=8546 \
```

## Flags explanation:

* `--externalcl`: Enables the use of an external CL.
* `--datadir=/data/erigon`: Defines the directory to be used for databases.
* `--chain=sepolia`: Specifies the Sepolia chain.
* `--authrpc.jwtsecret=/jwt`: Sets the location of the JWT authentication code in hex encoding. This is used by Erigon (the EL) and the CL (in this case Lighthouse) to authenticate communication.
* `--authrpc.addr=0.0.0.0`: Allows the engine API to be accessed from any address.
* `--http.api=engine,eth,net,web3`: Enables the necessary APIs for external clients and Caplin.
* `--ws`: enables WebSocket-based communication, which is optional.

{% hint style="info" %}
Note that many pre-merge flags, such as `--miner.etherbase`, are no longer useful, as block rewards and other validator-related configurations are now controlled by the Consensus Layer (CL).
{% endhint %}
