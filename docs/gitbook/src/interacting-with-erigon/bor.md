---
description: Accessing Polygon Validator and Bor Consensus Data
---

# bor

The `bor` namespace provides Polygon-specific RPC methods that are only available when running Erigon on Polygon networks (Mainnet, Amoy testnet, etc.). These methods expose functionality specific to the Bor consensus engine, including validator information, snapshots, and proposer sequences.

The bor namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon and is only functional when running on Polygon networks with the Bor consensus engine.

### Network Compatibility

* The bor namespace is only available when running Erigon on Polygon networks (Mainnet, Amoy testnet, etc.)
* These methods will return errors if called on non-Polygon networks or when the Bor consensus engine is not active
* The methods require the underlying Bor consensus engine to be properly configured and running

### Consensus Integration

* All bor methods interact directly with the Bor consensus engine and validator set management
* The methods provide access to Polygon's unique consensus features like validator snapshots and proposer sequences
* These APIs are essential for applications that need to understand Polygon's validator dynamics and block production

### Usage in Polygon Ecosystem

* These methods are commonly used by Polygon validators, delegators, and applications that need validator information
* The snapshot and signer methods are particularly useful for understanding the current validator set and their roles
* Root hash methods are used for checkpoint verification and cross-chain communication

***

## **bor\_getSnapshot**

Returns the validator snapshot at a given block number, containing information about the current validator set and their voting power.

**Parameters**

| Parameter | Type          | Description                                     |
| --------- | ------------- | ----------------------------------------------- |
| number    | QUANTITY\|TAG | Block number or "latest", "earliest", "pending" |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getSnapshot","params":["latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                                         |
| ------ | ------------------------------------------------------------------- |
| Object | Snapshot object containing validator information and voting details |

***

## **bor\_getAuthor**

Returns the author (block proposer) of a block at the given block number or hash.

**Parameters**

| Parameter     | Type                | Description                      |
| ------------- | ------------------- | -------------------------------- |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or block hash |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getAuthor","params":["0x1b4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type           | Description                              |
| -------------- | ---------------------------------------- |
| DATA, 20 BYTES | The address of the block author/proposer |

***

## **bor\_getSnapshotAtHash**

Returns the validator snapshot at a specific block hash.

**Parameters**

| Parameter | Type           | Description       |
| --------- | -------------- | ----------------- |
| hash      | DATA, 32 BYTES | Hash of the block |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getSnapshotAtHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                                                  |
| ------ | ---------------------------------------------------------------------------- |
| Object | Snapshot object containing validator information at the specified block hash |

***

## **bor\_getSigners**

Returns the list of authorized signers (validators) at a given block number.

**Parameters**

| Parameter | Type          | Description                                     |
| --------- | ------------- | ----------------------------------------------- |
| number    | QUANTITY\|TAG | Block number or "latest", "earliest", "pending" |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getSigners","params":["latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                            |
| ----- | ------------------------------------------------------ |
| Array | Array of validator addresses authorized to sign blocks |

***

## **bor\_getSignersAtHash**

Returns the list of authorized signers (validators) at a specific block hash.

**Parameters**

| Parameter | Type           | Description       |
| --------- | -------------- | ----------------- |
| hash      | DATA, 32 BYTES | Hash of the block |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getSignersAtHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                                  |
| ----- | ---------------------------------------------------------------------------- |
| Array | Array of validator addresses authorized to sign blocks at the specified hash |

***

## **bor\_getCurrentProposer**

Returns the address of the current block proposer based on the current validator set and proposer selection algorithm.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getCurrentProposer","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type           | Description                         |
| -------------- | ----------------------------------- |
| DATA, 20 BYTES | The address of the current proposer |

***

## **bor\_getCurrentValidators**

Returns the current validator set with their details including voting power and other metadata.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getCurrentValidators","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                                 |
| ----- | --------------------------------------------------------------------------- |
| Array | Array of validator objects with their addresses, voting power, and metadata |

***

## **bor\_getSnapshotProposerSequence**

Returns the proposer sequence for a given block, showing the order in which validators are expected to propose blocks.

**Parameters**

| Parameter     | Type                | Description                      |
| ------------- | ------------------- | -------------------------------- |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or block hash |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getSnapshotProposerSequence","params":["latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                                      |
| ------ | ---------------------------------------------------------------- |
| Object | BlockSigners object containing the proposer sequence information |

***

## **bor\_getRootHash**

Returns the root hash for a range of blocks, used for checkpoint verification and state synchronization.

**Parameters**

| Parameter | Type     | Description           |
| --------- | -------- | --------------------- |
| start     | QUANTITY | Starting block number |
| end       | QUANTITY | Ending block number   |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getRootHash","params":["0x1", "0x100"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                 |
| ------ | ------------------------------------------- |
| STRING | The root hash for the specified block range |

***

## **bor\_getVoteOnHash**

Returns voting information for a specific block hash, used in the Bor consensus mechanism.

**Parameters**

| Parameter | Type           | Description                                     |
| --------- | -------------- | ----------------------------------------------- |
| hash      | DATA, 32 BYTES | Hash of the block to get voting information for |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"bor_getVoteOnHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                            |
| ------ | ------------------------------------------------------ |
| Object | Voting information object for the specified block hash |
