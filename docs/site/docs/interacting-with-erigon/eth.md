---
title: "eth"
description: "Standard Ethereum JSON-RPC methods: blocks, transactions, state, logs, and filters."
sidebar_position: 1
---


# eth

The `eth` namespace is the foundational and most commonly used API set in Ethereum's JSON-RPC interface. It provides core functionality for interacting with the Ethereum blockchain, enabling users and applications to read blockchain state and submit transactions.

Key methods within this namespace allow you to check an account's balance (`eth_getBalance`), get the current block number (`eth_blockNumber`), retrieve transaction details (`eth_getTransactionByHash`), and send signed transactions to the network (`eth_sendRawTransaction`, `eth_sendRawTransactionSync`). Essentially, the `eth` namespace contains all the fundamental tools needed to observe and participate in the life of the chain.

### API usage

For API usage refer to the below official resources:

* [https://ethereum.org/en/developers/docs/apis/json-rpc/](https://ethereum.org/en/developers/docs/apis/json-rpc/)
* [https://ethereum.github.io/execution-apis/](https://ethereum.github.io/execution-apis/)

The sections below cover only the places where Erigon adds a method that is not in that
standard set, or where its behaviour differs from it. They are not an exhaustive
compliance statement: anything not listed here is expected to follow the spec, but a
deviation that has not yet been documented may still exist.

### eth\_getProof

`eth_getProof` returns Merkle proofs for account state and storage slots, as defined in [EIP-1186](https://eips.ethereum.org/EIPS/eip-1186). It is stable and production-ready as of Erigon v3.4.

To enable historical proof support, activate commitment history storage at startup:

```text
--prune.include-commitment-history=true
```

:::warning
**RAM requirement:** Historical `eth_getProof` requires at least **+32 GB RAM** to operate efficiently. Running without sufficient memory will severely degrade node performance.
:::

This enables faster retrieval of Merkle proofs for any executed block.

### eth\_getStorageValues

`eth_getStorageValues` reads several storage slots for one or more accounts in a single call, reducing round-trips compared to multiple `eth_getStorageAt` calls. It originated as an Erigon extension and has since been adopted into the [Ethereum execution APIs](https://github.com/ethereum/execution-apis/blob/main/src/eth/state.yaml) (April 2026). It takes two parameters.

**Parameters**

| Parameter   | Type             | Description                                                                                       |
| ----------- | ---------------- | ------------------------------------------------------------------------------------------------- |
| requests    | OBJECT           | Maps each account address to an array of 32-byte storage slot keys (hex-encoded)                   |
| blockNumber | STRING or NUMBER | Optional. Block number, block hash or tag (`"latest"`, `"earliest"`, etc.). Defaults to `"latest"` |

**Example**

```bash
curl --data '{"jsonrpc":"2.0","method":"eth_getStorageValues","params":[{"0xdAC17F958D2ee523a2206206994597C13D831ec7":["0x0000000000000000000000000000000000000000000000000000000000000000","0x0000000000000000000000000000000000000000000000000000000000000002"],"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48":["0x0000000000000000000000000000000000000000000000000000000000000002"]},"latest"],"id":1}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

An object mapping each requested address to an array of 32-byte values, in the same order as the slot keys requested for that address. Slots that were never written return zero.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": [
      "0x000000000000000000000000c6cde7c39eb2f0f0095f41570af89efc2c1ea828",
      "0x0000000000000000000000000000000000000000000000000000000000000000"
    ],
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": [
      "0x0000000000000000000000000a06be16275b95a7d2567fbdae118b36c7da78f9"
    ]
  }
}
```

**Limits**

* A single call may request at most 1024 slots in total, counting all addresses together. Larger requests are rejected.
* An empty `requests` object returns error code `-32602` with the message `empty request`.
* When `blockNumber` is a block hash, the block must be canonical.

### eth\_getWitness and eth\_getTxWitness

Neither method exists in the standard set. They return a state witness for the
*pre-state* of a block: the serialized trie node stream needed to re-execute it
without a full state database.

**Parameters**

| Method | Parameter | Type | Description |
| ------ | --------- | ---- | ----------- |
| `eth_getWitness` | blockNumberOrHash | STRING, NUMBER or OBJECT | Block to build the witness for |
| `eth_getTxWitness` | blockNumberOrHash | STRING, NUMBER or OBJECT | Block containing the transaction |
| `eth_getTxWitness` | txIndex | QUANTITY | Index of a transaction within that block |

:::info
`eth_getTxWitness` currently only bounds-checks `txIndex` — an index past the end of the
block is an error, but the witness it returns is the same whole-block witness
`eth_getWitness` produces. Two cases are not rejected: the check narrows the index with
`int(txIndex)`, so on a 64-bit build a value from `0x8000000000000000` upward wraps
negative and passes; and at the genesis block the check never runs at all, because both
methods share one implementation that returns the empty witness as soon as the block
number resolves to `0`.
:::

**Returns**

`DATA` — the serialized witness. On the normal path Erigon decodes the bytes it is about
to return and checks that they rebuild the parent state root, so the witness is
self-verified. Two early returns skip that check because they produce no witness to
verify: the genesis block, and any block whose access set turns out to be empty. Both
return the empty witness.

**Requirements**

Both methods need commitment history for any block above genesis, so start the node with:

```text
--prune.include-commitment-history
```

The genesis block is the one exception: the empty witness is returned before the
commitment-history check, so `eth_getWitness` and `eth_getTxWitness` succeed at block `0`
on a node without commitment history. For every other block, without it the call fails
with an error starting with `eth_getWitness requires commitment history`; the message
continues with a restart hint that names the
`--prune.experimental.include-commitment-history` alias rather than the canonical flag
above. If the requested block is older than the retained commitment history, the call
fails with an error starting with `commitment history pruned`, followed by the retained
range.

### eth\_fillTransaction

`eth_fillTransaction` takes a partially specified transaction, fills in what is
missing, and returns it unsigned — it neither signs nor submits anything. The method
was added to the execution-apis specification in July 2026; Erigon (matching geth)
deviates from it in the return shape, which keeps the `raw` field the spec dropped.

**Parameters**

1. `Object` — a transaction object in the same shape `eth_call` accepts.

**Fills in**

* `nonce` — from the pending nonce of `from`, or `0` when `from` is absent
* `value` — `0` when absent
* `gas` — from `eth_estimateGas` against `latest`
* `chainId` — the node's chain ID; a mismatching supplied value is an error
* fee fields — `gasPrice` before London, otherwise `maxFeePerGas` / `maxPriorityFeePerGas` from the gas oracle
* `maxFeePerBlobGas` — for blob transactions, twice the current blob gas price

**Returns**

An object with `raw`, the RLP-encoded unsigned transaction, and `tx`, the same
transaction in JSON form. The standardized result carries only `tx`; the extra `raw`
field is the geth-compatible shape.

:::info
KZG commitment and proof generation from raw blobs is not implemented. Blob
transactions must already carry their `blobVersionedHashes`.
:::

### Block number parameter format

Erigon accepts more block-number forms than the standard schema, which allows only a
`0x`-prefixed hex quantity string or a named tag.

Accepted everywhere a block is selected:

* a `0x`-prefixed hex string without leading zeros — `"0x3"`, `"0x2ed119"`
* a bare, unquoted JSON integer — `3`
* the named tags `"earliest"`, `"latest"`, `"safe"`, `"finalized"`, `"pending"`
* `null`, whose handling depends on the parameter's type, not on the method:
  * a plain **block-number** parameter accepts it — `BlockNumber.UnmarshalJSON` maps
    top-level `null` to `latest`, so `eth_getBlockByNumber`, `trace_block` and the rest
    of that family take it even though the parameter is required;
  * an **optional** block-or-hash parameter accepts it too: the selector is left unset
    and the method's own default applies, `latest` for most;
  * a **required** block-or-hash parameter rejects it —
    `BlockNumberOrHash.UnmarshalJSON` decodes `null` into an empty struct and then fails
    with `at least one of BlockNumber or BlockHash is needed if a dictionary is
    provided` (`rpc/types.go`). This holds for every such method, including
    `eth_getBlockReceipts`, `eth_getBlockAccessList`, `eth_simulateV1`, `eth_getWitness`,
    `eth_getTxWitness`, `eth_getBlockByHash` and `trace_replayBlockTransactions`

Rejected everywhere:

* a quoted decimal string — `"3"`, `"100"`
* hex with leading zeros — `"0x01"`
* hex without the prefix — `"ff"`

:::warning
**Breaking change in v3.6.** Quoted decimal strings such as `"3"` used to be accepted
and are now rejected with `hex string without 0x prefix`, returned as an
`invalid params` error. Callers relying on that form must switch to `"0x3"`, the bare
integer `3`, or a named tag.
:::

Two further forms exist: the Erigon-specific tag `"latestExecuted"`, and the string
`"null"`, which means `latest`. Both are parsed by `BlockNumber.UnmarshalJSON`, so they are
accepted unconditionally by methods whose parameter is a plain block number — such as
`eth_getBlockByNumber`, `eth_getBlockTransactionCountByNumber` and `trace_block`.

Methods taking a block-number-or-hash selector — `eth_call`, `eth_getBalance`,
`eth_getProof` and the rest — reject both **as top-level strings** only.
`BlockNumberOrHash.UnmarshalJSON` tries the object form first, and that path decodes the
`blockNumber` field through `BlockNumber`, which does accept them. So on those
block-number-or-hash methods the object-wrapped forms work:

```json
{"blockNumber": "latestExecuted"}
{"blockNumber": "null"}
```

while the bare strings `"latestExecuted"` and `"null"` fail on those same methods, because
the top-level string switch does not list them.

### Filter lifetime

The standard spec does not say how long a filter lives. In Erigon, a filter created by
`eth_newFilter`, `eth_newBlockFilter`, or `eth_newPendingTransactionFilter` is evicted
once it has gone **5 minutes** without being polled. `eth_getFilterChanges` and
`eth_getFilterLogs` both count as a poll and reset the clock, so a filter stays alive on
a quiet chain as long as the client keeps asking. A call against an evicted filter
returns `filter not found`.

Change the window, or turn eviction off entirely, with:

```text
--rpc.subscription.filters.timeout 15m   # longer window
--rpc.subscription.filters.timeout 0     # keep filters indefinitely
```

Eviction applies only to these polling filters. WebSocket subscriptions live as long as
their connection does.
