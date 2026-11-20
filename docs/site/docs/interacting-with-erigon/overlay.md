---
title: "overlay"
description: "overlay_ namespace: historical state access optimized for archive queries."
sidebar_position: 14
---


# overlay

The `overlay` namespace is an **Erigon-specific** extension that allows you to replay historical blocks with a modified contract bytecode, without deploying anything on-chain. It is designed for analytics use cases where you need to inject custom event emissions into an existing contract and retrieve the logs those events would have produced.

:::warning
The `overlay` namespace requires **historical state to be available**. All methods replay historical transactions and state — a node with sufficient history retention (e.g. `--prune.mode=archive`) is needed; the methods will fail if the requested block's state has been pruned.
:::

Enable the namespace with `--http.api=...,overlay`.

## Timeouts

Two flags control how long overlay operations can run:

* `--rpc.overlay.getlogstimeout` — overall timeout for an `overlay_getLogs` call (default: `5m0s`)
* `--rpc.overlay.replayblocktimeout` — per-block replay timeout within a `getLogs` call (default: `10s`)

---

## Methods

### `overlay_getLogs`

Replays a range of historical blocks with an optional **state override** (modified bytecode, balance, nonce, or storage) and returns the logs that would have been emitted.

This is equivalent to running `eth_getLogs` on a hypothetical version of the contract — useful for retroactively adding event emissions to contracts that were deployed without them.

**Parameters**

| # | Name | Type | Description |
|---|------|------|-------------|
| 1 | `filter` | `FilterCriteria` | Same filter object as `eth_getLogs`: `fromBlock`, `toBlock`, `address`, `topics` |
| 2 | `stateOverride` | `StateOverride` (optional) | Map of `address → overrides`. Each override can set `code`, `balance`, `nonce`, `state` (full state replacement), or `stateDiff` (partial state patch). Same format as the state override in `eth_call`. |

**Returns**

Array of log objects (same structure as `eth_getLogs`).

**Example**

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "overlay_getLogs",
    "params": [
      {
        "fromBlock": "0x1000000",
        "toBlock":   "0x1000010",
        "address":   "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
      },
      {
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {
          "code": "0x<your_modified_bytecode>"
        }
      }
    ],
    "id": 1
  }'
```

Blocks in the requested range are replayed concurrently (up to `runtime.NumCPU()` workers). Each block is independently replayed from the state at `blockNumber - 1`.

Without state overrides, replay preserves the original gas budgets and charges fees normally. With state overrides, blocks containing only ordinary transactions use the RPC gas allowance and skip gas-fee debits. This allows replacement code to use more gas and replay to continue when an override changes how a later transaction is funded.

In this mode, replay can credit the transaction value to its recipient without debiting an underfunded sender. Gas refunds are also skipped, while tips are still credited to the block producer. These balance changes can affect later transactions and their logs.

Blocks containing a frame transaction preserve the original gas budgets and normal fee accounting for **every transaction in the block**, even with overrides. Frame assertions can observe the fee effects of earlier transactions. An override that leaves a payer underfunded returns an error; code that exceeds its original budget runs out of gas. The RPC gas cap does not apply to these blocks or to replay without overrides.

---

### `overlay_callConstructor`

Replays the transaction that deployed a contract using replacement initcode at the original address, including for `CREATE2`. It returns the runtime bytecode left at that address when the transaction finishes.

**Parameters**

| # | Name | Type | Description |
|---|------|------|-------------|
| 1 | `address` | `Address` | The address of the already-deployed contract |
| 2 | `code` | `Bytes` | The replacement initcode to run at deployment |

**Returns**

```json
{ "code": "0x<runtime_bytecode>" }
```

A successful deployment returns `null` if its runtime code is empty, the factory reverts it, or `SELFDESTRUCT` deletes it in the same transaction. A failed `POST_TX` assertion also returns `null` for deployments it rolls back in the transaction body. Deployments from the payment-approval frame survive that rollback.

If an accepted transaction never reaches the target creation during replay, the RPC returns `null`. For example, the factory may run out of gas before it executes `CREATE` or `CREATE2`.

If the factory retries a rolled-back creation, the RPC returns the retry's result. A constructor failure is reported as an RPC error when no code remains.

**Gas**

Ordinary transactions use the RPC gas allowance for the top-level replay. Frame transactions keep their signed gas budgets. A factory deployment uses the gas forwarded by `CREATE` or `CREATE2`. Under EIP-150, the factory keeps 1/64 of its remaining execution gas after creation costs, rounded down. A factory called with an explicit gas limit can run out of gas storing a larger runtime even when the RPC gas cap is higher.

From Shanghai onward, both the factory's input to `CREATE` or `CREATE2` and the replacement must fit the fork's initcode size limit. An oversized factory input fails before the VM applies the replacement. The per-word analysis charge uses the replacement size. Memory expansion uses the original memory range, and `CREATE2` uses the original input for its address and hashing charge.

**Example**

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "overlay_callConstructor",
    "params": [
      "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      "0x<replacement_initcode>"
    ],
    "id": 1
  }'
```

Erigon uses its internal Otterscan service to find the deployment transaction. It replays the preceding transactions in the block to restore the state at deployment.
