---
description: 'Erigon RPC overlay Namespace: Stateless Contract Bytecode Injection for Analytics'
---

# overlay

The `overlay` namespace is an **Erigon-specific** extension that allows you to replay historical blocks with a modified contract bytecode, without deploying anything on-chain. It is designed for analytics use cases where you need to inject custom event emissions into an existing contract and retrieve the logs those events would have produced.

{% hint style="warning" %}
The `overlay` namespace requires **historical state to be available**. All methods replay historical transactions and state — a node with sufficient history retention (e.g. `--prune.mode=archive`) is needed; the methods will fail if the requested block's state has been pruned.
{% endhint %}

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

---

### `overlay_callConstructor`

Replays the **constructor transaction** of an existing contract using a different bytecode and returns the effective creation code that would have been stored.

This is useful for understanding what a contract would have initialised to if it had been deployed with different logic, without redeploying anything.

**Parameters**

| # | Name | Type | Description |
|---|------|------|-------------|
| 1 | `address` | `Address` | The address of the already-deployed contract |
| 2 | `code` | `Bytes` | The replacement bytecode to use instead of the original deployment bytecode |

**Returns**

```json
{ "code": "0x<effective_creation_code>" }
```

**Example**

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "overlay_callConstructor",
    "params": [
      "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      "0x<your_replacement_bytecode>"
    ],
    "id": 1
  }'
```

Internally, this method:
1. Looks up the contract's creation transaction via the Otterscan API
2. Replays all transactions in that block up to (but not including) the creation tx
3. Executes the creation tx using the provided bytecode instead of the original
4. Returns the effective code that would have been stored at the contract address

{% hint style="info" %}
`overlay_callConstructor` requires the Otterscan API (`--http.api=...,ots,overlay`) to resolve the contract creation transaction.
{% endhint %}
