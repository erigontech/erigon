---
description: 'Erigon RPC parity Namespace: OpenEthereum Compatibility'
---

# parity

The `parity` namespace provides a limited subset of the OpenEthereum (formerly Parity) JSON-RPC API for compatibility with tooling that targets the original Parity client.

{% hint style="warning" %}
Only **one** method from the original OpenEthereum `parity_*` specification is implemented in Erigon. All other `parity_*` methods (account management, vault, signing, transaction queue, etc.) are not supported. Use the equivalent `eth_*` methods instead.
{% endhint %}

Enable the namespace with `--http.api=...,parity`.

For the full original OpenEthereum `parity_*` specification, see the [OpenEthereum JSON-RPC parity module documentation](https://openethereum.github.io/JSONRPC-parity-module) (archived).

---

## Methods

### `parity_listStorageKeys`

Returns all storage keys for a given contract address, paginated.

**Parameters**

| # | Name | Type | Description |
|---|------|------|-------------|
| 1 | `address` | `Address` | The contract address to query storage for |
| 2 | `quantity` | `integer` | Number of storage keys to return |
| 3 | `offset` | `Bytes` (optional) | Pagination offset — the storage key to start from |
| 4 | `blockNumber` | `BlockNumberOrHash` | Block to query. Must be `"latest"` (other tags and block numbers are not supported) |

**Returns**

Array of storage key hashes (`Bytes[]`).

**Example**

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "parity_listStorageKeys",
    "params": [
      "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      10,
      null,
      "latest"
    ],
    "id": 1
  }'
```
