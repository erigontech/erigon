---
description: Internal Methods for Erigon Development and Debugging
---

# internal

The **`internal_`** methods are for development and debugging utilities and must be explicitly included in the `--http.api` flag if customizing enabled namespaces.

## **internal\_getTxNumInfo**

Returns transaction number information for development and debugging purposes. This is part of Erigon's internal APIs and may change without notice.

**Parameters**

| Parameter | Type     | Description                 |
| --------- | -------- | --------------------------- |
| txNum     | QUANTITY | Internal transaction number |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"internal_getTxNumInfo","params":["0x1"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type     | Description                    |
| -------- | ------------------------------ |
| Object   | Transaction number information |
| blockNum | QUANTITY                       |
| idx      | QUANTITY                       |
