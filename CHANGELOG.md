# Changelog

## Unreleased

### Breaking Changes

#### `debug_trace*` RPC: `enableMemory` / `enableReturnData` replace `disableMemory` / `disableReturnData`

Aligns Erigon with the execution-apis specification ([ethereum/execution-apis#762](https://github.com/ethereum/execution-apis/pull/762)) and Geth behavior.

**What changed:**

| Field | Before (Erigon) | After (Erigon / Geth / Spec) |
|-------|-----------------|------------------------------|
| Memory in trace | `disableMemory` (default: included) | `enableMemory` (default: excluded) |
| Return data in trace | `disableReturnData` (default: included) | `enableReturnData` (default: excluded) |

The change is **twofold**:
1. The JSON key is renamed (`disable*` → `enable*`).
2. The default value is inverted: previously memory and return data were **included** by default (opt-out model); now they are **excluded** by default (opt-in model), matching the spec and Geth.

**Migration:**

```jsonc
// Before — disable memory explicitly
{ "disableMemory": true }

// After — enable memory explicitly
{ "enableMemory": true }

// Before — memory included by default (no flag needed)
{}

// After — must opt in
{ "enableMemory": true }
```

Affected RPC methods: `debug_traceTransaction`, `debug_traceBlockByHash`, `debug_traceBlockByNumber`, `debug_traceCall`.
