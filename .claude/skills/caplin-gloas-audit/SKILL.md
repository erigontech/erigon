---
name: caplin-gloas-audit
description: Scan Caplin codebase for GLOAS (EIP-7732) compatibility issues. Finds unsafe direct accesses to ExecutionPayload, LatestExecutionPayloadHeader, BlobKzgCommitments, and SignedExecutionPayloadEnvelope that may panic or return stale data in GLOAS. Use before PRs touching cl/ code.
allowed-tools: Grep, Read, Bash
---

# GLOAS Compatibility Audit

Scan the `cl/` directory for code that directly accesses fields or methods that are nil, stale, or moved in GLOAS (EIP-7732 / ePBS).

## Background

In GLOAS:
- `BeaconBody.ExecutionPayload` is **nil** (payload moved to `SignedExecutionPayloadEnvelope`)
- `BeaconBody.BlobKzgCommitments` is **nil** (commitments moved to `SignedExecutionPayloadBid.Message`)
- `state.LatestExecutionPayloadHeader()` is **stale/zero** (use `GetLatestBlockHash()` instead)
- `*SignedExecutionPayloadEnvelope` is **nil for EMPTY blocks** (builder didn't deliver payload)

Safe access patterns:
- `blk.Version() >= clparams.GloasVersion` guard before ExecutionPayload access
- `GetBlobKzgCommitments()` instead of direct `.BlobKzgCommitments`
- `GetLatestBlockHash()` instead of `LatestExecutionPayloadHeader().BlockHash` for GLOAS states
- `if envelope != nil` guard before any `envelope.Message.*` access

## Audit Steps

### Step 1: Find direct ExecutionPayload field accesses

Search for `.ExecutionPayload.` accesses in `cl/` (excluding test files and comments):

```
pattern: \.ExecutionPayload\.
path: cl/
glob: *.go
```

For each hit, read ~10 lines of surrounding context and determine:
- Is it inside `if ... < clparams.GloasVersion` or `if ... < GloasVersion`?
- Is it inside an `else if version >= BellatrixVersion` branch (which implicitly excludes GLOAS)?
- Is it guarded by a pre-GLOAS version check elsewhere in the function?

If **not guarded** → flag as **REAL BUG** with file:line and suggested fix.
If guarded → flag as **SAFE** (briefly note why).

### Step 2: Find LatestExecutionPayloadHeader() usages

Search for:
```
pattern: LatestExecutionPayloadHeader\(\)
path: cl/
glob: *.go
```

For each hit, read context and determine:
- Is it in a function that only runs pre-GLOAS (e.g., inside `< GloasVersion` guard)?
- Does it use `.BlockHash` without a GLOAS check? → **STALE DATA BUG**
- Does it use `.Time`, `.BlockNumber`, `.StateRoot` without check? → **STALE DATA BUG**

Correct GLOAS replacement for block hash: `state.GetLatestBlockHash()`

### Step 3: Find direct BlobKzgCommitments field accesses

Search for:
```
pattern: \.BlobKzgCommitments[^)]
path: cl/
glob: *.go
```

Exclude `GetBlobKzgCommitments()` calls (those are already safe).

For each hit, check if it's guarded by `stateVersion.Before(clparams.GloasVersion)` or equivalent.

If **not guarded** → flag as **REAL BUG**. Suggest replacing with `GetBlobKzgCommitments()`.

### Step 4: Find unsafe envelope accesses

GLOAS introduces `*cltypes.SignedExecutionPayloadEnvelope` as a separate parameter in callbacks and sync functions. An envelope is **nil for EMPTY blocks** (builder didn't deliver payload). Any access without a nil guard is a bug.

Search for:
```
pattern: envelope\.Message
path: cl/
glob: *.go
```

For each hit, check that it is inside an `if envelope != nil` (or equivalent) guard.

Safe pattern:
```go
if envelope != nil {
    _ = envelope.Message.Payload.BlockNumber  // safe
}
```

Unsafe pattern (REAL BUG):
```go
_ = envelope.Message.Payload.BlockNumber  // panics for EMPTY blocks
```

### Step 5: Report

Output a structured report:

```
## GLOAS Audit Report

### REAL BUGS (need fixing)
- file:line — description — suggested fix

### DEFERRED (known, intentional)
- file:line — description

### SAFE (verified)
- count of verified-safe hits

### SUMMARY
X real bugs, Y deferred, Z safe
```

If no real bugs found, confirm the codebase is GLOAS-safe for the scanned patterns.

## Notes

- Focus on `cl/` only (Caplin). EL code (`execution/`, `eth/`) is unaffected.
- Test files (`_test.go`) can be skipped unless they test GLOAS paths.
- The `cl/transition/machine/block.go` dispatch is the source of truth: GLOAS paths call `ProcessExecutionPayloadBid`, Bellatrix paths call `ProcessExecutionPayload` — use this to verify guards.
