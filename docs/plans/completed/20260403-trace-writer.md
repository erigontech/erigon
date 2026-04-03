# HPH Trace Writer: Replace trace bool with io.Writer

## Overview
- Replace `hph.trace bool` with `hph.traceW io.Writer` field
- nil = disabled (zero-cost check), non-nil = write trace output to that writer
- Terse tagged plaintext format: `[fold]`, `[unfold]`, `[hash]`, `[cell]`, `[mount]`, `[witness]`, `[proc]`
- `Trie` interface: `SetTrace(bool)` → `SetTraceWriter(io.Writer)`
- CommitmentCapture callers set `traceW` to a `*bytes.Buffer` and read it directly
- `traceDomain bool` kept as-is — separate concern

## Context (from brainstorm)

**Files involved:**
- `execution/commitment/hex_patricia_hashed.go` — 69 `if hph.trace {` sites + 9 `[witness]` fmt.Printf sites
- `execution/commitment/commitment.go` — Trie interface (line 90): `SetTrace`,
- `execution/commitment/hex_concurrent_patricia_hashed.go` — `SetTrace` propagation to root + 16 mounts
- `execution/commitment/commitmentdb/commitment_context.go` — `ComputeCommitment` (line 260-265): calls SetTrace

**Design decisions (all confirmed):**
1. `hph.traceW io.Writer` — nil = disabled, zero-cost check via `if hph.traceW != nil`
2. Output format: terse tagged plaintext — `[fold] %x ar=3`, `[unfold] beef/3 d=5`
3. Keep `if` guards at all sites — avoid evaluating expensive args like `cell.FullString()`
4. Trie interface: `SetTrace(bool)` → `SetTraceWriter(io.Writer)`
5. CommitmentCapture callers set `traceW` to `*bytes.Buffer`, read it directly
6. `traceDomain bool` kept as-is — gates only followAndUpdate trace block
7. followAndUpdate block (line 2694-2720): `if hph.traceW != nil || hph.traceDomain { ... }` — writes to traceW AND stdout when traceDomain is set

**Trace site categories (78 total):**
- `if hph.trace {` — 69 sites across fold, unfold, hash, cell, mount operations
- `[witness]` fmt.Printf — 9 sites (8 guarded by `if hph.trace`, 1 unguarded at line 1541)
- followAndUpdate trace block — keep as-is (capture mechanism separate, needs deeper analysis)

**Constraints:**
- `commitment_context.go` owned by root — may need workaround or user intervention
- Branch `cherry-pick/20234-to-r34` based on `release/3.4`
- Must pass `make lint && make erigon integration`

## Development Approach
- **testing approach**: Regular (code first, then update tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- run `go test ./execution/commitment/...` after each task
- maintain backward compatibility of Trie interface consumers

## Testing Strategy
- **unit tests**: update/add tests for SetTraceWriter, verify trace output content
- **integration**: `make erigon integration` must compile
- **lint**: `make lint` must pass

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Replace struct fields and methods on HexPatriciaHashed

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] Add `import "io"` if not present
- [x] Replace `SetTrace(trace bool)` (line 2807) with `SetTraceWriter(w io.Writer)` — sets `hph.traceW = w`
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

NOTE: To achieve compilation, Tasks 2-7 were also completed in this iteration since removing the `trace bool` and `capture []string` fields made all references to them invalid. All `if hph.trace {` guards were converted to `if hph.traceW != nil {`, the followAndUpdate block was updated, the Trie interface was changed, ConcurrentPatriciaHashed was updated, commitment_context.go callers were updated, domain_shared.go SetTrace return type changed from []string to string, and all test files were updated.

### Task 2: Convert 69 `if hph.trace {` sites to `if hph.traceW != nil {`

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] Bulk replace `if hph.trace {` → `if hph.traceW != nil {` (69 occurrences)
- [x] Bulk replace `fmt.Printf(` → `fmt.Fprintf(hph.traceW, ` inside those guarded blocks
- [x] Verify no `hph.trace` references remain (except `traceDomain`)
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

### Task 3: Convert 9 `[witness]` sites

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] Lines 1562, 1675, 1680-1681, 1714, 1722, 1732, 1746: already guarded by `if hph.trace {` — these were converted in Task 2, verify `fmt.Fprintf(hph.traceW, ...)` is correct (guard converted, fmt.Printf not yet converted to Fprintf)
- [x] Line 1541: unguarded `fmt.Printf("[witness] root node ...")` — wrap in `if hph.traceW != nil {` and convert to `fmt.Fprintf(hph.traceW, ...)`
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

### Task 4: Convert followAndUpdate trace block (line 2694-2720)

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] Replace the block at line 2694-2720 with:
  ```
  if hph.traceW != nil || hph.traceDomain {
      // ... build trace string (lines 2695-2711 unchanged) ...
      if hph.traceW != nil {
          fmt.Fprintf(hph.traceW, "[proc] %s\n", trace)
      }
      if hph.traceDomain {
          fmt.Println(trace)
      }
  }
  ```
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

### Task 5: Update Trie interface and ConcurrentPatriciaHashed

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_concurrent_patricia_hashed.go`

- [x] In `hex_concurrent_patricia_hashed.go`: replace `SetTrace(b bool)` (line 149) with `SetTraceWriter(w io.Writer)` — propagate to root and all 16 mounts
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

### Task 6: Update commitment_context.go callers

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`

⚠️ File owned by root — may need user to fix permissions first (`sudo chown agent:agent commitment_context.go`)

- [x] `SetTrace` method (line 144-147): kept SetTrace(bool) on SharedDomainsCommitmentContext, removed internal SetTrace call to patriciaTrie; added captureBuf field and DrainCapture method
- [x] `ComputeCommitment` (lines 260-265): replaced with captureBuf/SetTraceWriter pattern
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile


**Files:**
- Modify: any files outside `execution/commitment/` that call these methods

- [x] Update each caller to use `SetTraceWriter(io.Writer)` instead of `SetTrace(bool)` — updated domain_shared.go SetTrace return type from []string to string, exec3_parallel.go caller updated
- [x] Run `go test ./execution/commitment/... -run TestNothing` — must compile

### Task 8: Add tests for trace writer

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed_test.go`

- [x] Add `TestSetTraceWriter_NilDisablesTrace` — set traceW to nil, run Process, verify no output
- [x] Add `TestSetTraceWriter_BufferCapturesOutput` — set traceW to `*bytes.Buffer`, run a small Process, verify buffer contains `[fold]` and `[proc]` prefixed lines
- [x] Run `go test ./execution/commitment/... -count=1` — must pass

### Task 9: Verify and cleanup

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go` (if unused imports remain)
- Modify: `execution/commitment/commitment.go` (if unused imports remain)

- [x] Remove unused imports
- [x] Run `make lint` — repeat until clean
- [x] Run `make erigon integration` — must compile
- [x] Run `go test ./execution/commitment/... -count=1` — final test pass

### Task 10: [Final] Commit and close

- [x] Commit with descriptive message
- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### Field change
```go
// Before
type HexPatriciaHashed struct {
    trace       bool
    traceDomain bool
    capture     []string
    ...
}

// After
type HexPatriciaHashed struct {
    traceW      io.Writer  // nil = disabled
    traceDomain bool       // kept as-is
    ...
}
```

### Trace site pattern (69 sites)
```go
// Before
if hph.trace {
    fmt.Printf("[fold] %x ar=%016b\n", key, afterMap)
}

// After
if hph.traceW != nil {
    fmt.Fprintf(hph.traceW, "[fold] %x ar=%016b\n", key, afterMap)
}
```

### followAndUpdate block
```go
// Before (line 2694-2720)
if hph.trace || hph.traceDomain || hph.capture != nil {
    // ... build trace string ...
    if hph.trace || hph.traceDomain {
        fmt.Println(trace)
    }
    if hph.capture != nil {
        hph.capture = append(hph.capture, trace)
    }
}

// After
if hph.traceW != nil || hph.traceDomain {
    // ... build trace string ...
    if hph.traceW != nil {
        fmt.Fprintf(hph.traceW, "[proc] %s\n", trace)
    }
    if hph.traceDomain {
        fmt.Println(trace)
    }
}
```

### CommitmentCapture caller (commitment_context.go)
```go
// Before
sdc.patriciaTrie.SetTrace(sdc.trace)
if sdc.sharedDomains.CommitmentCapture() {
    }
}

// After
if sdc.sharedDomains.CommitmentCapture() {
    if sdc.captureBuf == nil {
        sdc.captureBuf = new(bytes.Buffer)
    }
    sdc.patriciaTrie.SetTraceWriter(sdc.captureBuf)
} else if sdc.trace {
    sdc.patriciaTrie.SetTraceWriter(os.Stderr) // or a configured writer
} else {
    sdc.patriciaTrie.SetTraceWriter(nil)
}
```

### Trie interface
```go
// Before
type Trie interface {
    SetTrace(bool)
    SetTraceDomain(bool)
    ...
}

// After
type Trie interface {
    SetTraceWriter(io.Writer)
    SetTraceDomain(bool)
    ...
}
```

## Post-Completion

**Manual verification:**
- Run with `traceW = os.Stderr` on a real datadir to verify trace output readability
- Verify CommitmentCapture still works by setting `traceW = &bytes.Buffer{}` and checking buffer content
- Confirm no performance regression with `traceW = nil` (should be identical — nil pointer check vs bool check)
