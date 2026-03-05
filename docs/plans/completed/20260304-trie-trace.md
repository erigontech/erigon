# Trie Trace: Capture & Replay Trie State for Debugging

## Overview
Add a feature to capture all data read during trie `Process` (branch nodes, account lookups, storage lookups) along with the input updates, and dump them to a TOML file. This file can then be loaded into `MockState` to reproduce the exact trie computation in a test — without exporting the entire production state tree.

**Problem**: Debugging trie issues requires reproducing the exact state that was present during processing. Currently this means exporting entire production trees.

**Solution**: A `RecordingContext` decorator that wraps `PatriciaContext`, intercepts all reads, and produces a self-contained TOML fixture file.

**Activation**: Set `ERIGON_TRIE_TRACE_FILE=/path/to/output.toml` — when set, tracing is enabled and output is written to that path.

## Context (from discovery)

**Key interfaces and types:**
- `PatriciaContext` interface (`execution/commitment/commitment.go:127`): `Branch()`, `Account()`, `Storage()`, `PutBranch()`, `TxNum()`
- `Update` struct with `Encode(buf, numBuf)` / `Decode(buf, pos)` for binary serialization
- `Updates.HashSort()` for iterating key-update pairs in hashed order
- `MockState` (`execution/commitment/patricia_state_mock_test.go`): `sm map[string][]byte`, `cm map[string]BranchData`
- `WrapKeyUpdates(tb, mode, hasher, keys, updates)` for creating test Updates
- `dbg.EnvString()` pattern in `common/dbg/dbg_env.go` with auto `ERIGON_` prefix
- TOML library: `github.com/pelletier/go-toml/v2` (already in go.mod)

**Integration point**: `ComputeCommitment()` in `execution/commitment/commitmentdb/commitment_context.go:368` — wraps context before `Process` call at line 422.

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes
- **CRITICAL: all tests must pass before starting next task**
- Run `make lint` after changes (run repeatedly per CLAUDE.md — linter is non-deterministic)

## Implementation Steps

### Task 1: Create RecordingContext decorator

**Files:**
- Create: `execution/commitment/recording_context.go`

- [x] Create `RecordingContext` struct wrapping `PatriciaContext` with mutex-protected maps for branches (`map[string][]byte`), accounts (`map[string][]byte`), storages (`map[string][]byte`)
- [x] Implement `NewRecordingContext(inner PatriciaContext) *RecordingContext`
- [x] Implement `Branch(prefix)` — delegates to inner, records `prefix → data` (copy bytes) on success
- [x] Implement `Account(plainKey)` — delegates to inner, records `plainKey → Update.Encode()` on success (non-nil, non-delete)
- [x] Implement `Storage(plainKey)` — same pattern as Account
- [x] Implement pass-through methods: `PutBranch()`, `TxNum()`
- [x] Verify builds: `go build ./execution/commitment/...`

### Task 2: Create TrieTrace TOML types with Save/Load

**Files:**
- Create: `execution/commitment/trie_trace.go`

- [x] Define `TrieTrace` struct with TOML tags: `Branches`, `Accounts`, `Storages` (all `map[string]string` hex→hex), `Updates []TraceKeyUpdate`
- [x] Define `TraceKeyUpdate` struct: `PlainKey string`, `Update string` (both hex-encoded)
- [x] Implement `BuildTrieTrace(rc *RecordingContext, updates *Updates) (*TrieTrace, error)` — converts recorded maps to hex strings, iterates `updates` via `HashSort` to capture input key-update pairs
- [x] Implement `(tt *TrieTrace) Save(path string) error` — marshal with `toml.Marshal`, write to file
- [x] Implement `LoadTrieTrace(path string) (*TrieTrace, error)` — read file, unmarshal with `toml.Unmarshal`
- [x] Verify builds: `go build ./execution/commitment/...`

### Task 3: Add env var and wire into ComputeCommitment

**Files:**
- Modify: `common/dbg/dbg_env.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`

- [x] Add `var TrieTraceFile = EnvString("TRIE_TRACE_FILE", "")` to `dbg_env.go`
- [x] In `ComputeCommitment`, after `trieContext` is created (line ~395) and before `Process` (line ~422): if `dbg.TrieTraceFile != ""`, create `RecordingContext` wrapping `trieContext`, call `ResetContext(recorder)`
- [x] After `Process` returns successfully: call `BuildTrieTrace(recorder, sdc.updates)` then `Save(dbg.TrieTraceFile)`, log result with `log.Info`
- [x] Handle errors gracefully — log warnings but don't fail the Process
- [x] Verify builds: `go build ./execution/commitment/commitmentdb/...`

### Task 4: Add test helper and round-trip test

**Files:**
- Create: `execution/commitment/trie_trace_test.go`

- [x] Implement `LoadTrieTraceIntoMockState(t testing.TB, path string) (*MockState, [][]byte, []Update)` — loads TOML, decodes hex values, populates `MockState.sm` (accounts + storages) and `MockState.cm` (branches), returns plainKeys and Updates for `WrapKeyUpdates`
- [x] Write round-trip test `TestTrieTraceRoundTrip`: create MockState → build updates with `UpdateBuilder` → `applyPlainUpdates` → `Process` with RecordingContext → `Save` to temp file → `LoadTrieTraceIntoMockState` into fresh MockState → `Process` again → assert same root hash
- [x] Write test for Save/Load TOML serialization (verify hex encoding correctness)
- [x] Run tests: `go test ./execution/commitment/ -run TestTrieTrace -v`

### Task 5: Verify acceptance criteria

- [x] Verify all requirements from Overview are implemented
- [x] Verify edge cases: empty updates, storage-only updates, account-only updates
- [x] Run full test suite: `go test ./execution/commitment/... -count=1`
- [x] Run lint: `make lint` (repeat until clean)

### Task 6: [Final] Update documentation

- [x] Update `CLAUDE.md` if new patterns discovered
- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### RecordingContext data flow
```
ComputeCommitment()
├── trieContext = sdc.trieContext(tx, txNum)     // real PatriciaContext
├── recorder = NewRecordingContext(trieContext)   // wrap it
├── sdc.patriciaTrie.ResetContext(recorder)       // install wrapper
├── Process() runs normally
│   ├── unfold → recorder.Branch(prefix) → records prefix→data
│   ├── followAndUpdate → recorder.Account(key) → records key→encoded
│   └── followAndUpdate → recorder.Storage(key) → records key→encoded
├── BuildTrieTrace(recorder, updates) → TrieTrace
└── trace.Save(path) → TOML file
```

### TOML format
```toml
[branches]
"0a0b" = "00030003abcdef..."

[accounts]
"00112233aabbccdd..." = "0501000a..."

[storages]
"00112233...4455..." = "1001abcd..."

[[updates]]
plain_key = "00112233..."
update = "0401000000000a..."
```

All keys and values are hex-encoded. Updates use `Update.Encode()`/`Decode()` binary format.

### Loading into MockState
- Branches: hex-decode key→value, store in `ms.cm[string(key)] = BranchData(value)`
- Accounts/Storages: hex-decode key→value, store in `ms.sm[string(key)] = value` (already encoded via `Update.Encode()`)
- Input updates: hex-decode each, `Update.Decode()` to get `[]Update`, use with `WrapKeyUpdates()`

## Post-Completion

**Manual verification:**
- Set `ERIGON_TRIE_TRACE_FILE=/tmp/trace.toml` and run erigon briefly against a synced datadir
- Inspect the output TOML file for correctness
- Load the trace in a test and verify the root hash matches
