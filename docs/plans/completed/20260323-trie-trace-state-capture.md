# Plan: Capture Trie Internal State in TrieTrace

**Status**: Complete
**Date**: 2026-03-23
**Branch**: `awskii/trie-recording-trace`

## Problem

The trie trace records reads (branches, accounts, storages) and input updates, but not the internal trie state (root cell, depths, branch flags, touch/after maps). Existing tests pass because they start with empty tries — `NewHexPatriciaHashed` initializes to empty state. In production, `seekCommitment` restores prior state via `SetState(cs.trieState)` before calling `Process`. Without that state in the trace, replays from production traces silently diverge because the trie starts empty instead of starting from the restored checkpoint state.

## Solution

Capture `EncodeCurrentState()` output before `Process` and embed it in the trace. On replay, call `SetState()` before `Process` to reproduce the exact trie starting point.

## Design Decisions

1. **TrieState as hex string in TOML** — consistent with how branches/accounts/storages are already stored. `omitempty` for backwards compatibility with old traces and empty-trie tests.
2. **BuildTrieTrace gets a `trieState []byte` parameter** — the caller owns the trie and captures state before Process. BuildTrieTrace just hex-encodes it.
3. **DecodeTrieState() helper on TrieTrace** — returns `([]byte, error)`, hex-decodes the field. Returns nil for empty string (empty trie / old trace).
4. **LoadTrieTraceIntoMockState returns 4th value** — `trieState []byte` so the caller can call `SetState()` on the trie.

## Files to Change

### 1. `execution/commitment/trie_trace.go`

**Add `TrieState` field to `TrieTrace` struct** (line ~44, before closing brace):
```go
type TrieTrace struct {
    BlockNum    uint64                       `toml:"block_num,omitempty"`
    TxNum       uint64                       `toml:"tx_num,omitempty"`
    Error       string                       `toml:"error,omitempty"`
    TrieState   string                       `toml:"trie_state,omitempty"`  // NEW
    Branches    map[string]string            `toml:"branches"`
    Accounts    map[string]string            `toml:"accounts"`
    Storages    map[string]string            `toml:"storages"`
    Updates     []TraceKeyUpdate             `toml:"updates"`
    PutBranches map[string]TraceBranchWrite  `toml:"put_branches,omitempty"`
}
```

**Modify `BuildTrieTrace` signature** — add `trieState []byte` parameter:
```go
func BuildTrieTrace(rc *RecordingContext, inputKeys map[string]struct{}, trieState []byte) (*TrieTrace, error)
```
Inside the function, after constructing `tt`, set:
```go
if len(trieState) > 0 {
    tt.TrieState = hex.EncodeToString(trieState)
}
```

**Add `DecodeTrieState()` method on `TrieTrace`**:
```go
// DecodeTrieState returns the decoded trie state bytes, or nil if no state
// was captured (empty trie or old trace format).
func (tt *TrieTrace) DecodeTrieState() ([]byte, error) {
    if tt.TrieState == "" {
        return nil, nil
    }
    return hex.DecodeString(tt.TrieState)
}
```

### 2. `execution/commitment/commitmentdb/commitment_context.go`

**In `ComputeCommitment`, capture trie state before Process** (around line 296, after `inputKeys := sdc.updates.PlainKeys()`):

```go
// Capture internal trie state before Process — required for replay.
// In production the trie has been restored via seekCommitment/SetState;
// without this snapshot, replay starts from empty state and diverges.
var trieState []byte
if hph, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
    trieState, err = hph.EncodeCurrentState(nil)
    if err != nil {
        log.Warn("[commitment] failed to encode trie state for trace", "err", err)
        trieState = nil // non-fatal, continue without state
    }
}
```

**Update the `BuildTrieTrace` call** (line ~301) to pass `trieState`:
```go
trace, traceErr := commitment.BuildTrieTrace(recorder, inputKeys, trieState)
```

### 3. `execution/commitment/trie_trace_test.go`

**Update `LoadTrieTraceIntoMockState` return signature** — add 4th return value `[]byte`:
```go
func LoadTrieTraceIntoMockState(t testing.TB, path string) (*MockState, [][]byte, []Update, []byte)
```
At the end, decode and return trie state:
```go
trieState, err := tt.DecodeTrieState()
require.NoError(t, err)
return ms, plainKeys, updates, trieState
```

**Update all callers of `LoadTrieTraceIntoMockState`** to accept 4th return value:
- `TestTrieTraceRoundTrip` (line 126): `state2, replayKeys, replayUpdates, _ := LoadTrieTraceIntoMockState(...)`
- `TestTrieTraceAccountOnlyRoundTrip` (line 196): same pattern
- `TestTrieTraceStorageOnlyRoundTrip` (line 242): same pattern

**Update all `BuildTrieTrace` calls in tests** to pass `nil` for trieState (empty trie tests):
- `TestTrieTraceRoundTrip` (line 118): `BuildTrieTrace(recorder, nil, nil)`
- `TestTrieTraceAccountOnlyRoundTrip` (line 186): same
- `TestTrieTraceStorageOnlyRoundTrip` (line 233): same
- `TestTrieTracePartialRoundTrip` (line 460): same
- `TestTrieTracePutBranchRecording` (line 515): same
- `TestTrieTraceErrorRoundTrip` (line 352): same

**Update `TestTrieTraceReplayFromFile`** (line 643) to restore state before Process:
```go
state, plainKeys, replayUpdates, trieState := LoadTrieTraceIntoMockState(t, tracePath)
trie := NewHexPatriciaHashed(length.Addr, state)
trie.ResetContext(state)
if len(trieState) > 0 {
    err := trie.SetState(trieState)
    require.NoError(t, err)
}
```

**Add new test: `TestTrieTraceNonEmptyStateRoundTrip`**:

This test verifies the critical path — recording and replaying with non-empty trie state:

1. Create accounts and Process them (builds up trie state)
2. Call `EncodeCurrentState()` to snapshot the non-empty state
3. Create new updates, wrap with `RecordingContext`, Process again
4. Call `BuildTrieTrace(recorder, nil, trieState)` with the captured state
5. Save trace to temp file, reload via `LoadTrieTraceIntoMockState`
6. Verify returned `trieState` is non-nil and non-empty
7. Create fresh `HexPatriciaHashed` on the loaded MockState
8. Call `SetState(trieState)` on the fresh trie
9. Process the same updates
10. Assert root hash matches the original

```go
func TestTrieTraceNonEmptyStateRoundTrip(t *testing.T) {
    t.Parallel()
    ctx := context.Background()

    // Phase 1: Build up trie state with initial accounts
    plainKeys1, updates1 := NewUpdateBuilder().
        Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
        Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
        Build()

    state1 := NewMockState(t)
    err := state1.applyPlainUpdates(plainKeys1, updates1)
    require.NoError(t, err)

    trie1 := NewHexPatriciaHashed(length.Addr, state1)
    upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys1, updates1)
    _, err = trie1.Process(ctx, upds1, "", nil, WarmupConfig{})
    require.NoError(t, err)
    upds1.Close()

    // Apply branch updates to state (simulating what commitmentdb does)
    trie1.branchEncoder.Load(func(prefix, data, prevData []byte) error {
        return state1.PutBranch(prefix, data, prevData)
    })

    // Snapshot the non-empty trie state
    trieState, err := trie1.EncodeCurrentState(nil)
    require.NoError(t, err)
    require.NotEmpty(t, trieState, "trie state should be non-empty after Process")

    // Phase 2: Apply more updates with recording, starting from non-empty state
    plainKeys2, updates2 := NewUpdateBuilder().
        Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
        Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 42).
        Build()

    err = state1.applyPlainUpdates(plainKeys2, updates2)
    require.NoError(t, err)

    recorder := NewRecordingContext(state1)
    trie1.ResetContext(recorder)

    upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys2, updates2)
    rootHash1, err := trie1.Process(ctx, upds2, "", nil, WarmupConfig{})
    require.NoError(t, err)
    upds2.Close()

    // Build trace with trie state
    trace, err := BuildTrieTrace(recorder, nil, trieState)
    require.NoError(t, err)
    require.NotEmpty(t, trace.TrieState, "trace should contain trie state")

    tracePath := filepath.Join(t.TempDir(), "stateful_trace.toml")
    err = trace.Save(tracePath)
    require.NoError(t, err)

    // Phase 3: Replay from trace with state restoration
    state2, replayKeys, replayUpdates, loadedState := LoadTrieTraceIntoMockState(t, tracePath)
    require.NotEmpty(t, loadedState, "loaded trie state should be non-empty")

    trie2 := NewHexPatriciaHashed(length.Addr, state2)
    trie2.ResetContext(state2)
    err = trie2.SetState(loadedState)
    require.NoError(t, err)

    upds3 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, replayKeys, replayUpdates)
    rootHash2, err := trie2.Process(ctx, upds3, "", nil, WarmupConfig{})
    require.NoError(t, err)
    upds3.Close()

    require.Equal(t, rootHash1, rootHash2, "replay with restored state should match original root hash")
}
```

### 4. `execution/commitment/trie_trace_test.go` — Update existing backwards compat test

**Update `TestTrieTraceBackwardsCompatibility`** (line 589) to verify TrieState is empty for old traces:
```go
require.Empty(t, loaded.TrieState)
```

**Update `TestTrieTraceNewFieldsRoundTrip`** (line 543) to include TrieState:
```go
tt := &TrieTrace{
    // ...existing fields...
    TrieState: "deadbeefcafebabe",  // arbitrary hex for round-trip test
}
// ...after loading...
require.Equal(t, tt.TrieState, loaded.TrieState)
```

## Implementation Order

1. Add `TrieState` field to `TrieTrace`, add `trieState` param to `BuildTrieTrace`, add `DecodeTrieState()` method
2. Update `LoadTrieTraceIntoMockState` to return 4th value
3. Update all test callers of `BuildTrieTrace` and `LoadTrieTraceIntoMockState` (compile fix)
4. Update `commitment_context.go` to capture and pass trie state
5. Add `TestTrieTraceNonEmptyStateRoundTrip`
6. Update `TestTrieTraceReplayFromFile` to restore state
7. Update `TestTrieTraceNewFieldsRoundTrip` and `TestTrieTraceBackwardsCompatibility`
8. Run `make lint && go test ./execution/commitment/... -count=1`

## Risks

- **`EncodeCurrentState` panics if `currentKeyLen > 0`** — this is fine because we call it *before* Process (not during), so the trie is in a clean state.
- **SetState with accountAddrLen/storageAddrLen > 0 calls `accountFromCacheOrDB`/`storageFromCacheOrDB`** — requires `ctx` (PatriciaContext) to be set. In replay, we call `ResetContext(state)` before `SetState`, so this is safe. The MockState will have the account/storage data loaded from the trace.
- **Large state encoding** — `EncodeCurrentState` encodes root cell + grid arrays. The encoded size is bounded (~few KB). Hex-encoding doubles it, which is fine for TOML.
- **ConcurrentPatriciaHashed** — the `ComputeCommitment` capture code type-asserts to `*HexPatriciaHashed`. For `ConcurrentPatriciaHashed`, we'd need `sdc.patriciaTrie.(*commitment.ConcurrentPatriciaHashed).RootTrie().EncodeCurrentState(nil)`. For this initial implementation, we only handle `HexPatriciaHashed` (the default variant). The `ConcurrentPatriciaHashed` case can be added later if needed.
