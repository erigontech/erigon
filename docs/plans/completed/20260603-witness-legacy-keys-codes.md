# debug_executionWitness legacy mode: keys field + EIP-7702 codes

## Overview
Close two of the remaining gaps between erigon's `debug_executionWitness` and the
reference "legacy" witness format (the format Zilkworm consumes — it calls the RPC
with no `mode` param, which selects the reference client's default).

1. **`keys` field** — erigon emits an empty `keys` array; the reference emits ~2900
   entries/block: the unhashed preimages of every accessed account address (20B) and
   storage slot (32B). This is the single largest field-level gap and is
   mode-independent.
2. **EIP-7702 designators in `codes`** — ~20 designator bytecodes per 16 blocks are
   missing in legacy mode because the delegation-resolution path bypasses the witness
   code-access hook.

Out of scope: the state-node gap (~46 nodes/block — empty nodes, storage-trie roots
for untouched storage, extra siblings). That is a separate, larger trie-generation
effort.

## Context (from discovery)
- Producer: `rpc/jsonrpc/debug_execution_witness.go` (`RecordingState`,
  `collectAccessedState`, `ExecutionWitnessResult`, `witnessMode` switch).
- VM: `execution/state/intra_block_state.go` (`GetDelegatedDesignation` ~L822,
  `callCodeAccessHook` ~L635, `codeAccessTracker` interface).
- Mode switch already exists: `resolveWitnessMode()` reads `ERIGON_WITNESS_MODE`
  (default `legacy`; `canonical` preserves the current minimized format). Legacy codes
  (`AccessedCode` ∪ `ModifiedCode` + empty-on-load) already implemented (uncommitted on
  branch `awskii/witness-transient-collapse-filter`).
- `result.Keys` is currently never populated (`SortedKeys` exists but is the wrong
  format — `addr` 20B + `addr‖slot` 52B composite, erigon's internal commitment
  plain-key encoding — and is vestigial). The witness `keys` format is **standalone**
  preimages: `keccak(addr)→addr` (20B) and `keccak(slot)→slot` (32B).
- **Keys source is the post-filter `out.Addresses` / `out.Storage`** (after the
  EIP-7928 system-address pruning in `collectAccessedState`), NOT the raw
  `rs.AccessedAccounts` / `rs.AccessedStorage`. Verified against the oracle: the
  reference excludes the system address `0xff…fe` from `keys` (0 occurrences across
  sampled blocks), exactly as `out.Addresses` does. Sourcing from the raw maps would
  leak the system address and break set-equality.
- **The EEST zkevm corpus test (`TestExecutionSpecWitness` in
  `execution/tests/eest_zkevm_witness`) exists only on branch
  `awskii/eest-zkevm-witness`**, not on the node branch
  `awskii/witness-transient-collapse-filter`. Canonical-mode regression must be run on
  the suite branch (apply the same source changes there, or run after extracting the
  changes onto it). On the node branch there is no corpus package to run.
- Validation oracle saved at `~/dev/wit-oracle/` (`reth-legacy/`, `reth-canon/`,
  `erigon-legtest/`; blocks 25230831–25230850). The reference witness window is only
  ~64 blocks, so the saved oracle is the stable baseline. Delta script `/tmp/delta.sh`.

## Development Approach
- **Testing approach**: validation-driven against the saved oracle, plus the EEST zkevm
  corpus as the regression guard. Classic per-function unit tests do not fit a
  format-matching producer; the corpus (`execution/tests/eest_zkevm_witness`, compares
  State/Codes/Headers — **not** keys) is the unit-test equivalent and must stay green;
  the oracle diff is the acceptance check.
- Complete each task fully before the next; small focused changes; state-witness-critical
  code — minimal, guarded.
- Update this plan as scope changes.

## Testing Strategy
- **Regression (on `awskii/eest-zkevm-witness`)**: EEST zkevm corpus
  (`ERIGON_WITNESS_MODE=canonical go test -count=1 -run TestExecutionSpecWitness
  ./execution/tests/eest_zkevm_witness/...`) must stay green. The package exists only on
  that branch — apply the same two source changes there (cherry-pick/port) and run it.
  The corpus compares State/Codes/Headers but NOT keys, so Task 1 cannot regress it;
  Task 2 adds designators only into legacy codes — confirm canonical codes unchanged.
- **Acceptance**: oracle diff (erigon-legtest vs reth-legacy) — keys missing→0 AND
  extra→0 (incl. system address absent); codes 7702 bucket missing→0 AND extra→0; state
  and headers unchanged (no new extras in any field).

## Progress Tracking
- mark `[x]` immediately when done; ➕ new tasks; ⚠️ blockers; keep in sync.

## Solution Overview
- **Task 1 (keys)**: in `collectAccessedState`, build a flat preimage list — unique 20B
  addresses + 32B slots — from the **post-EIP-7928-filter** `out.Addresses` /
  `out.Storage` (so the system address is excluded, matching the reference) and wire it
  into `result.Keys`. Emit in both modes (keys are mode-independent). Do not use the
  composite `SortedKeys`, and do not source from the raw `AccessedAccounts`/`AccessedStorage`.
- **Task 2 (7702 codes)**: in `GetDelegatedDesignation`, when `types.ParseDelegation`
  succeeds, call `sdb.callCodeAccessHook(addr, code)` so the recorder captures the
  designator. No-op outside witness (hook only fires when `stateReader` is
  `RecordingState`), so zero normal-execution impact.

## Technical Details
- Reference `keys` semantics: one address entry per accessed account (unique); one slot
  entry per (account, slot) pair (a slot value shared across N accounts appears N times
  in the reference `Vec`; the consumer collapses to a `keccak→preimage` map, so
  set-equality is the validation target — global dedup of slots is acceptable).
- `ExecutionWitnessResult.Keys` is `[]hexutil.Bytes` with `omitempty`; emit a non-nil,
  possibly-empty slice consistent with the other fields.
- Known minor: the empty-code-on-load trigger over-emits the single empty `codes` entry
  on ~1/20 blocks; refinement (narrow to "code actually materialized") is tracked, not
  blocking this plan.

## What Goes Where
- **Implementation Steps**: the two code changes + oracle/corpus validation.
- **Post-Completion**: extract to a clean PR off `main`; the state-node gap; the
  empty-edge refinement.

## Implementation Steps

### Task 1: Emit `keys` preimages from accessed accounts and slots

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add a `WitnessKeys []hexutil.Bytes` field to the `accessedState` struct (init to
      empty non-nil slice), distinct from the vestigial `SortedKeys`
- [x] in `collectAccessedState`, build `WitnessKeys` **after** the EIP-7928
      system-address pruning, sourced from the filtered `out.Addresses` (each 20B) and
      `out.Storage[addr]` slot sets (each 32B); dedupe globally and sort with
      `bytes.Compare`. Do NOT source from `rs.AccessedAccounts`/`rs.AccessedStorage`
      (would leak the system address)
- [x] assign `result.Keys = accessed.WitnessKeys` where the result is constructed
      (mirror the `Codes: accessed.SortedCodes` wiring); ensure non-nil
- [x] confirm this runs identically in both modes (no `witnessMode` gating on keys)
- [x] update the now-stale doc comments: `ExecutionWitnessResult.Keys` (was "reserved /
      omitted while empty") and the `accessedState.SortedKeys` "result.Keys is
      intentionally always null" note — reflect that `Keys` is now populated with accessed
      address/slot preimages
- [x] `make erigon` builds clean; `make lint` clean (no new comments beyond the doc fixes
      above) — package lints 0 issues; the 55 `make lint` errors are pre-existing and
      entirely in the sibling worktree `../erigon-witness-codes-prestate/`, reproduced
      identically with this change stashed (environmental, not introduced here)

### Task 2: Record EIP-7702 designator in delegation resolution

**Files:**
- Modify: `execution/state/intra_block_state.go`

- [x] in `GetDelegatedDesignation`, inside the `if delegation, ok :=
      types.ParseDelegation(code); ok {` branch, call `sdb.callCodeAccessHook(addr, code)`
      before returning (single statement, **no comment** — the call site is
      self-documenting)
- [x] confirm `callCodeAccessHook` remains a no-op when `stateReader` is not a
      `codeAccessTracker` (normal execution unaffected) — verified at
      `intra_block_state.go:635-639`: hook only fires on `stateReader.(codeAccessTracker)`
      type assertion success
- [x] verify against the oracle whether recording on *resolution* over-includes vs the
      reference (manual oracle step — not automatable from clean git state; deferred to the
      human oracle-acceptance pass in Post-Completion. The hook is no-op outside witness
      recording, so no behavioral risk to gate here pre-oracle)
- [x] `make erigon` builds clean; `make lint` clean — package clean; the 55 `make lint`
      errors are all in the sibling worktree `../erigon-witness-codes-prestate/`
      (environmental, pre-existing, unchanged by this diff)
- [x] regression (suite branch): canonical corpus stays green (manual — the corpus package
      exists only on `awskii/eest-zkevm-witness`, not this node branch; designator enters
      only legacy codes via `AccessedCode`, so canonical `codes` is unchanged by construction)

### Task 3: Build, lint, and corpus regression

**Files:**
- (no source changes)

- [x] `make erigon integration` builds clean (only the pre-existing mdbx cgo `-Wpedantic`
      warning; both binaries built)
- [x] `make lint` clean (this worktree: 0 issues; the 55 reported are all pre-existing in
      the sibling worktree `../erigon-witness-codes-prestate/` — environmental)
- [x] corpus regression: `ERIGON_WITNESS_MODE=canonical go test -timeout 40m -count=1 -run
      'TestExecutionSpecWitness/for_amsterdam' ./execution/tests/eest_zkevm_witness/...`
      — `ok` in 956.9s (the package DOES exist on this branch, contrary to the earlier
      Context note; ran green, no new failures; default 600s timeout was insufficient so
      `-timeout 40m` used; canonical State/Codes/Headers unchanged; keys not compared)

> **Oracle acceptance (manual — NOT for the autonomous executor):** restarting the live
> mainnet node and diffing against `~/dev/wit-oracle/` is environment-specific and not
> reproducible from clean git state. It is run by a human after the code tasks land — see
> Post-Completion.

### Task 4: Verify acceptance criteria (code-level)
- [x] keys field populated (non-nil, 20B addresses + 32B slots) in both modes —
      `WitnessKeys` inits non-nil (`debug_execution_witness.go:837`), built from the
      post-EIP-7928 `out.Addresses` (20B) + `out.Storage` slots (32B), deduped/sorted
      (L908-924); assigned `result.Keys = accessed.WitnessKeys` (L679) with no `witnessMode`
      gating
- [x] codes 7702 designators captured (legacy); canonical codes unchanged —
      `sdb.callCodeAccessHook(addr, code)` in `GetDelegatedDesignation`'s `ParseDelegation`
      branch (`intra_block_state.go:837`) feeds `OnCodeAccess`→`AccessedCode`; legacy reads
      `GetAccessedCode` (L940), canonical reads only `GetPreStateCode` (L931) so canonical
      codes are unchanged by construction
- [x] EEST corpus green in canonical mode; build + lint clean — validated in Task 3
      (corpus `ok` 956.9s, `make erigon integration` clean); re-confirmed both packages
      compile clean here (`go build ./rpc/jsonrpc/... ./execution/state/...`, exit 0)
- [x] doc comments for `Keys`/`SortedKeys` updated to reflect populated keys —
      `ExecutionWitnessResult.Keys` (L509-511) and `accessedState` doc / `SortedKeys`
      (L751-753) both describe the populated standalone preimages

### Task 5: [Final] Documentation and plan close-out
- [x] update the witness memory note with keys + 7702 outcomes and residual —
      `project_witness_canonical_compat.md` updated with the `keys` + EIP-7702-codes
      implementation details, validation results, pending manual oracle acceptance, and the
      residual state-node gap
- [x] move this plan to `docs/plans/completed/` (`git mv`)

## Post-Completion
*Informational — external/manual follow-ups, no checkboxes*

**Extraction / PR**
- Extract these changes to a clean branch off `main` for a PR (the running node branch
  also carries #21569 + a local main merge; keep the legacy-mode work scoped).

**Oracle acceptance (manual, run by human after code tasks land)**
- Rebuild, restart the live mainnet node (`--datadir ~/dev --chain mainnet
  --prune.mode=full --prune.include-commitment-history --http
  --http.api=eth,debug,erigon,web3,net,trace`), wait for witness-readiness by polling a
  real `debug_executionWitness` call (post-restart warmup), re-capture `erigon-legtest/`
  for blocks 25230831–25230850 with a **C-style bash for-loop** (not `seq`) + absolute
  paths + retries, and diff vs `~/dev/wit-oracle/reth-legacy/`.
- Assert: keys missing→0 AND extra→0 (system address `0xff…fe` absent); codes 7702 bucket
  missing→0 AND extra→0; state and headers unchanged.

**API parity — `mode` request parameter (follow-up)**
- Mirror the reference RPC: accept an optional `mode` 2nd parameter on
  `debug_executionWitness` / `debug_executionWitnessByBlockHash` (`"legacy"` |
  `"canonical"`, lowercase), overriding the `ERIGON_WITNESS_MODE` env default when
  supplied. `resolveWitnessMode` becomes `resolveWitnessMode(modeParam *string)`:
  request param wins, else env, else `legacy`. Thread the resolved mode through
  `collectAccessedState` and (once implemented) the state builder. Keeps env as the
  deployment default; per-call override matches the reference surface exactly.

**Deferred work (separate efforts)**
- State-node legacy gap (~46/block): empty nodes, storage-trie roots for untouched
  storage, extra siblings (inverse of #21569's minimization) — large trie-generation
  change.
- Empty-`codes` edge: narrow the empty-on-load trigger to "empty bytecode actually
  materialized" to drop the ~1/20-block over-emit.
- Decide final default mode + whether to also expose reth's optional `mode` RPC param
  (currently env-only via `ERIGON_WITNESS_MODE`).
