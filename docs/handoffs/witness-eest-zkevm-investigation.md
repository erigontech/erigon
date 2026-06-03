# `debug_executionWitness` vs EEST zkEVM v0.4.0 — Handoff

State of the witness-correctness investigation against the EEST `zkevm@v0.4.0`
corpus. Hand off to a future Claude session.

Last updated by an in-flight session: 2026-05-30.

## TL;DR

- We built an in-process test suite (`execution/tests/eest_zkevm_witness/`) that
  runs the EEST `zkevm@v0.4.0` fixtures through Erigon's `debug_executionWitness`
  and compares the result to each fixture's canonical `executionWitness` field.
- **SOLVED (2026-05-30): the dominant codes mismatch is in-block-CREATED bytecode
  over-inclusion.** Erigon built the witness `codes` from `GetAccessedCode()` (all
  code reached via GetCode/GetCodeSize — Geth `witness.AddCode` semantics). Reth's
  Canonical mode (the EEST filler) only emits **pre-state** code (loaded from DB by
  codehash); a stateless verifier re-derives created code by replaying the block.
  Fix = source `codes` from `GetPreStateCode()`. → **PR #21518** (against `main`).
- Result with the fix on the #21491 baseline: **2,837 / 2,881 pass (98.5%)**,
  codes mismatches **924 → 11**, 0 stateless-exec / state-root regressions, 111
  newly passing, **0 regressions**. Run log: `/tmp/eest-run-prestate-codes.log`.
- #21518 **supersedes #21517** (the EIP-6780 transient filter, which only caught
  the `deleted ∧ ∉ pre-state` subset). The pre-state rule covers all in-block
  creates: surviving cross-tx selfdestruct-to-self, in-block 7702 delegation
  markers, CREATE2-recreate, transient creates.
- Static proof: across all 924 pre-fix mismatches, **100% (925/925)** of the extra
  bytecodes were absent from pre-state. None came from pre-state.

### Prior framing (superseded, kept for context)

- Old best: **2,726 / 2,871 (94.6%)** with `main` + #21491 + #21517. Baseline
  `main`-alone **2,515 (87.6%)**. The "two mechanisms / unknown +1" framing below
  is resolved by the pre-state rule — it was always one mechanism (in-block creates).

## Live PRs / branches / worktrees

| Path | Branch | Purpose | PR |
|---|---|---|---|
| `/Users/awskii/org/wrk/erigon-eest-zkevm` | `awskii/eest-zkevm-witness` | Test suite (`execution/tests/eest_zkevm_witness/`) + `test-fixtures.json` entry + Makefile target + CI shard | #21487 (Draft) |
| `/Users/awskii/org/wrk/erigon-witness-codes-prestate` | `awskii/witness-codes-prestate` | **THE FIX** — witness `codes` from `GetPreStateCode()` not `GetAccessedCode()`. Off `main`. | **#21518** |
| `/Users/awskii/org/wrk/erigon-test-pr21491` | `awskii/test-eest-pr21491` | Scratch where #21518's change was first validated on the #21491 baseline (run log `/tmp/eest-run-prestate-codes.log`) | — |
| `/Users/awskii/org/wrk/erigon-witness-eip6780` | `awskii/witness-eip6780-transient-codes` | EIP-6780 transient-bytecode filter — **SUPERSEDED by #21518** | #21517 |
| `/Users/awskii/org/wrk/erigon-test-pr21491` | `awskii/test-eest-pr21491` | Scratch: PR #21491 + suite, used as baseline for diffs | — |
| `/Users/awskii/org/wrk/erigon-test-delegate-hyp` | `awskii/test-eest-delegate-hyp` | Scratch: EIP-7702 delegation-probe + EIP-6780 v2 patches (the v2 numbers in the table above come from here) | — |
| `/Users/awskii/org/wrk/erigon-test-codesize-hyp` | `awskii/test-eest-codesize-hyp` | Scratch: EXTCODESIZE side-channel experiment (REFUTED) | — |
| `/Users/awskii/org/wrk/erigon-test-wbas`, `…-pr21491`, `…-wfc`, `…-wbas-pr21491` | various | Scratch worktrees from candidate evaluation; safe to delete | — |

The wcs / wfc / wbas-* worktrees are evaluation residue and can be removed once
this handoff is no longer needed. The four "scratch" worktrees above retain
useful diff history; keep them until #21517 lands.

Other PRs in scope:
- **#21307** — umbrella tracking issue ("debug_executionWitness should be 100%
  correct"). #21487, #21491, #21517 are all referenced under it.
- **#21491** — lupin012's `fix_witness_zero_zero`. Fixes state-witness
  over-inclusion via `detectCollapseSiblings` (skips zero→zero collapse keys).
  Orthogonal to the codes mismatches.
- **#21487** (Draft) — the test suite. The CI-gating blocker that keeps it
  Draft is explicit in its description.

## Reth as the canonical reference

EEST fixtures' `executionWitness` field is produced by the EEST witness-filler:
`https://github.com/kevaundray/reth.git`, branch `jsign-witness-filler`, binary
`witness-filler`. Mainline Reth implements the same canonical format as
`ExecutionWitnessMode::Canonical` in `crates/trie/common/src/execution_witness.rs`
and `crates/revm/src/witness.rs`. The fixtures are ground truth — running Reth
ourselves is unnecessary for comparison.

### What Canonical mode emits (Reth source, paraphrased)

`bytecode` field, exact rule from `crates/revm/src/witness.rs`:

```rust
ExecutionWitnessMode::Canonical => {
    let mut codes: Vec<_> = statedb.cache.contracts.values()
        .map(|c| c.original_bytes())
        .filter(|code| !code.is_empty())
        .collect();
    codes.sort_unstable();
    codes
}
```

Three rules:
1. Only contracts in `statedb.cache.contracts` (EVM-touched during execution).
   Legacy additionally chains in `statedb.bundle_state.contracts` — contracts
   created in this block. **Canonical excludes "created bytecode."**
2. Filter empty bytecodes (`!code.is_empty()` — zero length, not `0x00`).
3. Sort lex-ascending by bytes.

`state` field (per docstring):
- No empty nodes.
- No storage trie root nodes if no storage accessed.
- Minimum siblings for post-state root (updates first, then deletions).
- Sorted lex-ascending.

Erigon's analog: `result.Codes` is built from `RecordingState.AccessedCode` —
the set of addresses whose code was read via `ReadAccountCode` or `OnCodeAccess`.
Erigon sorts by `keccak256(content)`, not lex by content. The pass-count
suggests this happens to match EEST anyway (likely EEST also dedup-sorts by
codeHash), but it is worth a careful look if surviving mismatches involve
ordering.

## Baseline / cumulative numbers

All measured by running the full corpus (2,871 fixtures, ~18 min serial,
fresh MDBX per fixture):

| Stack | Pass | Codes mm | State mm | Erigon more | Erigon fewer | Equal-diff |
|---|---:|---:|---:|---:|---:|---:|
| `main` + suite | 2,515 (87.6%) | 1,176 | ~6,776 | 3,923 | 4,128 | 0 |
| + #21491 alone | **2,711 (94.1%)** | 1,176 | **15** | 1,193 | 48 | 0 |
| + #21517 (this PR, stacked on #21491) | **2,726 (94.6%)** | **924** | 15 | 943 | 46 | 0 |
| + EXTCODESIZE side-channel (refuted) | 76 | 22,153 | 0 | 211 | 20,896 | 1,111 |

(`Erigon more` / `fewer` / `Equal-diff` count *array-length-mismatch direction*
across all codes / state / headers comparison errors.)

Direction of the remaining 924 codes mismatches under #21491 + #21517:

| expected → got | count | delta |
|---|---:|---:|
| 5 → 6 | 599 | +1 |
| 6 → 7 | 169 | +1 |
| 7 → 8 | 94 | +1 |
| 4 → 5 | 15 | +1 |
| 8 → 9 | 15 | +1 |
| 6 → 8 | 12 | +2 |
| 5 → 7 | 9 | +2 |
| 4 → 6 | 3 | +2 |

**The +1 pattern dominates and points at a single per-fixture mechanism.**

## The current problem (concrete)

The dominant surviving failure is a per-fixture **+1 over-inclusion** in `codes`.
Concrete example used as a probe:

```
test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests/for_amsterdam/
  tangerine_whistle/eip150_operation_gas_costs/eip150_selfdestruct/
  selfdestruct_state_access_boundary.json
```

For variant `alive_beneficiary-has_balance-same_tx-cold-contract-exact_gas`,
EEST expects 5 codes (4 system predeploys at `0x...fffffffe` + 1 CALL contract
whose code does `PUSH32 <init> ... CREATE ... CALL`). Erigon emits 6 — the
extra is `7334b0f2ad2b4b1c9e13ac87ad3afbc10188f4a79eff`, the runtime code of
the `PUSH20 <addr> SELFDESTRUCT` proxy that the CALL contract creates and
calls in the same tx. This is the EIP-6780 transient pattern; #21517 catches
this exact case.

The remaining 599 base-case mismatches survive **#21517**, so a *different* +1
mechanism is in play for those.

## What's been ruled out

Don't re-walk these.

1. **EXTCODESIZE over-inclusion** — refuted. EEST *does* include
   EXTCODESIZE-touched codes (when we excluded them: 20,896 under-includes
   appeared). Scaffolding in `awskii/test-eest-codesize-hyp` for reference.
2. **EIP-7702 `GetDelegatedDesignation` probe** — partially helps
   (`-53` mismatches) but introduces 5 regressions because the delegation
   marker code for EIP-7702 senders genuinely belongs in the witness. The
   no-record reader scaffolding in `awskii/test-eest-delegate-hyp` is sound
   but the call-site change in `GetDelegatedDesignation` over-corrects. Not
   the +1 culprit.
3. **`CreatedContracts ∩ DeletedAccounts`** as the EIP-6780 discriminator —
   doesn't work. `IntraBlockState.Selfdestruct` clears
   `stateObject.createdContract` so `stateWriter.CreateContract` is never
   called for EIP-6780 contracts. Use the discriminator in #21517 instead:
   `addr ∈ DeletedAccounts ∧ addr ∉ PreStateCode`.

The four "next leads" below (EIP-2929 pre-warming, SELFDESTRUCT beneficiary,
`stateObject.Code()` eager-load, `ResolveCode` callers) were all chasing the
same phantom: a +1 over-inclusion that turned out to be **in-block-created
bytecode**, not a spurious pre-state read. The pre-state rule (#21518) resolves
all of them at once. They are left here only so a future session doesn't re-open
them as if unexplored — they are closed.

## Residual failures after #21518 (the real next leads)

44 fixtures still fail (`/tmp/eest-run-prestate-codes.log`). Breakdown:

1. **`eip8025_optional_proofs/witness_validation_*` (~25)** — EEST *negative*
   tests that ship a deliberately truncated/padded witness (`validation_codes_
   missing_*`, `validation_state_missing_*`, `validation_headers_missing_*`) to
   exercise a verifier's rejection path. Our suite compares Erigon's *correct*
   witness against the intentionally-broken one → false mismatch. These are a
   limitation of the compare-only methodology, **not Erigon bugs.** They may need
   the suite to special-case eip8025 validation fixtures (do NOT add `t.Skip` —
   see CLAUDE.md; instead detect-and-assert-rejection or exclude by fixture kind).

2. **`eip8025_optional_proofs/witness_headers/*` + `witness_validation_headers/*`
   + frontier `blockhash` (~13 headers mm)** — BLOCKHASH-opcode ancestor-header
   inclusion in the witness. Could be a genuine Erigon gap (witness `headers`
   set) OR more eip8025 negative tests. Needs triage: compare expected vs got
   header sets for `witness_headers_blockhash_at_offset.json`.

3. **`witness_state_replay_order/*` + `witness_validation_state/*` (~10 state
   mm)** — trie-node ordering (insert-before-delete) and missing/extra nodes.
   Mix of eip8025 negative tests and possible real ordering differences.

4. **One genuine positive `codes` residual:** `witness_codes_create_same_hash_
   then_read.json` — a pre-existing account `0x37f536…` has 1-byte code `0x00`;
   the test creates a new contract with the *same* code/hash and reads it. EEST
   emits 4 codes (excludes `0x00`); we emit 5. Our pre-state read of `0x37f536`'s
   `0x00` looks spurious — the only remaining true over-inclusion. Worth a look
   but it's a single same-codehash edge case.

5. **`exec/other` (~11)** — no witness-category line; failed earlier (block
   exec, RLP decode, etc.). Includes `prague/eip7702_set_code_tx/*` delegation
   clearing, `cancun` blobhash/beacon-root. Some are likely Amsterdam-EIP
   in-progress gaps (see memory `amsterdam-eips-in-progress.md`), some negative.

## How to run the corpus

### One-time setup

In any worktree:

```bash
# Already in test-fixtures.json on main since the suite landed via #21487/this branch.
make test-fixtures-zkevm    # downloads ~221MB, sha256-verifies, extracts
                             # caches under ./test-fixtures-cache/eest_zkevm/

# Or share the already-extracted cache across worktrees:
ln -snf /Users/awskii/org/wrk/erigon-eest-zkevm/test-fixtures-cache \
        /path/to/new/worktree/test-fixtures-cache
```

The shared cache is ~2.5 GB extracted. Don't re-extract per worktree.

### Run

```bash
cd /path/to/worktree
go test -count=1 -v -timeout 60m \
  ./execution/tests/eest_zkevm_witness/... \
  > /tmp/eest-run-<tag>.log 2>&1
# 18-19 min serial; one fresh MDBX per fixture. Don't parallelize.
```

For background runs via the Claude harness, pass the `go test` command
directly through `run_in_background: true` (not via `nohup ... &`) so the
harness tracks the actual test process and notifies on exit.

### Tally a run

```bash
LOG=/tmp/eest-run-<tag>.log
grep -oE "TestExecutionSpecWitness/[^ ]+\.json" $LOG | sort -u > /tmp/_all.txt
grep -E "FAIL: TestExecutionSpecWitness" $LOG \
  | grep -oE "TestExecutionSpecWitness/[^ ]+\.json" | sort -u > /tmp/_fail.txt

echo "total:   $(wc -l < /tmp/_all.txt)"
echo "fail:    $(wc -l < /tmp/_fail.txt)"
echo "pass:    $(comm -23 /tmp/_all.txt /tmp/_fail.txt | wc -l)"

# Mismatch direction (all of state/codes/headers combined)
grep -oE "(expected|got) \([0-9]+\):" $LOG | awk '
/^expected/ {e=$0; gsub(/[^0-9]/,"",e); have=1; next}
/^got/ && have {g=$0; gsub(/[^0-9]/,"",g);
  if(g>e)m++; else if(g<e)f++; else eq++; have=0}
END{print "more:",m+0," fewer:",f+0," equal:",eq+0}'

# Per-category mismatch counts
grep -cE "block [0-9]+ state witness mismatch"   $LOG
grep -cE "block [0-9]+ codes witness mismatch"   $LOG
grep -cE "block [0-9]+ headers witness mismatch" $LOG
grep -cE "stateless block execution failed"      $LOG
grep -cE "state root mismatch after stateless"   $LOG
```

### Set-diff between runs

```bash
# Newly passing in run B vs A:
comm -13 /tmp/_B_fail.txt /tmp/_A_fail.txt | wc -l
# Newly failing in B vs A:
comm -23 /tmp/_B_fail.txt /tmp/_A_fail.txt | wc -l
```

### Investigating one fixture

```bash
# Find the mismatch block for a specific fixture
LN=$(grep -n "selfdestruct_state_access_boundary.json" $LOG \
     | head -1 | cut -d: -f1)
sed -n "${LN},$((LN+50))p" $LOG

# Decode a fixture's pre-state and expected witness
FX=test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests/.../some.json
jq -r 'to_entries[0].value.pre | to_entries[]
       | "\(.key)  code=\(if .value.code == "0x" then "<empty>" else .value.code[0:60] end)"' "$FX"
jq -r 'to_entries[0].value.blocks[0].executionWitness.codes[] | .[0:80]' "$FX"
```

## Key code locations (Erigon)

| Concern | File:Line | What |
|---|---|---|
| Witness result type | `rpc/jsonrpc/debug_execution_witness.go:495` | `ExecutionWitnessResult` struct |
| Witness builder entry | `rpc/jsonrpc/debug_execution_witness.go:524` | `DebugAPIImpl.ExecutionWitness` |
| Recording state | `rpc/jsonrpc/debug_execution_witness.go:43` | `RecordingState` — `AccessedCode`, `PreStateCode`, `ModifiedCode`, `DeletedAccounts`, `CreatedContracts` |
| `ReadAccountCode` (recorder) | `rpc/jsonrpc/debug_execution_witness.go:225` | overlay-then-inner; populates `AccessedCode` + `PreStateCode` |
| `ReadAccountCodeSize` (recorder) | `rpc/jsonrpc/debug_execution_witness.go:259` | calls `ReadAccountCode` to ensure stateless verifier can answer; this is intentional, do not "fix" without paired verifier work |
| `OnCodeAccess` hook | `rpc/jsonrpc/debug_execution_witness.go:425` | cache-hit path; fires from `IBS.callCodeAccessHook` |
| `collectAccessedState` | `rpc/jsonrpc/debug_execution_witness.go:821` | builds `accessedState` (Codes, CodeReads, etc.) — **this is where #21517 patches** |
| Stateless verifier | `rpc/jsonrpc/debug_execution_witness.go:1335` | `witnessStateless`; its `ReadAccountCode` consults `codeMap` keyed by codeHash |
| `IBS.GetCode` / `GetCodeSize` | `execution/state/intra_block_state.go:642` / `709` | calls `callCodeAccessHook` on cache hits |
| `IBS.GetDelegatedDesignation` | `execution/state/intra_block_state.go:822` | EIP-7702 probe; reads code via `stateObject.Code()` → `ReadAccountCode` |
| `IBS.Selfdestruct` | `execution/state/intra_block_state.go:1441` | sets `stateObject.selfdestructed = true`, clears `createdContract`; **does not call `stateWriter.DeleteAccount`** — that happens in `updateAccount` at end-of-tx |
| `updateAccount` | `execution/state/intra_block_state.go:2018` | end-of-tx writer dispatch; emits `DeleteAccount` if `selfdestructed || emptyRemoval`, emits `CreateContract` only if `createdContract` survived to here |
| `IBS.codeAccessTracker` interface | `execution/state/intra_block_state.go:631` | `OnCodeAccess(addr, code)` — implemented only by `RecordingState` |

## Reth code locations (for canonical reference)

```
paradigmxyz/reth:crates/trie/common/src/execution_witness.rs
  → ExecutionWitnessMode enum (Legacy / Canonical) with doc strings
paradigmxyz/reth:crates/revm/src/witness.rs
  → ExecutionWitnessRecord.record_executed_state — the bytecode-inclusion rule
paradigmxyz/reth:crates/trie/trie/src/witness.rs
  → mode.is_canonical() gates state-witness shape (storage-update set, root-node)
paradigmxyz/reth:crates/storage/provider/src/providers/state/{latest,historical}.rs
  → witness construction defaults: always_include_root_node when !canonical
```

EEST filler reference:
- `kevaundray/reth#jsign-witness-filler` — the binary that fills the
  `executionWitness` field in EEST fixtures. Ground truth.

## Operational notes

- **Don't run two corpus runs in parallel.** Each one wants ~5–8 GB of disk
  for ephemeral MDBX state and is CPU-bound. Sequential.
- **Background test launches via the harness:** call `go test ...`
  directly with `run_in_background: true`. Don't wrap with `nohup ... &` —
  if you do, the wrapper exits in seconds and the harness completion event
  fires on the wrapper instead of the test, leaving the real test untracked.
- **Worktree gopls noise:** the editor's go-list often complains about
  workspace inclusion for ad-hoc worktrees (`undefined: DebugAPIImpl` etc.).
  Trust `go build ./...`, not the editor diagnostics.
- **Lint:** `make lint` is non-deterministic; run twice and ensure both
  report `0 issues.` before pushing. CLAUDE.md enforces this pre-push.

## Memory files

`/Users/awskii/.claude/projects/-Users-awskii-org-wrk/memory/`:
- `MEMORY.md` — index
- `amsterdam-eips-in-progress.md` — note that Amsterdam EIPs (7708/7778/7843/7928)
  are being implemented in a separate worktree the user hasn't surfaced; affects
  how to interpret block-execution failures vs witness-comparison failures.
