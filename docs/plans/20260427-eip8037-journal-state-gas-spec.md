# EIP-8037 Journal-Walk State-Gas Accounting (bal-devnet-4 v2)

## Context

EIP-8037 (Amsterdam) introduced multi-dimensional gas: a regular-gas dimension and a state-gas dimension that prices state growth. The current implementation charges state gas eagerly at every state-touching opcode (SSTORE, CREATE, CALL-to-empty, SELFDESTRUCT, code deposit) and unwinds those charges via ad-hoc refund rules:

- SSTORE 0→X→0 inline credit via `evm.CreditStateGasRefund`, with cross-frame plumbing through `state_gas_refund_pending` for DELEGATECALL/CALLCODE chains (PR #2733).
- CREATE silent-failure refunds of `GAS_NEW_ACCOUNT` on insufficient balance, nonce overflow, depth, and address collision (PR #2704).
- Code-deposit halt unwind that rolls back initcode state-gas charges (PR #2595).
- Same-tx SELFDESTRUCT refund driven by `IntraBlockState.SameTxSelfDestructedNewAccounts()` at tx finalize.
- Frame revert/halt accounting in `evm.handleFrameRevert` that restores spill, refund-tracker, and reservoir at depth-dependent boundaries.

This sprawl has produced bugs the snøbal-devnet-4 test suite has surfaced, and there is concern that more cases are lurking. The new design eliminates per-opcode state-gas charging entirely. State gas is computed once per call/create frame at commit time by walking the journal segment that frame produced. On revert/halt the journal undoes everything → no state gas charged for that frame. All historical refund paths collapse into one rule: each frame's net contribution is `walk_total − committed_children_total`; charge if positive, credit if negative.

## Design Decisions (confirmed)

1. **Charge timing**: pure frame-end. Opcodes charge regular gas only.
2. **Reservoir model**: keep `mdgas.MdGas{Regular, State}`, `SplitTxnGasLimit`, full-reservoir-pass-to-child, and spill-into-`gas_left` when reservoir exhausted.
3. **Per-frame delta accounting**: each frame at commit walks its journal segment and computes `delta = walk_total − committed_children_total`. If `delta > 0`, charge `delta × cpsb` from the frame's reservoir (with spill into `gas_left`; OOG → revert). If `delta < 0`, credit `|delta| × cpsb` back to the frame's reservoir AND decrement `evm.executionStateGas`. This single mechanism handles every cross-frame interaction (SSTORE 0→X→0 across siblings/DELEGATECALL chains, create-then-destroy across frames, etc.) without any refund plumbing. The walk uses **`storageChange.originalValue`** (= tx-entry value, captured at SetState time) as the "new slot" determinant, matching spec PR 11573's rule and ensuring OOG attribution at the spec-correct frame.
4. **SELFDESTRUCT filter applied only at the top-frame walk**: sub-frames walk *without* the `.newlyCreated && .selfdestructed` filter (so EIP-6780 net-0 cases temporarily over-count and OOG mid-execution exactly as the spec wants — state IS allocated transiently). At the top frame's commit (depth==0), the walk DOES apply the filter, which makes the top-frame walk `< committed_children_total`, producing the negative delta that credits the over-charge back. Multiple selfdestructs of the same account are deduplicated naturally because the walk keys on address (first occurrence per address).
5. **Top-level CREATE tx**: pass `excludeAddr` so the contract address (already covered by the 112×cpsb intrinsic) is not double-counted by the top-frame walk. Snapshot stays where it is; the top-level `createObjectChange` is just skipped in the walk. The exclusion is derived in-place from `depth == 0` inside `evm.create()` (where the contract address is already a local parameter) — no coordination from TxnExecutor needed, no field on the EVM struct.
6. **Per-depth `committedChildBytes` accumulator on the EVM**, populated by each child as it commits. No journal-range bookkeeping (`frameChildRanges` is unnecessary because the running total of children's contributions, combined with the parent's full-segment walk and credit-on-negative-delta, is strictly more correct than range exclusion — the latter would miss cross-frame SSTORE 0→X→0).
7. **System calls (EIP-2935 / EIP-4788 / EIP-7002 / EIP-7251 etc.) charge state gas internally via the journal walk, but with a dedicated pre-sized reservoir** (per [EIPs PR 11573 commit `d2a0230`](https://github.com/ethereum/EIPs/pull/11573/changes/d2a023056187fb17c94e9477cadd076a0f817760)). The system-call MdGas is initialised as `{Regular: 30_000_000, State: STATE_BYTES_PER_STORAGE_SET × CPSB × SYSTEM_MAX_SSTORES_PER_CALL}` where `SYSTEM_MAX_SSTORES_PER_CALL = 16`. Total `SYSTEM_CALL_GAS_LIMIT = 30_000_000 + 32 × 1_174 × 16 = 30_601_088`. System calls remain not subject to `TX_MAX_GAS_LIMIT`, do not count against the block gas limit, and do not contribute to either `block_regular_gas_used` or `block_state_gas_used` — but they DO walk the journal at frame commit and DO charge state gas from their dedicated reservoir (so each system call can SSTORE up to 16 fresh slots without OOG'ing on state gas).

## Algorithm

At each call/create frame:

```
Frame entry (evm.call / evm.create):
    frameStart  = ibs.JournalLength()
    push 0 onto evm.committedChildBytes                  // per-depth accumulator

Frame body runs (regular gas charged inline as today; no state-gas charges).

Frame commit (success path, IsAmsterdam, !RestoreState):
    frameEnd   = ibs.JournalLength()
    applyFilt  = (depth == 0)                            // filter only at top frame
    excludeC   = address if (in evm.create() && depth == 0) else NilAddress
                                                         // address is the local param of evm.create();
                                                         // for evm.call() it's always NilAddress
    walkTotal  = ibs.ComputeFrameStateBytes(frameStart, frameEnd, applyFilt, excludeC)
    childTotal = evm.committedChildBytes[depth]
    delta      = int64(walkTotal) - int64(childTotal)

    if delta > 0:
        stateGas = uint64(delta) * cpsb
        deduct stateGas from gas.State first, spill into gas.Regular
        if insufficient → err = ErrOutOfGas (then handleFrameRevert path)
        else evm.executionStateGas += stateGas
    elif delta < 0:
        creditGas = uint64(-delta) * cpsb
        gas.State += creditGas                           // credit current frame
        evm.executionStateGas -= creditGas               // shrink tx total
    // delta == 0 → nothing to do

    pop evm.committedChildBytes[depth]
    if depth > 0:
        evm.committedChildBytes[depth-1] += walkTotal    // propagate to parent

Frame revert/halt:
    poppedChildBytes = pop evm.committedChildBytes[depth]    // do NOT add to parent
    if IsAmsterdam && !RestoreState && poppedChildBytes > 0:
        // EIP-8037: restore all state gas consumed by committed descendants
        // to this frame's reservoir (which is then propagated to the parent
        // via restoreChildGas). Spec: "On child revert or exceptional halt,
        // all state gas consumed by the child, both from the reservoir and
        // any that spilled into gas_left, is restored to the parent's
        // reservoir." At depth==0 this naturally zeroes evm.executionStateGas
        // because the top frame's accumulator holds the sum of all charged
        // bytes for the tx.
        restoreGas = poppedChildBytes * cpsb
        gas.State += restoreGas
        evm.executionStateGas -= restoreGas                  // saturate at 0
    RevertToSnapshot                                         // journal undoes everything
    burn regular gas on exceptional halt (same as today)

Tx finalize (TxnExecutor.Execute):
    blockStateGas = intrinsicStateGas + evm.executionStateGas
    // No refund logic in TxnExecutor — credits/charges already baked in.
```

`ComputeFrameStateBytes(start, end, applyFilter, excludeCreate)` walks `journal.entries[start:end]` linearly (no range exclusions). It accumulates:

- **Account creations** — `createObjectChange` and `resetObjectChange`, first occurrence per address. Skip if `excludeCreate == account`. Skip if stateObject is nil. If `applyFilter` AND stateObject is `.newlyCreated && .selfdestructed`, skip. Otherwise +112.
- **Code deposit** — `codeChange`, first per address. If `applyFilter` AND `.newlyCreated && .selfdestructed`, skip. When `prevhash` is empty AND current `stateObject.code` is non-empty, +`len(code)`.
- **Storage 0→non-zero** — `storageChange`, dedup by `(address, key)`. If `applyFilter` AND `.newlyCreated && .selfdestructed`, skip. **Use the entry's `originalValue` field (= tx-entry value, captured at SetState time via `GetCommittedState`).** When `originalValue.IsZero()` AND current value (`stateObject.GetState(key)`) is non-zero, +32.
- All other entry types (`selfdestructChange`, `balanceChange`, `nonceChange`, `refundChange`, `addLogChange`, `transientStorageChange`, `accessList*Change`, `touchAccount`, `balanceIncrease*`, `fakeStorageChange`) are skipped.

**Why `originalValue` (tx-entry) instead of first-prev-in-segment**: matches spec PR 11573's "new slot" rule, which references the slot's tx-entry value (not frame-entry). Without this, a parent that wrote 0→X and then a child that wrote X→Y would over-charge the parent and under-charge the child relative to the spec, producing the same tx-level total but different per-frame OOG attribution. With `originalValue`, the deepest committed observer that sees `originalValue=0 AND current!=0` is the one that charges, and OOG fires at the spec-correct frame.

## Why this resolves the existing edge cases

| Old edge case | New behavior |
|---|---|
| SSTORE 0→X→0 same frame | Walk: `originalValue=0 AND current=0` → 0 bytes. No refund needed. |
| SSTORE 0→X→0 across DELEGATECALL/CALLCODE chains, or sibling frames | Creator frame sees `originalValue=0 AND current=non-zero` at its commit time → +32 charge. Clearer frame and parent see `originalValue=0 AND current=0` → 0. Parent's `committedChildBytes` includes the creator's +32, parent's walk yields 0 → parent.delta=−32 → credit. `state_gas_refund_pending` plumbing deleted entirely. |
| CREATE silent failure (collision, balance, depth, nonce) | No journal entries produced (snapshot pushed/popped) → 0 bytes for that frame. No `CreditStateGasRefund` needed. |
| Code-deposit halt | `SetCode` not called when deposit OOGs → no `codeChange` in journal → 0 code bytes. Plus regular-gas rollback (still needed). |
| Same-tx SELFDESTRUCT (within or cross frame, single or multiple times) | Sub-frames walk without filter → may charge transiently (spec-correct: state was real during execution). At top-frame commit the filter applies, making top.walk < committed_children_total → negative top.delta → credit. Per-address dedup is automatic (walk keys on address; multiple selfdestructs of same account → 1 skip). |
| Frame OOG state-gas restoration to parent | Frame revert → journal undoes → no commit-time charge fired. Nothing to restore. |
| Top-level revert state-gas restoration | `evm.executionStateGas = 0` on top-level revert. Tx-state-gas = intrinsic only. |
| Per-frame OOG attribution matches spec | `originalValue` (tx-entry) drives "new slot" check; the deepest committed observer with `originalValue=0 AND current!=0` charges and OOGs, matching spec PR 11573. |

## Alignment with EIP-8037 PR 11573

PR [ethereum/EIPs#11573](https://github.com/ethereum/EIPs/pull/11573) ("Update EIP-8037: fixed CPSB + frame accounting") rewrites EIP-8037 to drop per-opcode charging in favour of frame-end accounting. Our plan is consistent with the PR's intent and produces matching tx-level totals, but the per-frame mechanism differs.

### Where we match
- **Constant CPSB = 1174** (PR's quantization-removed table). Already hardcoded in `eip8037.go`.
- **Frame-end charging only** — opcodes do not emit state-gas charges.
- **Reverted/halted frames produce no debits or credits.**
- **SELFDESTRUCT accounting deferred to top-frame commit** (spec calls it "transaction end"; for us this is `evm.create()` / `evm.call()` at `depth==0`, which is the same moment).
- **Each created/cleared slot or account accounted for exactly once across the call stack** (we get this property automatically from the first-prevalue-in-segment rule + `committedChildBytes` delta math).
- All previously-needed refund machinery (SSTORE 0→X→0 inline credit, CREATE silent-failure refund, code-deposit halt unwind, same-tx SELFDESTRUCT refund at TxnExecutor) is removed in both designs.

### Where the mechanism differs
The spec emits per-slot charges and refunds at every frame commit by examining three reference values (`tx-entry`, `frame-entry`, `frame-exit`). Our plan emits per-frame *deltas* (charge or credit) where the walk uses `tx-entry` (via `storageChange.originalValue`) as the new-slot determinant, combined with `committedChildBytes` accumulator + delta math.

The two approaches produce the same per-frame charge attribution for the **new slot** rule (which is the rule that matters for OOG), and the same tx-level totals for every scenario.

What we don't emit explicitly:
- The spec's **"cleared slot, zero at tx start" refund** (`frame-entry != 0 AND frame-exit == 0 AND tx-entry == 0`) is not emitted as a separate refund step. In our plan, the slot simply isn't charged in the first place (the walk's new-slot check requires `current != 0`), so there's nothing to refund. The spec's literal text reads "Refund STATE_BYTES_PER_STORAGE_SET × CPSB" which would create gas without a prior charge in some traces — we read this as the spec intending a matched-pair (refund cancels a charge), so omitting both keeps the net total correct.

### OOG attribution
With `originalValue` driving the new-slot rule, our plan attributes the per-frame charge to the same frame the spec does — the deepest committed observer with `originalValue=0 AND frame-exit!=0`. Tight-reservoir OOG fires at the spec-correct frame. No consensus divergence on storage-slot OOG.

For **accounts and code**, no equivalent tx-entry field is needed because the EVM's existing collision rule (`evm.create()` checks `nonce != 0 || !contractHash.IsEmpty() || hasStorage`) ensures at most one `createObjectChange` (or `resetObjectChange`) and at most one main-frame `codeChange` per address per tx:
- Pre-existing accounts (EIP-161 prunes empties) collide on CREATE.
- Newly-created → SELFDESTRUCT → re-CREATE is blocked: SELFDESTRUCT leaves nonce/code untouched until tx finalize, so the second CREATE collides on `nonce != 0`.
- EIP-7702 auth-list `codeChange` happens before the top-frame snapshot (intrinsic processing) so it's outside every frame's walk segment.

So the walk's "first createObjectChange/codeChange per address" rule is spec-correct without any tx-entry comparison. No remaining divergence.

### Recent spec changes (PR 11573 commits after `d2a0230`)

The PR has continued to evolve. Each commit and its impact on our plan:

| Commit | Subject | Impact on plan |
|---|---|---|
| `3f190787` | Add gas used rules | None — formalises `execution_regular_gas_used` and `execution_state_gas_used` as per-tx counters that increase on charges and decrease on refunds. Already matches our `evm.regularGasConsumed` and `evm.executionStateGas`. |
| `b8193df` | Fix errors in eip | Two adjustments: (a) cleared-slot regular-gas refund of `GAS_STORAGE_UPDATE - GAS_COLD_SLOAD - GAS_WARM_ACCESS = 2800` to `refund_counter` (we already emit `SstoreSetGasEIP8037 - WarmStorageReadCostEIP2929 = 2900-100 = 2800` ✓); (b) **EIP-7702 auth refund now also decreases `execution_state_gas_used`** — required code update, see below. Also clarifies system-tx gas formula and CREATE2 hashing cost (both already aligned). |
| `8daeab6` | Fix gas used error | None — `execution_*_gas_used` initialised to 0, not to intrinsic. Already matches. |
| `421279b` | Fix additional errors | None — receipt = `tx_gas_used` post-refund post-floor (matches); CPSB now formally a "fixed parameter" (matches); EIP-7825 contract-size limit applies "when CPSB = 1174". |
| `46faf2a` | Jochem's review | None — wording. |
| `3535f03` | Small fixes from Jochem review | None — `requires:` list updated to `2780, 6780, 7702, 7825, 7976, 7981, 8038`; SELFDESTRUCT explicitly aligned with EIP-6780 (matches). |
| `731a276` | Add ERC-4337 interaction | None on consensus. New informational subsection: bundlers/EntryPoint must account for state-gas explicitly because `GAS` opcode returns `gas_left` only and cannot observe `state_gas_reservoir`. Cross-user-operation subsidy risk noted. **No execution-client behavior change** — purely a recommendation for ERC-4337 implementations layered on top. |

**Required code change (commit `b8193df`)**: Spec now states "`execution_state_gas_used` decreases by the corresponding amount" when an EIP-7702 authority is non-empty. Previously our impl added the 112×cpsb refund to `gas_remaining.State` (reservoir replenish) but left `blockStateGasUsed = imdGas.State + executionStateGas` at the worst-case value. Per the new spec, `block_state_gas_used` must also drop by `stateIgasRefund`. Fix applied in `execution/protocol/txn_executor.go` at the Amsterdam refund branch and the gasBailout/no-refund Amsterdam branch:
```go
st.blockStateGasUsed = imdGas.State + st.evm.ExecutionStateGas() - stateIgasRefund
```
Subtraction is safe: by construction `stateIgasRefund = 112 × cpsb × num_existing_auths` and `imdGas.State ≥ 135 × cpsb × num_auths ≥ stateIgasRefund`.

**Open question (commit `b8193df`)**: The spec text says the state-gas refund happens "in parallel with EIP-7702's `PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST` regular-gas refund", suggesting both refunds fire under Amsterdam. Under our current impl the regular-gas refund is only applied pre-Amsterdam (the `else` branch of `verifyAuthorities`). With Amsterdam values (`PER_AUTH_BASE_COST_EIP8037 = 7500`, intrinsic regular per auth = 7500), refunding `25000 - 12500 = 12500` regular gas would exceed the per-auth charge, producing negative net regular cost. This appears to be ambiguous spec wording rather than intended behavior — leaving the impl unchanged on this point pending clarification with spec authors.

## Test status with `snobal-devnet-5` fixtures

After the fixture submodule was bumped to `snobal-devnet-5` (the user's manual update aimed at the latest spec), running `TestExecutionSpecBlockchainDevnet` with `-count=1`:

| Stage | File-level failures | Notes |
|---|---|---|
| Initial (after fixture bump) | 146 | Mostly cross-fork tests (Prague/Cancun/etc.) running under Amsterdam |
| After empty-account skip in `liveAccount` | 52 | Net improvement of 94 file-level (1+ k sub-test) failures |

**Empty-account skip** added in `IntraBlockState.ComputeFrameStateBytes` `liveAccount`:
```go
if applyFilter && so.data.Empty() {
    return so, false
}
```
Rationale: at top-frame walk, an account that is empty per EIP-161 (`nonce==0 && balance==0 && codeHash==EmptyCodeHash`) will be pruned at tx finalize, so it must not be counted as a created account. This catches `AddBalance(X, 0)` to a non-existent X, which TouchAccount-creates an empty stateObject that emits `createObjectChange{X}` even though no real new account persists.

### Remaining 52 file-level failures (post-fix)

| Category | Count | Description |
|---|---|---|
| `amsterdam/eip8037` | 16 | Direct EIP-8037 spec tests (state-gas accounting edge cases) |
| `cancun/eip6780_selfdestruct` | 12 | EIP-6780 same-tx selfdestruct |
| `frontier/opcodes` | 3 | Basic opcode gas under Amsterdam |
| `amsterdam/eip7928` | 3 | Block Access List interactions |
| Other | 18 | spread across many forks/EIPs |

**Recurring failure pattern (eip8037)**: state-gas off by exactly `32×CPSB` (one storage slot) or `112×CPSB` (one new account). Sample: `sstore_restoration_sub_frame_revert[CALL]` gives 76,137 vs expected 38,570 (diff = 37,567 ≈ 32×CPSB).

**Root cause: chargeFrameStateGas OOG burned gas as exceptional halt.** When the frame-end state-gas charge can't cover (reservoir + remaining regular gas < required), our impl returned `ErrOutOfGas` which routes through `handleFrameRevert`'s exceptional-halt path — burning the frame's remaining `gas.Regular` and adding it to `evm.regularGasConsumed`. That's where the spurious +37,567 came from.

**EELS reference behavior** (verified by tracing `tests/amsterdam/eip8037_state_creation_gas_cost_increase/test_state_gas_sstore.py::test_sstore_restoration_sub_frame_revert` in `ethereum/execution-specs@devnets/snobal/5`):
- `apply_frame_state_gas` (`vm/interpreter.py`) sets `evm.error = OutOfGasError()` directly without raising `ExceptionalHalt`.
- Process_message's `try/except` only matches `ExceptionalHalt` for the gas-burn path. So apply_frame_state_gas's OOG bypasses it — `evm.gas_left` is preserved.
- `incorporate_child_on_error` returns the child's `gas_left` to the parent reservoir.
- Net: a frame-state-gas OOG behaves like a REVERT (state rolled back, gas returned to parent), NOT like a true exceptional halt.

**Fix applied** in `execution/vm/`:
- New error: `ErrFrameStateGasOOG` (in `errors.go`).
- `chargeFrameStateGas` returns `ErrFrameStateGasOOG` (was `ErrOutOfGas`).
- `handleFrameRevert` treats `ErrFrameStateGasOOG` like `ErrExecutionReverted` — preserves `gas.Regular`, no burn into `regularGasConsumed`.

**Test impact**: `TestExecutionSpecBlockchainDevnet` cache-busted: 52 → **31 file-level failures**. EIP-8037 direct scope: 16 → **2 failures**. 14 EIP-8037 tests fixed by this single change. Cancun/EIP-6780 selfdestruct cluster (12 tests) and Frontier/Constantinople tests (~10 tests) still failing — those have a different root cause (likely related to selfdestruct gas accounting, separate investigation).

### Fix #2: excludeCreate refund for same-tx CREATE+SELFDESTRUCT

**Test traced**: `test_selfdestruct_in_create_tx_initcode` (top-level CREATE tx where initcode SELFDESTRUCTs to a fresh beneficiary). Expected `gas_used = 131,488`; our impl produced `262,976` (extra 112×CPSB).

**Root cause via EELS comparison**:
- Intrinsic charges 112×CPSB for the contract address.
- EELS's `compute_state_byte_diff` at frame end gives `byte_delta = +112` (only the SELFDESTRUCT beneficiary; the contract is `existed_at_frame_entry=true` since the snapshot was taken AFTER `move_ether` and `mark_account_created`).
- EELS's `process_message_call` then refunds 112×CPSB (and code bytes if any) for accounts in `accounts_to_delete ∩ created_accounts`. Net execution_state = 0.
- Our impl: walk excludes the contract via `excludeCreate` and counts the beneficiary `+112`. No corresponding refund mechanism for the contract → over-counts.

**Fix applied** in `IntraBlockState.ComputeFrameStateBytes` and `evm.chargeFrameStateGas`:
- New `excludeCreateRefund` second return value from `ComputeFrameStateBytes`. When `applyFilter && excludeCreate ≠ NilAddress` and the contract is `newlyCreated && selfdestructed`, returns 112 (+ len(code) if present) — mirroring EELS's `accounts_to_delete` refund.
- `chargeFrameStateGas` subtracts `excludeCreateRefund` from the frame's positive delta; if the result is negative it credits back.

**Test impact**: 31 → **30 file-level failures**. EIP-8037 direct scope: 2 → **1 failure**. Remaining EIP-8037 case (`sstore_restoration_charge_in_ancestor`) is now a *receipt hash mismatch* (not gas-used), indicating receipt field divergence in CALLCODE/DELEGATECALL variants — separate issue from gas accounting.

### Remaining 30 failures — need per-cluster investigation

Three fixes brought 146 → 30. The remaining 30 cluster as follows:

| Cluster | Tests | Pattern | Status |
|---|---|---|---|
| `cancun/eip6780_selfdestruct/*` | 12 | Diff = 1×112×CPSB (or other). **Verified test-isolation bug**: each failing variant PASSES when run truly alone (single `-run` regex), but FAILS when sibling tests in the same fixture file run first. Confirmed for `selfdestruct_not_created_in_same_tx_with_revert.json` — variant 2 fails but variants 1, 2, 3 individually all pass. The cross-contamination is at the subtest level (`t.Run` within a single fixture file), and persists even with `-parallel 1`. The 81-byte SD code our impl shows in failing runs (with CREATE at pc=42, deploying 5 phantom contracts) is NOT the SD contract code for the failing test — it matches `test_recursive_contract_creation_and_selfdestruct`'s SD code. Each subtest creates a fresh `ExecModuleTester` (own tmpdir DB) but some package-level state (sync.Pool? cache?) leaks between them. **Defensive reset of `stateObjectPool` returned objects did NOT fix the bug.** | **Not fixed**. Needs deeper investigation of which package-level state persists between subtests. |
| `frontier/opcodes` + `create` | 5 | Various opcode/create gas patterns under Amsterdam fork. Different from cancun pattern. | Untraced |
| `prague/{6110,7002,7251}` | 3 | System requests (deposits, withdrawals, consolidations). | Untraced |
| `tangerine_whistle/eip150_selfdestruct` | 2 | Diff varies; `gas used 25943, header 37568` (under-counts) AND `gas used 157316, header 37803` (over-counts). Mixed pattern. | Untraced |
| `constantinople/eip1052_extcodehash` | 2 | Diff = 131488 (1×112×CPSB) and 135010 (115×CPSB, irregular). | Untraced |
| `amsterdam/eip8037 sstore_restoration_charge_in_ancestor` | 1 | Receipt hash mismatch (not gas), CALLCODE/DELEGATECALL variants. | Untraced |
| `amsterdam/eip7708 selfdestruct_to_system_address` | 1 | Selfdestruct to system address (`0xff...fe`). | Untraced |
| Others | 4 | byzantium staticcall, cancun create, shanghai warm_coinbase, frontier scenarios | Untraced |

Each cluster needs:
1. Generate EELS trace via `cd /tmp/execution-specs && uv run fill -v <test path> --fork Amsterdam --traces --evm-dump-dir=/tmp/traces`.
2. Compare `result.json`'s gasUsed with our impl's output.
3. Identify which addresses/storage events EELS counts vs ours.
4. Fix root cause in walk or chargeFrameStateGas.

The tooling is set up at `/tmp/execution-specs` (`devnets/snobal/5` branch) with `uv` deps installed. Each trace iteration takes ~30 seconds.

## Session end state

- **146 → 19 file-level failures** (87% reduction).
- EIP-8037 direct scope: **17 → 1 failure** (`sstore_restoration_charge_in_ancestor`, receipt hash mismatch — separate from gas accounting).
- 5 fixes applied, all backed by EELS reference-impl traces.
- `make lint` clean.

### Fix #4: resetObjectChange does not contribute +112 to walk total

**Root cause** (verified against EELS `compute_state_byte_diff` in
`forks/amsterdam/state_tracker.py`): EELS only adds +112 for an account when
`account_now != None && !existed_at_frame_entry && !existed_at_tx_entry`. A
pre-existing account being deployed to (Erigon's `resetObjectChange`) fails
the third condition — the account record already counted toward block-state
at the prior funding tx, so the new CREATE's frame-end byte_delta does NOT
re-charge 112.

**Fix applied** in `IntraBlockState.ComputeFrameStateBytes`:
- `resetObjectChange` does NOT add +112 to `total` (only `createObjectChange`
  does — that's the `account didn't exist at tx entry` case).
- The address is still tracked in `acctData` so the EIP-6780 refund path
  (see fix #5) refunds 112 unconditionally for `accounts_to_delete ∩
  created_accounts`.

### Fix #5: explicit per-tx EIP-6780 SELFDESTRUCT refund at top frame

**Root cause** (verified by EELS trace of
`test_create_selfdestruct_same_tx[selfdestruct_contract_initial_balance_100000-single_call-CREATE]`):
EELS's `compute_state_byte_diff` at top-frame end charges +112 for D ONLY
in balance_0 (D fresh). For balance_100000 (D existed at tx entry), no +112
is charged. EELS THEN refunds 112 + len(code) + non_zero_storage_bytes
unconditionally for any address in `accounts_to_delete ∩ created_accounts`
(via `process_message_call`'s top-level refund loop). This produces:

  | balance | top-frame +112 for D | refund 181 for D | net for D |
  |---------|----------------------|-------------------|-----------|
  | 0       | yes (charged)        | yes (refunded)    | 0         |
  | 100_000 | no                   | yes (-112 offset) | -112      |

The -112 in balance_100000 offsets some other charge (e.g., the intrinsic
112 for the top-level CREATE contract C), giving the test's expected
`block_state_gas_used = max(0, intrinsic + execution) = max(0, 112 + (-112) +
slots) = (slots) bytes`.

Our previous walk applied an EIP-6780 filter at the top frame (skipping
walk entries for `newlyCreated && selfdestructed` addresses). That filter
worked for balance_0 (D's bytes effectively cancelled in walk total) but
NOT for balance_100000, because the walk-and-delta math always sums to the
top-frame walkTotal — and with the filter, both cases produced the same
total. We needed to drive net balance_100000 NEGATIVE.

**Fix applied**:
1. `ComputeFrameStateBytes` now returns `(total, accountRefund)`. Total no
   longer applies the EIP-6780 filter — it counts everything per the rules
   (createObjectChange, codeChange, storageChange).
2. At top frame (`applyFilter`), `accountRefund` sums per-address
   `accountBytes + codeBytes + storageBytes` for each address in
   `newlyCreated && selfdestructed`. This mirrors EELS's
   `accounts_to_delete ∩ created_accounts` refund.
3. `chargeFrameStateGas` subtracts `accountRefund` from delta:
   `delta = walkTotal - childTotal - accountRefund`. Negative delta credits
   `gas.State` and decrements `evm.executionStateGas`.
4. `excludeCreate` (top-level CREATE C's address) is pre-tracked in `acctData`
   with `accountBytes=112` because C's `createObjectChange` lands BEFORE the
   top frame's `frameStart` (the snapshot is pushed AFTER `CreateAccount`)
   and so isn't seen by the walk. Without pre-tracking, the unified refund
   couldn't refund C.

### Fix #6: signed `executionStateGas` — allow negative for refund offset

**Root cause**: with the new explicit-refund mechanism (fix #5), the credit
at top frame can exceed the per-tx running `executionStateGas` total. Under
the old `uint64` semantics, the credit saturated at 0, leaving the
intrinsic-only block-state-gas charged. EELS's `state_gas_used` is signed
(can go negative), and `tx_state_gas = max(0, intrinsic_state_gas +
state_gas_used)` clamps at the block-level uint64 counter.

**Fix applied** in `execution/vm/evm.go`:
- `evm.executionStateGas` field changed from `uint64` to `int64`.
- `ExecutionStateGas()` now returns `int64`.
- Credit / restore paths in `chargeFrameStateGas` and `handleFrameRevert`
  no longer saturate at 0 — they subtract `int64(creditGas)` directly.
- `useMdGas` casts `originalGas` to `int64` when adding.
- `txn_executor.go`'s `blockStateGasUsed` calc now does `max(0, int64(imdGas.State) +
  executionStateGas)` before assigning to the `uint64` block counter.

### Fix #7: nullify `versionedWrites[AddressPath]` on `createObjectChange.revert` (2026-04-29)

**Root cause** (verified by tracing
`tests/cancun/eip6780_selfdestruct/test_selfdestruct_revert.py::test_selfdestruct_created_in_same_tx_with_revert[outer_selfdestruct_after_inner_call-same_tx]`):

When `createObjectChange{addr}` is reverted (frame revert pops it from
the journal), `revert()` removes `addr` from `sdb.stateObjects` but
leaves the `versionedWrites[addr][AddressPath]` entry that
`createObject()` emitted via `versionWritten`. A subsequent same-tx
read of `addr` via `getStateObject → getVersionedAccount → versionedRead`
finds the stale `WriteSetRead` value (an empty `*accounts.Account`),
treats `addr` as existing, and calls `stateObjectForAccount` which
re-adds `addr` to `stateObjects` *without* firing a new
`createObjectChange`. The next non-reverted SELFDESTRUCT to that addr
then writes a `balanceChange` with `wasCommited=false` and tags the
stateObject as `newlyCreated=false`. Walk misses the +112 byte charge
even though EELS counts the address as freshly created at frame end.

**Symptom**: under-charge by exactly 112 bytes × CPSB on a family of
selfdestruct-with-revert tests (~130 file-level failures across the
suite, but only the cancun selfdestruct_revert pair shows the 112-byte
diff cleanly; the rest exhibit downstream effects on the
versionedWrites cleanup at finalize).

**Fix applied** in `execution/state/journal.go`,
`createObjectChange.revert()`:
```go
if s.versionMap != nil {
    s.versionedWrites.UpdateVal(ch.account, AccountKey{Path: AddressPath}, (*accounts.Account)(nil))
}
```
Nullifying (not deleting) preserves the entry in `versionedWrites` so
that `MakeWriteSet`'s parallel-mode cleanup loop (line 2304) still
sees the address as reverted and clears stale entries from the global
versionMap. The `nil` value causes subsequent versionedRead to return
nil for AddressPath, so `getStateObject` doesn't materialize a phantom
stateObject. CodeHashPath stays as the reverted-empty-account
EmptyCodeHash, which is the correct value for a non-existent address.

### Fixes summary table

| Fix | What | Tests fixed |
|---|---|---|
| 1 (earlier) | empty-account skip in `liveAccount` | ~94 file-level |
| 2 (earlier) | `ErrFrameStateGasOOG` (soft OOG) | 14 EIP-8037 tests |
| 3 (earlier) | excludeCreateRefund (initcode SELFDESTRUCT) | 1 EIP-8037 |
| 4 (earlier) | resetObjectChange → no +112 in total | balance_100000 cancun cluster |
| 5 (earlier) | unified EIP-6780 refund at top frame | most of cancun cluster |
| 6 (earlier) | int64 executionStateGas | enables fix #5's negative offset |
| 7 (prior session) | nullify versionedWrites[AddressPath] on createObjectChange.revert | ~130 file-level failures across forks |
| 8 (this session) | EIP-161 empty-account filter at ALL frames in walk | frontier under-charges (constant_gas, scenarios, value_transfer); tangerine_whistle eip150 selfdestruct cluster (16); byzantium eip214 staticcall_nested_call_to_precompile; shanghai warm_coinbase; constantinople eip1052; prague system contract under-charges (3 files) |
| 9 (this session) | EIP-8037 cleared-slot rule in walk + signed walkTotal/childTotal + propagate only positive walkTotal | cancun create_oog_from_eoa_refunds (6 receipt-hash); cancun selfdestruct receipt-hash family (3); amsterdam sstore_restoration_charge_in_ancestor (6 receipt-hash) |
| 10 (this session) | clamp underflow when gasRemaining > initialGas (state-gas credits) | enables fix #9 receipt computation; recursive selfdestruct receipt mismatches |
| 11 (this session) | resetObjectChange charges +112 when `prev.original.Empty()` | enables 0xff..fe address creation when system call's TouchAccount-then-prune sequence preceded |
| 12 (this session) | getStateObject treats EIP-161-empty versionMap accounts as non-existent | selfdestruct_to_system_address |

Result: 146 → **0 file-level failures** (100% reduction). All 16,389 subtests pass.

### Fixes 8–12 (this session) — root causes and rationale

**Fix #8: EIP-161 empty-account filter at ALL frames** (`IntraBlockState.ComputeFrameStateBytes` `liveAccount`)

The fix #1 empty-account skip was guarded by `applyFilter` (top frame only). EELS's `compute_state_byte_diff` applies the filter at every frame: `account_now != None` uses EIP-161 "exists" semantics (empty accounts are None). Removing the `applyFilter &&` guard fixes:

- STATICCALL to a non-existent address: `evm.call(STATICCALL, …, addr=c0f6dc, …)` does `AddBalance(addr, 0)` to trigger a touch — emits `createObjectChange{c0f6dc}` followed by `touchAccount{c0f6dc}` BEFORE the inner frame's `frameStart`. At the inner frame's commit (depth=1), the walk previously charged +112 for the empty `c0f6dc` (filter off at sub-frames), forcing a state-gas OOG that reverted the parent's effects (e.g., the SSTORE 0→1 that the test was verifying). With the filter at all frames, `c0f6dc` is correctly skipped, the inner frame doesn't OOG, and the SSTORE survives. Fixed `frontier/opcodes/value_transfer_gas_calculation` and the entire frontier/tangerine/shanghai/constantinople/prague-system-contract cluster.

**Fix #9: EIP-8037 cleared-slot rule + signed walkTotal/childTotal + positive-only propagation** (`IntraBlockState.ComputeFrameStateBytes` storageChange handler; `EVM.committedChildBytes`/`chargeFrameStateGas`/`propagateChildBytes`)

EELS's `compute_state_byte_diff` returns a SIGNED byte_delta — positive for new state, negative for cleared state (`value_now == 0 && frame_entry != 0 && tx_entry == 0` → −32). Erigon's walk previously returned uint64 and only emitted +32 for new slots; the cleared-slot −32 case wasn't recognised. Three coordinated changes:

1. `ComputeFrameStateBytes` returns `int64` total. The storageChange handler now also emits `total -= 32` when `originalValue == 0 && prevalue != 0 && current == 0` (the cleared-slot rule). Because `seenSlot` dedups to the FIRST entry per (addr, key) in the segment, `e.prevalue` is the slot's value at frame-entry (for sub-frames) or tx-entry (for top frames).

2. `EVM.committedChildBytes` and `propagateChildBytes` use signed `int64` to carry the negative walkTotal up. `chargeFrameStateGas` does signed delta math: `delta = walkTotal − childTotal − accountRefund`; `delta > 0` charges, `delta < 0` credits.

3. `propagateChildBytes` propagates ONLY positive walkTotals to the parent. EELS's `apply_frame_state_gas` updates `state_gas_used` only on positive `this_call_cost`; negative path credits the reservoir but leaves `state_gas_used` unchanged. Mirroring this in Erigon: a child with a negative walkTotal credits its own gas.State (via the `delta < 0` path) and the credit propagates to the parent through the gas return mechanism, but the parent's `committedChildBytes` (= EELS `already_paid`) stays at 0 for that child. Otherwise the parent over-charges by the absolute value of the child's credit.

Crucial subtlety inside `chargeFrameStateGas` for the credit path: `executionStateGas` is decremented by the `accountRefund` portion of the credit (capped by `creditGas`), NOT by the full credit. The cleared-slot portion of the credit goes to the reservoir (state_gas_reservoir in EELS terms) but doesn't decrement the per-tx `executionStateGas` counter. EIP-6780 same-tx-CREATE+SELFDESTRUCT of an account that EXISTED at tx-entry needs `executionStateGas` to go negative to offset intrinsic_state — that path is preserved via the accountRefund decrement.

Together these fixed `cancun/create/create_oog_from_eoa_refunds` (6 sub-tests, all `sstore_callcode/sstore_delegatecall × no_oog/oog_*` variants), the `cancun/eip6780_selfdestruct/recursive_contract_creation_and_selfdestruct` and `recreate_self_destructed_contract_different_txs` receipt-hash failures, and `amsterdam/eip8037 sstore_restoration_charge_in_ancestor` (6 CALLCODE/DELEGATECALL variants).

**Fix #10: clamp `txnGasUsedB4Refunds` underflow when state-gas credits exceed consumption** (`TxnExecutor` Apply and Execute paths)

`txnGasUsedB4Refunds = initialGas.Total() - gasRemaining.Total()` underflowed when EIP-6780 same-tx-CREATE+SELFDESTRUCT credits (and now cleared-slot credits) made `gasRemaining.State` exceed `initialGas.State`. The receipt's `cumulativeGasUsed` ended up as `2^64 - small_number`. Clamp to 0 when remaining exceeds initial; the downstream `max(FloorGasCost, …)` ensures a valid receipt gas value. Applied at both ApplyMessage paths in `txn_executor.go`.

**Fix #11: resetObjectChange charges +112 when `prev.original.Empty()`**

The walk's resetObjectChange handler previously skipped the +112 charge always, on the rationale that `resetObjectChange` corresponds to a previously-existing account. But `previous` may be a stateObject for an account that was effectively non-existent at tx-entry — e.g., a system address that an earlier system call's TouchAccount materialized as an empty stateObject, then EIP-161-pruned at the system call's FinalizeTx. From EELS's `compute_state_byte_diff` perspective such accounts have `existed_at_tx_entry = False` and qualify for +112 on creation. `prev.original.Empty()` is the correct signal: `original` is preserved across multiple resets and reflects the state at the very first creation, so an empty `original` indicates the account didn't exist when this stateObject's lineage began.

**Fix #12: getStateObject treats EIP-161-empty versionMap accounts as non-existent**

In the parallel-executor path, `getStateObject` calls `getVersionedAccount` to read the address from the versionMap. If a system call earlier in the same block had touched the address (creating an empty stateObject that survives into the versionMap), `getVersionedAccount` returns a non-nil empty Account. Previously Erigon called `stateObjectForAccount(addr, account)` to materialise it as a stateObject WITHOUT firing any journal entry, then `GetOrNewStateObject` saw a non-deleted stateObject and skipped `createObject`. The walk never saw a `createObjectChange` for an address that EELS counts as freshly-created at the frame end, missing +112×CPSB.

Fix: in `getStateObject`, when `getVersionedAccount` returns a non-nil but EIP-161-empty Account, cache it in `nilAccounts` and return nil. Subsequent accesses see the cached nil; `AddBalance(addr, non_zero)` flows through `GetOrNewStateObject → createObject(addr, nil)` → fires `createObjectChange`. Fixes `amsterdam/eip7708 selfdestruct_to_system_address` (the test's SELFDESTRUCT transfers value to 0xff…fe, which prior block-init system calls had emptied; with the fix Erigon properly counts +112 for the system address re-creation, matching EELS's expected `block_state_gas_used = 131,488`).

## Found test challenges

After implementing the plan and running `TestExecutionSpecBlockchainDevnet` from `execution/tests/eest_devnet/`, **1,845 of 15,429 subtests fail** (~12%). The implementation is consistent with EIP-8037 PR 11573, but the EEST fixtures pre-date PR 11573 and were generated against the old opcode-time state-gas-charging semantics. (Initial run before adding the revert-time state-gas restoration was 1,857; the restoration fix moved 12 tests from FAIL to PASS.)

The fixture submodule at `execution/tests/execution-spec-tests` is pinned to commit `6c2af7fc95d1c0aa781898b1a7ad78769a536d7f` ("add snolbal-devnet-4 fixtures with static cpsb"), which is from **before** PR 11573's "fixed CPSB + frame accounting" rewrite. As a result the fixtures expect:

- State-gas charges to fire at the SSTORE/CREATE/CALL-to-empty opcodes inline, spilling into `gas_left` when `state_gas_reservoir == 0`.
- Receipt gas to subtract the `stateGasConsumed` on top-level revert/halt (effectively refunding the spilled regular gas).

Our implementation (per PR 11573):
- Charges state gas only at frame commit via journal walk.
- On top-level revert/halt, the commit-time charge never fires → state gas = 0; remaining regular gas is fully burned.

The math on a representative failure confirms the divergence comes from this single source.

### Pattern verification: `dupn_stack_underflow.json`

Test path: `for_amsterdam/amsterdam/eip8024_dupn_swapn_exchange/dupn/dupn_stack_underflow.json` → `test_dupn_stack_underflow[fork_Amsterdam-blockchain_test_from_state_test-dupn_underflow_imm_0]`.

Tx gas limit: 1,000,000. Bytecode: `PUSH1 1 PUSH1 0 SSTORE` then DUPN underflow (exceptional halt at top frame).

| Quantity | Old EIP-8037 (fixture) | PR 11573 (our impl) |
|---|---|---|
| SSTORE 0→1 regular gas | 5,000 | 5,000 |
| SSTORE 0→1 state gas | 32 × 1,174 = 37,568 (charged inline, spills to `gas_left`) | 0 (deferred to frame commit) |
| Frame commit fires? | n/a (charged inline) | No — exceptional halt aborts |
| Remaining gas at halt | 979,000 − 5,000 − 37,568 = 936,432 | 979,000 − 5,000 = 974,000 |
| Burned on halt | 936,432 | 974,000 |
| `regularGasConsumed` | 5,000 + 936,432 = 941,432 | 5,000 + 974,000 = 979,000 |
| Receipt subtracts `stateGasConsumed`? | 37,568 (yes) | 0 (no) |
| Block `gas_used` (header) | 21,000 + 941,432 = **962,432** | 21,000 + 979,000 = **1,000,000** |

Difference: exactly 37,568 = 32 × CPSB (one slot-set worth of state gas).

### Failing test categories

All five failing categories live under `for_amsterdam/amsterdam/`:

| Category | Representative failing tests |
|---|---|
| `eip8024_dupn_swapn_exchange` (DUPN/SWAPN/EXCHANGE) | `dupn/dupn_stack_underflow.json::test_dupn_stack_underflow[*]` (all 6 imm variants) |
| `eip7954_increase_max_contract_size` (max code size) | `max_code_size/max_code_size_deposit_gas.json::test_max_code_size_deposit_gas[short_one_gas]` |
| `eip7928_block_level_access_lists` (BAL — block-level access lists) | `block_access_lists_opcodes/bal_create_oog_code_deposit.json`; `bal_create_contract_init_revert.json`; `bal_create2_collision.json`; `bal_create_and_oog.json[CREATE/CREATE2 × oog_before/after_target_access]`; `block_access_lists/bal_net_zero_balance_transfer.json[zero_balance_zero_transfer_selfdestruct]`; `bal_nonexistent_account_access_read_only.json[staticcall]`; `bal_aborted_storage_access.json[invalid]`; `bal_precompile_call.json[0x01..0x100]` |
| `eip7708_eth_transfer_logs` (ETH transfer logs) | `transfer_logs/zero_value_operations_no_log.json[selfdestruct]`; `transfer_logs/selfdestruct_to_system_address.json`; `transfer_logs/failed_create_with_value_no_log.json[initcode_invalid]`; `transfer_logs/create_collision_no_log.json[CREATE/CREATE2]`; `transfer_logs/create_out_of_gas_no_log.json[create_out_of_gas_code_deposit]` |
| `eip8037_state_creation_gas_cost_increase` (the EIP we refactored) | broad coverage; many subtests across the EIP-8037 fixture set |

Common failure mode across all categories: the fixture's expected `gasUsed` reflects the OLD EIP-8037 state-gas spillover behaviour; under our PR-11573-aligned model, `gasUsed` is higher by some multiple of 32 × CPSB or 112 × CPSB depending on what state-gas charges the test exercises.

### Other test signals

- `make lint` — clean.
- `make test-short` — passes for all packages.
- `make erigon` and `make integration` — both build clean.
- Pre-Amsterdam fork tests in `TestExecutionSpecBlockchain` (and the per-fork `*Cancun*` / `*Prague*` / `*Osaka*` variants) — pass; no divergence outside Amsterdam.

### Recommended next step

Per the `erigon-implement-eip` skill ("question the tests — do not silently fix them"): the EEST fixtures need to be regenerated against the PR-11573-aligned python-spec implementation. Until that lands upstream, the 1,845 Amsterdam-EIP fixture failures are expected and should be documented in the PR description rather than worked around in the implementation.

If a sooner check is needed, options are:
1. Wait for upstream EEST regeneration against PR 11573 and re-pin the submodule.
2. Hand-craft per-test expected values in a local override layer (fragile and high-maintenance).
3. Hold the implementation behind a temporary fork-rules feature flag while both spec lines stabilise (defers the divergence rather than resolving it).

## Edge cases (worked traces)

The "originalValue (tx-entry) + current-value-at-commit" rule, combined with the per-frame `delta = walk − committedChildBytes` charge/credit, handles these without any opcode-level refund plumbing. All scenarios assume EIP-8037 (`IsAmsterdam`) and `cpsb = cost_per_state_byte`. Since `storageChange.originalValue` is the slot's value at tx start (captured via `GetCommittedState`), pre-S=X scenarios short-circuit to 0 bytes immediately — the slot was already non-zero at tx start, so no rule can identify it as "new state".

### Storage-slot scenarios

For each scenario: `S` is a single storage slot on some address. Pre-tx-committed value is stated. Frame nesting is given as `T → F` (T calls F as child). Siblings means the parent calls them in sequence.

| # | Scenario | Per-frame charges/credits | Net executionStateGas |
|---|---|---|---|
| 1 | Pre-S=0; F: 0→X→0 (same frame) | F.walk: originalValue=0, curr=0 → 0; F.delta=0 | 0 |
| 2 | Pre-S=0; T → F1 (0→X) → F2 (X→0) | F2: originalValue=0, curr=0 → 0; F1.walk: originalValue=0, curr=0 → 0; T: 0 | 0 |
| 3 | Pre-S=X; T → F1 (X→0) → F2 (0→Y) | originalValue=X non-zero everywhere → 0 bytes at every frame; no charges or credits | 0 |
| 4 | Pre-S=0; T → F1 (0→X) → F2 (X→Y) | F2.walk: originalValue=0, curr=Y → +32 charge (deepest observer); F1.walk: originalValue=0, curr=Y → +32, committedChildBytes=32, delta=0; T: same, delta=0 | 32 (charged at F2, matches spec) |
| 5 | Pre-S=X; T → F1 (X→Y) → F2 (Y→Z) | originalValue=X non-zero → 0 everywhere | 0 |
| 6 | Pre-S=0; T → F1 (0→X) (siblings) F2 (X→0) F3 (0→Y) | F1: originalValue=0, curr=X → +32 charge; F2: originalValue=0, curr=0 → 0; F3: originalValue=0, curr=Y → +32 charge; T.committedChildBytes=64; T.walk: originalValue=0, curr=Y → +32; delta=32−64=−32 credit | 32 |
| 7 | Pre-S=X; T → F1 (X→0) → F2 (0→Y) → F3 (Y→Z) | originalValue=X non-zero → 0 everywhere | 0 |
| 8 | Pre-S=0; F1 (0→X), F2 sibling reverts after starting an SSTORE | F2 reverts → its journal entry gone; F1: originalValue=0, curr=X → +32 charge; T: same, delta=32−32=0 | 32 |
| 9 | Pre-S=X; T → DELEGATECALL F1 (X→0) → DELEGATECALL F2 (0→0)¹ | F2: noop (no journal entry²); F1: originalValue=X, curr=0 → 0; T: same → 0 | 0 |
| 10 | Pre-S=0; T → DELEGATECALL chain F1→F2→F3, each does 0→X then X→0 across the chain | originalValue=0, curr at each frame's commit reflects intermediate state; net 0 cancels at top | 0 |
| 11 | Pre-S=0; T → A_1 (0→X) → A_2 child (X→0) → A_3 grandchild (0→Y) | A_3.walk: originalValue=0, curr=Y → +32 charge (deepest observer); A_2.walk: originalValue=0, curr=Y → +32, committedChildBytes=32, delta=0; A_1.walk: originalValue=0, curr=Y → +32, committedChildBytes=32, delta=0; T: same, delta=0 | 32 (charged once at A_3, matches spec) |
| 12 | Pre-S=0; same as #11 but A_3 reverts | A_3 journal entries gone; A_2/A_1: originalValue=0, curr=0 (S reverted to 0) → 0; T: 0 | 0 |
| 13 | Pre-S=X; T → A_1 (X→0) → A_2 (0→Y) → A_3 (Y→0) | originalValue=X non-zero → 0 everywhere | 0 (slot deletes back to 0; no new state) |
| 14 | Pre-S=0; T calls F1 (0→X), F2 (X→0), F3 (0→Y) as **sequential siblings** | F1: +32 charge; F2: originalValue=0, curr=0 → 0; F3: +32 charge; T.committedChildBytes=64; T.walk: originalValue=0, curr=Y → +32; delta=32−64=−32 credit | 32 (slot ends at Y from 0) |
| 15 | Pre-S=0; SSTOREs by deepest frames under **different parents**: T→F1→F1a (0→X); T→F2→F2a (X→0); T→F3→F3a (0→Y) | F1a: +32 charge; F2a: 0; F3a: +32 charge; F1/F2/F3 walks see same originalValue=0/curr → +32/0/+32, but committedChildBytes covers them so delta=0/0/0; T.committedChildBytes=64; T.walk: originalValue=0, curr=Y → +32; delta=−32 credit | 32 |
| 16 | Pre-S=0; **mixed nesting + siblings**: T→F1 (F1 SSTORE 0→X then F1→F1a does X→0; F1 commits); T→F2 (does 0→Y) | F1a: originalValue=0, curr=0 → 0; F1.walk: originalValue=0, curr=0 (F1a cleared) → 0, delta=0; F2: +32 charge; T: originalValue=0, curr=Y → +32, delta=32−32=0 | 32 |
| 17 | Pre-S=0; **sibling cancellation + parent direct write**: T→F1 (0→X), T→F2 (X→0), T→F3 (0→Y), then T directly SSTORE Y→Z | F1+32, F2:0, F3+32; T.walk: originalValue=0, curr=Z → +32; T.committedChildBytes=64; delta=−32 credit | 32 (slot ends at Z from 0) |
| 18 | Pre-S=0; **same-tree across siblings**: T→F1 (which has F1a doing 0→X and F1b doing X→0 as siblings under F1); T→F2 (does 0→Y) | F1a: +32 charge; F1b: 0; F1.walk: originalValue=0, curr=0 → 0, delta=0−32=−32 credit; F2: +32 charge; T: originalValue=0, curr=Y → +32, delta=32−(0+32)=0 | 32 |

#### Case A: pre-set slot ending cleared (pre-S=X, final=0). All net **0** state gas — slot is being deleted, no new state.

With `originalValue=X` (non-zero) on every storageChange entry for S, the new-slot rule never triggers (it requires `originalValue.IsZero()`). Every frame's walk yields 0 bytes for S. No charges, no credits, no per-frame delta. Total: **0** for all variants below.

| # | Scenario | Trace |
|---|---|---|
| 19 | Pre-S=X; **sequential siblings**: T→F1 (X→0), F2 (0→Y), F3 (Y→0) | originalValue=X non-zero → 0 everywhere |
| 20 | Pre-S=X; **different parents**: T→F1→F1a (X→0); T→F2→F2a (0→Y); T→F3→F3a (Y→0) | originalValue=X non-zero → 0 everywhere |
| 21 | Pre-S=X; **mixed nesting+siblings**: T→F1 (F1: X→0 then F1→F1a does 0→Y; F1 commits); T→F2 SSTORE Y→0 | originalValue=X non-zero → 0 everywhere |
| 22 | Pre-S=X; **same-tree across siblings**: T→F1 (which has F1a:X→0 and F1b:0→Y); T→F2 (Y→0) | originalValue=X non-zero → 0 everywhere |

#### Case B: pre-set slot ending still-set (pre-S=X, final=Y non-zero). All net **0** state gas — slot was already non-zero, no new state.

Same short-circuit as Case A: `originalValue=X` non-zero never triggers the new-slot rule.

| # | Scenario | Trace |
|---|---|---|
| 23 | Pre-S=X; **3-deep nested**: T→A_1 (X→Y') →A_2 child (Y'→Z') →A_3 grandchild (Z'→Y) | originalValue=X non-zero → 0 everywhere |
| 24 | Pre-S=X; **sequential siblings**: T→F1 (X→Y'), F2 (Y'→0), F3 (0→Y) | originalValue=X non-zero → 0 everywhere |
| 25 | Pre-S=X; **different parents**: T→F1→F1a (X→Y'); T→F2→F2a (Y'→0); T→F3→F3a (0→Y) | originalValue=X non-zero → 0 everywhere |
| 26 | Pre-S=X; **mixed nesting+siblings**: T→F1 (F1: X→Y' then F1→F1a does Y'→Z'; F1 commits); T→F2 SSTORE Z'→Y | originalValue=X non-zero → 0 everywhere |
| 27 | Pre-S=X; **same-tree across siblings**: T→F1 (F1a:X→Y', F1b:Y'→0); T→F2 (0→Y) | originalValue=X non-zero → 0 everywhere |

¹ DELEGATECALL/CALLCODE write to caller's storage, so `storageChange.account` keys to the storage owner — same dedup rules apply.
² A 0→0 SSTORE is a no-op and emits no journal entry (existing IBS behavior).

### Account-creation / SELFDESTRUCT scenarios

| # | Scenario | Per-frame charges/credits | Net executionStateGas |
|---|---|---|---|
| 28 | F creates account X with no code, no slots; F commits | F.walk: createObjectChange{X} → +112 charge; T (filter): X exists, not selfdestructed → +112, delta=0 | 112 |
| 29 | F creates X with code length L and N non-zero slots; F commits | F.walk: 112 + L + 32N charge; T (filter): same, delta=0 | 112 + L + 32N |
| 30 | F creates X then SELFDESTRUCTs X (same frame) | F.walk (no filter): 112+L+32N charge; T.walk (filter applied, X is `.newlyCreated && .selfdestructed`) → 0; delta=−(112+L+32N) credit | 0 |
| 31 | T → F1 creates X; T → F2 (sibling) SELFDESTRUCTs X | F1 charges 112 (or 112+L+32N); F2: 0; T (filter): X skipped → 0; delta=−(112+...) credit | 0 |
| 32 | T → F1 creates X; T → F1 then SELFDESTRUCTs X (creator destroys) | F1.walk (no filter): 112+... charge; T (filter): X skipped → 0, credit | 0 |
| 33 | T → F1 → F2 creates X; F1 SELFDESTRUCTs X (parent destroys child's creation) | F2 charges 112+...; F1.walk (no filter): 112+... (its own segment includes F2's createObjectChange), delta=112+...−(112+...)=0; T (filter): 0, delta=−(112+...) credit | 0 |
| 34 | Multiple SELFDESTRUCT of same newly-created X (e.g. F1 creates X; F2, F3, F4 each SELFDESTRUCT X) | F1 charges 112+...; F2/F3/F4: 0 each; T.walk (filter): X skipped exactly once (per-address dedup); credit fires once | 0 |

#### Cross-frame CREATE+SELFDESTRUCT variants (mirror of storage cases #11–#18)

| # | Scenario | Per-frame charges/credits | Net |
|---|---|---|---|
| 35 | **3-deep nested**: T→F1→F1a creates X; F1a→F1ab SELFDESTRUCTs X | F1ab: selfdestructChange (skip) → 0; F1a.walk: createObjectChange (no filter), selfdestructChange (skip) → +112 charge; F1.walk: same → +112, delta=112−112=0; T.committedChildBytes=112; T (filter): X skipped → 0; delta=−112 credit | 0 |
| 36 | **Different parents**: T→F1→F1a creates X; T→F2→F2a SELFDESTRUCTs X | F1a: +112 charge; F1.walk: +112, delta=0; F2a: 0; F2: 0; T.committedChildBytes=112; T (filter): X skipped → 0; delta=−112 credit | 0 |
| 37 | **Mixed nesting+siblings**: T→F1 (F1 creates X then F1→F1a SELFDESTRUCTs X; F1 commits); T→F2 (does something else not touching X) | F1a: 0; F1.walk: createObjectChange (its own), selfdestructChange (F1a's) → +112 charge; F2: 0; T (filter): X skipped → 0; T.delta=−112 credit | 0 |
| 38 | **Same-tree across siblings**: T→F1 (F1a creates X, F1b SELFDESTRUCTs X as siblings under F1); T→F2 (no-op for X) | F1a: +112 charge; F1b: 0; F1.walk: createObjectChange (F1a's), selfdestructChange (F1b's) → +112, delta=112−112=0; F2: 0; T (filter): X skipped → 0; delta=−112 credit | 0 |
| 39 | **Creator nested, destroyer at top-level sibling**: T→F1→F1a creates X; F1 returns to T; T→F2 SELFDESTRUCTs X | F1a: +112 charge; F1.walk: +112, delta=0, totalForParent=112; F2: 0; T.committedChildBytes=112; T (filter): X skipped → 0; delta=−112 credit | 0 |
| 40 | **Spread destroys**: T→F1 creates X; T→F2 SELFDESTRUCTs X; T→F3 SELFDESTRUCTs X again | F1: +112 charge; F2: 0; F3: 0 (selfdestructChange{prev=true} just skipped like all selfdestructChange); T.committedChildBytes=112; T (filter): X skipped once by per-address dedup → 0; delta=−112 credit | 0 |

#### CREATE/CALL boundary scenarios

| # | Scenario | Per-frame charges/credits | Net executionStateGas |
|---|---|---|---|
| 41 | CREATE silent failure (collision / depth / balance / nonce overflow) at depth ≥ 1 | Frame's snapshot pushed and immediately popped on failure → no journal entries → walks see nothing | 0 |
| 42 | Code-deposit OOG: initcode runs to completion, but `len(code) × cpsb + Keccak256WordGas × ⌈L/32⌉` exceeds remaining gas | `SetCode` not called → no `codeChange` in journal; `createObjectChange` is in journal but EIP-3541/etc. handling will revert via `handleFrameRevert` if applicable. If the create-frame still commits with `code = []` (Homestead pre-fork rules): walk counts +112 for the account but NOT len(code) since current code is empty | 112 (no code charge) |
| 43 | Top-level CREATE tx, contract address C: intrinsic charges 112×cpsb for C; execution then runs initcode and deploys L bytes | Top-frame walk uses `excludeCreate=C` so createObjectChange{C} skipped; codeChange{C} counts L bytes; intrinsic + execution = 112 + L | 112×cpsb (intrinsic) + L×cpsb (execution) |
| 44 | Frame mid-execution OOG (regular gas exhausted before frame commit) | Frame reverts via existing path; no commit-time walk fires → no charge | 0 (for that subtree) |
| 45 | Top-level revert (tx OOG at top frame, or top REVERT) | `evm.executionStateGas = 0` set on top-level revert path; tx-state-gas = intrinsic only | 0 (intrinsic only) |
| 46 | SystemAddress sys-call (e.g. EIP-2935 history-storage update) | SysCallContract initialises `gasRemaining = {Regular: 30_000_000, State: 32 × CPSB × 16 = 601_088}` per the SYSTEM_CALL_GAS_LIMIT formula. The frame-commit walk fires (gated on `IsAmsterdam && !RestoreState` only — no special flag) and the per-frame state-gas charge draws from the dedicated reservoir (covers up to 16 fresh storage writes; e.g. 16-slot history buffer for EIP-2935). System calls do not contribute to `block_regular_gas_used` or `block_state_gas_used` — TxnExecutor is not involved, so the EVM's accumulated `executionStateGas`/`regularGasConsumed` are simply discarded after the call returns. | not contributing to block gas; sys call's own reservoir covers up to `SYSTEM_MAX_SSTORES_PER_CALL` slot writes |

### Mixed-dimension scenarios

| # | Scenario | Behavior |
|---|---|---|
| 47 | EIP-7702 auth list at tx start; auth target later does normal SSTORE inside main frame | Auth processing runs before top-frame `Call()` snapshot → auth's codeChange/nonceChange are at journal indices < `frameStart`; top-frame walk doesn't see them. Intrinsic_state_gas already charges 23×cpsb per auth. Main-frame SSTORE counted as usual. **No double-count.** |
| 48 | CALL to non-existent account, value=0 (post-Spurious Dragon) | EIP-161: short-circuit, account not materialized → no journal entry → 0 bytes |
| 49 | CALL to non-existent account, value>0 | Inside `evm.call()` the snapshot is pushed FIRST, then `CreateAccount(addr, false)` fires → `createObjectChange` lands in the **called** frame's segment. The called frame's walk picks up +112. Matches spec (deepest committed observer charges). |
| 50 | SELFDESTRUCT to non-existent beneficiary with value transfer | `AddBalance(beneficiary, balance)` runs inside the destroying frame's body → `createObjectChange` in that frame's segment → frame's walk attributes +112 to the destroying frame. Matches spec. |

## Files to Modify

### `execution/state/intra_block_state.go`
- Add `JournalLength() int` returning `len(sdb.journal.entries)`.
- Add `ComputeFrameStateBytes(start, end int, applyFilter bool, excludeCreate accounts.Address) uint64` — implements the walk above.
- **Delete** `SameTxSelfDestructedNewAccounts()` and the `SelfDestructedNewAccount` struct (lines 1441–1473) — no longer needed; the filter at top-frame walk subsumes it.

### `execution/state/journal.go`
- **Add `originalValue uint256.Int` field to `storageChange` struct.** This is the slot's value at tx start (captured via `GetCommittedState` when the SSTORE happens). Used by `ComputeFrameStateBytes` to determine whether a write creates new state per spec.
- Same field on `fakeStorageChange` (debug path); production walk skips fakeStorageChange anyway, so not strictly required.
- Update `storageChange.revert(...)` — no behavior change; originalValue is just stored, not used in revert.
- Add a typed switch helper `(*journal) walkSegment(start, end int, fn func(idx int, e journalEntry))` if useful for keeping the walk logic in one place. Optional polish.

### `execution/state/state_object.go` (and/or `intra_block_state.go`)
- In `stateObject.SetState(key, value)` (the journal-emitting path): when appending `storageChange{...}`, populate `originalValue` from `sdb.GetCommittedState(addr, key)`. Erigon's IBS already caches committed values via `originStorage`, so this lookup is O(1) after first hit per `(addr, key)` per tx.
- The existing journal-emit site is in `intra_block_state.go` around line 268 (`sdb.journal.append(storageChange{...})`). Add the lookup there.

### `execution/protocol/params/protocol.go`
- Add `SystemMaxSstoresPerCall = 16` (per [PR 11573 commit `d2a0230`](https://github.com/ethereum/EIPs/pull/11573/changes/d2a023056187fb17c94e9477cadd076a0f817760)) — upper bound on fresh storage slots a single system call writes.
- Add a helper `SystemCallGasLimit(cpsb uint64) uint64` returning `30_000_000 + StateBytesPerStorageSlot × cpsb × SystemMaxSstoresPerCall` (or use the existing `SysCallGasLimit = 30_000_000` and a separate `SystemCallStateReservoir(cpsb uint64) uint64 = 32 × cpsb × SystemMaxSstoresPerCall` to keep the regular and state portions clearly separated).

### `execution/vm/evm.go`
- Add to `EVM` struct:
  - `executionStateGas uint64` (replaces `stateGasConsumed` for block accounting).
  - `committedChildBytes []uint64` (per-depth stack of running children-bytes accumulators).
- **No `chargeStateGas` flag.** Under PR 11573 commit `d2a0230`, system calls also charge state gas via the journal walk (with a pre-sized reservoir, see `SysCallContract` below). The walk's gate is just `IsAmsterdam && !RestoreState`. Block-accounting exemption for system calls is automatic because they don't go through `TxnExecutor`.
- **Remove**: `stateGasConsumed`, `revertedSpillGas`, `stateGasRefund` fields and their accessors. Remove `CreditStateGasRefund(...)` and `RefundTxStateGas(...)` methods entirely.
- In `evm.call()` (CALL/CALLCODE/DELEGATECALL/STATICCALL) and `evm.create()`:
  - At entry: capture `frameStart := ibs.JournalLength()`; push `0` onto `committedChildBytes`.
  - On success commit (`IsAmsterdam && !RestoreState`): compute `frameEnd`, walk via `ComputeFrameStateBytes(frameStart, frameEnd, depth==0, excludeAddr)`. Compute `delta = int64(walkTotal) - int64(committedChildBytes[depth])`. If positive, charge from `gas.State` (with spill); on insufficient set `err = ErrOutOfGas`. If negative, credit `gas.State` and decrement `executionStateGas`. Pop self; if `depth > 0`, add `walkTotal` to `committedChildBytes[depth-1]`.
  - On revert/halt: pop self; do not propagate to parent. Top-level revert (`depth==0 && err != nil`) sets `evm.executionStateGas = 0`.
- Strip `handleFrameRevert`'s state-gas branches (lines 246–299). Keep only `RevertToSnapshot` and the regular-gas burn on exceptional halt.
- In `evm.create()`:
  - Remove the inline `useMdGas(... stateGas := len(ret) * cpsb ...)` at line 685–687 — `codeChange` from `SetCode` is picked up by the walk.
  - Simplify the `preDepositGas`/`preDepositStateGasConsumed` rollback to just `gasRemaining = preDepositGas` (regular only).
  - Drop `savedStateGasConsumed`, `savedStateGasRefund`, `initialChildState` locals (and matching ones in `evm.call()`).

### `execution/vm/operations_acl.go`
- `makeGasSStoreFunc`:
  - Line 76–80 (create slot): drop the `State: 32 * cpsb` from the Amsterdam branch — return regular only.
  - Line 100–105 (X→0 reset): drop `evm.CreditStateGasRefund(callContext, 32 * cpsb)`. Keep `AddRefund(...)` for regular-gas refund counter.
- `makeSelfdestructGasFn` (lines 295–301): drop the Amsterdam state-gas branch.

### `execution/vm/gas_table.go`
- `statefulGasCall` empty-account 112×cpsb branch (around lines 514–518): drop. The `createObjectChange` from `AddBalance` to a non-existent account is picked up by the walk.

### `execution/vm/instructions.go`
- `execCreate` (lines 1024–1086): delete the `accountStateGas` pre-deduction block (1027–1038). Delete the `CreditStateGasRefund` on failure (1076–1078).
- `opSelfdestruct6780` (lines 1341–1385): no state-gas changes needed in the opcode itself; the gas table changes above are sufficient.
- `opCall`/`opCallCode`/`opDelegateCall`/`opStaticCall`: no signature changes. The CallStipend regular-gas correction at lines 1126–1128 / 1177–1179 stays as-is.

### `execution/vm/interpreter.go`
- `useMdGas` for `mdgas.StateGas`: make the spillover-into-`Regular` path **transactional**. If neither `State` alone nor `State + spilled-Regular` can cover the requested amount, return `(initial, false)` **without mutating** `initial.State` (don't pre-zero it). This is required so that the revert-time restoration in `evm.call()`/`evm.create()` (which adds `committedChildBytes × cpsb` back to `gas.State`) reflects the correct pre-charge state. Without this fix, a failed-charge attempt would silently lose `State` gas during the half-applied deduction, causing the parent's reservoir to be under-restored on the subsequent revert.

### `execution/protocol/txn_executor.go`
- No need to communicate the top-level contract address — `evm.create()` derives it from `depth == 0` and the local `address` parameter at commit time.
- No need to set a `chargeStateGas` flag — the walk is gated on `IsAmsterdam && !RestoreState` only.
- In the refund/finalize block (around lines 608–697 area):
  - Replace `evm.StateGasConsumed()` with `evm.ExecutionStateGas()`.
  - Drop `evm.RevertedSpillGas()` from the receipt-gas formula.
  - **Delete the `SameTxSelfDestructedNewAccounts`-driven refund block entirely** — handled inside the EVM via the top-frame credit.
- `block_state_gas_used = intrinsic_state_gas + evm.ExecutionStateGas()`.
- `block_regular_gas_used = intrinsic_regular_gas + evm.regularGasConsumed`.
- `ApplyFrame` (RIP-7560 path): same treatment, ensure `executionStateGas` and `committedChildBytes` are reset between AA passes.

### `execution/protocol/block_exec.go` (`SysCallContract` / `SysCallContractWithBlockContext`)
- No flag to set — the walk fires automatically on Amsterdam (gated only on `IsAmsterdam && !RestoreState`). What changes is the gas split:
- Initialise `mdGas` with the new SYSTEM_CALL_GAS_LIMIT split:
  ```go
  cpsb := blockContext.CostPerStateByte
  mdGas := mdgas.MdGas{
      Regular: params.SysCallGasLimit,                          // 30_000_000
      State:   32 * cpsb * params.SystemMaxSstoresPerCall,      // dedicated reservoir
  }
  ```
  This matches `SYSTEM_CALL_GAS_LIMIT = 30_000_000 + STATE_BYTES_PER_STORAGE_SET × CPSB × SYSTEM_MAX_SSTORES_PER_CALL` from PR 11573 commit `d2a0230`. With CPSB=1174 the reservoir is `32 × 1174 × 16 = 601_088` state gas, enough to cover up to 16 fresh storage writes per call (sufficient for EIP-2935 history-buffer updates, EIP-4788 beacon-root, EIP-7002 / EIP-7251 system-contract operations).
- For pre-Amsterdam forks the `State: 0` initialisation stays (no state-gas dimension before EIP-8037). Gate the reservoir initialisation on `chainConfig.Rules(blockContext...)` having `IsAmsterdam`.
- System calls remain not subject to `TX_MAX_GAS_LIMIT`, do not count against the block gas limit, and do not contribute to either `block_regular_gas_used` or `block_state_gas_used` — automatic in our model because `SysCallContract` does not invoke `TxnExecutor`, so there is no add-step for the EVM's `executionStateGas`/`regularGasConsumed`. After the call returns, the EVM is discarded and its counters are dropped.

### `execution/tracing/hooks.go`
- Add `GasChangeFrameStateGas` to the `GasChangeReason` enum.
- Regenerate `gen_gas_change_reason_stringer.go` (`go generate ./execution/tracing/`).
- The frame-commit `useMdGas` call passes this reason. Credits emit a positive `OnGasChange(gas.State, gas.State+credit, reason)` event.

### Removed code paths (tracking)
- `evm.CreditStateGasRefund`, `evm.RefundTxStateGas`, `evm.stateGasRefund`, `evm.revertedSpillGas`, `evm.stateGasConsumed`, `evm.StateGasConsumed`, `evm.RevertedSpillGas`.
- All Amsterdam state-gas charge paths in `gas_table.go`, `operations_acl.go`, `instructions.go`.
- The `savedStateGas*` save/restore plumbing in `evm.call()`/`evm.create()` and `handleFrameRevert`.
- `IntraBlockState.SameTxSelfDestructedNewAccounts()` and `SelfDestructedNewAccount` struct.
- `evm.chargeStateGas` field and `evm.SetChargeStateGas` setter — the walk is gated solely on `IsAmsterdam && !RestoreState`.

## Critical files
- `execution/state/intra_block_state.go`
- `execution/state/journal.go`
- `execution/vm/evm.go`
- `execution/vm/instructions.go`
- `execution/vm/operations_acl.go`
- `execution/vm/gas_table.go`
- `execution/protocol/txn_executor.go`
- `execution/tracing/hooks.go`

## Verification

### Unit tests to add/update
1. `execution/protocol/misc/eip8037_test.go` — reactivate; add cases for:
   - Single-frame CREATE tx: intrinsic 112×cpsb only; execution = 0.
   - CREATE with code deposit: intrinsic 112×cpsb + execution `len(code)*cpsb`.
   - CREATE that fails (collision, balance, OOG, REVERT): intrinsic only.
   - SSTORE 0→X (same frame): execution +32×cpsb.
   - SSTORE 0→X→0 same frame: execution = 0 (single-frame walk handles).
   - SSTORE 0→X→0 across DELEGATECALL chain (1, 2, 3 hops): execution = 0 via top-frame negative-delta credit.
   - SSTORE 0→X→0 across sibling frames: same — top-frame credit.
   - CREATE-then-SELFDESTRUCT same frame: F charges 112+code+slots, T's filtered walk yields negative delta → credit; net 0.
   - Cross-frame CREATE-then-SELFDESTRUCT (sibling destroys): same — net 0.
   - Multiple SELFDESTRUCTs of same newly-created account: credit fires once (per-address dedup at top walk).
   - Frame-end OOG: positive delta exceeds reservoir+gas_left → frame reverts; tx-state-gas = intrinsic only.
   - SystemAddress sysCall (EIP-2935 history-buffer ring update writes 1 slot per block; EIP-4788 beacon-root similar): SysCallContract initialises reservoir = `32 × cpsb × 16 = 601_088`, walk fires (gated only on `IsAmsterdam && !RestoreState`) and charges state gas for the slot writes (well within the dedicated reservoir budget). Verify the call does not contribute to `block_*_gas_used`.
2. `execution/vm/gas_table_test.go` — drop assertions on state-gas charges for SSTORE/CALL/SELFDESTRUCT (now zero by design).
3. `execution/protocol/txn_executor_test.go` — keep gas-pool tests; add frame-end-OOG regression.
4. Tracer fixtures expecting per-opcode `GasChangeCallCodeStorage` for state-gas events must be updated to expect `GasChangeFrameStateGas` once per frame (potentially with a credit direction at parent commits).

### Pre-merge checklist
- `make lint` clean.
- `make test-short` passes.
- `make test-all` passes.

### Alignment with Mario's TODO test list

The page at https://notes.ethereum.org/@marioevz/eip-8037-todo-tests lists three test cases that the EIP-8037 implementation must satisfy. All three are handled correctly by the journal-walk algorithm without code changes:

1. **"TX spending all balance from an account should not refund account destroy refund"** — A pre-existing account whose entire balance is drained by a tx must NOT trigger a 112×cpsb refund (it was never charged in the first place). In our walk: only `createObjectChange`, `resetObjectChange`, `codeChange`, and `storageChange` contribute bytes. `balanceChange` entries are skipped, and the SELFDESTRUCT filter only fires on accounts with `.newlyCreated && .selfdestructed` — a pre-existing account drained to 0 does not match that predicate, so the top-frame walk does not skip it (and there's nothing to skip, since no `createObjectChange` was emitted for it). Net effect: 0 charge, 0 refund.

2. **"Contract creation -> return successfully -> gas to sstore"** — A top-level CREATE tx that deploys code AND writes a storage slot. Covered by edge case #43: the intrinsic charges 112×cpsb (account); the top-frame walk uses `excludeCreate=C` to skip the contract's `createObjectChange` (avoiding double-count); `codeChange` contributes `len(code)` bytes; `storageChange` contributes 32 bytes. Total state gas: `112×cpsb (intrinsic) + (len(code) + 32)×cpsb (execution)`.

3. **"Not enough gas to pay for account creation at the end of the tx, but still got refund from a self-destruct"** — A frame creates account A and then SELFDESTRUCTs A within the same tx; even if the test runs near OOG limits, the create + self-destruct pair must net to 0 state gas. Covered by edge cases #30, #31, #34, #35–40: the deepest creator frame charges 112+code+slots at its commit (no filter); the top-frame walk applies the filter, sees A is `.newlyCreated && .selfdestructed`, and skips it; this makes `top.walkTotal < top.committedChildBytes`, producing a negative delta that credits the over-charge back. Per-address dedup at the walk level ensures the credit fires exactly once even if A is SELFDESTRUCTed multiple times.

The four design questions on Mario's page are also resolved by the implementation:
- **Which frame is charged for account creation?** The frame in which `createObjectChange` lands. For `CALL` to non-existent address with value, that's the called frame (snapshot is pushed before `CreateAccount`). For top-level CREATE tx, the contract account is intrinsic-charged (with `excludeCreate` to prevent double-count). For nested `CREATE`, the create-frame charges via its own `createObjectChange`.
- **Code deposit charge frame?** Always the create-frame, because `codeChange` is emitted by the create-frame's `SetCode` call right before commit.
- **Double-charging via intrinsic_state_cost?** No — `excludeCreate` parameter to `ComputeFrameStateBytes` skips the top-level CREATE's contract account from the walk.

## Rollout
All changes are gated on `chainRules.IsAmsterdam` — pre-Amsterdam paths are untouched. Cutover is automatic at the fork-transition block, exercising both code paths in a single devnet run.
