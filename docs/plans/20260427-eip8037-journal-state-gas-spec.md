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

## Algorithm

At each call/create frame:

```
Frame entry (evm.call / evm.create):
    frameStart  = ibs.JournalLength()
    push 0 onto evm.committedChildBytes                  // per-depth accumulator

Frame body runs (regular gas charged inline as today; no state-gas charges).

Frame commit (success path, IsAmsterdam, chargeStateGas):
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
    RevertToSnapshot                                     // journal undoes everything
    burn regular gas on exceptional halt (same as today)
    pop evm.committedChildBytes[depth]                   // do NOT add to parent
    on top-level revert (depth==0, err != nil):
        evm.executionStateGas = 0

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
| 46 | SystemAddress sys-call (e.g. EIP-2935 history-storage update) | `evm.chargeStateGas = false` on sys-call paths → frame-commit walk skipped entirely | 0 (sys-call must always succeed) |

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

### `execution/vm/evm.go`
- Add to `EVM` struct:
  - `executionStateGas uint64` (replaces `stateGasConsumed` for block accounting).
  - `committedChildBytes []uint64` (per-depth stack of running children-bytes accumulators).
  - `chargeStateGas bool` (false during `SysCallContract` paths to avoid charging EIP-2935 / EIP-7002 system writes).
- **Remove**: `stateGasConsumed`, `revertedSpillGas`, `stateGasRefund` fields and their accessors. Remove `CreditStateGasRefund(...)` and `RefundTxStateGas(...)` methods entirely.
- In `evm.call()` (CALL/CALLCODE/DELEGATECALL/STATICCALL) and `evm.create()`:
  - At entry: capture `frameStart := ibs.JournalLength()`; push `0` onto `committedChildBytes`.
  - On success commit (Amsterdam, `chargeStateGas`): compute `frameEnd`, walk via `ComputeFrameStateBytes(frameStart, frameEnd, depth==0, excludeAddr)`. Compute `delta = int64(walkTotal) - int64(committedChildBytes[depth])`. If positive, charge from `gas.State` (with spill); on insufficient set `err = ErrOutOfGas`. If negative, credit `gas.State` and decrement `executionStateGas`. Pop self; if `depth > 0`, add `walkTotal` to `committedChildBytes[depth-1]`.
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

### `execution/protocol/txn_executor.go`
- Set `evm.chargeStateGas = true` in `Execute()` (default) and `false` in `SysCallContract` callers (search for `SysCallContract*` to enumerate sites).
- No need to communicate the top-level contract address — `evm.create()` derives it from `depth == 0` and the local `address` parameter at commit time.
- In the refund/finalize block (around lines 608–697 area):
  - Replace `evm.StateGasConsumed()` with `evm.ExecutionStateGas()`.
  - Drop `evm.RevertedSpillGas()` from the receipt-gas formula.
  - **Delete the `SameTxSelfDestructedNewAccounts`-driven refund block entirely** — handled inside the EVM via the top-frame credit.
- `block_state_gas_used = intrinsic_state_gas + evm.ExecutionStateGas()`.
- `block_regular_gas_used = intrinsic_regular_gas + evm.regularGasConsumed`.
- `ApplyFrame` (RIP-7560 path): same treatment, ensure `chargeStateGas`, `executionStateGas`, and `committedChildBytes` are reset between AA passes.

### `execution/tracing/hooks.go`
- Add `GasChangeFrameStateGas` to the `GasChangeReason` enum.
- Regenerate `gen_gas_change_reason_stringer.go` (`go generate ./execution/tracing/`).
- The frame-commit `useMdGas` call passes this reason. Credits emit a positive `OnGasChange(gas.State, gas.State+credit, reason)` event.

### Removed code paths (tracking)
- `evm.CreditStateGasRefund`, `evm.RefundTxStateGas`, `evm.stateGasRefund`, `evm.revertedSpillGas`, `evm.stateGasConsumed`, `evm.StateGasConsumed`, `evm.RevertedSpillGas`.
- All Amsterdam state-gas charge paths in `gas_table.go`, `operations_acl.go`, `instructions.go`.
- The `savedStateGas*` save/restore plumbing in `evm.call()`/`evm.create()` and `handleFrameRevert`.
- `IntraBlockState.SameTxSelfDestructedNewAccounts()` and `SelfDestructedNewAccount` struct.

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
   - SystemAddress sysCall (EIP-2935): no state-gas charging; sys call always succeeds regardless of journal mutations.
2. `execution/vm/gas_table_test.go` — drop assertions on state-gas charges for SSTORE/CALL/SELFDESTRUCT (now zero by design).
3. `execution/protocol/txn_executor_test.go` — keep gas-pool tests; add frame-end-OOG regression.
4. Tracer fixtures expecting per-opcode `GasChangeCallCodeStorage` for state-gas events must be updated to expect `GasChangeFrameStateGas` once per frame (potentially with a credit direction at parent commits).

### Pre-merge checklist
- `make lint` clean.
- `make test-short` passes.
- `make test-all` passes.

## Rollout
All changes are gated on `chainRules.IsAmsterdam` — pre-Amsterdam paths are untouched. Cutover is automatic at the fork-transition block, exercising both code paths in a single devnet run.
