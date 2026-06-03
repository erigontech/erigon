# Amsterdam block-import gas mismatches — Handoff

Investigation into the EEST `zkevm@v0.4.0` fixtures that fail **block import**
(not witness) on the Amsterdam fork with a gas-used mismatch. Hand off to a
fresh context.

Created: 2026-05-31. Do not commit this file.

## RESOLVED 2026-06-01

Root cause: EIP-8037 per-authorization AUTH_BASE state gas
(`STATE_BYTES_PER_AUTH_BASE 23 * CPSB 1530 = 35190`) was over-refunded in
`execution/protocol/txn_executor.go` `verifyAuthorities`. The refund condition
`hasDelegation || auth.Address == 0x0` forgave AUTH_BASE for *any* delegation
clear, including a no-op clear of an authority with no pre-existing delegation —
which must keep the charge. Erigon under-charged by one AUTH_BASE per
undelegated-clearing auth (`got = header - 35190`), or by `35190 - regular`
when the regular dimension then became binding (the Δ6690 rows).

The "feature-independent" framing below was a red herring: every failing row is
the `tx_type_4` (EIP-7702, 1 auth) parametrization of its fixture — beacon_root
and blobhash just have a tx_type_4 variant. delegated variants passed because
`hasDelegation=true` made the refund correct.

Fix: refund AUTH_BASE only when `hasDelegation`. Verified: full `for_amsterdam`
zkevm suite went from 12 distinct gas mismatches to **0**; remaining suite
failures are pre-existing witness mismatches (separate work). Landed on branch
`awskii/amsterdam-7702-authbase-state-gas` (off `main`).

## TL;DR

- ~9 fixtures in the `eest_zkevm_witness` suite fail at **chain import**, before
  any witness is built. Error shape:
  `block test failed: insertion into chain failed: ... code: BadBlock err:
  updateForkChoice: [4/6 Execution] invalid block, gas used by execution:
  <got>, in header: <want>`.
- Erigon **under-charges** gas: `got < want`. The dominant delta is a fixed
  **Δ35190**, and it is **feature-independent** — it appears identically in
  cancun `eip4788_beacon_root`, cancun `eip4844_blobs/blobhash*`, AND prague
  `eip7702_set_code_tx/*` fixtures. That points at a **block/tx-level Amsterdam
  gas charge that Erigon does not apply**, not a per-opcode or per-EIP bug.
- This is an **EVM/protocol gas-rule gap**, upstream of the witness code. The
  witness work (debug_executionWitness producer/verifier) is unrelated and
  cannot fix it — the block never finishes import, so the witness is never
  queried.
- `gas used` is identical under serial vs parallel/BAL execution (parallelism
  does not change gas), so this is **not** a BAL-executor artifact.

## The exact failures + gas deltas

Measured from the corpus run (`/tmp/eest-run-b4val.log`), distinct
(fixture, got, want) gas mismatches:

| Δ (want−got) | got | want | fixture |
|---:|---:|---:|---|
| 35190 | 379440 | 414630 | prague/eip7702_set_code_tx/set_code_txs/delegation_clearing |
| 35190 | 195840 | 231030 | prague/eip7702_set_code_tx/set_code_txs/delegation_clearing |
| 35190 | 316710 | 351900 | prague/eip7702_set_code_tx/set_code_txs/delegation_clearing_and_set |
| 35190 | 183600 | 218790 | prague/eip7702_set_code_tx/set_code_txs/delegation_clearing_tx_to |
| 35190 | 281520 | 316710 | prague/eip7702_set_code_tx/set_code_txs/valid_tx_invalid_auth_signature |
| 35190 | 133110 | 168300 | prague/eip7702_set_code_tx/set_code_txs_2/double_auth |
| 35190 | 183600 | 218790 | cancun/eip4788_beacon_root/beacon_root_contract/tx_to_beacon_root_contract |
| 35190 | 281520 | 316710 | cancun/eip4844_blobs/blobhash_opcode/blobhash_gas_cost |
| 35190 | 183600 | 218790 | cancun/eip4844_blobs/blobhash_opcode_contexts/blobhash_opcode_contexts_tx_types |
| 34260 | 36120 | 70380 | prague/eip7702_set_code_tx/set_code_txs_2/double_auth |
| 6690 | 28500 | 35190 | prague/eip7702_set_code_tx/set_code_txs/delegation_clearing_tx_to |
| 6690 | 28500 | 35190 | amsterdam/eip7928_block_level_access_lists/block_access_lists_eip7702/bal_7702_null_address_delegation_no_code_change |

Distinct deltas observed: **{6690, 34260, 35190}**. The recurring constant
**35190** shows up as both the dominant delta and as a header gas-used total in
the Δ6690 rows — worth treating as a candidate Amsterdam gas constant.

### Partial-pass within a fixture (important clue)

`delegation_clearing.json` has 4 parametrizations; **2 pass, 2 fail**:

- PASS: `delegated_account-not_self_sponsored`, `delegated_account-self_sponsored`
- FAIL: `undelegated_account-not_self_sponsored`, `undelegated_account-self_sponsored`

So it is **not** a flat per-block charge applied unconditionally (that would
fail all four). The missing charge is **conditional** on a code path the
`undelegated_account` clearing variants hit and the `delegated_account` ones do
not. This is the highest-signal lead for isolating the rule.

## What this is NOT (ruled out)

1. **Witness construction / `debug_executionWitness`.** The failure is at
   `RunWithTester` → `insertBlocks` → staged-sync execution stage `[4/6]`. The
   witness re-exec loop (`ExecutionWitness`, which uses `protocol.ApplyMessage`
   but skips the header gas-used check) is never reached.
2. **PR #21497 (BAL witness access-set).** Pure witness construction, no gas
   logic. Cannot make a non-importing block import. (Being closed as abandoned.)
3. **BAL / parallel executor.** Gas-used is independent of serial-vs-parallel;
   the EVM computes the same number either way. The suite runs with
   `ExperimentalBAL=true` but that does not change gas accounting.
4. **A wholesale "Amsterdam EVM not implemented".** Partial-pass within a single
   fixture rules this out — most of the path works; one conditional charge is
   missing.

## Leads (start here)

1. **Find the source of Δ35190.** It is feature-independent (beacon-root, blob,
   7702 all show it), so look for a per-tx or per-block Amsterdam gas component
   gated on `rules.IsAmsterdam` that is missing or mis-applied. Candidates to
   audit: intrinsic-gas computation for Amsterdam, any new Amsterdam system-call
   gas (EIP-2935/4788/7002/7251-style), EIP-7928 BAL-related charge, EIP-7918
   blob base-fee floor, EIP-7623 calldata floor interaction.
   - Decompose 35190: e.g. is it `k * 25000`/`12500` (EIP-7702 auth costs),
     `21000 + X`, or a fixed system charge? It is not an obvious multiple of the
     7702 PER_AUTH constants, which argues for a block/tx-level charge.
2. **Use the `undelegated` vs `delegated` split** in `delegation_clearing` to
   bisect: diff the two execution traces; the gas should diverge at exactly the
   step that applies the missing charge.
3. **`origin/debug_gas_mis` branch** (6 commits) already adds debugging
   scaffolding for exactly this: `dbg.TraceDynamicGas` SSTORE-gas prints in
   `execution/vm/operations_acl.go:makeGasSStoreFunc`, a `seg find-slot`
   subcommand, `TRACE_SLOT_ADDR/KEY` env, and a fork-choice-fail panic hook. Use
   it (or cherry-pick the tracing) to dump per-op dynamic gas on a failing
   fixture. NOTE: it is **instrumentation only — no fix**.
4. Main already has `if rules.IsAmsterdam { ... }` branches in
   `makeGasSStoreFunc` (`execution/vm/operations_acl.go`); confirm whether the
   missing charge belongs there or in intrinsic/tx-setup gas.

## How to reproduce

The failing fixtures import a single Amsterdam block and fail at stage `[4/6]`.

```bash
# Whole suite (the failures appear regardless of the witness work):
cd <worktree-with-suite>   # e.g. a checkout of the eest_zkevm_witness suite (#21487)
go test -count=1 -v -timeout 60m -run \
  'TestExecutionSpecWitness/for_amsterdam/prague/eip7702_set_code_tx/set_code_txs/delegation_clearing' \
  ./execution/tests/eest_zkevm_witness/... 2>&1 | tee /tmp/gas.log
grep "gas used by execution" /tmp/gas.log
```

Fixture corpus path:
`test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests/for_amsterdam/...`.
Decode a fixture's block/txs:

```bash
FX=test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests/for_amsterdam/prague/eip7702_set_code_tx/set_code_txs/delegation_clearing.json
jq -r 'to_entries[0].value.blocks[0].blockHeader.gasUsed' "$FX"
jq -r 'to_entries[0].value.blocks[0].transactions[] | {type, to, gas: .gasLimit, authList: (.authorizationList|length?)}' "$FX"
```

Note: the `eest_zkevm_witness` suite forces `ExperimentalBAL=true`
(`witness_test.go`), and EIP-7928 Amsterdam blocks carry a `blockAccessList`.
This does not affect gas accounting but is required for the runner.

## Key files

| Concern | Location |
|---|---|
| Intrinsic / tx-setup gas | `execution/protocol/txn_executor.go` (`IntrinsicGas*`, `InclusionContributions`, EIP-7702 auth handling ~line 758-814) |
| Dynamic opcode gas (SSTORE/ACL) | `execution/vm/operations_acl.go` (`makeGasSStoreFunc`, `IsAmsterdam` branch) |
| Gas table | `execution/vm/gas_table.go` |
| Amsterdam fork gate | `execution/chain/chain_config.go` (`IsAmsterdam`) |
| Block validation (gas-used check) | staged-sync execution stage `[4/6]`; error surfaces via `updateForkChoice` / `insertBlocks` |
| Debug tracing scaffolding | `origin/debug_gas_mis` branch (TraceDynamicGas, find-slot, panic hook) |

## Context / related

- Witness producer/verifier work (separate, mostly done at 2866/2881): PRs
  #21518/#21529 (merged), #21531/#21532/#21539/#21543 (open), plus a pending
  verifier (`VerifyWitness`) + suite consumer-mode. See
  `docs/handoffs/witness-eest-zkevm-investigation.md`.
- The 9 gas failures are the residual that the witness work cannot move; they
  gate on the Amsterdam EVM gas implementation.
- Memory note: Amsterdam EIPs (7708/7778/7843/7928) are being implemented in a
  separate worktree; this gas gap is likely part of that unfinished work.
- `awskii/exec-witness-mpt-amsterdam` branch is stale (behind main); not a
  source of gas fixes.
