# VersionMap account-lifecycle paths — end-to-end review

## Why this exists

Profiling the 20× `warm-extcodehash` outlier (the biggest peer gap) pinned the cost to
`Empty()` → `getVersionedAccount` → `refreshVersionedAccount` (a whole-account read +
4-field refresh, each field re-probing SelfDestruct under the versionMap RWMutex): 25%
refresh + 19% lock in that cell. The obvious "read the field per-field, skip the refresh"
spot-fix turned out **not** to be semantically free: a protective test showed `Empty()`
keys off **AddressPath** (does the record exist?), not the individual fields, so a naive
per-field `Empty()` changes behaviour in the field-write-without-AddressPath edge.

That coupling is the point. The parallel executor's versionMap carries **four synthetic
account-lifecycle paths** that were added incrementally to make parallel match serial:

| path | meaning | sole/primary writer | why added |
|---|---|---|---|
| `AddressPath` | record exists / was created (base account) | `createObject` | #19266 — record the *nil* read so OCC catches Tx0-creates / Tx1-assumed-absent |
| `SelfDestructPath` | destroyed | `Selfdestruct` | ordered before BalancePath so same-tx SD zeroes balance |
| `IncarnationPath` | re-created (incarnation bump) | Create / SD | precise "re-created" signal; BalancePath overfires for every gas payer |
| `CreateContractPath` | contract-create flag | createObject path | EIP-6780 / metamorphic CREATE2 |

vs. the real state fields: Balance, Nonce, Code, CodeHash, CodeSize, Storage.

These four grew one bug at a time ("serial worked, parallel didn't"). Now that parallel is
correct, the goal is to examine the lifecycle logic **for its own internal consistency** as
one model — not more spot fixes — **without introducing regressions**. The perf wins
(dropping the whole-account refresh, cheap per-field warm reads) should fall out of a
cleaner lifecycle model rather than being bolted on.

## End-to-end account-lifecycle flow (paths touched)

- **Create** (CREATE / first transfer-in): `createObject` writes `AddressPath` (base
  record) + `CodeHashPath` (invalidate stale pre-create GetCodeHash) + `IncarnationPath` /
  `CreateContractPath`; later setters write `BalancePath`/`NoncePath` as deltas.
- **Mutate existing** (balance transfer): reader = `AddressPath` base ⊕ field overlay
  (`refreshVersionedAccount`); writer records **only** the touched field path. No AddressPath.
- **Destroy**: writes `SelfDestructPath=true`, `IncarnationPath`, `BalancePath=0`. No
  AddressPath. Cross-tx readers see nil via the SelfDestructPath gate in `getVersionedAccount`.
- **Revive / metamorphic CREATE2**: re-writes `AddressPath` (same txIdx → `>=` revival
  check; sub-fields use strict `>`).
- **Read** (`GetBalance`/`Empty`/`Exist`): route through `AddressPath` first (existence +
  base), then field overlay (GetBalance) or `data.Empty()` on the reconstructed account.
  A nil AddressPath read is **recorded** so OCC can invalidate on a later create.

## Are the four paths redundant / ordered? (enumeration finding)

**They are designed as independent fields, but semantically encode one lifecycle.**
The `AddressEntry` invariant (versionmap.go:86-96) is explicit: *"no consumer treats
AddressEntry as a transactional whole… reads, writes, mark-estimate/complete, delete and
validation all operate at (Path, Key) granularity."* So at the design level the four
lifecycle paths are just four independent cells. But they jointly answer one question —
what is this account's lifecycle state (exists? destroyed-at? re-created-at?) — and that
interdependence is implemented as **scattered cross-checks**, not one model:

- `validateReadImpl` (versionmap.go:897-958): a sub-field read with no cell folds onto
  AddressPath; storage/property reads cross-validate against AddressPath **and**
  SelfDestructPath; a nil-AddressPath read additionally checks IncarnationPath.
- `getVersionedAccount` (intra_block_state.go:980-1008): SelfDestruct gate + revival.
- `versionedStateReader.ReadAccountData` (versionedio.go:1346-1373): SelfDestruct gate + revival.
- Enum order (versionmap.go:50-55): SelfDestructPath **before** BalancePath so `updateWrite`
  zeroes the same-tx balance — a load-bearing processing order.

**Internal inconsistency (concrete):** "revived" is computed three different ways —
`validateReadImpl` uses a `{Balance,Nonce,CodeHash}` write `> destructTxIndex`;
`getVersionedAccount` uses `AddressPath >= destructTxIndex` OR `{Balance,Nonce,CodeHash} >`;
`versionedStateReader` uses `AddressPath >=`. Three sites, three revival definitions
(`>=` vs `>`, with/without AddressPath). This is the accretion — each was added to fix one
parallel-vs-serial bug in one path.

**Redundancy candidate:** `CreateContractPath` appears mostly in exclusion lists
(versionmap.go:901) and `noValueRead` (versionmap.go:1050) with no clear distinct
contribution vs `IncarnationPath` (both signal "re-created"). `IncarnationPath` *is* load-
bearing and distinct (validateReadImpl:950-958 uses it precisely because BalancePath
"overfires for every gas payer"). AddressPath (existence) and SelfDestructPath (destruction)
are clearly non-redundant.

**Test coverage:** the individual scenarios are covered — versionmap_test.go,
versionedio_test.go, versioned_read_paths_test.go, parallel_fixes_test.go (path-level) +
the SD/recreate suite (state/database_test.go, tests/statedb_chain_test.go,
tests/blockchain_test.go). The **gap**: no single test pins that all three revival sites
agree on the same lifecycle verdict — which is exactly the consistency the rationalization
must not break, and the invariant to add before touching it.

**Direction:** replace the scattered independent-field cross-checks with **one authoritative
lifecycle resolver** (`account lifecycle @ txIndex → {exists, destroyedAt, recreatedAt}`)
that reads, `Empty`/`Exist`, and validation all consult with a single consistent revival
definition. That (a) removes the three-way inconsistency, and (b) removes the per-read
over-work (each read/Empty re-deriving lifecycle via ad-hoc probes → the profiled 25%/19%),
with the no-regression suite as the gate.

## Internal-consistency questions to resolve (the review)

1. **Existence vs re-creation vs create-flag overlap.** AddressPath (exists), IncarnationPath
   (re-created), CreateContractPath (contract-created) all signal facets of "the account came
   into being." Do all three carry independent information the validator/revival logic needs,
   or can the model be reduced? The `>=` (AddressPath) vs `>` (sub-fields) revival split is a
   known subtlety (#21590) — is it consistent across every reader (`getVersionedAccount`,
   `versionedStateReader.ReadAccountData`, `validateReadImpl`)?
2. **Is `refreshVersionedAccount` (whole-account reconstruction) necessary at all** if reads
   are per-field with AddressPath consulted only for existence? It exists to keep the
   record-level stateObject consistent with per-field cells — the "unnecessary per-tx cache"
   (Mark). A per-field read model that consults AddressPath *only* for the existence/empty
   decision would remove the refresh without the whole-account overlay.
3. **The recorded nil-AddressPath read** (#19266) is load-bearing for create/absent conflict
   detection. Any per-field `Empty()`/read rewrite must preserve it (the invariant documented
   at `intra_block_state.go:541-549`).
4. **SelfDestruct probe per read.** `versionedReadCore` re-probes `ReadSelfDestruct` (RWMutex)
   on every field read; it's a per-addr value stable within a tx. Once the lifecycle model is
   settled, this is memoizable per-addr per-tx (the 19% lock lever) — but only after the
   revival/`>=` interactions above are pinned.

## Approach

- **Understand → pin invariants as tests → rationalize → measure.** Before changing the
  lifecycle logic, capture the current cross-tx invariants (create/absent race, SD-zero,
  same-tx-SD-still-alive per EIP-6780, metamorphic revival) as focused tests using realistic
  AddressPath+field writes (two-IBS-sharing-a-versionMap), so any rationalization that changes
  behaviour fails loudly.
- **No regressions is the hard gate:** the SD/recreate/OCC suite (`TestGeneratedTraceApiCollision`,
  `TestRecreateAndRewind`, `TestSelfDestructReceive`, `TestDeleteRecreateSlotsAcrossManyBlocks`,
  the versionmap validation tests) + hive engine-api + eest-devnet(BAL) + the perf cells
  (`warm-extcodehash`, `*-contract`) re-measured on the harness.
- The perf targets (drop the whole-account refresh, cheap per-field reads, memoized SD probe)
  are the *outcome* of a consistent lifecycle model, validated against the above.

## Perf baseline (before any change)
`perf_results/rebaseline-postmerge-21536-100M-serial.csv` (merged binary vs B0). Micro-bench
`BenchmarkWarmExtCodeHash` = 471 ns/op, 408 B, 7 allocs (Empty()+GetCodeHash warm path).
