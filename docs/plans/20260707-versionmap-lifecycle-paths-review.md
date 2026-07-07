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

**Internal inconsistency (concrete — a 2-vs-1, not three ways).** The two *readers* agree
exactly: `getVersionedAccount:987-1004` and `versionedStateReader.ReadAccountData:1352-1369`
both compute revived as `AddressPath >= destructTx` **OR** `{Balance,Nonce,CodeHash} > destructTx`.
The *validator* `validateReadImpl:907` computes it as `{Balance,Nonce,CodeHash} > destructTx`
only — it **omits the `AddressPath >=` arm**. The gap bites precisely in the same-tx
metamorphic SD+CREATE2 case: a revival that re-writes AddressPath (and CodeHash) at the
*same* txIdx as the destruct is caught by the readers' `AddressPath >=` but missed by the
validator's strict `>` (CodeHash written *at* destructTx fails `> destructTx`). Readers then
surface the revived account while the validator marks the read invalid → the divergence to
characterize (is it benign conservative re-exec, or a spurious-abort/correctness issue?).
This is the accretion — each arm added to fix one parallel-vs-serial bug in one path.

**Pinned (revival_consistency_test.go).** Characterization confirms the divergence is *not*
observable on any production history: a same-tx metamorphic SD+CREATE2 re-writes the read's
own field (Balance/CodeHash) at the revival tx, so a stale pre-destruct read is invalidated by
`checkVersion` before the revival arm matters — reader and validator agree. The verdicts only
diverge in a synthetic AddressPath-*only* revival (no field co-write), which `createObject`
never emits (it always co-writes CodeHash at 1664). So the validator's missing `AddressPath >=`
arm is safe **only because of that co-write** — a fragile, load-bearing invariant the
rationalization must keep (or make the AddressPath arm explicit at the validator).

**Two distinct kinds of redundancy** (don't conflate them):
1. *Lifecycle redundancy — traced, and the candidate cleared.* `CreateContractPath` looked
   redundant vs `IncarnationPath` in the *validation* path (it only appears in exclusion lists,
   versionmap.go:901, and `noValueRead`, versionmap.go:1050). But the trace (gap 3) shows it is
   **not** redundant: it carries a distinct signal consumed on the **apply** side —
   `rw_v3.go:206` uses `d.createContract` to `DomainDelPrefix(StorageDomain)`, clearing stale
   storage before re-creation (mirroring `Writer.CreateContract`), and it marks contract
   creation so a newly-deployed empty contract is not pruned (intra_block_state.go:1835). It is
   contract-specific (a plain account create does not set it), whereas `IncarnationPath` is the
   read-validation revival discriminator (validateReadImpl:950-958, distinct because
   BalancePath "overfires for every gas payer"). All four lifecycle paths are non-redundant.
2. *Code-family redundancy* — `CodePath`/`CodeHashPath`/`CodeSizePath` co-write as a trio
   (one code change bumps all three) but are kept separate as a **read-cost** optimization
   (answer EXTCODEHASH/EXTCODESIZE without loading bytes). This one is intentional and stays;
   the lever is unifying the *write* stamp, not removing a path. See the state-field section.

**Test coverage:** the individual scenarios are covered — versionmap_test.go,
versionedio_test.go, versioned_read_paths_test.go, parallel_fixes_test.go (path-level) +
the SD/recreate suite (state/database_test.go, tests/statedb_chain_test.go,
tests/blockchain_test.go). Three **gaps** — the invariants to pin *before* touching anything:
1. No single test pins the three revival sites (`getVersionedAccount:987`,
   `versionedStateReader:1352`, `validateReadImpl:907`) against one lifecycle verdict — in
   particular the readers-vs-validator `AddressPath >=` gap in same-tx metamorphic revival.
   The exact consistency the rationalization must not break. (First test to add.)
2. Code-trio value-vs-noValue split — **traced and pinned** (code_trio_validation_test.go):
   the divergence is reachable only for a StorageRead-sourced collision (not MapRead) and is
   benign (CodeHash tiebreaker keeps a still-matching cold read valid; Code/CodeSize are
   conservatively invalidated). It is an intentional read-cost optimization, not a bug.
3. `CreateContractPath` contribution — **traced and pinned** (create_contract_path_test.go):
   contract creation records it, a plain account creation does not. The trace concluded it is
   *not* redundant (distinct apply-side storage-clear role), so it is not a removal candidate;
   the test guards against a "fold into IncarnationPath" simplification dropping the signal.

**Direction:** replace the scattered independent-field cross-checks with **one authoritative
lifecycle resolver** (`account lifecycle @ txIndex → {exists, destroyedAt, recreatedAt}`)
that reads, `Empty`/`Exist`, and validation all consult with a single consistent revival
definition. That (a) removes the three-way inconsistency, and (b) removes the per-read
over-work (each read/Empty re-deriving lifecycle via ad-hoc probes → the profiled 25%/19%),
with the no-regression suite as the gate.

## The six state-field paths (enumeration — the rest of the picture)

Beyond the four lifecycle paths, the map carries six real state fields. Enumerating each
one's write sites, read function, validation class and revival participation exposes a
**second, different kind of redundancy** (the code trio) and a validation-class asymmetry
the lifecycle review must not disturb.

| path | writers (intra_block_state.go) | read fn | in-mem refresh | validation class | revival participant |
|---|---|---|---|---|---|
| `BalancePath` | AddBalance/SubBalance/SetBalance (906/930), transfer (1141/1157), SD-zero (1383), create (1853) — **every gas payer** | `readBalance` | `refreshBalance` | **value** (`liveBalance`/`eqUint256`) | yes (revival probe 907) |
| `NoncePath` | SetNonce (1173) | `readNonce` | `refreshNonce` | **value** (`liveNonce`/`eqUint64`) | yes (revival probe 907) |
| `CodePath` | SetCode trio (1231), finalise trio (2914) | `readCode` | `refreshCode` | **noValue** (status only) | no (excluded at 901) |
| `CodeHashPath` | SetCode trio (1232), finalise trio (2915), **create alone (1664)** | `readCodeHash` | `refreshCodeHash` | **value** (`liveCodeHash`/`eqCodeHash`) | yes (revival probe 907) |
| `CodeSizePath` | SetCode trio (1233), finalise trio (2916) | `readCodeSize` | — (none) | **noValue** (status only) | no |
| `StoragePath` | SetState (1293), SD/create clear (2014/2147) | `readState` | — (per-key) | **value** (`liveStorage`/`eqUint256`) | via AddressPath+SelfDestruct cross-validate (929-945) |

Findings from the state-field enumeration:

- **Code-family redundancy (a real, distinct redundancy).** `CodePath`, `CodeHashPath`,
  `CodeSizePath` are *always written as a trio* (1231-1233, 2914-2916) — one code change
  bumps all three. As lifecycle/versioning signals they are redundant (the hash alone
  identifies the code; size is derivable from bytes). They are kept separate purely as a
  **read-cost optimization**: EXTCODEHASH / EXTCODESIZE must answer without loading the full
  code bytes. So — unlike `CreateContractPath` — this redundancy is *intentional and load-
  bearing on the read side*; the rationalization should preserve the three read entry points
  but can unify how a *write* stamps them (one code-write → one lifecycle bump).
- **Validation-class asymmetry (traced, pinned in code_trio_validation_test.go).**
  `CodeHashPath` is a **value path** (tiebreaker) but `CodePath`/`CodeSizePath` are
  **noValueRead**. The split is observable **only for a StorageRead-sourced read** — a cold
  read that saw no VM entry at exec time but now collides with a concurrent Done flush. For a
  MapRead the tiebreaker is bypassed (validateReadImpl:894 uses `checkVersion` for every
  path), so map reads show no asymmetry. In the StorageRead collision (no BAL) a still-matching
  CodeHash read survives via the tiebreaker while the co-written Code/CodeSize reads invalidate.
  The divergence is **benign** — the hash is unchanged so the read is accurate; the version/
  status checks are conservative, never wrong — and it is exactly what lets an EXTCODEHASH-only
  tx skip a re-execution on such a collision. Not a bug: an intentional read-cost optimization.
- **`CodeHashPath` double role.** It is written both in the code trio *and* alone by
  `createObject` (1664) to invalidate a stale pre-create `GetCodeHash`. That second write is
  a lifecycle signal riding a state-field path — the one place a state field does lifecycle
  work, and a candidate to fold into the lifecycle resolver.
- **`refreshVersionedAccount` refreshes exactly {Balance, Nonce, Incarnation, CodeHash}**
  (1019/1039/1059/1079), each taking the max version. These are precisely the value/revival
  paths minus Storage. `Empty()` needs only Balance/Nonce/CodeHash; Incarnation is refreshed
  but unused by Empty — the profiled waste. CodeSize/Code/SelfDestruct/CreateContract are
  *not* in the whole-account refresh (they have their own read paths), confirming the refresh
  is a partial, Empty-oriented reconstruction, not a true whole-account load.

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
`perf_results/rebaseline-postmerge-21536-100M-serial.csv` (merged binary vs B0).

**Micro-bench caveat (corrected 2026-07-07):** `BenchmarkWarmExtCodeHash` (~471 ns/op, 408 B,
7 allocs) does **not** exercise `refreshVersionedAccount`. With only field cells and an empty
reader, `readAccount` returns nil and `Empty()` short-circuits on the absent path
(`getVersionedAccount` returns before the refresh). Reproducing the warm-refresh path in a
unit bench needs a populated reader/domains, not just versionMap cells — verified: writing
AddressPath alone still reads empty. **The 20× warm-extcodehash gap must be profiled/measured
on the benchmarkoor cell** (per the perf skill), not this micro-bench. The refresh/SD-probe
over-work is real in the cell but small in absolute isolation; a production change to the hot
OCC read path is not justified by the micro-bench and must be gated on a benchmarkoor A/B.
