# versionedio as the single source for OCC + BAL; IBS → EVM context

Status: design. Branch `mh/ibs-stateobject-opt`. Prereqs landed this session:
the finalize refactor (10 commits, `WriteSet.Finalize`, EIP-6780 derived from the
write-set) — hive eest-devnet BAL **2572/0** — plus `2e91432843` (Exist/Empty
read cells, not cached `so.data`).

## North star (Mark)

**IBS holds no state other than the version map and the journal.** All
account/field state flows through the versionedio **cells** (reads + writes,
real + pseudo fields); the journal is the intra-tx revert backbone. Every other
account-state cache on IBS is a temporary to be folded away. Both the OCC
read-set and the EIP-7928 BAL derive from versionedio alone. Reads ultimately
return a cell. IBS is a thin EVM-context adapter.

### IBS field inventory (target)

Keep — the two pillars:
- `versionMap` + `versionedReads` + `versionedWrites` (the version-map interface).
- `journal` + `revisions` (revert backbone).

Keep — legitimate per-tx EVM-execution context (not account-state cache):
- `refund`, `txIndex`, `blockNum`, `version`, `logs`/`logSize`,
  `accessList` (EIP-2929), `transientStorage` (EIP-1153), `stateReader`,
  `trace`/`tracingHooks`, `dep`.

**Remove — account-state caches that duplicate the version map:**
- `stateObjects` / `stateObjectsDirty` — the record-level cache; read cells.
- `nilAccounts` — non-existence cache; the version map answers absence.
- `balanceInc` — deferred balance-increase map (versionMap path already carries
  it as `BalanceIncreaseSet`).
- `addressAccess` / `recordAccess` — the access map (a **temporary fix**); folds
  into an `AccessPath` pseudo-read.
- `sdProbe` / `sdProbeEpoch` — SD-probe memo over the version map.

`refreshVersionedAccount` exists only to keep `stateObjects`' `so.data` in sync;
it dies with the cache.

## Why the naive path failed (findings this session)

- `refreshVersionedAccount` is not just `so.data` cache-sync: the balance reads
  it generates are **non-internal**, so they land in the BAL (every accessed
  account's balance appears in its BAL entry). Removing it changes the EIP-7928
  read-set → only eest-devnet BAL validates it. See `AsBlockAccessList`
  (versionedio.go:2300, `updateReadBalance`); `internal` is set only by
  `MarkReadsInternal` (STATICCALL-rejection).
- The nonce write-slice stormed (OCC `too many validator-invalid retries`) not
  because its prev-read is spurious, but because the fast path recorded a
  **different field-set** than `refresh` (address+nonce vs
  balance+nonce+incarnation+codeHash) → OCC churn. `AddBalance` slice works
  because an add genuinely depends on prior balance (BalancePath read is real).

## Target read-field set for a touched account (Mark)

- **Incarnation: definitely NOT** a required BAL read — drop it.
- Nonce/codeHash: probably not required either.
- Whatever IS required is generated **algorithmically** at BAL-build time from
  the access-set + state, not as an execution-path side effect.

Confirm empirically before coding: read the EIP-7928 fixture BAL structure and
trace one storage-only-write account's expected entry to see exactly which
read-fields the reference emits.

## Components (each eest-devnet-BAL gated)

1. **Algorithmic BAL balance-read generation.** At `AsBlockAccessList`, for each
   accessed account, emit its balance read by looking the value up (versionMap /
   state at build time) instead of relying on `refresh` having recorded it.
   Decouples BAL-read generation from the OCC read path.
2. **Remove `refreshVersionedAccount`** (3 sites: getVersionedAccount:1074,
   getStateObject cached:1584, getStateObject stateReader-path:1675). Execution
   path goes lean (OCC records only genuine dependency reads; no incarnation).
   The three removals are mechanical and build. Two things must be handled with
   them:
   - **`SetCode` net-zero elision** (the hard part). It reads `so.data.CodeHash`
     (baseCodeHash) *and* `so.original.CodeHash` (matchesOriginal). Without
     refresh, BOTH are the domain-committed value and miss a prior-tx
     CodeHashPath-only write. baseCodeHash is easily cell-sourced
     (`GetCodeHash`), but `original` must become the **tx-start versionMap
     codehash**, not the domain original — see
     `TestSetCodeParallel_RevertToOriginalBug` (TX88 clears code, TX90 re-sets
     it; matchesOriginal wrongly fires against domain original A). This elision
     is **BAL-load-bearing**: `applyToCode` does NOT net-zero-fold code changes,
     so the write-time elision is what folds an EIP-7702 delegate-then-reset —
     dropping it changes the BAL. So `original` needs a correct tx-start cell
     source; verify against eest.
   - Update the 4 footprint tests (`TestApplyVersionedWrites_*GeneratesBalanceRead`,
     `TestVersionedRead_G4_RefreshRecordsTypedDefaultInReadSet`) — they assert
     the now-confirmed BAL-irrelevant balance reads; rewrite to the lean model.
3. **Write slices** (nonce/code/storage): drop stateObject materialization for
   existing-live accounts (mirror `writeBalanceVersioned`), prev via a
   **non-recording** read (versionedWrites-then-base). Now consistent with the
   lean materialized footprint.
4. **Access map → `AccessPath` pseudo-field.** Replace `addressAccess` /
   `MarkAddressAccess` / `AccessedAddresses` with an `AccessPath` pseudo-read in
   `versionedReads` carrying the non-revertable "real EVM access" bit (the thing
   reads/writes can't currently express, gating the SYSTEM_ADDRESS filter). Then
   `AsBlockAccessList` derives everything from reads/writes and the map drops.
5. **stateObject → cells.** Field getters/setters and `Exist`/`Empty` source
   cells; `so.data` is no longer a reconciled cache. `refresh` fully gone.

## Component D detail (Mark)

- **IBS StateWriter mechanism is fully removable.** Writes flow
  versionedio → `WriteSet.Apply` → SharedDomains; the `NoopWriter`-to-suppress-
  output-until-needed pattern is obsolete. The whole StateWriter surface off IBS
  is unused → delete it.
- **StateV3** is superseded by versionedio → SharedDomains processing; collapse it.
- **The reader stays, but as a pure adapter over SharedDomains** (needed for both
  latest and historic reads). Make it a thin pass-through, no logic.

## Execution notes

- Consensus-sensitive: gate **every** step with `make eest-devnet` (hive branch
  `devnets/bal/7`, fixtures `tests-bal@v7.3.2`) — the BAL suite is the only thing
  that catches read-set footprint regressions. ~40 min/iteration → design first.
- Commit each coherent sub-step build-clean for bisectability; full
  `execution/tests` + `state -race` + eest at the end of each component.
- The finalize work is already a complete, validated, shippable deliverable if
  this rework is deferred.
