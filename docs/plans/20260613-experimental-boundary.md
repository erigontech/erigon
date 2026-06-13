# Experimental boundary statement — snapshot-flow + Mode-B unwind

**Status**: pre-PR draft. Defines what is stable / committed surface vs
what is explicitly experimental and may change before production
adoption.

**Branch**: `feat/snapshot-flow-app-integration`

**Date**: 2026-06-13

## Why this statement

This branch ships a working implementation of in-process Mode-B
unwind (admin `debug_setHead` past the changeset window, into the
snapshot-trim + commitment-anchor + Caplin-restart path) and the
snapshot-flow component architecture that drives publication /
consumption of snapshot file sets.

The work has crossed the "actually works end-to-end" threshold —
multiple iterations of unwind soak (depths 5k → 60k) pass on
hoodi, with Caplin auto-reanchor after each setHead. But several
surface decisions encoded in the implementation are NOT intended
to be permanent commitments — they are experimental choices, made
to ship the working demo, that should be revisited and standardised
before production rollout.

This document marks the line between the two.

## What is STABLE (the committed surface)

The following are the load-bearing decisions of this change. They
have been validated end-to-end, they will not change post-PR
without a separate design-cycle, and downstream consumers can rely
on them.

### 1. `debug_setHead` semantics

- Returns success (`{}`) when the unwind completes through the
  full Mode-A or Mode-B path. Caller can treat success as
  durable.
- Returns a structured error (`-32000`) when a precondition fails:
  weak-subjectivity window, orphan inventory entries, missing
  Provider wiring. Errors carry actionable text — `restart erigon`
  or `delete files X, Y, Z` — so the operator can recover without
  reading source.
- The dispatch boundary between Mode-A (within-changeset) and
  Mode-B (past-changeset) is internal; callers do not select.
- Caplin restart on `flow.UnwindCompleted` is automatic; the EL
  publishes the event and the storage component routes it.

### 2. Inventory as ground truth

- Inventory is the authoritative source for "what files are
  available." Disk presence is reconciled to inventory at startup
  (`scanLocalBlockFilesAtStartup`); subsequent operations consult
  inventory, not disk.
- Provider operations that touch snapshot files (Unwind's
  snapshot-trim sub-op, orphan-precondition check, straddle
  rebuild) read state through the inventory abstraction.
- Local-only files appear in inventory; nothing is discovered via
  back-door disk scans during operation.

### 3. Mode-B unwind primitive

The architectural shape — `Provider.Unwind` decomposed into
`snapshot-trim` → `DB-reset` → `commitment-anchor` sub-ops, each
independently testable — is committed.

- Snapshot-trim handles aligned cuts (whole file removal) and
  non-aligned cuts (straddle file slice + leftover seed) with
  the chunk-aligned newTo boundary.
- DB-reset wipes block-data tables past the new head but
  preserves `kv.HeaderTD` (canonical lookup target for Caplin's
  next forward block).
- Post-unwind verifier checks the DB image is consistent with
  the new head, fails loudly with table-by-table diagnostics
  on mismatch.

### 4. Caplin in-process re-anchor

The `CaplinService` lifecycle — `Start` / `Stop` / `Restart` —
under a single mutex with cancel-then-spawn semantics. The
component's `onUnwindCompleted` handler triggers `Restart` so the
fresh Caplin checkpoint-syncs and forward-syncs after every
mode-B unwind. Established by the
`docs/plans/20260610-mode-b-cl-rewind-orchestrator-only.md` design
and verified by the live soak.

### 5. The pre-PR gate framework

Three explicit gates before production adoption:

1. **Unwind reliably working** — 5-iter soak at depths
   5k/10k/30k/60k/30k passes; kill-mid soak passes;
   fresh-sync-then-soak passes.
2. **Fork reliably working** — sibling soak harness exercises
   fork creation, divergence, and convergence.
3. **Producer client distribution** — multi-peer retest of the
   announcement chain.

Gates are checked, not assumed. The implementation is gated on
their passing.

## What is EXPERIMENTAL (subject to change)

These choices are present in the implementation as the most
practical way to ship a working demo, but they are NOT intended
as permanent commitments. They are flagged for review in
iteration 2, before production adoption.

### 1. File naming format

Today's filenames encode `(domain, fromStep, toStep, kind)`. Two
known changes are pending:

- **Step indices → txNum boundaries** (Proposal 1). Step
  indices are derived from txNum / stepSize, where stepSize is
  a chain-config constant. Step encoding locks files' identity
  to a chain config; txNum is canonical. This is a *functional*
  change: it changes what files mean, not just how they're
  named.

- **Content-addressed names** (Proposal 2). Adding a truncated
  SHA-256 Merkle root to filenames so wrong-bytes files get a
  wrong name. Layered on top of P1's canonical metadata.

Until P1+P2 land, current filenames should be treated as the
working format for the demo, not the production canonical form.

### 2. Manifest schema

`chain.toml` today mixes "what the chain is" (file set + hashes
+ fork timing) with "how to fetch files" (torrent infohashes,
webseed URLs). Proposal 3 splits these into a signed `def.toml`
plus an advisory `transports.toml`.

Until P3 lands, the current `chain.toml` schema is the working
demo format. Code that produces / consumes it should be
treated as a layer over a soon-to-change canonical form.

### 3. Transport mechanism

The current transport stack — BitTorrent v1 + HTTPS webseeds —
is the working implementation. The proposals call out a future
where:

- The content hash (P2) is transport-independent.
- The transport map (P3) is advisory.
- Untrusted transports (third-party IPFS, mirrors, sneakernet)
  become safe because integrity travels with the content.

The current code assumes the transports it knows about. P3's
split would change the trust model and decouple transports from
the chain definition.

### 4. The OCC parallel-exec interaction

The soak runs with `EXEC3_PARALLEL=false` as a working-around for
a known OCC race that produced a gas-mismatch on hoodi block
3,007,852 in earlier sessions. The OCC fix is tracked in a
separate work stream (memory pin "OCC FIX PR → 3.5 MILESTONE").
Production adoption of this branch is independent of the OCC
fix's resolution, but production soaks will need both fixes
present to validate against parallel-exec.

### 5. The setHead-via-`debug` RPC namespace

The current implementation hangs setHead off `debug_setHead`.
For production this should likely move to an `admin_` namespace
(per Geth's convention) or a dedicated `setHead_` namespace
(per the operation's specificity). The semantics stay the same;
the endpoint name is provisional.

### 6. Step-aligned block snapshot retire boundaries

`chunkAlignedToBlock` rounds down to the nearest 1000-block
boundary. The 1000-block alignment is the existing block-snapshot
retire cadence; it's load-bearing for the straddle path. P1's
txNum boundaries may surface a different alignment as the
canonical unit; whether 1000-block alignment survives the
transition is open.

## What stays the SAME under any iteration-2 outcome

Independent of how iteration-2 lands:

- Mode-B unwind's three-phase decomposition (snapshot-trim →
  DB-reset → commitment-anchor).
- The Inventory-as-ground-truth principle.
- The CL re-anchor primitive after every mode-B unwind.
- The pre-PR gate framework.
- The `debug_setHead` error contract (precondition refusal vs
  silent wedge).

## Sequencing summary

```
EXPERIMENTAL DEMO (this PR)
  ↓
Pre-PR gates pass (unwind + fork + multi-peer distribution)
  ↓
PR opens; proposal-doc references attached
  ↓
ITER 2 PROPOSAL CYCLE
  P1 (txNum boundaries) → P2 (content hashes) → P3 (chain def split)
  ↓
PRODUCTION ADOPTION
  - Iter-2 surface lands as the new canonical form
  - Demo-era surfaces (chain.toml grammar, step-indexed filenames,
    BT v1-only transport) get dual-encoded then retired
  - Federated history-network integration begins
  ↓
CORE ERIGON DEPLOYMENT
```

The boundary in this document is what separates "the experimental
demo PR is production-ready" from "production is ready to adopt."
The first requires the gates above; the second requires iter-2 to
land.
