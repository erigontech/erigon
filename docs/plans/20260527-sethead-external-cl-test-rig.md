# SetHead mode B — external CL conformance test rig

Spec for Group F item 21 of the Phase 1 closing plan. Lands the
contract a future hive simulator (or local docker-compose rig)
must satisfy when exercising erigon's admin SetHead mode-B against
a real beacon node (Lighthouse / Prysm / Lodestar).

This file is the SPEC — the rig itself is not in the erigon repo.
The rig lives next to `/erigon/mark/hive/simulators/eth2/` where
the existing erigon + lighthouse-bn integrations run.

## Why this needs an external CL

The internal Engine API tests in `execution/engineapi/*_test.go`
exercise an in-process EngineAPI server. They cover the contract
shape but not the per-CL recovery behaviour. The mode-B design
calls out a real open question
(`docs/plans/20260525-admin-sethead-unwind-design.md` §
"CL forkchoice coordination"):

> "After mode B, the CL's forkchoice state is stale and the next
> Engine API call from CL → EL (FCU, NewPayload) will reference
> unknown hashes. … Does the CL recover naturally? … Does it loop
> / error / wedge?"

Answering this requires real CL processes — Lighthouse, Prysm,
Lodestar — each driving an FCU at a head past erigon's now-
unwound tip.

## Scenario flow

The rig must drive exactly this sequence:

1. **Bring up erigon + an external CL.** Erigon runs with
   `--snap.block-aligned-boundaries` so mode B is engagable.
   The CL is a fresh instance (its forkchoice store will track
   the chain's normal head as it executes forward).

2. **Execute blocks forward via the CL's normal FCU/NewPayload
   drive**, past at least two step boundaries so erigon's retire
   has produced commitment files at multiple step boundaries.

3. **Truncate ChangeSets3 below `target+2`** where `target` is
   the latest step-boundary block — lifts `CanUnwindToBlockNum`
   past target so SetHead dispatch picks mode B. (Production
   doesn't have this knob exposed; the rig achieves the same
   state via either: (a) test-only debug RPC; (b) running long
   enough that pruning naturally removes the diffsets; (c) a
   direct MDBX manipulation of the ChangeSets3 table between EL
   stop/restart.)

4. **Call `debug_setHead(target)`** via JSON-RPC against erigon.

5. **The CL's NEXT FCU then references blocks past the new tip.**
   The Engine API contract requires either:
   - `SYNCING` response from erigon if the CL's head isn't known
     locally — the CL then re-anchors / re-syncs forward.
   - `INVALID` with `latestValidHash` pointing at the new tip —
     the CL reorgs to that.

   What MUST NOT happen: erigon panics, wedges, or returns an
   undefined response that the CL doesn't know how to handle.

## Assertions

Per-CL subtest (Lighthouse, Prysm, Lodestar) must verify:

- `debug_setHead` returns nil (mode B completes end-to-end).
- The next FCU/NewPayload from the CL succeeds (either `SYNCING`
  or `INVALID`, never panic / undefined).
- Erigon's canonical head equals `target` after SetHead returns.
- Within a CL-specific recovery window (~5 minutes for
  Lighthouse, ~3 minutes for Prysm — both estimates pending
  measurement) the CL re-anchors and resumes forward execution
  from `target`.

## CL-specific notes

- **Lighthouse**: re-anchors via the existing checkpoint sync
  logic; accepts `SYNCING` responses from EL. Expected to recover
  without restart. Pending measurement: actual recovery window.

- **Prysm**: similar recovery; the open question per the design
  doc is whether the CL's already-running forkchoice state
  recovers mid-run vs requiring restart. Pending measurement:
  does the CL re-anchor automatically?

- **Lodestar**: TBD — no prior measurement against this scenario.

## Wiring outline

The rig should be a hive simulator structured like
`simulators/eth2/engine/`:

- `Dockerfile` for the simulator harness.
- Test scripts that orchestrate: erigon startup → CL startup →
  block execution → ChangeSets3 truncation → setHead → CL
  observation.
- Per-CL test matrix toggled by the simulator's client selection.

## Acceptance for closing item 21

- The rig is checked in under `simulators/eth2/sethead-mode-b/`
  (or equivalent path) outside this repo.
- The CI for that hive directory runs the three-CL matrix on every
  erigon main-branch update.
- Documented recovery windows + observed behaviour for each CL.
- A reference run posted to the test plan summarising findings.

The work belongs in the hive repository — this spec is the
contract.
