# Fork Test Reshape Plan

**Date:** 2026-07-28
**Branch:** `feat/snapshot-flow-app-integration`
**Depends on:** the componentization + fork component landings this session
(commits `89175ee888` through `8b4731f289`).

---

## Context

The fork transition can now be driven two ways:

1. **In-process (no restart)** — `debug_setFork` RPC → `Ethereum.SetFork`
   (thin wrapper) → `fork.Controller.Transition`. Every captor is
   Stop/Reconfigure'd, chain.Config is swapped, captors Start again, the
   process continues on the target chain.
2. **Restart-between-transitions** — stop erigon, restart with
   `--chain=<target>`. This is the fallback when the in-process swap
   returns `RestartRequired=true`, and remains the shipped path when the
   operator wants a clean state.

Both are supported by the same `fork.Controller` + captor set. **Both
should have test coverage** — running only one hides regressions in the
other.

Existing fork test coverage (from earlier sessions) is uneven:
- Offline `snapshots fork-from` CLI has a determinism test
  (`scripts/fork-from-twice-diff.sh`).
- Multi-node fork scenarios exist
  (`p2p_twonode_samefork_test.go`, `p2p_full_replication_test.go`).
- Live E2E fork initiator + follower via `erigon-launch-hoodi-fork-child.sh`.
- No dedicated coverage for `debug_setFork` RPC transitions.
- No coverage for chained same-node transitions (parent → fork → parent).
- No fork soak driver analogous to the unwind soak.

## Non-goals

- Not aiming for a fully independent CI job yet — plan is the tests
  first, wire them into CI once each is stable.
- Not rewriting existing multi-node fork tests — they cover the P2P
  distribution path and stay as they are; the plan below adds
  complementary in-process + soak coverage.
- Not adding cross-family fork transitions (different beacon config) —
  the Caplin launch closure rebind is a same-family exemplar. Cross-
  family is deferred to when CaplinConfig recomputation is wired.

---

## Test tiers

The reshape is structured in four tiers. Each tier verifies a smaller
surface with cheaper iteration; regressions get caught at the lowest
tier that reproduces them.

| Tier | Surface | Iteration cost | Repro cost |
|---|---|---|---|
| 1 · Unit | `fork.Controller` against mock Runtime | seconds | trivial |
| 2 · Integration | `fork.Controller` against real captor set, in-process | ~30 s | moderate |
| 3 · E2E | `debug_setFork` RPC + `integration set_fork` CLI against live erigon | ~5–15 min | full-node reproduce |
| 4 · Soak | Chained transitions across many iterations, both models | ~30–120 min | needs seed replay |

Rule: tier N's failure never depends on tier N+1's setup. If a Controller
transition sequence bug can reproduce at tier 1, it must not require
tier 3 to catch.

---

## Tier 1 — Unit (mostly landed)

**Location:** [node/components/fork/controller_test.go](../../node/components/fork/controller_test.go)

Already covers:
- `Transition` rejects empty / self / unknown target chain
- `applyChainConfigSwap` happy path: Stop → Reconfigure → SetChainConfig
  → SwapChainConfig → ApplyPostSwapHooks → Start ordering
- Post-swap Start failure surfaces `RestartRequired=true`

Add:
1. **Reconfigure failure short-circuits** — a Reconfigurable that errors
   during phase 2 must abort the swap; the Restartables should already
   be Stopped (accepted state) and `RestartRequired=true` returned.
2. **Stop failure aborts before any swap** — a Restartable whose Stop
   errors must leave the runtime untouched; SwapChainConfig should not
   have fired.
3. **Post-swap-hooks-panic recovery** (nice-to-have) — assert that a
   panicking hook doesn't leak the process into a state with
   Restartables permanently stopped. Probably out of scope for the
   Controller itself; document as a Runtime responsibility.

Each of these is a small extension to `controller_test.go` using the
existing mockRestartable + mockReconfigurable + fakeRuntime scaffolding.

---

## Tier 2 — Integration (new)

**Location:** [node/components/fork/controller_integration_test.go](../../node/components/fork/controller_integration_test.go)
(new file)

Uses real `sentrycomp.Provider`, real `storagecomp.Provider`, real
`caplincomp.CaplinService`, real `txpool.TxPool`, real
`downloader.Provider`, real `manifestexchange.Provider` — but wired
into a minimal in-process Runtime the test constructs directly (no
full Ethereum backend, no networking).

Scenarios:

1. **Round-trip parent → fork → parent** on a synthetic chain pair.
   Two `chain.Config` instances share genesis; the second is marked
   `Parent="parent"` with a `CutBlock`. Test constructs a Runtime with
   all six captors, transitions to fork, transitions back to parent,
   asserts each captor's SetChainConfig fired both times.

2. **Sequence robustness** — pump several transitions in a row without
   sleeping between them. Assert no captor leaks running goroutines
   between swaps (check via `pprof`-lite: goroutine count stable).

3. **Reconfigure-failure recovery** — install a Reconfigurable that
   fails on target A but succeeds on target B. Assert:
   - After failed transition to A, `RestartRequired=true`.
   - Runtime chain.Config unchanged (validate via
     `CurrentChainConfig()`).
   - Subsequent transition to B succeeds.

Test scaffolding: a `newRealRuntime(t)` helper that wires up minimal
versions of each real captor with a temp datadir. Fake network deps
(no p2p listeners) so tests can run in parallel.

---

## Tier 3 — E2E (new)

Two sub-tiers, both against a live single-node erigon.

### 3a. RPC path — in-process transition

**Location:** [rpc/jsonrpc/debug_api_set_fork_e2e_test.go](../../rpc/jsonrpc/debug_api_set_fork_e2e_test.go) (new)

Follows the pattern of the existing `debug_api_set_head_e2e_test.go`.
Sets up a full backend, calls `debug_setFork(target)`, asserts:

- `RestartRequired=false` on success.
- Chain identity everywhere reflects target: `admin_nodeInfo`
  advertises new fork ID; `debug_chainConfig` returns target.
- Post-transition head is `CutBlock`.
- Subsequent block insertion via engine API succeeds on the new chain.

Scenarios:
1. **Parent → fork** on a synthetic parent/fork chain pair.
2. **Fork → parent** (reverse).
3. **Same-node repeat** — three sequential transitions on one node.
4. **Reject: target has no parent relationship** — asserts the RPC
   error message names both chains.
5. **Reject: current head below CutBlock** — the "no unwind needed"
   guard error.

### 3b. CLI path — integration set_fork against live erigon

**Location:** [scripts/fork-rpc-transition.sh](../../scripts/fork-rpc-transition.sh) (new)

A bash driver that:
- Boots erigon on a parent chain with pre-populated datadir.
- Waits for `debug_chainConfig` to report parent.
- Runs `./build/bin/integration set_fork --chain=<target>`.
- Parses the JSON result; asserts `restart_required=false`.
- Polls `debug_chainConfig` until it reports target.
- Continues to run a small workload (a few `engine_newPayload` calls
  via `trigger_fcu`?) to prove the node is live post-transition.

Success criteria: the script exits 0 and the erigon log shows no
component wedge messages.

### 3c-status (2026-07-29): partial — restart-fallback needs artifact parity with `snapshots fork-from`

Landed:

- `1f89878d11` — real step→block map in ValidateForkDatadir. Fork
  datadir validator no longer classifies known-pre-cut state files as
  straddle (~200 → 44 false positives).
- `73517e6183` — `TrimPostCutSiblings` post-swap hook. Removes
  accessor/history/idx files whose step range extends past cut+1.
  Standalone Tier 3c now boots cleanly.

Blocked:

- The debug_setFork RPC transition trims files correctly but does
  NOT emit the other artifacts the fork erigon expects on boot:
    - `cl-config.yaml` (Caplin config; `snapshots fork-from`
      derives it from parent's beacon config)
    - `parent-cut.json` sidecar (block-cut marker consumed by
      downstream tools)
    - Cleared txpool DB entries (parent-chain txns still in
      chaindata; fork boot logs a warning per rejected txn but
      doesn't fail)
- Tier 4 smoke iter with model=restart therefore fails at Phase 5
  (fork erigon boots + validates the datadir but the public HTTP
  RPC doesn't open — likely waiting on Caplin subsystem readiness
  that never fires without cl-config.yaml).

To close 3c end-to-end, debug_setFork's post-swap hooks must produce
the same artifact set `snapshots fork-from` produces:
`cmd/snapshots/forkfrom/forkfrom.go writeForkCLConfig` +
whatever parent-cut.json emitter lives in that path. A separate PR.
Until then, the shipped operator workflow for restart transitions
remains `snapshots fork-from` (documented in the fork-restart-
transition.sh header).

### 3c. Restart-between-transitions path

**Location:** [scripts/fork-restart-transition.sh](../../scripts/fork-restart-transition.sh) (new)

A bash driver that exercises the Phase 1 fallback deliberately:
- Boots erigon on parent, sync to a tip.
- Stop erigon cleanly (SIGTERM, wait for exit).
- Relaunch erigon with `--chain=<target>` against the same datadir.
- Assert the second erigon comes up + reports target as its chain.

This is the "restart-required" story for operators who prefer a
clean lifecycle over the in-process swap. Also serves as regression
coverage for the current shipped path (Phase 1).

---

## Tier 4 — Soak (new)

**Location:** [scripts/fork-soak-until-stopped.sh](../../scripts/fork-soak-until-stopped.sh) (new,
analogous to `scripts/soak-until-stopped.sh`)

Chained fork transitions on a single long-lived erigon. Randomizes:
- **Model:** RPC in-process vs. process-restart, per iteration.
- **Direction:** parent → fork or fork → parent, per iteration.
- **Depth of unwind before transition:** shallow / mid / deep, using
  the same regime classification as unwind soak.
- **Dwell time** between transitions (0 s to 5 min) so retire + merge
  cycles land at unpredictable points in the transition schedule.

Assertions per iteration (analogous to unwind soak Phase 5 disk-clean):
- Post-transition disk state has no orphan files for the pre-swap chain
  (no leftover `chain.parent.*.toml`, no dangling `.torrent` sidecars
  pointing at pre-cut infohashes).
- Post-transition head equals the CutBlock for that direction.
- No commitment-root mismatch errors in the log window between the
  transition RPC and the driver's next probe.

Stop-on-fail by default (matches `soak-until-stopped.sh` convention),
`KEEP_GOING=true` to continue for statistical soak runs.

Seed-replayable: script accepts `SEED=<hex>`; hashes it to drive
`math/rand` for all randomization choices; prints the seed on each
iteration so a failure can be re-run deterministically.

---

## Infrastructure needs

Adding this tier stack requires two pieces of infrastructure that
don't exist yet:

### `newRealRuntime(t)` test helper — Tier 2

A helper under `node/components/fork/testing_runtime.go` (build-tag
`testing` or plain, since it lives beside test files) that constructs
a Runtime with:
- Temp datadir with two synthetic chain specs (`parent`, `fork`) that
  share genesis and differ only in `ChainID` + `Parent`/`CutBlock`.
- All six captors wired via their real constructors, with test-shaped
  deps (in-memory KV, no p2p listeners, no torrent client).

Reusable by any future fork-related integration test.

### Fork-soak driver harness — Tier 4

The `fork-soak-until-stopped.sh` script needs:
- A running erigon on a preserved-datadir base state (analogous to
  unwind soak's `unwind-fresh-sync-then-soak.sh` Phase 3 gate).
- Randomized-choice utilities in bash (already exist in unwind soak;
  reuse the seed hashing + `math/rand` bridge).
- A way to programmatically restart erigon under the driver's control
  (needed for the "process-restart" model iterations). Existing
  `erigon-launch-*.sh` scripts assume operator-driven start; the
  fork-soak driver becomes the operator.

---

## Priority order

Suggested landing sequence — each depends on the ones above:

1. **Tier 1 extensions** — 3 additional Controller unit tests (Stop
   failure, Reconfigure failure, sequence). Small; low risk.
2. **Tier 2 integration test** — `newRealRuntime(t)` helper + the
   round-trip scenario. Establishes the "real captors in one process"
   pattern.
3. **Tier 3a RPC E2E** — one scenario (parent → fork), assert
   `RestartRequired=false` + chain identity swap.
4. **Tier 3b CLI script** — `fork-rpc-transition.sh`, exercises
   `integration set_fork` end-to-end.
5. **Tier 3c restart script** — `fork-restart-transition.sh`, the
   Phase 1 fallback path.
6. **Tier 4 soak driver** — chained transitions with randomization.

Steps 1–5 are the foundation the soak driver in step 6 relies on;
each of them is independently useful for CI or debugging even
without the soak on top.

---

## Non-plan (deferred)

- **Cross-family fork transitions** (different beacon config) —
  requires CaplinConfig recomputation logic that isn't wired.
- **Multi-node fork transitions via `debug_setFork`** — the existing
  multi-node fork scenarios cover the P2P distribution side; adding a
  cross-node in-process transition test is a follow-up when we need
  it.
- **CI integration** — each new script/test lands ready to run but
  isn't wired into workflow files until it's proven stable via manual
  runs.

---

## Rules

- Every new test / script goes through Red → Green → Refactor per the
  project TDD guidance.
- No `t.Skip` for a failing fork test — root-cause and fix, or hand
  off with the repro recipe.
- The soak driver's stop-on-fail default is non-negotiable — a silent
  failure in a long-running soak is the pattern that produced
  `#21153`.
- Comments in test files follow the same policy as production code:
  the WHY, not the WHAT.
