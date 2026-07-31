# Fork Test Scope + Start-Once Leak Inventory

**Date:** 2026-07-31
**Branch:** `feat/snapshot-flow-app-integration`
**Supersedes discussion scope of:** `20260728-fork-test-reshape.md` (that
plan defines Tiers 1-4; this one defines the *matrix* Tiers exercise and
the *discipline* by which we close it).

---

## Why this document

Fork testing is not "does the fork feature work". Fork is the acid test
for the whole snapshot-flow-app-integration change set because it is the
one operation that pushes erigon against its fundamental architectural
assumption: **start once, follow the chain.**

Every start-once leak in the codebase becomes observable when you switch
chains at runtime. Unwind, peer distribution, and chain.toml
publish/consume individually exercise ONE such assumption each; fork
exercises the intersection.

The other soaks in this stream (unwind, peer distribution) each cover a
single well-defined operation. Fork is intrinsically multi-dimensional
(multiple node roles × start conditions × transition models × directions
× chaining × trust configs × prune modes × timing). We can't close it in
one pass — we need a **staged approach** with different disciplines per
stage.

## Two-phase model

### Phase 1 — Well-defined narrow matrix, unwind-style discipline

Single-node, controller-orchestrated, matches what is already in tree.
Small enough to reason about; big enough to exercise every start-once
leak the current code has.

Discipline: **fix everything. Zero flakes, zero skips, zero documented
workarounds.** Same standard as the landed unwind soak. Phase 1 is the
substrate — if it's unstable, Phase 2 attribution is ambiguous and the
iteration cycle becomes unbounded.

### Phase 2 — Broader matrix, pragmatic discipline

Multi-node, cold-start followers, sibling forks, cross-fork traffic,
churn-during-transition, trust-cascade edge cases, partition injection.

Discipline: **fix OR document + error message + spec statement.** Each
Phase 2 leak triggers a per-case decision:
- Fix if straightforward and the cost is proportionate to user value.
- Document as a constraint (with clear operator-facing error text) if the
  fix would require architecturally invasive work not in scope.

The published spec becomes the user contract. Users get predictable
behaviour + actionable errors instead of hidden failure modes.

Phase 2 does **not** start until Phase 1 is locked.

---

## Phase 1 matrix

| Dimension | Phase 1 value |
|---|---|
| Node count | 1 |
| Roles | initiator only |
| Transition model | in-process (RPC) + restart |
| Direction | parent → fork + fork → parent |
| Chaining | 1 transition + N chained (Tier 4 soak) |
| Initial state | warm-pre-cut (already-synced parent datadir) |
| Trust config | happy-path UCAN only |
| Prune mode | `--prune.mode=minimal` |
| Churn during transition | none |

Phase 1 stability targets:
- Zero flakes (every test deterministic across N reruns).
- Hermetic (suite launches its own parent from clean state; no
  dependency on an operator-managed long-running process).
- Bounded wall-clock (iteration must be tractable).
- Seed-reproducible (same seed → same result).
- Green under load (passes even while unwind/peer soak run
  concurrently — the cross-soak goal).

Phase 1 coverage-to-tests map:

| Phase 1 dimension | Tests | Tier | Status |
|---|---|---|---|
| Controller orchestration (unit) | `controller_test.go` | 1 | pass |
| Controller with real captors | `controller_integration_test.go` | 2 | pass |
| Fork creation determinism | `scripts/fork-from-twice-diff.sh` | 3 | pass |
| Fork creation shape (Flavour 1) | `scripts/fork-from-fresh.sh` | 3 | pass |
| In-process transition (parent→fork) | `scripts/fork-rpc-transition.sh` | 3b | **FAIL (F2)** |
| Restart transition | `scripts/fork-restart-transition.sh` | 3c | **FAIL (F3)** |
| Chained transitions | `scripts/fork-soak-until-stopped.sh` | 4 | rpc-model only; empirical pending |
| Adjacent captor contract | `manifest_exchange` and adjacent test suites | 1 | **FAIL under load (F1)** |

## Open methodology decision — transaction load (spammer)

**Not phase-locked yet. Captured to avoid losing.**

Every fork test currently runs against an **empty tx stream** — the parent
chain progresses because Caplin delivers blocks, but nothing is
submitting transactions. That means:

- The txpool's fork transition is untested with real load. Pool state
  (pending nonces, sender caches, replacement rules, gas pricing) may
  fork correctly with 0 txs but fail with N txs — never observed.
- Block production during/after fork transition is untested with real
  content. Empty blocks succeed; blocks with transactions may hit
  ordering, pricing, or gas-limit issues fork-specific.
- Gas pricing coordination across the fork boundary is untested.

The end-user shape of fork is fork-with-transaction-content, not
fork-of-empty-chain. Reaching completeness requires exercising with
load.

Two ways to fit spammer into the phase model:

- **As Phase 3 (separate stage after Phase 2 lock):** dedicated live-load
  phase. Uses different discipline (statistical, not deterministic).
  Trade-off: keeps Phase 1/2 fast + deterministic, but delays discovery
  of load-related bugs.
- **As an iteration dimension inside Phase 1/2:** every scenario runs
  with/without spammer. Trade-off: earlier load exposure, but variability
  makes deterministic assertions harder and iteration wall-clock grows.

Candidates: `ethpandaops/spamoor` (rich pattern support), simple
tx-injection scripts, or a custom harness with predictable
tx-per-second rates.

Decision deferred. Whenever the decision is made, add spammer wiring +
per-scenario expected behaviour + assertion approach (deterministic if
Phase 1/2, statistical if Phase 3) here.

## Phase 2 matrix (deferred)

Documented here as the roadmap; not started until Phase 1 locked.

Additional dimensions:
- Node count: 2, 3, N with churn
- Roles: publisher, consumer, follower (cold-start), transient peer
- Initial state: cold (empty datadir), warm-post-cut, mid-transition,
  mid-fork-with-local-churn
- Fork tree: single, siblings, nested
- Cross-fork traffic
- Trust config: expired UCAN, missing UCAN, wrong-issuer UCAN, cascading
  UCANs, revoked UCAN
- Prune mode: archive, minimal, mid-window
- Timing: transition-during-block-production, transition-during-unwind,
  transition-during-retire
- Failure injection: network partition, disk pressure, aggressive rebuild
- Chain.toml v3 CL-config inlining (per
  `20260729-chaintoml-v3-fork-identity.md`)

For each Phase 2 leak, per-leak decision block:
1. Signature (what breaks)
2. Root cause (why)
3. Fix cost / architectural weight
4. **Decision:** fix / document + error / defer
5. If fix: linked commit + test
6. If document + error: spec statement + operator-facing error text + test
   that error fires

---

## Start-once leak inventory (as of 2026-07-31)

Every entry is a place where erigon's "start once, follow chain"
assumption breaks under fork transition. Phase column: **1** = must fix
before Phase 1 lock; **2** = surfaces in Phase 2 matrix.

### L1 — `currentContext` is uninterruptible (Phase 1)

- **Signature:** `SetHead(<block>): execution did not become quiescent
  within 2m0s (currentContext is still set)` — reproduced 2026-07-31 as
  F2 (Tier 3b) and F3 (Tier 3c).
- **Root cause:** `ExecModule.SetHead` (`execution/execmodule/set_head.go`)
  acquires the exec semaphore, but a wedged forkchoice retry loop holds
  `currentContext` indefinitely. The 2min quiescence timeout expires
  before the retry loop yields. The retry loop yields nothing because the
  underlying execution failure (nonce too high, mismatched state) never
  self-heals.
- **Impact:** Fork transition cannot be initiated on a node whose
  execution stage is in a persistent-failure retry loop.
- **Decision:** **fix (Phase 1)**. Two components:
  1. Fork test infrastructure: launch parent hermetically per suite run,
     so accumulated 13-day divergent state can't wedge the test.
  2. Erigon: `debug_setFork` (and by extension `debug_setHead`) should
     be able to interrupt a wedged forkchoice retry — retry loops on
     invalid blocks shouldn't hold the semaphore indefinitely against a
     preemption signal.

### L2 — Manifest exchange test flakes under load (Phase 1)

- **Signature:** `FAIL github.com/erigontech/erigon/node/components/manifest_exchange`
  under fork-test-suite run (with concurrent unwind soak); passes on
  solo re-run.
- **Root cause:** to be determined — needs reproduction under simulated
  load (goroutine CPU pressure or concurrent test suite).
- **Impact:** Suite reliability. A flaky adjacent test masks real
  regressions in Phase 1.
- **Decision:** **fix (Phase 1)**. Reproduce, root-cause, close timing
  dep.

### L3 — Chain identity latched at boot (Phase 1)

- **Signature:** components read `chain.Config` at construction; changing
  chain mid-process requires Stop→Reconfigure→Start of every captor.
- **Root cause:** original design assumption — process serves one chain.
- **Impact:** `debug_setFork` orchestration is inherently invasive; every
  captor added to the codebase must implement `Restartable` +
  `Reconfigurable` correctly or fork transition breaks.
- **Decision:** **fix — landed**. `fork.Controller` (`node/components/fork/controller.go`)
  handles the orchestration. Enforced by Tier 1 unit tests + Tier 2
  integration tests with real captors. New captors added to the codebase
  must be added to the controller's Restartable/Reconfigurable sets.

### L4 — Long-running node accumulates divergent state (Phase 1)

- **Signature:** parent erigon running 13 days is stuck in a `nonce too
  high` retry loop on a specific block; cannot advance, cannot be
  fork-transitioned.
- **Root cause:** node accumulated divergent execution state from prior
  test cycles + mode-B unwinds; no self-heal path exists to reconcile
  divergence against canonical.
- **Impact:** Any test that assumes a long-running parent will
  eventually hit this. Explicitly what happened today.
- **Decision:** **fix (Phase 1)** — hermetic parent lifecycle. Fork test
  suite launches parent per run and tears down after. Long-running-node
  self-heal is a separate concern deferred to Phase 2 (or a separate
  workstream) — it's a "start once accumulates divergence" issue that
  doesn't only affect fork.

### L5 — Datadir belongs to one chain (Phase 1, restart model)

- **Signature:** fork datadir + parent datadir share files; snapshots
  named per-chain. The restart-transition path reuses the parent's
  datadir with a `--chain=<fork>` flag change.
- **Root cause:** original design assumption — one datadir, one chain.
- **Impact:** restart transition must trim post-cut siblings before
  restarting, or the fork erigon boots against inconsistent files.
- **Decision:** **fix — landed via TrimPostCutSiblings post-swap hook**
  (`20260728-fork-test-reshape.md:181-192`). Test coverage in Tier 3c —
  currently blocked by L1 (F3).

### L6 — Snapshot files advertised at fixed genID (Phase 2)

- **Signature:** chain.toml v2 per-node naming
  (`chain.v2.<enr-fp>.<genID>.toml`) exists but multi-publisher fork
  scenarios (multiple nodes on the same fork advertising the same fork
  ancestry) not exercised.
- **Root cause:** fork identity model still evolving
  (`20260729-chaintoml-v3-fork-identity.md` is "proposed").
- **Impact:** unclear until multi-publisher forks tested.
- **Decision:** **Phase 2**. Fix-vs-document TBD.

### L7 — Caplin loads CL config at boot (Phase 1, restart model)

- **Signature:** Caplin refuses to start on a fork datadir without
  `cl-config.yaml`; historically blocked Tier 3c restart soak.
- **Root cause:** boot-time load of CL config, no runtime reload path.
- **Impact:** restart-transition needed a cl-config emit path.
- **Decision:** **fix — landed via forkexport writer + applyForkWriteCLConfig
  post-swap hook** (`20260728-fork-test-reshape.md:181-192`).

### L8 — Downloader torrent state persists across chain (Phase 2)

- **Signature:** torrents from parent chain remain in downloader after
  fork transition; not deauthed.
- **Root cause:** downloader state is process-wide, not chain-scoped.
- **Impact:** cosmetic today; potential for wrong-chain snapshot data if
  torrent state affects future retrieval.
- **Decision:** **Phase 2**. Fix-vs-document TBD.

### L10 — Parallel-exec race during initial-sync ProcessFrozenBlocks (Phase 1, external to fork)

- **Signature:** `[4/6 Execution] rw exit err="invalid block: apply loop
  exited (reachedMaxBlock=false lastBlockResult=<N> maxBlockNum=<M>) but
  1 block(s) had tx-results without a blockResult: [<N+1>]"` followed
  by `Could not start execution service err="ProcessFrozenBlocks: ..."`
  and `Invalid block during parallel initial sync — halting process`.
  Erigon exits voluntarily during OtterSync's ProcessFrozenBlocks phase.
- **First seen:** 2026-07-31 fresh hoodi sync via
  `scripts/fork-test-hermetic.sh`. Head stalled at 0 for 300s → hermetic
  wrapper stagnation-detected + trap-killed.
- **Root cause:** parallel-exec harness computed txResults for block
  N+1 but the blockResult signal was lost. Internal invariant
  violation. Likely a race in the parallel exec loop under high initial-
  batch load (OtterSync processes large ranges at once).
- **Impact:** Fresh hoodi sync intermittently fails during OtterSync.
  Blocks Phase 1 fork tests (they need a fresh parent). Not fork-
  specific — this is a general erigon initial-sync bug that also affects
  unwind soak's cycle-start behaviour, though the unwind soak has
  succeeded so far.
- **Decision:** **fix (Phase 1)** — parallel-exec race in
  ProcessFrozenBlocks needs to be closed. Blocker for Phase 1 fork
  test hermetic infrastructure.

### L9 — Multi-node convergence requires coordinated chain switch (Phase 2)

- **Signature:** initiator + follower must both switch chains; if they
  don't, they can't communicate.
- **Root cause:** chain identity is per-node; no distributed protocol for
  "everyone switch to fork now".
- **Impact:** operational — how does a multi-node deployment coordinate?
- **Decision:** **Phase 2**. Likely document as constraint ("operator
  responsibility to coordinate") + provide clear peer-mismatch error.

---

## Phase 1 close-out plan

### Immediate work items

1. **L2: manifest_exchange flake** — reproduce under load, root-cause,
   fix.
2. **L1 + L4: hermetic parent lifecycle** — modify `fork-test-suite.sh`
   (or a new script) to launch its own parent, wait for live tip, run
   Tier 3+, tear down. This unblocks F2 and F3.
3. **L1: setHead preemption of wedged forkchoice** — deferred pending
   evaluation. May or may not be fixable within Phase 1 scope. If not:
   escalate as Phase 2 or a separate workstream, but Phase 1 must still
   have a way to run against a healthy parent (hermetic launch, item 2
   above, achieves this).

### Phase 1 lock criteria

- Full `fork-test-suite.sh --with-e2e --with-soak` completes with all
  tiers pass.
- Suite passes 3× consecutively (no flakes).
- Suite passes while unwind soak is running concurrently.
- Every leak L1-L5, L7 either closed (fix landed + regression test) or
  worked around within Phase 1 scope (hermetic infra).

Only after all criteria met: start Phase 2 enumeration.

---

## Related

- `docs/plans/20260728-fork-test-reshape.md` — defines Tier 1-4
  structure; this doc defines the matrix each Tier covers.
- `docs/plans/20260630-fork-testing-scenarios.md` — catalog of Category
  A-F scenarios; largely maps to Phase 2 dimensions.
- `docs/plans/20260718-fork-testing-decisions.md` — converged design
  decisions for fork identity + trust; feeds Phase 1 (happy-path UCAN)
  and Phase 2 (edge cases).
- Memory `fork-multinode-e2e-findings-2026-07-26.md` — the manual
  multi-node test that revealed Bug 1 + Bug 2; Phase 2 will automate
  this scenario.
- Memory `unwind-soak-fix-batch-2026-07-30.md` — reference for the
  discipline Phase 1 must match.
