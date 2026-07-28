# `debug_setFork` — Atomic Runtime Fork Transition

**Date:** 2026-07-28
**Branch:** `feat/snapshot-flow-app-integration`
**Status:** Design phase — needs review before implementation.
**Depends on:** [20260609-mode-b-cl-rewind-gap.md](20260609-mode-b-cl-rewind-gap.md) Phase 2 (Caplin re-anchor); the mode-B unwind fix stack + ensure-history cleanup fix.

## Summary

`debug_setFork(chainName string)` transitions a running erigon node
onto a different chain — either forward (parent → fork) or backward
(fork → parent) — without a process restart. Same datadir, same
nodekey, same enode identity, same Inventory, same uptime. Operator-
symmetric: the same RPC call rolls a node onto a fork today and back
onto the parent tomorrow.

Companion: `debug_setHeadToCut()` — a no-arg helper that unwinds the
currently-loaded fork to its `CutBlock` without the operator having
to look up the block number.

Reuses the `Provider.Unwind` primitive the current mode-B soak is
validating; adds a chain.Config pointer swap coordinated across
every component holding one; hot-swaps Caplin via the
cancel-and-relaunch pattern the CL-rewind MVI already anchored at
2026-06-09.

## Why now

- `snapshots fork-from` (offline CLI) exists and is tested — but
  operators want a runtime primitive, not an offline extract that
  requires downtime + a new datadir per fork.
- The mode-B unwind soak is stabilising with the ensure-history
  cleanup fix landing this week; the destructive-primitive concern
  the design docs raised in May ("partial transition leaves the
  datadir unrecoverable") is answered by that same fix stack.
- The fork soak we're planning needs a scenario for "transition
  in place" that the current CLI + reboot flow makes noisy to run.

## What the operator sees

```
$ curl -X POST -H "Content-Type: application/json" --data \
    '{"jsonrpc":"2.0","method":"debug_setFork","params":["hoodi-fork-1785091449"],"id":1}' \
    http://127.0.0.1:19545
{"jsonrpc":"2.0","id":1,"result":{
  "from_chain": "hoodi",
  "to_chain": "hoodi-fork-1785091449",
  "unwound_from": 3299999,
  "unwound_to": 3287776,
  "caplin_reanchored_at_slot": 3200000,
  "elapsed": "12.4s"
}}
```

Symmetric reverse:

```
$ curl ... --data '{... "params":["hoodi"] ...}'
{... "from_chain": "hoodi-fork-1785091449", "to_chain": "hoodi", ...}
```

## Semantics

### Direction: parent → fork

1. Target `chainName` resolves to a `chain.Config` where
   `config.Parent == currentChain.Name` (or `currentChain.Name` is a
   valid parent per `ValidParentTrustRoots[]`).
2. Unwind EL to `targetConfig.CutBlock`. If already at/below CutBlock,
   no unwind needed.
3. Swap chain.Config pointers in every component that captured one.
4. Cancel Caplin's current `RunCaplinService` goroutine, wipe its
   forkchoice store's slot pointers (already implemented by the CL
   MVI's `UserInitiatedRewindToBlock`), relaunch with the fork's
   `CaplinConfig` — the fork's checkpoint-sync URL is the new
   anchor source.
5. Resume: sentry advertises new fork network ID / status; downloader
   stamps the fork's chain identity on subsequent V2 manifests;
   manifest_exchange fork-ID filter accepts the fork's ID; publisher
   emits `chain.v2.<fp>.<seq>.toml` with the fork's [parent] section.

### Direction: fork → parent

1. Target `chainName` resolves to a `chain.Config` where
   `chain.Config.Parent == ""` AND the currently-loaded fork's
   `chain.Config.Parent == chainName`.
2. Unwind EL to fork's `CutBlock` (same block — parent and fork
   are byte-identical up to CutBlock by construction).
3. Same pointer-swap dance in reverse: components pick up the
   parent's chain.Config; sentry advertises parent's network ID;
   downloader's chain identity reverts to parent's genesis fork
   schedule.
4. Cancel fork's Caplin, relaunch with parent's `CaplinConfig` +
   parent's checkpoint-sync URL. Anchor at parent's tip slot; Caplin
   catches up from CutBlock to parent's tip.

### Reversibility invariant

For any node N with chain state at block B ≥ CutBlock:

    N.debug_setFork(fork) → N.debug_setFork(parent)
    is behaviourally identical to
    N did nothing except unwind to CutBlock and re-execute.

I.e. the roundtrip is a no-op except for the unwind's local-state
side effects (chaindata rolled back to CutBlock; snapshots pruned
to step covering CutBlock).

## Primitives

| Primitive | Status | File |
| --- | --- | --- |
| `Provider.Unwind(ctx, toBlock, opts)` | ✓ landed, soak-verified | `node/components/storage/provider_unwind.go` |
| `ensureHistoryForUnwindWalk` + torrent-drop cleanup | ✓ landed 2026-07-27 | `node/components/storage/provider_unwind_history_ensure.go` |
| Caplin MVI (subscribes to `flow.UnwindCompleted`) | ✓ landed 2026-06-09 | `node/components/caplin/provider.go` |
| `PersistentBlockCollector.Wipe(ctx)` | ✓ landed | `cl/persistence/block_collector/collector.go` |
| `ForkChoiceStore.UserInitiatedRewindToBlock` | ✓ landed | `cl/phase1/forkchoice/on_operation.go` (approx) |
| Caplin cancel-and-relaunch with re-anchor slot | ✗ NOT built — Phase 2 of CL MVI | this design ships it as a prerequisite |
| `chainspec.ChainSpecByNameOrForkDatadir` | ✓ landed | `execution/chain/spec/spec.go` |
| RPC handler surface | ✗ NOT built — this design's core delivery | new: `rpc/jsonrpc/debug_api.go` |

## chain.Config swap contract

grep-audit of who captures the pointer (from live 2026-07-28 audit):

- `backend.chainConfig` — the canonical field.
- `sentry.Provider.ChainConfig` — feeds status data + network ID.
- `storage.Provider.ChainConfig` — feeds validators + fork ID filter.
- `storage/receipt_root_validator.ChainConfig` — receipt validation.
- `Downloader.SetChainIdentity(genesisFork, forks)` — V2 manifest
  fork-ID field.
- `manifest_exchange` fork-ID filter — accepts/rejects peer manifests.
- Numerous function-scoped variables and closures across the stack.

The swap protocol:

1. **Quiesce**: pause execmodule (existing primitive via
   `ExecModule.SetHead`'s semaphore). Blocks new payloads.
2. **Unwind**: `Provider.Unwind(ctx, targetConfig.CutBlock, opts)`.
   Fails loud on any inconsistency; caller returns an actionable error
   and does NOT swap.
3. **Cancel Caplin**: signal the `RunCaplinService` goroutine's ctx
   to cancel. Wait for its clean shutdown (state persisted to
   MDBX; nothing left in flight).
4. **Swap pointers**: under an appropriate write lock, set:
   - `backend.chainConfig = &targetConfig`
   - `sentryProvider.ChainConfig = &targetConfig`
   - `storageProvider.ChainConfig = &targetConfig`
   - `Downloader.SetChainIdentity(newGenesisFork, newForks)`
   - manifest_exchange fork-ID filter re-bound
   - Any other pointer captured at Init time — audit checklist below.
5. **Publish `flow.UnwindCompleted{ToBlock: CutBlock, TipBlock: prevTip}`**
   so any subscriber that listens for the storage-bus signal (Caplin
   MVI, mx, etc.) sees the transition.
6. **Relaunch Caplin**: spawn `RunCaplinService` with the new chain's
   `CaplinConfig` + checkpoint-sync URL. Caplin re-anchors from the
   new checkpoint.
7. **Resume**: unquiesce execmodule. Chain proceeds on the new config.

Failure at any step (2)–(6) leaves the swap partial. The safe policy
is: the transition is transactional only through step (2). If (3)–(6)
fail, the datadir is at CutBlock but chain.Config still points at the
original chain. Log loudly and require operator restart. This is
acceptable because (a) unwind to CutBlock is safe under either chain
config — both parent and fork agree on state at CutBlock; (b) restart
picks up whichever chain.Config is on disk (the operator sets it).

### Pointer-swap audit checklist

Before implementation, `grep -n 'chain\.Config\|ChainConfig\b'` and
map every captor into one of three buckets:

- **Bucket A — updated by swap step (4)** above. Explicit re-write.
- **Bucket B — recomputed from a Bucket-A field per-call**.
  Automatic once Bucket A is updated.
- **Bucket C — captured in a closure that outlives the swap**. These
  are BUGS in the current codebase — a captured-at-Init pointer that
  can't be updated at runtime means that component IS the reason
  Shape C is week+ of work rather than 1-2 days. Each Bucket C
  finding is either (i) refactored to hold a `*atomic.Pointer[chain.Config]`
  or a getter function, or (ii) reset via cancel-and-relaunch (like
  Caplin).

Design phase produces an audit table with every captor classified.
Implementation phase updates the code accordingly.

## Caplin hot-swap

Uses the cancel-and-relaunch pattern from the CL MVI's Phase-2
sketch — Phase 2 was called out at
[mode-b-cl-rewind-mvi-shipped-2026-06-09](../../.claude/projects/-erigon-mark-hive-clients-erigon-erigon/memory/mode-b-cl-rewind-mvi-shipped-2026-06-09.md)
but never implemented. `debug_setFork` implements it as its own
prerequisite:

1. `caplin1.RunCaplinService` wrapped in a goroutine keyed by a
   restart-signalling channel.
2. `debug_setFork` sends the restart signal with the new
   `CaplinConfig` payload.
3. Wrapper cancels current ctx, waits for RunCaplinService to
   return, then re-invokes with the new config.
4. New instance boots via checkpoint sync at the new chain's URL.
   State from prior instance is discarded (Caplin's chaindata is
   scoped to its own MDBX tables; the swap wipes those tables as
   part of the wrapper's tear-down).

If no checkpoint-sync URL is configured for the target chain, the
call fails cleanly at step (1) validation — before any unwind runs.

## Test surface

### Unit

- **`TestDebugSetFork_ParentToForkAndBack`** — same node, roundtrip.
  Assert reversibility invariant: post-roundtrip Merkle root at
  CutBlock equals pre-roundtrip Merkle root; nodekey unchanged; enode
  ID unchanged.
- **`TestDebugSetFork_RejectsIncompatibleChain`** — target's Parent
  doesn't match current chain; RPC returns actionable error, no
  unwind, no swap.
- **`TestDebugSetFork_RejectsMissingCaplinConfig`** — target chain
  has no `CaplinConfig` or no checkpoint-sync URL; RPC fails at
  validation.
- **`TestDebugSetFork_FailureMidSwapLeavesUnwoundButRestartable`** —
  inject a fault at step (4)/(5); verify datadir is at CutBlock,
  chain.json indicates original chain, node restarts cleanly.

### Integration

- **`TestP2P_DebugSetFork_TwoNode_Transition`** — two nodes on
  parent, both `debug_setFork(fork)`, verify both stamp fork's
  chain-toml on ENR + converge on fork's canonical view.
- **`TestP2P_DebugSetFork_LoneNode_Rollback`** — one node parent →
  fork → parent, verify final state == initial state (except for
  any tip-region advancement while on the fork).

### Live E2E (fork soak scenario)

- Fork soak's transition scenario: launch node on parent, run cycle
  for N iters, `debug_setFork(fork)`, run cycle for M iters,
  `debug_setFork(parent)`, verify no wedges, no lost state,
  canonical convergence with peers throughout.

## Risk areas

1. **Component pointer capture** — Bucket C findings from the audit.
   Each one is either a refactor or a cancel-and-relaunch trade-off.
   Total work depends on how many Bucket-C captors exist.
2. **Caplin state persistence across swaps** — Caplin's MDBX tables
   are scoped to its chain. Swapping chain means either:
   (a) wiping those tables (simple; loses old chain's beacon state,
   which is fine since operator is transitioning), or
   (b) keeping per-chain sub-databases (invasive).
   MVP does (a).
3. **Peer-set churn on transition** — post-swap the node's fork ID
   changes; peers holding old fork ID reject subsequent messages.
   Sentry handles this via the eth-handshake protocol version bump;
   peer-set effectively empties + refills against the new chain's
   discv5 network. Downtime for pure-P2P visibility is bounded by
   discv5 re-crawl (~seconds to minutes).
4. **Rollback semantics if step (4)+ fails** — see failure protocol
   above. Datadir at CutBlock is safe under either chain.

## Non-goals

- **Multi-fork navigation** — `debug_setFork(fork-A)` followed by
  `debug_setFork(fork-B)` where B has a different CutBlock than A.
  Requires unwinding across CutBlocks; punt to a follow-up.
- **Fork chain generation** — creating a new fork spec at runtime
  (that's `snapshots fork-from`, staying CLI).
- **CL genesis provisioning** — the target chain must already have
  `cl-config.yaml` + `genesis.ssz` provisioned in the datadir (or
  a checkpoint-sync URL that serves them). `debug_setFork` doesn't
  fetch those.

## Implementation phases

**Phase 0 — chain.Config captor audit.** Grep + read; produce the
bucket-classified table. **Complete 2026-07-28** — findings inline
below.

**Phase 1 — RPC handler + unwind.** New `debug_setFork` in
`debug_api.go`. Validates target, unwinds via ExecModule's existing
setHead path, publishes `flow.UnwindCompleted`. Skips component
swaps; leaves chain.Config unchanged. Verifies the unwind alone
works end-to-end. Ships as `restart_required: true` return value.

**Phase 2 — Bucket-A pointer swaps.** Implements the 16 direct
struct-field rewrites from the audit table. Each is a per-file
edit that adds a setter method (or exposes the field via an
accessor) + wires the setter call into the swap sequence.

**Phase 3 — Bucket-C: Caplin cancel-and-relaunch.** Implements the
CL MVI's punted Phase 2 as the debug_setFork prerequisite.
Cancel-and-relaunch wrapper around `RunCaplinService`.

**Phase 4 — Bucket-C: Downloader chain-identity + mx fork-ID
filter rebind.** Re-invoke `SetChainIdentity` with new chain's
identity; rebind manifest_exchange filter closure.

**Phase 5 — `debug_setHeadToCut` convenience.** Small wrapper
that reads the current chain's CutBlock and calls the existing
`debug_setHead(CutBlock)`.

**Phase 6 — Integration tests + fork soak scenario.** The tests
above + wiring into the fork soak driver.

## Phase 0 audit findings (2026-07-28)

**Bucket A — persistent captors, direct pointer rewrite** (16
struct-field captors):

| # | Captor | File |
| --- | --- | --- |
| 1 | `Ethereum.chainConfig` | `node/eth/backend.go` |
| 2 | `sentry.Provider.ChainConfig` | `node/components/sentry/provider.go` |
| 3 | `sentry_multi_client.MultiClient.ChainConfig` | `p2p/sentry/sentry_multi_client/sentry_multi_client.go` |
| 4 | `StatusDataProvider.chainConfig` | `p2p/sentry/status_data_provider.go` |
| 5 | `storage.Provider.ChainConfig` | `node/components/storage/provider.go` |
| 6 | `receipt_root_validator.ChainConfig` | `node/components/storage/receipt_root_validator.go` |
| 7 | `execmodule.executor.chainConfig` | `execution/execmodule/executor.go` |
| 8 | `execmodule.Dispatcher.chainConfig` | `execution/execmodule/notification_dispatcher.go` |
| 9 | `stagedsync.ExecuteBlockCfg.chainConfig` | `execution/stagedsync/stage_execute.go` |
| 10 | `stagedsync.SenderCfg.chainConfig` | `execution/stagedsync/stage_senders.go` |
| 11 | `stagedsync.SnapshotCfg.chainConfig` | `execution/stagedsync/stage_snapshots.go` |
| 12 | `stageloop.StageLoop.chainConfig` | `execution/stagedsync/stageloop/stageloop.go` |
| 13 | `freezeblocks.RoSnapshots.chainConfig` | `db/snapshotsync/freezeblocks/block_snapshots.go` |
| 14 | `snapshotsync.Merger.chainConfig` | `db/snapshotsync/merger.go` |
| 15 | `txpool.TxPool.chainConfig` | `txnprovider/txpool/pool.go` |
| 16 | `privateapi.ethBackend.chainConfig` | `node/privateapi/ethbackend.go` |

Each gets a `SetChainConfig(*chain.Config)` (or equivalent) setter
that atomically stores the pointer. Swap step (4) walks the list.

**Bucket B — per-call transient** (updated automatically): all of
`execution/exec/*`, `execution/protocol/*`, `execution/vm/*`,
`execution/tracing/*`, `execution/builder/*`, `rpc/transactions/*`,
`rpc/jsonrpc/eth_simulation.go`, `stagedsync/block_post_validator.go`,
`execution/verify/*`. These take chain.Config as a function
argument each call; the caller sources it from a Bucket-A field.
**Zero swap work here — automatic.**

**Bucket C — needs refactor or cancel-and-relaunch** (4 items):

| # | Captor | Approach |
| --- | --- | --- |
| C1 | Caplin (`RunCaplinService` goroutine) | Cancel + relaunch — Phase 3 above; also delivers CL MVI Phase 2 |
| C2 | `Downloader.SetChainIdentity(genesisFork, forks)` | Re-invoke setter with new chain's identity (setter already exists) — Phase 4 |
| C3 | `stagedsync.exec3_parallel` in-flight workers | Quiesce execmodule first (existing primitive); parallel drain; then swap. No refactor needed. |
| C4 | `manifest_exchange` fork-ID filter closure | Re-bind after swap. Explicit rebind or setter update — Phase 4 |

**Bucket D — test scaffolding, ignored**: `execmoduletester`,
`harness/`, `_test.go`, `engineapitester/`, `mock_cl.go`,
`cmd/evm/runner.go`.

**Out of scope — Polygon (Bor/Heimdall)**: `polygon/bor/*`,
`polygon/heimdall/*`, `polygon/sync/*`, `polygon/tracer/*` all
carry their own chain.Config captors. Fork transitions on Bor
sidechains are not intended for MVP; hoodi is non-Bor so it does
not gate the fork soak. Future work: extend Bucket A + C to
polygon/ if fork transitions are ever added for Bor chains.

## Related plans + memory

- [20260609-mode-b-cl-rewind-gap.md](20260609-mode-b-cl-rewind-gap.md) — CL rewind MVI (Phase 1 built, Phase 2 still open — this design ships it).
- [20260630-fork-testing-scenarios.md](20260630-fork-testing-scenarios.md) — Flavour 3 lists `debug_setFork` as the intended mechanism for in-place transitions.
- [20260718-fork-testing-decisions.md](20260718-fork-testing-decisions.md) — names `debug_setHeadToCut` as a planned convenience RPC.
- [mode-b-cl-rewind-mvi-shipped-2026-06-09.md](../../.claude/projects/-erigon-mark-hive-clients-erigon-erigon/memory/mode-b-cl-rewind-mvi-shipped-2026-06-09.md) — the anchor-floor open finding that Phase 3 must solve.
