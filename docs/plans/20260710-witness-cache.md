# `debug_executionWitness` — eager in-memory witness cache

## Abstract

Serve `debug_executionWitness` for recent blocks from an in-memory ring populated eagerly, right
after each canonical block commits, instead of rebuilding the witness on every RPC call. The cache
holds the last ≤`N` legacy-mode witnesses (bounded by a byte cap), keyed by `(blockNum, blockHash)`.
It is **embedded-RPC-only**, requires the historical-commitment schema, and touches **no
block-execution / consensus code** — the builder is a downstream read-only observer of the existing
new-canonical-header notification, and it reuses the exact on-demand build path so a cached witness
is byte-identical to the on-demand one.

## Problem

`debug_executionWitness` re-executes the whole block and re-folds the commitment trie on every call
(`rpc/jsonrpc/debug_execution_witness.go:700` `ExecutionWitness`). Measured on a mainnet-archive node
this is ~1.24s/witness (erigon) vs ~0.31s (reth), split: `buildWitnessTrie` 57.7%,
`detectCollapseSiblings` 17.4%, `verifyWitnessStateless` 13.4%, `buildAccessedState` (block re-exec)
10.8%. Tip-following consumers (provers, stateless verifiers) request witnesses for blocks near head
as they arrive — exactly the access pattern a small recent-tip cache serves at near-zero cost, since
the node already executed and folded each block once.

## Design

### Locked decisions

- **Legacy mode only in the cache.** `resolveWitnessMode` defaults to legacy; legacy is a superset of
  canonical but **not** cheaply trimmable (the extra nodes are unlabeled in the flat node list, and
  legacy's `0x80` empty-storage node is rejected by stateless verification, so legacy bytes can't be
  served to a canonical consumer). Canonical requests always fall through to on-demand.
- **Embedded-RPC only, structurally.** `jsonrpc.APIList(...)` (`rpc/jsonrpc/daemon.go:46`) is the one
  builder of `DebugAPIImpl` for every topology. Only the embedded call site
  (`node/eth/backend.go:1185`) passes a non-nil cache; standalone `rpcdaemon`
  (`cmd/rpcdaemon/main.go:59`) and `cmd/mcp/main.go:255` pass `nil`. The handler treats a `nil` cache
  as "not enabled". No config-boolean gate is needed — the nil is the gate.
- **Requires `--prune.experimental.include-commitment-history`.** Without it `ExecutionWitness` errors
  (`debug_execution_witness.go:716`); every build would fail and the cache would stay empty. The
  builder must not start unless the schema is enabled.
- **Safe-commit gate = poll committed head (Fork 1).** The builder does not assume whether
  `OnNewHeader` fires before or after the DB commit. On each header it opens its own `BeginTemporalRo`
  and, if the committed head `< num`, backs off and retries until the committed head `≥ num` **and**
  the canonical hash at `num == hash`, then builds against the committed DB only (never the overlay).
  Timing-agnostic and single-feed.
- **Tip-biased coalesce-to-latest builder (Fork 2).** One worker: each cycle non-blocking-drains the
  header channel to the newest header (reconciling evictions for every drained header along the way),
  then builds that newest one if tip-gated. Under the sane regime (build < block-time) this builds
  every block with no skips; under overload it keeps the freshest tip + best-effort trailing and lets
  holes fall through to on-demand. Rides the existing 8-deep `PrioritizedSend` buffer + drop-oldest.
- **Store the finished `*ExecutionWitnessResult` (Fork 3).** Post-sort, immutable after build. Serve
  returns the pointer and the RPC layer marshals per request exactly as on-demand does → byte-identical
  by construction, and it is already the compact form (raw node/code bytes, not hex — hex expansion
  happens only at wire-marshal, identically for both paths). `maxmb` counts the raw byte lengths.
- **Verify is a failure-parity gate, not a byte gate.** `verifyWitnessStateless` does not mutate the
  result (the `0x80` append + final sort run after it). Keep it in the builder; **on verify failure,
  do not cache** — so a latent bad witness falls through to on-demand, which errors consistently,
  rather than being served from cache.

### Invariants (load-bearing)

1. Cached bytes are byte-identical to on-demand → the builder and handler call the same
   `buildWitnessResult` seam; never fork the build logic.
2. No block-execution / consensus / chain-tip code is modified.
3. The builder owns its own `BeginTemporalRo` tx and its own `NewSharedDomains` for the whole build,
   single-threaded, `Rollback`/`Close` only after the build completes. Never hand the tx or any
   domain-read byte slice to a goroutine that outlives `Rollback` (the per-worker-pin use-after-munmap
   trap). This is exactly what the on-demand handler already does per request, so a background build is
   just one more concurrent reader.
4. A cached `*ExecutionWitnessResult` is immutable after insertion; serves share the pointer read-only
   and must never mutate it.
5. Serve only on exact `(num, hash)` match and legacy mode; any other request misses → on-demand.

### Reorg handling

The new-canonical-header feed *is* the reorg signal. On unwind, `NotifyNewHeaders` walks
`kv.HeaderCanonical` — already rewritten to the new hashes — from `unwindPoint+1` and re-fires
`OnNewHeader` with the new `(num→hash)` (`execution/stagedsync/stage_finish.go`,
`execution/execmodule/notification_dispatcher.go`). So the builder's own subscription drives eviction:
per header, evict any cached entry at that num whose hash differs, advance the known head, and drop any
entry above the new head (covers a shorter new chain). Because `PrioritizedSend` may drop under burst,
reconcile the whole range below each received head (or verify hash on serve) so a missed event
self-heals.

## Tasks

### Task 0: Extract the `buildWitnessResult` seam (pure refactor)

Split `ExecutionWitness` (`rpc/jsonrpc/debug_execution_witness.go:700`) so the pipeline from
`chainConfig` through `buildAccessedState` → `NewSharedDomains` → `detectCollapseSiblings` →
`buildWitnessTrie` → `collectAccessedHeaders` → `verifyWitnessStateless` → `0x80`-append → sort becomes
a method `buildWitnessResult(ctx, tx, info witnessBlockInfo, mode witnessMode) (*ExecutionWitnessResult, error)`,
where `info` is the `resolveWitnessBlock` result. `ExecutionWitness` becomes: resolve mode, open tx,
check commitment-history, resolve block, call `buildWitnessResult`.

- **Success:** no behavior change; existing witness tests pass unchanged.
- **TDD note:** pure refactor (existing tests are the safety net). But `TestExecutionWitness` asserts
  structure, not golden bytes, so add a one-shot before/after byte-equality guard: capture the
  `ExecutionWitness` result bytes for a fixture block before the extraction and assert equality after,
  so the append-vs-sort tail can't silently reorder.

### Task 1: `witnessCache` type (TDD)

New file `rpc/jsonrpc/witness_cache.go` (copyright 2026). Unexported type `witnessCache` in package
`jsonrpc` (no new package — the field lives on `DebugAPIImpl`, same package, no import cycle).

- Ring keyed `(blockNum, blockHash)` → `*ExecutionWitnessResult`, guarded by an `RWMutex`.
- Two caps: entries ≤ `blocks` (≤96) and resident bytes ≤ `maxmb·MiB`; evict oldest (lowest num) when
  either binds. Resident bytes = Σ(len of state nodes + codes + keys + headers) per entry.
- Ops: `get(num, hash) (*ExecutionWitnessResult, bool)`; `put(num, hash, *ExecutionWitnessResult)`;
  `reconcile(headers []headerRef)` — evict num-with-different-hash, drop entries above the highest head;
  size/metrics accessors.
- **Success + TDD (Red→Green per case):** count-cap eviction; byte-cap eviction; `get` miss on
  hash-mismatch; `reconcile` evicts an orphaned hash and drops above-head entries; concurrent
  get/put race-free (`-race`).

### Task 2: Serve hook

In `ExecutionWitness`, after mode resolution: if `api.witnessCache != nil && mode == legacy`, resolve
the canonical hash for the number cheaply (`rpchelper.GetBlockNumber` — a hit does not need the full
`resolveWitnessBlock`) and `get(num, hash)`; on hit return the cached result. Miss / canonical / nil →
the unchanged on-demand path. Increment hit/miss metrics (Task 5).

- **Success:** a legacy request for a cached `(num,hash)` returns the cached pointer; canonical and
  uncached requests are unaffected; `nil` cache path identical to today.
- **TDD:** unit test with a hand-populated `witnessCache` asserting hit returns the stored result and
  canonical bypasses it.

### Task 3: Eager builder loop

New file `rpc/jsonrpc/witness_cache_builder.go` (copyright 2026). `buildWitnessResult` is a
`*DebugAPIImpl` method that needs a fully wired `BaseAPI` (filters, stateCache, blockReader, engine,
dirs, evmCallTimeout) plus `db`/`ethBackend`/`gascap`, and `APIList` builds its `debugImpl` internally
and returns only `[]rpc.API` — so the builder cannot reach that impl. Resolve this by constructing a
**builder-owned** `*DebugAPIImpl` from the same args `APIList` receives, sharing the single
`*witnessCache` pointer with the serve-side impl. Two impls, one cache. Entry point:
`RunWitnessCacheBuilder(ctx, dbg *DebugAPIImpl, headerCh <-chan [][]byte, notifications *shards.Notifications)`
where `dbg` is the wired builder-owned impl (its `witnessCache` field is the shared pointer).

Decompose into pure, testable helpers (keep the loop itself thin — it is not unit-testable):

- `shouldBuild(num uint64, singleHeaderBatch bool, lastSeen, frozen uint64) bool` — the tip-gate:
  `singleHeaderBatch && num > frozen && num == lastSeen`. Pure; TDD it.
- `waitCommittedHead(ctx, db, num, hash) (kv.TemporalTx, bool, error)` — opens a **fresh**
  `BeginTemporalRo` on each attempt (an MDBX RO tx is a fixed snapshot, so it must be re-opened between
  retries, not re-read), checks Finish progress and the canonical hash at `num`; returns the committed
  tx once head `≥ num` and hash matches, `(nil,false,nil)` if the block reorged away, with bounded
  backoff. TDD the decision logic against a fake progress source.

Loop: on each `[][]byte` batch — `rlp.DecodeBytes` each header (mirror `rpc/rpchelper/filters.go:907`),
`cache.reconcile` the batch, coalesce (non-blocking drain) to the newest header; if `shouldBuild(...)`:
`waitCommittedHead(...)` → `resolveWitnessBlock` on that committed tx →
`buildWitnessResult(ctx, tx, info, witnessModeLegacy)` → on success `cache.put(num, hash, result)`, on
any error (including verify failure) do **not** cache (log + metric) → `Rollback`. Respect `ctx` for
shutdown.

- **Success:** with the builder running against a synced node, a legacy `debug_executionWitness` for a
  recent block is a cache hit and byte-identical to the same call with the cache disabled.
- **TDD:** unit-test `shouldBuild` and `waitCommittedHead`'s decision logic (Red→Green). For the
  full-path parity test, reuse the existing harness `rpcdaemontest.CreateTestExecModule` (the one
  `TestExecutionWitness` uses — `rpc/jsonrpc/debug_api_test.go:1092`, with
  `statecfg.EnableHistoricalCommitment()`): build a witness via the builder path and assert its bytes
  equal the on-demand `ExecutionWitness` result for the same block — no live archive node needed. Add a
  reorg case asserting a post-reorg request misses and falls through.

### Task 4: Flags, config, wiring, gating

- **Flags** (`cmd/utils/flags.go`, model on `RpcGasCapFlag` `cli.UintFlag`): `WitnessCacheBlocksFlag`
  (`witness.cache.blocks`, default 0 = off, enforced ≤ 96) and `WitnessCacheMaxMBFlag`
  (`witness.cache.maxmb`, default 1024).
- **Register** in `node/cli/default_flags.go` (near lines 99–104).
- **Config struct** `httpcfg.HttpCfg` (`cmd/rpcdaemon/cli/httpcfg/http_cfg.go:30`): add
  `WitnessCacheBlocks uint`, `WitnessCacheMaxMB uint`. Populate in `node/cli/flags.go` (the
  `setEmbeddedRpcDaemon` `HttpCfg{…}` block near line 434/491) via `ctx.Uint(...)`; add cobra `UintVar`
  binds in `cmd/rpcdaemon/cli/config.go` so the flag parses standalone (cache stays nil there).
- **Thread the cache** as one new param on `APIList` **only** (`rpc/jsonrpc/daemon.go:46`); set it
  in-package right after the impl is built — `debugImpl.witnessCache = cache` (`daemon.go:57`). **Do not**
  change `NewPrivateDebugAPI` — that would break ~15 existing callers (`debug_api_test.go`,
  `gen_traces_test.go`, `eth_callMany_test.go`, `cmd/evm/zkevmrunner.go`). Add the
  `witnessCache *witnessCache` field to `DebugAPIImpl` (`rpc/jsonrpc/debug_api.go:80`). Standalone
  (`cmd/rpcdaemon/main.go:59`) and mcp (`cmd/mcp/main.go:255`) pass `nil`.
- **Embedded wiring** (`node/eth/backend.go`, just before the `APIList` call at line 1185): gate on the
  **DB-persisted** flag `rawdb.ReadDBCommitmentHistoryEnabled` (open a quick RO tx on `chainKv`, matching
  the handler at `debug_execution_witness.go:712`), **not** the CLI flag — a datadir built without
  commitment history reports off even when the flag is passed. If `WitnessCacheBlocks > 0` and the
  persisted flag is on: construct the `witnessCache`, subscribe a header channel via
  `s.notifications.Events.AddHeaderSubscription()` (mirror line 1174), construct the builder-owned
  `*DebugAPIImpl` (Task 3), start `RunWitnessCacheBuilder`, pass the cache into `APIList`, and register
  teardown under a **new** field `s.unsubscribeWitnessCache` (do not reuse `unsubscribeEthstat`). If
  `WitnessCacheBlocks > 0` but the persisted flag is off, log a clear warning and leave the cache nil.
- **Success:** embedded node with the flag + commitment-history builds and serves from cache; embedded
  node with the flag but no commitment-history logs and no-ops; standalone rpcdaemon with the flag set
  parses it and serves on-demand only (nil cache); default (flag 0) is a complete no-op.

### Task 5: Metrics + docs

- Counters (existing metrics facility): `hit`, `miss`, `build_ok`, `build_fail_verify`, `build_fail_other`,
  `evict`, `coalesce_drop`, plus gauges `bytes_resident`, `entries_resident` and a `build_duration`
  histogram.
- Flag docs / help text: embedded-only, requires `--prune.experimental.include-commitment-history`,
  `maxmb` is the real cap (a 1GB cap holds ~85 median-mainnet blocks; 96 blocks can be ~1.1–2.4GB raw).

## Testing

- Task 1 cache unit tests (count/byte eviction, reconcile, hash-mismatch, `-race`).
- Task 0 guarded by the existing witness byte-identity tests.
- Task 3 parity test: builder-path result bytes == on-demand result bytes for the same block.
- Reorg: reconcile evicts orphan → post-reorg request misses → on-demand.
- Gating: standalone no-ops (nil cache); flag-without-commitment-history logs and no-ops.
- `make lint && make erigon integration` before every push (lint is non-deterministic — run to clean).

## Open item (non-blocking)

`OnNewHeader` pre/post-commit ordering in `fcuBackgroundCommit` mode is unresolved (the two
investigations disagreed). The Fork-1 poll-committed-head gate makes correctness independent of the
answer. Optional follow-up: trace the `forkchoice.go` dispatch/commit ordering once; if the header is
provably post-commit in all modes, the poll can be dropped for a direct committed read.

## Out of scope

- Canonical-mode caching (always on-demand).
- Populating the cache in a standalone rpcdaemon (cross-process; not possible via a shared map).
- `eth_getProof` / `eth_getWitness` (per-call granularity, different key shape).
- Persistence across restart (cold on restart; warms within one window).
- A BAL-driven read-set handoff to skip re-execution — deferred until EIP-7928 lands; it becomes a read
  of block data with no chain-tip change, so nothing here needs to anticipate it.
