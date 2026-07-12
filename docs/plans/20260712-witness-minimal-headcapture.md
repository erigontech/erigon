# Witness serving on a minimal node via head-capture (pinned parent snapshot)

## Overview
Serve `debug_executionWitness` for the last N blocks (N up to the LRU cap, hard max 96) on a **minimal** Erigon node — one that keeps account/storage/code domain history (~100k blocks) but has **no commitment-domain history** and does not generate it.

Today both the eager witness cache and on-demand serving are hard-gated on commitment history being present (`node/eth/backend.go:1190-1205`, `rpc/jsonrpc/debug_execution_witness.go:743`, `rpc/jsonrpc/eth_call.go:674`), so a `--prune.mode=minimal` node cannot serve witnesses at all. This plan adds a **head-capture** mode that builds each head block's witness with the **commitment** parent state read from a *pinned parent RO snapshot* (where commitment-latest == parent commitment) and the **plain** parent/block-end state read from the account/storage/code history a minimal node already keeps. Witnesses are served **cache-only** (typed out-of-window error on miss, never a history recompute).

Benefit: minimal nodes serve recent-block witnesses with zero commitment-history storage and no new code on the consensus commitment fold.

## Context (from discovery)
- **Repo/worktree:** `/Users/awskii/org/wrk/wt/witness-cache`, branch `awskii/witness-cache`. Language: Go.
- **Witness cache (existing on this branch):**
  - `rpc/jsonrpc/witness_cache.go` — `witnessResultCache = lru.Cache[common.Hash, *ExecutionWitnessResult]` **(a type ALIAS, `:30`)**, cap `witnessCacheMaxBlocks = 96` (`:25`), `newWitnessResultCache` (`:32`). Count-only today (no byte cap). Method call sites via promotion: `.Add`/`.Get`/`.Contains`/`.Len` at `witness_cache_builder.go:189/288/289`, `debug_execution_witness.go:767`.
  - `rpc/jsonrpc/witness_cache_builder.go` — `WitnessCacheShouldEnable(blocks uint, commitmentHistoryEnabled bool)` (`:124`), `NewWitnessCacheBuilderAPI` (`:134`), `RunWitnessCacheBuilder` (`:154`), `waitCommittedHead` (`:79`, opens a fresh committed RO tx after the head commits — **retained**, it is the committed ≥B tx for plain history), `decideCommittedHead` (`:56`), `shouldBuild` (`:70`), `buildAndCache` (`:247`).
  - `rpc/jsonrpc/debug_execution_witness.go` — `ExecutionWitness` (`:722`) + hard gate (`:743`), `serveFromWitnessCache` (`:758`), `buildWitnessResult` (`:786`), `detectCollapseSiblings` (`:1128-1204`) installs `NewSplitHistoryReader(tx, firstTxNumInBlock, endTxNum, false)` (`:1144`) — **commitment @ parent (firstTxNumInBlock), plain @ block-end (endTxNum)** — and asserts `computedRootHash == block.Root()` (`:1181`); `buildWitnessTrie` (`:1210`) installs `sdCtx.SetHistoryStateReader(tx, firstTxNumInBlock)` (`:1223`).
  - `rpc/jsonrpc/eth_call.go` — `GetWitness` (`:640`) / `getWitness` (`:648`, on `BaseAPI`/`APIImpl`, returns RLP `hexutil.Bytes`, canonical-only, **does not touch the cache**) + hard gate (`:674`); `getProof` (`:450`).
  - `node/eth/backend.go` — enable gate `:1190-1205`, `ReadDBCommitmentHistoryEnabled` (`:221`, `:1195`), sole `WitnessCacheShouldEnable` call site (`:1200`), witness header subscription `AddHeaderSubscription` (`:1208`).
  - `cmd/utils/flags.go` — `WitnessCacheBlocksFlag` = `--witness.cache.blocks` (`:447`). Config struct is `cmd/rpcdaemon/cli/httpcfg/http_cfg.go` (`HttpCfg.WitnessCacheBlocks:80`); embedded registration `node/cli/default_flags.go:100`; field set at `node/cli/flags.go:497`; standalone-daemon registration `cmd/rpcdaemon/cli/config.go:139`.
- **Commitment readers:** `execution/commitment/commitmentdb/reader.go` — `LatestStateReader` (`:30`, `NewLatestStateReader` `:41`, reads via `GetLatest`), `HistoryStateReader` (`:100`), `SplitStateReader` (`:181`, `NewCommitmentSplitStateReader(commitmentReader, plainStateReader, withHistory)` `:226`), `CommitmentReplayStateReader` (`:234`, `NewCommitmentReplayStateReader(ttx, tx, tsd, plainStateAsOf)` `:238` — commitment-from-latest ⊕ plain-from-history, the exact shape we need but with both bound to one tx). Install via `SharedDomainsCommitmentContext.SetCustomHistoryStateReader` (`commitment_context.go:164`).
- **Grounded facts driving the design** (memory `witness-minimal-node-headcapture`):
  - The header feed fires BEFORE the block's `sd.Commit` (`execution/execmodule/forkchoice.go:693-698`, dispatch-from-overlay pre-flush), so a tx already pinned for B-1 IS the parent(B) snapshot when B's header arrives.
  - At a tx pinned at end-of-(B-1), `GetLatest(CommitmentDomain, …)` equals parent(B) commitment for any key; commitment-domain `GetAsOf` at a past txNum is served only by history (`db/state/domain.go:1519-1523`, no latest fallback) and returns nil on minimal — so the pinned snapshot is the only viable parent-commitment source. **Plain** (account/storage/code) parent and block-end state come from the history minimal retains, exactly as the current build reads them.
  - The witness build must un-blind the read∪write union, so a tee on the canonical fold (which walks modified-key paths only) is insufficient and races default-on trie-warmup workers. The pinned-snapshot build avoids a tee entirely.

## Development Approach
- **Testing approach:** Regular (code first, then tests) — **every task includes its own tests before the next task starts**, scoped to what is verifiable at that task (no cross-task assertions parked behind a skip).
- Complete each task fully (code + tests passing) before the next.
- Small, focused changes; reuse the existing witness driver verbatim — **only the commitment-domain reader source changes** (parent commitment from the pinned tx instead of from history); plain-state reads are unchanged.
- Maintain backward compatibility: full/archive nodes with commitment history keep the existing recompute-on-miss behavior unchanged; head-capture is an opt-in mode selected once at construction.
- Update this plan when scope changes during implementation.
- **Comment policy:** put rationale (mmap-pin discipline, fail-closed reasoning) in commit messages, not source comments; enforce invariants in code where practical.

## Testing Strategy
- **Unit tests:** required every task — success + error/edge cases, table-driven where natural. Go: `go test ./rpc/jsonrpc/... ./execution/commitment/...`.
- **Parity diff helper (Task 9):** a **pure** function comparing two witness node sets by raw node bytes (a node's RLP is its identity), returning byte-identity + `erigon_only`/`reth_only` counts, unit-tested with synthetic witnesses — no datadir, no skip. The real dual-build comparison against a commitment-history datadir is a Post-Completion manual step that reuses this helper.
- **Race:** `go test -race` on the builder (Task 8).
- **No UI / e2e-Playwright surface.** The real minimal-node + reth-stateless strict-parity run is external (Post-Completion).

## Progress Tracking
- Mark completed items `[x]` immediately.
- New tasks: `➕` prefix. Blockers: `⚠️` prefix.
- Keep the plan in sync with actual work.

## Solution Overview
- **Mechanism — pinned parent snapshot for commitment only (decided; alternatives ruled out by code-grounded investigation):** hold a rolling RO temporal tx that lags the tip by one committed block; its commitment-latest is the parent(B) commitment plane. Combine it with the existing committed ≥B tx (from `waitCommittedHead`) that supplies plain account/storage/code history at both the parent (`firstTxNumInBlock`) and block-end (`endTxNum`) txNums. When block B's header arrives, the tx pinned for B-1 is the commitment-parent source; build B's witness, validate, cache by hash, then roll the pin forward to end-of-B for B+1.
- **Two txs per build; one reader change:** compose `NewCommitmentSplitStateReader(commitmentReader = LatestStateReader(pinnedParentTx), plainStateReader = HistoryStateReader(committedTx @ txNum), withHistory=false)`. This is a dual-tx variant of the existing `CommitmentReplayStateReader`. `detectCollapseSiblings` keeps plain @ `endTxNum`; `buildWitnessTrie`/read-set keep plain @ `firstTxNumInBlock`; both take commitment from the pinned parent latest. Everything else in `buildWitnessResult` is reused verbatim, including its own `WithSequentialCommitment` SharedDomains (which sidesteps the parallel-trie no-`Witnesses` gap).
- **No consensus-path code:** the build runs in the background builder goroutine with read-only txs; no tee/write on the canonical commitment context, so it cannot perturb the consensus root.
- **Validation unchanged and non-bypassable:** `witnessRoot == expectedParentRoot`, `computedRoot == expectedBlockRoot`, `verifyWitnessStateless` remain hard preconditions of `Add()`. An incomplete/stale view fails closed → block not cached → out-of-window. Never a wrong witness.
- **Serve cache-only:** in head-capture mode a miss returns a typed out-of-window error; by-hash requests are canonical-checked so a still-resident orphan isn't served as canonical. `eth_getWitness` (RLP, uncached) returns out-of-window in head-capture mode rather than recomputing.
- **Inherent, documented limitation:** tip-eager + cache-only. After restart the cache is empty and cannot be back-filled; it re-warms forward, so the last ~N blocks are out-of-window until N new blocks pass.

## Technical Details
- **Rolling pin state:** the builder holds at most one "parent" `kv.TemporalTx` plus its `(blockNum, blockHash)` identity, in addition to the per-build committed ≥B tx. On each accepted head it asserts the pinned tx's committed `Finish == B-1` and `ReadCanonicalHash(B-1) == pinnedHash`; builds; then rolls forward (open a new pin at end-of-B, close the old). On any canonical mismatch (reorg/unwind at or below the pin) it drops and re-pins from the current committed head.
- **Tx binding:** the build's SharedDomains binds to the committed ≥B tx (post-B txNum context); commitment reads are redirected to the pinned B-1 tx through the split reader; `SeekCommitment` resolves the parent patricia state (`KeyCommitmentState`) via the pinned tx's commitment-latest. This dual binding is the core new complexity and gets an explicit test.
- **mmap-pin discipline:** `BeginTemporalRo` → `AggregatorRoTx` refcount keeps all mmap views valid across merges while a tx is open (safe to hold ~1 block). Read ONLY through the txs; `common.Copy` any bytes retained past them (avoid the parallel-commitment use-after-munmap SIGSEGV). Do not `Rollback` mid-build.
- **Mode flag on the shared cache struct:** builder impl and serve impl read the same `headCapture`/`cacheOnly` fields so they cannot diverge.
- **Byte cap:** `--witness.cache.maxmb` becomes a real resident-bytes cap; eviction triggers on whichever of count (≤96) or bytes binds first. Release both txs immediately after each build.

## What Goes Where
- **Implementation Steps (`[ ]`):** all code, unit tests, and the pure parity diff helper + its synthetic unit test.
- **Post-Completion (no checkboxes):** real synced minimal-node run, dual-build byte-parity against a commitment-history datadir, reth-stateless strict-parity, L2 keep-up measurement, and PR creation — all require external systems/datadirs.

## Implementation Steps

### Task 1: Add head-capture and byte-cap flags

**Files:**
- Modify: `cmd/utils/flags.go`
- Modify: `cmd/rpcdaemon/cli/httpcfg/http_cfg.go`
- Modify: `node/cli/default_flags.go`
- Modify: `node/cli/flags.go`
- Modify: `cmd/rpcdaemon/cli/config.go`

- [x] add `WitnessCacheHeadCaptureFlag` (`--witness.cache.head-capture`, bool, default false) and `WitnessCacheMaxMBFlag` (`--witness.cache.maxmb`, uint, default 0 = count-only) next to `WitnessCacheBlocksFlag` in `cmd/utils/flags.go:447`
- [x] add `WitnessCacheHeadCapture bool` and `WitnessCacheMaxMB uint` to `HttpCfg` in `cmd/rpcdaemon/cli/httpcfg/http_cfg.go` (beside `WitnessCacheBlocks:80`)
- [x] register both flags in `node/cli/default_flags.go` (beside `&utils.WitnessCacheBlocksFlag:100`); set the fields in `node/cli/flags.go` (beside `:497`); register in the standalone daemon `cmd/rpcdaemon/cli/config.go:139` for flag parity (head-capture is embedded-only at runtime — document that the standalone daemon parses but does not act on it)
- [x] write tests: config parsing sets both new fields from flags (default + set); existing `witness.cache.blocks` behavior unchanged
- [x] run tests - must pass before next task

### Task 2: Convert the cache alias to a struct with mode + resident-bytes cap

**Files:**
- Modify: `rpc/jsonrpc/witness_cache.go`
- Modify: `rpc/jsonrpc/witness_cache_builder.go` (call-site updates if needed)
- Modify: `rpc/jsonrpc/debug_execution_witness.go` (call-site at `:767` if needed)
- Create/Modify: `rpc/jsonrpc/witness_cache_test.go`

- [x] convert `witnessResultCache` from a type alias to a wrapper struct embedding the `*lru.Cache[...]` pointer so promoted `.Add`/`.Get`/`.Contains`/`.Len` still resolve at `witness_cache_builder.go:189/288/289` and `debug_execution_witness.go:767`
- [x] add mode fields (`headCapture bool`, `cacheOnly bool`) and plumb through `newWitnessResultCache`; add accessors used by builder + serve (single source of truth, no duplicated booleans)
- [x] add resident-bytes accounting off `len(cachedJSON)` (result struct `debug_execution_witness.go:538-558`) with byte-cap eviction via `lru.NewWithEvict`; evict on whichever of count(≤96)/bytes binds first
- [x] write tests: byte-cap evicts when bytes exceed `maxMB` even below the count cap; count cap still applies when `maxMB==0`; eviction is oldest-out; a hash re-add refreshes recency; mode fields round-trip
- [x] run tests - must pass before next task

### Task 3: Dual-tx commitment-latest ⊕ plain-history split reader

**Files:**
- Modify: `execution/commitment/commitmentdb/reader.go`
- Modify: `execution/commitment/commitmentdb/reader_test.go` (create if absent)

- [ ] add a constructor that composes, via `NewCommitmentSplitStateReader` (`:226`), a `LatestStateReader` bound to a **pinned parent tx** (commitment domain only) with a `HistoryStateReader` bound to a **separate committed tx** at a caller-supplied `plainStateAsOf` txNum (account/storage/code) — a dual-tx variant of `CommitmentReplayStateReader` (`:234`); `withHistory=false`
- [ ] confirm `PutBranch` no-ops for this reader (history-mode false path) so the build's own SharedDomains discards branch writes on `Close`
- [ ] write tests: commitment reads resolve from the pinned tx's latest; plain reads resolve from the committed tx's history at the given txNum; two different `plainStateAsOf` values (parent vs block-end) route correctly; values returned are copies (no retained mmap alias)
- [ ] write tests: `WithHistory()==false`; unknown/empty keys behave as the underlying readers
- [ ] run tests - must pass before next task

### Task 4: Head-capture build path (swap only the commitment reader)

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Create/Modify: `rpc/jsonrpc/debug_execution_witness_test.go`

- [ ] add a head-capture variant of `buildWitnessResult` (`:786`) that takes both the pinned parent tx and the committed ≥B tx and installs the Task-3 dual-tx reader in `detectCollapseSiblings` (replacing `NewSplitHistoryReader` at `:1144`, keeping plain @ `endTxNum`) and in `buildWitnessTrie` (replacing `SetHistoryStateReader` at `:1223`, plain @ `firstTxNumInBlock`); commitment side reads the pinned parent latest in both phases
- [ ] bind the build's SharedDomains to the committed ≥B tx and confirm `SeekCommitment` resolves the parent `KeyCommitmentState` via the pinned tx's commitment-latest (the dual-tx binding)
- [ ] keep the accessed/read-set collection unchanged (re-execute B via history at `firstTxNumInBlock` on the committed tx) — only commitment sourcing changes
- [ ] keep the three validation gates non-bypassable; on any gate failure return an error so nothing is cached; guard the `ExecutionWitness` hard gate (`:743`) by head-capture mode so it is not hit on minimal
- [ ] write tests (task-verifiable only): the dual-tx reader is installed on both the collapse-detection and trie phases with the correct per-phase `plainStateAsOf` (assert reader composition/txNums); a build whose `expectedBlockRoot`/`witnessRoot` gate fails returns an error and yields no result
- [ ] run tests - must pass before next task

### Task 5: Rolling one-block-lag pin lifecycle (keep waitCommittedHead)

**Files:**
- Modify: `rpc/jsonrpc/witness_cache_builder.go`
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go` (create if absent)

- [ ] add rolling-pin state: at most one held parent `kv.TemporalTx` with `(num, hash)` identity; helpers to open a pin at the current committed head and to roll it forward
- [ ] KEEP `waitCommittedHead` (`:79`) — it supplies the committed ≥B tx for plain history (including block-end at `endTxNum`, which is unavailable pre-commit); the pinned B-1 tx supplies commitment-latest. Route both into the Task-4 build
- [ ] before trusting the pin as parent, assert its committed `Finish == B-1` and `ReadCanonicalHash(B-1) == pinnedHash`; keep the `ReadCanonicalHash(B) == wantHash` / `decideCommittedHead` (`:56`) gate so a losing-fork head is never cached; on canonical mismatch at/below the pin, drop and re-pin from the current committed head
- [ ] route `buildAndCache` (`:247`) through the head-capture build when mode is head-capture; release both txs immediately after each build, then roll the pin forward
- [ ] write tests: happy path consumes pinned parent + committed tx and rolls forward; `Finish==B-1` assertion rejects a mispinned tx; reorg drops and re-pins; the full head-capture `buildAndCache` populates the cache on success and caches nothing on gate failure (the cross-task cache assertion deferred from Task 4 lands here); tip jump >1 coalesces/skips → out-of-window, no crash (use a fake TemporalRoDB / in-mem fixture)
- [ ] run tests - must pass before next task

### Task 6: Enablement wiring (decouple from commitment history)

**Files:**
- Modify: `rpc/jsonrpc/witness_cache_builder.go`
- Modify: `node/eth/backend.go`
- Modify: `rpc/jsonrpc/witness_cache_wiring_test.go`

- [ ] change `WitnessCacheShouldEnable` (`:124`) to `(blocks uint, commitmentHistoryEnabled, headCapture bool) bool` = `blocks>0 && (commitmentHistoryEnabled || headCapture)`; update the sole production call site (`backend.go:1200`)
- [ ] in `node/eth/backend.go:1190-1205`, stop force-disabling when `ReadDBCommitmentHistoryEnabled` is false but head-capture is on; in that branch construct the rolling-pin builder and set the cache `cacheOnly=true`/`headCapture=true`; pass `WitnessCacheMaxMB` through `NewWitnessCacheBuilderAPI`/`newWitnessResultCache`
- [ ] preserve the full/archive path unchanged: commitment history present → existing recompute-on-miss + cache, `cacheOnly=false`
- [ ] update `TestWitnessCacheShouldEnable` in `rpc/jsonrpc/witness_cache_wiring_test.go` (this is the test that fails to compile on the 2→3-arg change) to the 8-combination truth table
- [ ] write tests: construction selects cache-only mode iff `headCapture && !commitmentHistory`; full/archive stays recompute-capable
- [ ] run tests - must pass before next task

### Task 7: Cache-only serve path + out-of-window + by-hash canonical check

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/eth_call.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`

- [ ] add a typed out-of-window error (two buckets: out-of-window vs reorged-away) returned by `ExecutionWitness` (`:722`) when `cacheOnly` is set and the cache misses — never fall through to the history recompute
- [ ] in `serveFromWitnessCache` (`:758`), require canonical on by-hash resolves (re-check `ReadCanonicalHash(num)==hash` against the serve tx before `Get`) so a still-resident orphan is not served as canonical; by-number serving unchanged
- [ ] `eth_getWitness` (`eth_call.go:648`): in head-capture mode return the typed out-of-window error (it is RLP/uncached and cannot recompute without history) rather than the current hard gate; leave the history-node behavior unchanged
- [ ] write tests: cache hit → served; cache miss in cache-only mode → out-of-window (no recompute); by-hash orphan → out-of-window; by-number → freshest canonical; full/archive mode still recomputes on miss (backward compat); eth_getWitness on head-capture → out-of-window
- [ ] run tests - must pass before next task

### Task 8: Reorg, fail-closed, and mmap-pin race tests

**Files:**
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`

- [ ] reorg unit test: a losing-fork block's pinned context is dropped at the canonical-hash gate and only the winning hash is cached; a by-hash request for a reorged-out height returns out-of-window
- [ ] fail-closed test: an incomplete/stale parent view fails `witnessRoot==expectedParentRoot` (or `expectedBlockRoot`) and the block becomes out-of-window — assert nothing is cached and no wrong witness is ever returned
- [ ] mmap-pin race/soak test: hold the rolling pin + committed tx while simulating snapshot merges / tip advance; assert consistent reads and no use-after-free; run under `go test -race`
- [ ] root-neutrality guard test: constructing/running the head-capture builder performs no writes on any canonical commitment context (read-only mode)
- [ ] run tests (including `-race` on the builder package) - must pass before next task

### Task 9: Pure witness-set parity diff helper

**Files:**
- Create: `rpc/jsonrpc/witness_parity.go`
- Create: `rpc/jsonrpc/witness_parity_test.go`

- [ ] add a pure exported helper comparing two witness node sets by raw node bytes (RLP = node identity), returning `{ byteIdentical bool, erigonOnly, rethOnly int, codesEqual, keysEqual bool }` — no datadir, no I/O, no skip
- [ ] write tests with synthetic witnesses: identical sets → `byteIdentical`, `erigonOnly==0`; a dropped node → `erigonOnly>0`; differing codes/keys flagged; empty/degenerate inputs handled
- [ ] run tests - must pass before next task

### Task 10: Verify acceptance criteria
- [ ] verify all Overview requirements: minimal node serves last-N witnesses, cache-only, out-of-window on miss, no commitment history required
- [ ] verify edge cases: reorg drop/re-pin, tip jump >1, restart (cold cache re-warms forward), byte-cap eviction, dual-tx binding
- [ ] run full unit suite: `go test ./rpc/jsonrpc/... ./execution/commitment/...`
- [ ] run `go test -race ./rpc/jsonrpc/...` on the builder
- [ ] `make lint` (or the repo lint target) clean

### Task 11: Update documentation
- [ ] document the two new flags and the head-capture mode (tip-only, cache-only, cold-after-restart) in the relevant flag/RPC docs
- [ ] update CLAUDE.md / notes only if a new load-bearing pattern was discovered
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring external systems/datadirs or manual action — informational only, no checkboxes.*

**Manual verification:**
- **Dual-build byte parity:** on a datadir that HAS commitment history, build each of a range of tip blocks BOTH ways (existing durable-history recompute vs the head-capture dual-tx path) and compare with the Task-9 helper; require `byteIdentical`, `erigonOnly==0`, equal codes/keys — with deliberate coverage of read-heavy blocks (SLOAD-only, BALANCE/EXTCODEHASH of untouched accounts, large static calls), the case a naive tee would drop. Run near the node (witnesses are ~20MB), not over a slow link.
- **Real minimal node run:** `--prune.mode=minimal --witness.cache.head-capture --witness.cache.blocks=N`; confirm recent-block witnesses serve, and out-of-window is returned outside the window and immediately after restart until re-warmed.
- **reth stateless strict parity:** feed cached witnesses to reth's stateless verifier (paradigmxyz/stateless); it must re-execute each block and reproduce the post-state root (the faithful oracle; Erigon's internal verify is weaker and only the pre-cache gate).
- **L2 keep-up measurement:** measure per-block head-capture build latency on a fast Arbitrum tip; confirm it stays under the slot interval and that skipped blocks degrade gracefully to out-of-window (the build must never move onto the exec goroutine).

**Fallback (only if rolling-pin + reorg re-pin bookkeeping proves operationally awkward):**
- Switch the commitment-parent source to **PutBranch pre-image capture**: record parent before-images of only the branches block B writes/deletes at the write seam (single-writer-safe under sequential commitment), plus a two-tier commitment reader (captured ring ⊕ `GetLatest`). Same witnesses, more moving parts. Contingency, not the primary path.

**External updates:**
- PR creation is manual and out of scope (terse, problem-first body; no Testing section; no Claude/Anthropic mentions or session links).
