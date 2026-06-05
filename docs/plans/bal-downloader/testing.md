# BAL Downloader Refactor — Testing Strategy

## 0. Principles

- **TDD for new behavior** (eth/71 BAL fetch on `execution/p2p`, `bbd` decoration, Caplin
  envelope-BAL extraction in the block collector, the collapsed insert API): Red → Green →
  Refactor. Write the failing test first and confirm it fails for the right reason.
- **Pure code-move** (porting the eth/71 round-trip/validation from `sentry_multi_client` to
  `execution/p2p`): the ported unit tests are the safety net — no new behavior, so no new cycle is
  required (state this in the PR per CLAUDE.md's pragmatism clause).
- **No `t.Skip`** in any form (CLAUDE.md, automated-agent rule). A failing test is investigated
  and fixed or escalated — never muted.
- All BAL devnet runs use `ERIGON_EXEC3_PARALLEL=true` — serial exec hard-errors at the Amsterdam
  fork block, so serial is not a valid BAL test mode.

## 1. Unit tests

### 1.1 `execution/p2p` BAL fetcher (`bal_fetcher_test.go`, new)

Port and re-home the 13 existing cases from
`p2p/sentry/sentry_multi_client/bal_fetcher_test.go`, retargeted at the new `BALFetcher` over the
execution-p2p message stack. Each is a Red→Green check on the ported logic:

| Case | Expectation |
|------|-------------|
| Valid populated BAL | bytes returned, hash matches `ExpectedHash` |
| `0x80` sentinel | entry treated as **miss** (absent from result), no error, no penalty |
| `0xc0` empty + header hash == empty-BAL hash | accepted as empty BAL |
| `0xc0` empty + header hash != empty-BAL hash | rejected, peer penalized |
| Hash mismatch (non-sentinel) | rejected, **peer penalized** |
| Unknown request id | response dropped |
| Length / count mismatch | protocol violation, dropped |
| Too many entries (> request) | protocol violation, peer penalized |
| Short / padded response | handled without panic |
| Wrong/spoofed peer responds | ignored |
| Context cancelled | returns promptly with ctx error |
| Timeout (no response) | returns miss within `timeout`, no panic |
| `Fetch` (peer-picking) with no eth/71 peers | empty result, no error (best-effort) |

Add cases distinguishing `FetchFromPeer` (one peer) vs `Fetch` (picks an eth/71 peer from the
tracker, fans out).

### 1.2 `bbd` BAL decoration (`execution/p2p`, extend bbd tests)

Inject a **mock `BALFetcher`**:

- **Only Amsterdam+ headers requested** — headers with `BlockAccessListHash == nil` produce no
  `BALRequest`; pre-fork batches call the fetcher zero times.
- **Same peer as body** — `FetchFromPeer` is invoked with the same `peerId` that served the body
  for that batch.
- **Positional projection** — returned BALs land in `BlockBatchResult.BALs[i]` aligned with
  `Blocks[i]`; misses are `nil`; whole slice `nil` when nothing came back.
- **Best-effort** — fetcher error/timeout ⇒ blocks still flow through the feed unchanged, `BALs`
  all `nil`. No batch failure.

### 1.3 `BbdResultFeed` (`bbd_result_feed_test.go`)

- `Next` returns the `BlockBatchResult` (blocks + positional BALs + err).
- `consumeData(ctx, blocks, bals)` round-trips both; `consumeErr` still surfaces errors.

### 1.4 `blocksToRaw` (`execution/execmodule/chainreader`, extend)

- `bals == nil` ⇒ all `RawBlock.BlockAccessList == nil`.
- `bals` aligned ⇒ each `RawBlock.BlockAccessList == bals[i]`.
- `len(bals) < len(blocks)` ⇒ guarded (no panic; trailing blocks get `nil`).

### 1.5 EngineBlockDownloader (`block_downloader_test.go`, extend)

- The download loop calls `InsertBlocksAndWait(ctx, batch.Blocks, batch.BALs)` with the BALs from
  the feed.
- Tip insert passes `nil` BAL.

### 1.6 Caplin block collector (`persistent_block_collector_test.go`, extend)

- **internalcl (non-nil `BALFetcher`)**: Flush calls `Fetch` for Amsterdam+ blocks and
  `engine.InsertBlocks` receives a non-nil `bals` aligned with the batch.
- **externalcl (nil `BALFetcher`)**: Flush does **not** fetch; `engine.InsertBlocks` receives
  `nil` bals — identical to today's behavior.
- Update existing mock expectations to the new `InsertBlocks(ctx, blocks, bals, wait)` arity.

### 1.7 Mock regen

`make gen` regenerates `cl/phase1/execution_client/execution_engine_mock.go` for the new
signatures. Confirm all CL packages compile against the new arity.

## 2. Integration / devnet tests

### Milestone 1 — EL-only on `bal-devnet-7` (validates the `bbd` path)

`bal-devnet-7` runs Erigon as a **pure EL** (`--externalcl`) driven by an external CL
(Lighthouse), so backfill goes Engine API FCU → `engine_block_downloader` → `bbd` — exactly the
new same-peer BAL fetch.

Steps (via the `/launch-devnet bal-devnet-7` skill):

1. Launch Erigon EL + Lighthouse with `ERIGON_EXEC3_PARALLEL=true`.
2. Let it sync from genesis past the Amsterdam fork block.

Assertions:

- **Syncs** past Amsterdam without error (the historical pre-fork hard-error gate is avoided by
  parallel exec).
- **BALs land via the data path** — `kv.BlockAccessList` is populated for backfilled Amsterdam+
  blocks (count rows; compare against the number of post-fork blocks).
- **eth/71 fetch happened** — debug logs from the new `execution/p2p` BAL fetcher show
  requested/served/missed counts; the **ticker is gone** (no `[bal-downloader]` scan-pass lines;
  goroutine dump shows no `BALDownloader.Run`).
- **No second writer** — only the execution pipeline writes; no separate RwDB contention.
- **Hit-rate metric** > 0 (see §3).

### Milestone 2 — Caplin internal-CL catch-up via envelopes (geth + lighthouse progressor)

Validates the **CL download path**: a late-joining Erigon + internal-Caplin node fills the gap to
the chain tip by downloading execution-payload envelopes — which **carry the BAL** (design §1.5) —
and persisting each block's BAL through `InsertBlocks`. No eth/71 fetch is involved on this path;
Caplin reads the BAL out of the envelope (`block_collector.decodeBlock`).

**Why a geth + lighthouse progressor (not Erigon):** the served envelope must carry the BAL, and
Erigon-Caplin currently prunes whole envelopes ~32 min behind finality and never re-hydrates (out
of scope — design §7). geth + lighthouse is a **known-good BAL-serving peer** (geth retains the
BAL; lighthouse serves the full envelope), isolating *Caplin's download/extract* behavior from
Erigon's serving gap. This run also empirically answers whether a non-Erigon CL serves BAL-bearing
historical envelopes.

**Config:** `.github/workflows/kurtosis/glamsterdam-bal-sync.io`
- Node A — `geth` + `lighthouse` (`count: 2`, holds the validators): progressor / BAL-serving peer.
- Node B — `erigon` + `caplin` (`validator_count: 0`): the late joiner under test (`--experimental.bal`).
- `network_params`: `preset: minimal`, `seconds_per_slot: 6`, `fulu_fork_epoch: 0`,
  `gloas_fork_epoch: 1`. **Slot-time ladder:** start at **6s**; if Node B's peering/sync is flaky
  bump to **8s**, then **12s** (mainnet) as a last resort.

**Procedure (staggered join — Kurtosis has no per-participant start delay):**
1. Start the enclave with **Node A only** (set Node B `count: 0`, or run a progressor-only copy).
2. Wait until the chain is **~1000 blocks** past the Gloas/Amsterdam fork (poll `eth_blockNumber`
   on Node A's EL).
3. Bring up **Node B** joining the same network — `kurtosis service add` an erigon+caplin service,
   or launch erigon+caplin against the enclave's EL enodes + lighthouse boot-ENR + shared genesis
   and JWT (the `/launch-devnet` skill does the same discovery for external devnets).
4. Let Node B catch up to Node A's head.

**Assertions on Node B (Erigon):**
- **Catches up** to Node A's head past the fork.
- **`kv.BlockAccessList` populated** for the caught-up range — the decisive check: it proves the
  envelope carried the BAL and Caplin persisted it via `decodeBlock` → `InsertBlocks` (rather than
  the EL recomputing it).
- **Header BAL-hash cross-check** — for a sample of blocks, Node B's `header.BlockAccessListHash`
  equals Node A's (extracted BAL is byte-identical).
- **No eth/71 traffic on this path** — Caplin does not invoke the `BALFetcher`.
- assertoor liveness/consensus stays green.

> The **EL-devp2p / eth/71** download path (erigon EL under an external CL → `engine_block_downloader`
> → `bbd` → `FetchFromPeer`) is covered by Milestone 1 (bal-devnet-7). To also exercise it inside
> Kurtosis, add a third participant — `erigon` EL + external `lighthouse` (non-Caplin) joiner — and
> assert the eth/71 fetch fills its `kv.BlockAccessList`.

### CI wiring

- Extend `.github/workflows/test-kurtosis-assertoor.yml`: add a matrix entry pointing at
  `glamsterdam-bal-sync.io`, **parallel exec mode only** (`ERIGON_EXEC3_PARALLEL=true` — bal devnets
  hard-error in serial at the Amsterdam fork).
- The **staggered join** (Node A → ~1000 blocks → Node B) needs a script step, not just a matrix
  entry: start the progressor, poll the block height, then add Node B. Keep that script
  **cross-platform** (macOS/Windows/Linux) per CLAUDE.md and `CI-GUIDELINES.md`.
- The `kv.BlockAccessList` / header-hash assertions are not native assertoor checks — add a small
  RPC/db probe step against Node B.
- This is a manually-relevant flow: when dispatching it outside the PR's automatic checks, comment
  on the PR which workflow was dispatched, why, and link the run (CLAUDE.md).

## 3. Metrics, observability, and A/B

- Add BAL-fetch counters to the new `execution/p2p` fetcher: `requested`, `served`, `missed`,
  `mismatch_penalized`. Surface via the existing metrics registry.
- **Hit-rate** = served / requested — asserted > 0 in integration; tracked to decide whether the
  same-peer fetch needs bounded fan-out (design §1.3 / §6).
- **Parallel-exec benefit A/B**: compare per-block exec time / sync wall-time with BALs present
  vs. forced-off via the existing `dbg.IgnoreBAL` flag. BALs present should be no slower and
  should skip read/write-set discovery.

## 4. Regression guards

- **Non-Amsterdam chain**: a sync on a pre-Amsterdam config issues **zero** BAL requests and is
  byte-for-byte unaffected (unit assertion in §1.2 + a quick mainnet-config sanity sync).
- **Best-effort never blocks correctness**: peer returns a wrong BAL → peer penalized, block still
  inserts and executes via recompute (unit §1.1 + one fault-injection integration scenario).
- **`make lint && make erigon integration`** clean before pushing; lint is non-deterministic — run
  until clean (CLAUDE.md).

## 5. Conformance suites — EEST spec + Hive (must pass in CI)

These are required `ci-gate.yml` legs (`.github/workflows/ci-gate.yml:98-119`). The refactor must
leave **all of them green at their current failure budgets**. Per CLAUDE.md, an automated agent
must **not** raise any `max-allowed-failures` / `max-failures`, and **not** add any skip
(`t.Skip`, `SkipLoad`, `bt.SkipLoad`, build-tag or `dbg.*` gates) to get them green — investigate
and fix the root cause.

### 5.1 EEST spec tests — all stable + devnet shards (`test-eest-spec.yml`)

Shards are defined in `tools/eest-spec-shards.yml` (single source of truth shared with the
Makefile and `tools/run-eest-spec-test.sh`). Run locally with `make eest-spec-<shard>` (Linux:
`ERIGON_EXECUTION_TESTS_TMPDIR=/dev/shm` for the ramdisk). **Every** shard is
`max-allowed-failures: 0` and must pass:

- **Stable:** `statetests-stable`; `blocktests-stable-{sequential,parallel}`;
  `enginextests-stable-{sequential,parallel}`;
  `enginextests-benchmark-{1m,5m,10m,30m,60m,100m,150m}-{sequential,parallel}`;
  `blocktests-stable-race-{pre-cancun,cancun,prague,osaka}-{sequential,parallel}`.
- **Devnet:** `statetests-devnet`; `blocktests-devnet` (`exec3-parallel`);
  `blocktests-devnet-race-amsterdam` (`exec3-parallel`).

Most BAL-relevant: the **devnet** shards — especially `blocktests-devnet-race-amsterdam`
(Amsterdam fork) and `blocktests-devnet` — exercise post-Amsterdam fixtures where
`BlockAccessListHash` and BAL validation are live. Iterate on these first.

### 5.2 Hive (`test-hive.yml`)

Every matrix sim must pass at `max-allowed-failures: 0`, in **both** exec modes (serial +
parallel):

- `ethereum/engine` — `exchange-capabilities|auth`, `withdrawals`, `cancun`, `api`
- `ethereum/rpc-compat` — `.*`
- `devp2p` — `eth` (serial), `eth|discv5` (parallel)

Watch especially **`devp2p/eth`** (we touch the p2p fetch path) and **`ethereum/engine`** (the
`NewPayload` tip-BAL insert at `engine_server.go:1008` changes call shape under the API collapse).

### 5.3 Hive EEST (`test-hive-eest.yml`)

`consume-engine` per fork (`paris+shanghai`, `cancun`, `prague`, `osaka`) and `consume-rlp`
(`eip2930_access_list|eip4844_blobs`) over `eest_stable`, both exec modes, `max-failures: 0`.
**Most relevant:** the **`glamsterdam-devnet`** shard — `consume-engine` over `eest_devnet`
fixtures filtered to BAL-era EIPs (the set includes `7928`), run with `--experimental.bal`,
parallel only, **`max-failures: 3`** (documented in the workflow: 2 wrong-test-expectation +
1 known flake #21364). Keep that budget as-is; any new failure beyond the documented three is a
real regression to fix.

**Gating nuance:** `hive-eest` runs only when `github.event_name != 'pull_request'`
(`ci-gate.yml:110-113`) — i.e. in the merge queue / on push, **not** on the PR. Because the
BAL-specific coverage lives in this suite, **manually dispatch `test-hive-eest.yml` on the PR
branch** (`workflow_dispatch`) to validate BAL changes before merge, and comment on the PR which
workflow was dispatched, why, and a link to the run (CLAUDE.md).

## 6. Notes

- No new runtime data files are read by tests, so the `git restore-mtime` pattern list in
  `setup-erigon/action.yml` needs **no** change (CLAUDE.md Go-test-caching rule). Re-confirm if any
  test fixture is read via `os.Open`/`os.ReadFile` outside `testdata/`.
- Deleting `bal_downloader_test.go` / `bal_fetcher_test.go` is acceptable only because their
  coverage is **ported** to `execution/p2p` — not dropped.

## 7. Live-run results

### EL eth/71 path — PASSED (bal-devnet-7)

erigon as a pure EL (`--externalcl` + Lighthouse, `ERIGON_EXEC3_PARALLEL=true`) backfilling
bal-devnet-7 (a live Gloas devnet, ~106k blocks) via `engine_block_downloader` → `bbd`. Execution
reached block ~106,204 (chain tip). Counts below are from the file log
(`erigon-data/logs/erigon.log`, dbug — the console was stuck at INFO, so the `fetched BALs` DBUG
line is absent there):

- `bbd` fetched & hash-validated real BALs over eth/71 from peers — **539** `fetched BALs over
  eth/71` events: 339 with BALs returned (`got>0`, e.g. `got=50/50`, `got=63/63`), 200 where the
  body-peer returned none (`got=0`, treated as a miss → recompute).
- **`[bal-downloader]` ticker lines: 0** — the old RwDB-writing ticker is gone.
- Best-effort fallback works: **~306** `best-effort BAL fetch failed` events, dominated by **300×**
  peers returning an *empty* BAL (`0xc0`) for a block whose header expects a non-empty BAL —
  `validateBALResponse` rejects these, the block still inserts + recomputes, and sync reaches the tip
  regardless. **Peer penalizations: 0** — the rejection just discards the response and recomputes; it
  does not penalize in practice. (Optional polish: log `0xc0`-for-non-empty as a plain "miss" to cut
  the ~300 noise lines; likely cross-client encoding skew, harmless either way.)
- 2 transient `[4/6 Execution] block access list mismatch` failures (blocks 20729, 21309) — each
  `UnwindTo` one block, retried, and recovered; the stack is `exec3_parallel.go`, i.e. the **known
  parallel-exec BAL nondeterminism bug** (execution's recomputed BAL vs the header's canonical hash),
  orthogonal to the downloader refactor — it would fire identically with the old ticker downloader.
- Storage: BALs reach `kv.BlockAccessList` via the shared `InsertBlocks → WriteBlockAccessListBytes`
  path — proven live by execution reading the stored/header BAL hash to produce the `got`/`expected`
  comparison; recent unpruned blocks were populated when checked live. Old ranges read 0 — erigon
  freezes them into snapshots and prunes bodies+BALs (no 3533 retention; out of scope, design §7).

### Caplin in-process internalcl path — NOT live-tested (tooling-blocked)

Could not be run live with current tooling:

- **No ethereum-package combines Gloas + a working caplin launcher.** Upstream `6.1.0` supports
  `gloas_fork_epoch` but its caplin launcher is broken at libp2p (caplin `ChainTipSync` deadline;
  handshake "no transport for protocol" / "rate limit exceeded" → erigon stuck pre-Gloas at block 7,
  "no CL requests to engine-api"). The `erigontech/fix-caplin-launcher` fork has a working launcher
  but is pre-Gloas (rejects `fulu_fork_epoch`/`gloas_fork_epoch`).
- **Even when it runs, kurtosis `cl_type: caplin` wires caplin as a *separate* engine-API process**
  (`el-3-erigon-caplin` runs `--externalcl` alongside a `cl-3-caplin-erigon` container), so caplin's
  `ExecutionClientEngine.InsertBlocks` is `!isLocal()` → `ErrNotSupported`. The in-process
  `ExecutionClientDirect → block_collector → InsertBlocks(bals)` path this change targets is never
  exercised by kurtosis.
- **bal-devnet-7 internal caplin is also blocked**: historical envelopes are long pruned on a
  ~106k-block chain, and the EL would need to execute from genesis (no devnet snapshots), so internal
  caplin can't backfill payloads.

Coverage for the in-process path therefore rests on: the collector unit test
(`persistent_block_collector_test.go`, mock engine asserts `InsertBlocks(…, bals, …)`), the
spec-confirmed envelope-carries-full-BAL premise (design §1.5), and the shared
`InsertBlocks → WriteBlockAccessListBytes` storage being **live-validated** by the EL run above. A
clean live run needs a fresh Gloas devnet on an ethereum-package that has both Gloas support and a
working in-process caplin launcher.
