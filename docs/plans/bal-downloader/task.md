# BAL Downloader Refactor — Task

## Problem

BAL = Block Access List (EIP-7928, activated at the Amsterdam/Glamsterdam fork). It is
an execution-layer sidecar that lets parallel execution skip the read/write-set discovery
phase. It is **best-effort for correctness**: when a block's BAL is absent, execution
recomputes it and validates the result against `header.BlockAccessListHash`, so a missing
BAL only costs efficiency, never correctness.

The current downloader (`p2p/sentry/sentry_multi_client/bal_downloader.go`) is a background
daemon driven by a 10s ticker. Each pass it scans ~256 blocks back from head, finds headers
with `BlockAccessListHash != nil` and no stored BAL, and fetches them over eth/71, writing
them directly to the chain DB via its own `kv.RwDB` handle.

### Criticisms of the current design

1. **Timing mismatch during sync/catch-up.** Blocks are inserted in large batches (engine
   block downloader: batches of 500 up to `LoopBlockLimit`, e.g. 5000) and executed shortly
   after via forkchoice. A 256-block / 10s ticker cannot keep its window aligned with what is
   being inserted and executed. During initial sync the BAL is almost never present in time,
   so the fetched BALs provide ~zero speedup and the network downloads are largely wasted.
2. **Second DB writer.** The downloader owns a `kv.RwDB` and writes BALs directly. MDBX is
   single-writer; this contends with the execution pipeline, which should be the sole writer.
   `ExecModule` is meant to coordinate the one-RwTx-at-a-time discipline.
3. **Code location.** BAL p2p logic is split between `p2p/protocols/eth` (wire) and
   `p2p/sentry/sentry_multi_client` (fetch + daemon), separate from the modern EL block-download
   p2p stack in `execution/p2p`.

At chain tip the BAL arrives from the CL via the Engine API `NewPayload` (`req.BlockAccessList`),
so the ticker is only relevant to historical/sync ranges — precisely where it underperforms.

## Proposal

Move BAL downloading out of the background ticker and into the block-download data path, so a
block's BAL travels with it and is present by the time the block is inserted/executed.

1. **EL devp2p path (`execution/p2p/bbd` `BackwardBlockDownloader`).** When downloading blocks
   from devp2p as an EL, for post-Amsterdam headers also fetch the corresponding BAL **in parallel**
   with bodies, **best-effort, from the same peer** that served the body, within ~the body-download
   timeout. Header + body + BAL flow together via `BbdResultFeed`; add a BALs field to
   `BlockBatchResult`. If the BAL doesn't arrive in time, proceed without it.
2. **No RwDB dependency.** BALs ride into `InsertBlocks` (which already persists
   `RawBlock.BlockAccessList`) instead of being written by a separate DB owner.
3. **Caplin internalcl path.** When Caplin runs as an internal CL in the same process as the
   Erigon EL, its beacon-block downloader does not exercise `bbd`. Reuse the **same BAL fetcher
   component**: for post-Gloas beacon blocks, fetch their BALs in parallel and feed them to the
   `ExecModule` via `InsertBlocks`. Caplin must distinguish its run mode (internalcl vs externalcl)
   so it only decorates with BALs when it is the in-process CL for an Erigon EL.
4. **Externalcl path.** When Caplin runs as a separate process talking to an Erigon EL via the
   Engine API, the EL handles BALs through its own `bbd` path — Caplin does nothing extra.
5. **Consolidation.** Move BAL p2p code into the `execution/p2p` package.

Note: BALs are not stored on the consensus layer and are not downloadable via the beacon chain,
which is why the in-process EL's eth/71 fetcher must supply them for the Caplin internalcl case.

## Run modes to support

| Mode | CL → EL transport | Block download path | BAL source |
|------|-------------------|---------------------|------------|
| Erigon EL + external CL (e.g. Lighthouse) | Engine API | `engine_block_downloader` → `bbd` | tip: `NewPayload`; history: `bbd` eth/71 fetch (new) |
| Erigon EL + internal Caplin (one binary) | in-process `ExecutionClientDirect` | Caplin beacon downloader → `InsertBlocks` | tip: recompute; history: Caplin eth/71 fetch (new) |
| Caplin externalcl → some Erigon EL | Engine API | the EL's own `bbd` | the EL handles it |

## Deliverables

- `task.md` (this file) — prompt summary.
- `design.md` — net-improvement analysis + implementation plan.
- `testing.md` — test strategy.

## Test strategy (high level)

- **EL-only first:** `bal-devnet-7` (via the `/launch-devnet` skill) with Erigon as a pure EL
  (`--externalcl` + external CL); confirm it syncs and fetches BALs over eth/71 during backfill.
  Requires `ERIGON_EXEC3_PARALLEL=true` (serial exec hard-errors at the Amsterdam fork block).
- **Caplin variants:** a local Kurtosis devnet (templates under `.github/workflows/kurtosis/`,
  driven by `test-kurtosis-assertoor.yml` / `glamsterdam.io`). Extend to **two nodes** — one to
  progress the chain and have BALs ready, a second joining later to download BALs alongside the
  beacon blocks with Caplin as an internal CL — to exercise both internalcl and externalcl flows.
