# BAL Downloader Refactor — Design

## 1. Verdict: is this a net improvement?

**Yes — clear net improvement.** The criticisms are accurate and the proposed direction
fits the existing code with surprisingly little new plumbing.

### 1.1 The criticisms hold

- **Timing mismatch is real.** During backfill the engine block downloader inserts in batches
  of `min(500, LoopBlockLimit)` and only executes once `insertedBlocksWithoutExec >=
  LoopBlockLimit` (`execution/engineapi/engine_block_downloader/block_downloader.go:226-268`).
  The head therefore jumps by thousands of blocks per execution cycle, while the ticker scans a
  256-block window every 10s (`p2p/sentry/sentry_multi_client/bal_downloader.go:71-109,176-226`).
  The window cannot track what is being inserted/executed, so during the exact phase where
  BAL-assisted parallel execution would help most (catch-up), the BAL is almost never present in
  time. Execution then recomputes the BAL anyway (`execution/stagedsync/bal_create.go:50-153`,
  `execution/stagedsync/exec3.go:598-614`), so the network fetches are largely wasted.

- **Second DB writer is real.** The ticker owns a `kv.RwDB` and writes BALs directly
  (`bal_downloader.go:52-69,296-310`). MDBX is single-writer; this contends with the execution
  pipeline, which is supposed to be the sole writer that coordinates the one-RwTx-at-a-time
  discipline.

- **Split code location is real.** BAL fetch + daemon live in `p2p/sentry/sentry_multi_client`,
  away from the modern EL block-download stack in `execution/p2p`.

### 1.2 The proposal fits existing infrastructure (key de-risking finding)

The "carry a BAL into the insert path" plumbing **already exists end to end**:

- `types.RawBlock` already has `BlockAccessList []byte` (`execution/types/block.go:745-751`).
- `ExecModule.InsertBlocks([]*types.RawBlock)` already persists it
  (`execution/execmodule/inserters.go:141-148` → `rawdb.WriteBlockAccessListBytes`).
- `ChainReaderWriterEth1` already carries BALs into that path: the insert→`RawBlock` conversion
  `blocksToRaw(blocks, accessLists)` already attaches `rb.BlockAccessList`
  (`execution/execmodule/chainreader/chain_reader.go:268-281`). The tip `NewPayload` path already
  uses it (`execution/engineapi/engine_server.go:1008`).

Today the sync path simply calls the **no-BAL** insert
(`block_downloader.go:239` → `InsertBlocksAndWait`). The refactor mostly amounts to *supplying the
BALs that the existing insert path already persists*. Because the only writers of
`WriteBlockAccessListBytes` are the ticker (removed) and `InsertBlocks`, routing BALs through
`InsertBlocks` also keeps ExecModule the **sole writer** — directly resolving criticism #2.

(The current code spells this with `…WithAccessLists` method variants taking a
`map[common.Hash][]byte`. Per the decision in §1.4 we collapse those away — see Step 4.)

### 1.3 The one genuine trade-off (eth/71 / EL-devp2p path only)

This applies **only** to the eth/71 path; the Caplin path gets the BAL guaranteed-present in the
envelope (§1.5) and has no such trade-off. Tying the eth/71 BAL fetch to the body peer within the
body timeout can yield a **lower BAL hit-rate** than a patient background fetcher that retries
across many peers — the body peer may simply not hold the separately-distributed BAL. This is
acceptable because:

1. BAL is best-effort for correctness — a miss costs only the recompute that already happens today.
2. The ticker's "patience" did not help during catch-up anyway (it never kept up).
3. If measured hit-rate is poor, the same-peer fetch can fan out to a bounded set of other
   eth/71 peers within the batch window (a tunable, not a redesign).

### 1.4 Decisions taken (from clarification)

- **Sequencing:** single combined effort — EL (`bbd`) path **and** Caplin internalcl path land together.
- **Historical BAL availability / serving:** rely on `WriteBlockAccessListBytes` inside
  `InsertBlocks`. Whatever the EL `bbd` path or Caplin-internalcl fetches and passes to
  `InsertBlocks` is persisted. **No separate persistence of execution-recomputed BALs.**
- **Code-move scope:** move the **fetch/client + orchestration** into `execution/p2p`. Keep the
  eth/71 wire packet types and the serving handler (`AnswerGetBlockAccessListsQuery`) in
  `p2p/protocols/eth` with the protocol/capability negotiation.
- **Insert API shape:** remove the `…WithAccessLists` method variants. `InsertBlocks` /
  `InsertBlocksAndWait` / `InsertBlockAndWait` (and Caplin's `ExecutionEngine.InsertBlocks` /
  `InsertBlock`) **always** take a positional `bals [][]byte` aligned with `blocks` — `nil` when
  there are none, `bals[i] == nil` for an individual miss. One name, BALs always present in the
  signature.

### 1.5 BAL distribution model and the download-flow split (clarified after EIP-7928 + client review)

EIP-7928 + the Gloas/ePBS consensus-specs + the clients settled *how the BAL travels*, and it
splits the **download** flow cleanly in two.

**The BAL rides inside the execution-payload envelope (CL path).** In Gloas/ePBS the BAL is a
full-bytes field of the SSZ `ExecutionPayload` (`block_access_list: ByteList[MAX_BYTES_PER_TRANSACTION]`),
so the envelope a CL gossips/serves carries the full BAL. There is **no** blinded/root-only wire
form — pruning bytes → `hash_tree_root` is a *local-storage* option only, and a served BAL-less
envelope would fail HTR/signature verification. Confirmed in consensus-specs
(`specs/gloas/beacon-chain.md`), Prysm (`ExecutionPayloadGloas.block_access_list`), and Erigon
(`cltypes.Eth1Block` SSZ schema); Lighthouse is spec-forced (the Amsterdam EL rejects `newPayload`
without the BAL, so it cannot interop without carrying it). EIP-7928 also has the **EL retain BALs
for the weak-subjectivity period (`3533 epochs`)** and exposes
`engine_getPayloadBodiesBy{Range,Hash}V2` so a CL that pruned the bytes re-hydrates them from its
EL before serving.

**Consequence — two download paths, two sources, no overlap:**

| Path | When | BAL source |
|------|------|-----------|
| Caplin internal-CL catch-up | erigon EL + in-process Caplin | **extract from the downloaded envelope** (`block_collector.decodeBlock` reads `payload.BlockAccessList`; `RlpHeader` derives the header hash from it) — no fetch |
| EL devp2p backfill | erigon EL under an external CL (`engine_block_downloader` → `bbd`) | **eth/71 `GetBlockAccessLists`** (EIP-8159) — eth/68 bodies carry no BAL |

So the earlier notion of having Caplin "decorate" blocks via an eth/71 fetch is **not needed**: the
envelope already carries the BAL, and a valid BAL-less envelope cannot exist. The `BALFetcher`
(eth/71) stays as the EL↔EL mechanism for the devp2p path only.

**Explicitly out of scope for this change** (they affect *serving*, not the download flow — see §7):

- **EL 3533-epoch BAL retention** — Erigon today deletes the BAL with the body (`DeleteBody` /
  `PruneBlocks`); there is no WSP-aligned retention. `…V2` already returns the BAL when present.
- **Caplin serving-side re-hydration** — Erigon's Caplin prunes whole envelopes ~32 min behind
  finality and never calls the `…V2` methods, so it cannot currently serve old envelopes. Because
  of this the sync test (testing.md) uses a **geth + lighthouse** progressor as a known-good
  BAL-serving peer instead of relying on an Erigon peer to serve historical envelopes.

---

## 2. Target architecture

The `execution/p2p.BALFetcher` performs the eth/71 round-trip + EIP-8159 validation. It is used by
the **EL devp2p path only** (`bbd`); Caplin reads the BAL from the envelope it already downloads
(§1.5) and does not use the fetcher.

```
                       ┌─────────────────────────────────────────────┐
                       │  execution/p2p.BALFetcher  (NEW)              │
                       │  • FetchFromPeer(reqs, peerId, timeout)       │  ← bbd: reuse the body peer
                       │  • EIP-8159 sentinel + keccak256 validation   │
                       │  • penalize peer on hash mismatch / violation │
                       │  uses MessageSender + MessageListener +       │
                       │       PeerTracker + PeerPenalizer (exec p2p)  │
                       └───────────────┬───────────────┬──────────────┘
                                       │               │
        ┌──────────────────────────────┘               └───────────────────────────┐
        │ EL + external CL                                          Erigon internal Caplin
        ▼                                                                          ▼
 bbd.downloadBlocksForHeaders                                  block_collector (GLOAS FULL blocks)
   fetch bodies from peer P  ──parallel──▶ FetchFromPeer(P)        decodeBlock reads BAL out of the
   build BlockBatchResult{Blocks, BALs [][]byte}                  execution payload envelope (no p2p)
        │                                                                          │
        ▼                                                                          ▼
 EngineBlockDownloader.downloadBlocks                          ExecutionEngine.InsertBlocks(blocks, bals, wait)
   InsertBlocksAndWait(blocks, bals [][]byte)                    → ChainReaderWriterEth1.InsertBlocks(…, bals)
        │                                                                          │
        └───────────────────────────┬──────────────────────────────────────────────┘
                                     ▼
                ExecModule.InsertBlocks([]RawBlock)  → rawdb.WriteBlockAccessListBytes (sole writer)
                                     ▼
                execution reads BAL via rawdb.ReadBlockAccessListBytes (overlay/DB); recompute on miss
```

Caplin needs no `BALFetcher` in **either** mode: as internal-CL it reads the BAL from the envelope
it downloads (§1.5); as externalcl talking to an Erigon EL it drives that EL's own `bbd` path (and
the EL gets tip BALs via `NewPayload`). So the fetcher is wired into `bbd` only and exposes a single
`FetchFromPeer` method (the earlier peer-picking `Fetch` variant was removed once the Caplin-fetch
idea was dropped).

---

## 3. Implementation plan

Ordered steps within the single combined effort. TDD applies to new behavior (steps 2, 4, 5, 6);
steps 1, 7, 8 are largely a code move where existing tests are the safety net (per CLAUDE.md
pragmatism clause — state this in the PR).

### Step 1 — Shared `BALFetcher` in `execution/p2p`

New file `execution/p2p/bal_fetcher.go`. Port the EIP-8159 round-trip + validation from
`p2p/sentry/sentry_multi_client/bal_fetcher.go`, rebuilt on the execution-p2p message stack.

```go
// BALRequest pairs a block hash with the expected BAL hash from its header so the
// fetcher can validate the EIP-8159 response in one place.
type BALRequest struct {
    Hash         common.Hash
    Number       uint64
    ExpectedHash common.Hash // == *header.BlockAccessListHash (non-nil ⇒ Amsterdam+)
}

type BALFetcher struct {
    logger          log.Logger
    messageSender   *MessageSender
    messageListener *MessageListener
    peerPenalizer   *PeerPenalizer
    peerTracker     *PeerTracker
}

// FetchFromPeer asks ONE peer (used by bbd: the peer that already served the body).
func (f *BALFetcher) FetchFromPeer(ctx context.Context, reqs []BALRequest, peerId PeerId, timeout time.Duration) (map[common.Hash][]byte, error)

// Fetch picks an eth/71 peer from the tracker and fans out (used by Caplin internalcl).
func (f *BALFetcher) Fetch(ctx context.Context, reqs []BALRequest, timeout time.Duration) (map[common.Hash][]byte, error)
```

- Send via a new `MessageSender.SendGetBlockAccessLists(ctx, peerId, eth.GetBlockAccessListsPacket66)`
  → `sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71` (`execution/p2p/message_sender.go`).
- Receive by registering a `MessageId_BLOCK_ACCESS_LISTS_71` observer on the execution-p2p
  `MessageListener`, matching `RequestId` (mirror `FetchBodies` request/await in
  `execution/p2p/fetcher_base.go`).
- Validation (ported, one place): per entry — `0x80` → `nil` (peer doesn't have it; best-effort
  miss, **not** an error, no penalty); `0xc0` → accept only if `ExpectedHash ==
  empty.BlockAccessListHash`; otherwise `keccak256(payload) == ExpectedHash` or **penalize peer**
  and drop. Reuse `common/empty.BlockAccessListHash` and the packet types from
  `p2p/protocols/eth/protocol.go` (kept in place).
- Returns a `map[common.Hash][]byte` of validated BALs (misses simply absent from the map).

> A narrow interface (`type BALFetcher interface { FetchFromPeer(...); Fetch(...) }`) is exported
> so `bbd` and Caplin depend on the behavior, not the concrete type — and so tests can mock it.

### Step 2 — `bbd` fetches BALs alongside bodies (same peer, best-effort)

In `execution/p2p/bbd.go`, `downloadBlocksForHeaders` already fetches a header batch's bodies from
an assigned peer in an errgroup (`bbd.go:479-545`). For that same `peerId`, after/parallel to
`FetchBodies`, fetch BALs for the batch's **Amsterdam+** headers only
(`header.BlockAccessListHash != nil`):

```go
balReqs := balRequestsForHeaders(headerBatch) // skip headers with nil BlockAccessListHash
if len(balReqs) > 0 {
    bals, _ := bbd.balFetcher.FetchFromPeer(ctx, balReqs, peerId, cfg.balBatchFetchTimeout)
    // best-effort: ignore the error, merge whatever came back
}
```

- `BackwardBlockDownloader` gains a `balFetcher BALFetcher` field (constructor param).
- New option `balBatchFetchTimeout` in `execution/p2p/bbd_options.go`, defaulting to
  `bodiesBatchFetchTimeout` (30s) — "following body download timeout."
- A BAL miss never blocks or fails a batch; blocks flow regardless.

### Step 3 — Carry BALs through `BbdResultFeed`

`execution/p2p/bbd_result_feed.go`:

```go
type BlockBatchResult struct {
    Blocks []*types.Block
    BALs   [][]byte // positional, aligned with Blocks; BALs[i]==nil for a miss; whole slice nil if none
    Err    error
}

// Next now returns the batch (single consumer updated accordingly).
func (rf BbdResultFeed) Next(ctx context.Context) (BlockBatchResult, error)
```

`consumeData` is extended to `consumeData(ctx, blocks, bals)`. `BALFetcher.FetchFromPeer` may use
a hash-keyed map internally (convenient for the Amsterdam-only subset and for misses); `bbd`
**projects it onto the positional `[][]byte` aligned with `Blocks`** here, so everything
downstream of the feed carries the BALs slice — never a map.

### Step 4 — Collapse the insert API; EngineBlockDownloader supplies the BALs

**Collapse the API.** In `execution/execmodule/chainreader/chain_reader.go`, delete
`InsertBlocksWithAccessLists` / `InsertBlocksAndWaitWithAccessLists` and fold the `bals` parameter
into the base methods (`bals [][]byte`, positional, nil-allowed):

```go
func (c ChainReaderWriterEth1) InsertBlocks(ctx context.Context, blocks []*types.Block, bals [][]byte) error
func (c ChainReaderWriterEth1) InsertBlocksAndWait(ctx context.Context, blocks []*types.Block, bals [][]byte) error
func (c ChainReaderWriterEth1) InsertBlockAndWait(ctx context.Context, block *types.Block, bal []byte) error

func blocksToRaw(blocks []*types.Block, bals [][]byte) []*types.RawBlock {
    raw := make([]*types.RawBlock, len(blocks))
    for i, b := range blocks {
        rb := &types.RawBlock{Header: b.Header(), Body: b.RawBody()}
        if i < len(bals) {
            rb.BlockAccessList = bals[i] // nil ⇒ no BAL for this block
        }
        raw[i] = rb
    }
    return raw
}
```

**Update every `ChainReaderWriterEth1` caller** to pass `bals`/`nil` (mechanical):

| Caller | New call |
|--------|----------|
| `engine_block_downloader/block_downloader.go:239` | `InsertBlocksAndWait(ctx, batch.Blocks, batch.BALs)` |
| `engine_block_downloader/block_downloader.go:167` (tip) | `InsertBlockAndWait(ctx, tip, nil)` |
| `engine_server.go:1008` (tip `NewPayload`) | `InsertBlocksAndWait(ctx, []*types.Block{block}, [][]byte{balBytes})` |
| `cmd/utils/app/import_cmd.go:362` | `InsertBlocksAndWait(ctx, chain.Blocks, nil)` |
| `execmodule/execmoduletester/exec_module_tester.go:795` | `InsertBlocksAndWait(ctx, chain.Blocks, balsSlice)` |
| `cl/.../execution_client_direct.go`, `execution_client_engine.go` | thread `bals` through (Step 5) |

> **Do not touch** `polygon/sync/*` or `node/gointerfaces/executionproto` `InsertBlocks` — those
> are the polygon store / gRPC `ExecutionServer` interfaces, unrelated to `ChainReaderWriterEth1`.

**Consume BALs in the downloader** — `downloadBlocks` loop (`block_downloader.go:226-268`):

```go
for batch, err := feed.Next(ctx); err == nil && len(batch.Blocks) > 0; batch, err = feed.Next(ctx) {
    if err := e.chainRW.InsertBlocksAndWait(ctx, batch.Blocks, batch.BALs); err != nil {
        return err
    }
    ...
}
```

No change to execution/forkchoice — `InsertBlocks` already persists the BAL, and execution
already reads it (`exec3.go:598-614`).

### Step 5 — Caplin internalcl path

**(a) ExecutionEngine interface** (`cl/phase1/execution_client/interface.go:38-59`): change the
existing methods to carry `bals` (same one-signature rule as the EL side — no `…WithAccessLists`):

```go
InsertBlocks(ctx context.Context, blocks []*types.Block, bals [][]byte, wait bool) error
InsertBlock(ctx context.Context, block *types.Block, bal []byte) error
```

- `ExecutionClientDirect` (`execution_client_direct.go:86,163,165,169`) and `ExecutionClientEngine`
  (`execution_client_engine.go:259,261,268`) thread `bals` into `chainRW.InsertBlocks(AndWait)`.
- The externalcl Engine-API RPC client accepts `bals` and ignores them (BAL travels via
  `newPayload` at tip; external Caplin does not backfill BALs).
- Existing internal callers that have no BAL (e.g. `execution_client_direct.go:86`) pass `nil`.
- Regenerate `execution_engine_mock.go` via `make gen`; update `persistent_block_collector_test.go`
  expectations to the new arity.

**(b) BlockCollector carries BALs — IMPLEMENTED via payload extraction, not eth/71 fetch.**

**Finding that changed this step:** Caplin's `Eth1Block.RlpHeader` computes
`header.BlockAccessListHash = keccak256(payload.BlockAccessList.Bytes())`
(`cl/cltypes/eth1_block.go:519-526`). That hash is part of the EL block hash the beacon block's
committed bid references, so for caplin+glamsterdam devnets to validate at all, **the Gloas
execution payload envelope must already carry the raw EIP-7928 BAL bytes** in
`payload.BlockAccessList`. The CL therefore does *not* need to fetch BALs over eth/71 — it can
read them out of the payload it already holds. (This refines the original prompt's assumption that
Caplin must download BALs separately: the *beacon block* doesn't carry the BAL, but the *execution
payload envelope* — separate in EIP-7732 ePBS — does.)

Implemented (`cl/phase1/execution_client/block_collector/persistent_block_collector.go`):

- `decodeBlock` now also returns `payload.BlockAccessList.Bytes()` (nil/empty when absent).
- `Flush` records each decoded block's BAL in a `balByHash map[common.Hash][]byte`.
- `insertBatch` projects that map into a positional `bals [][]byte` aligned with the batch and
  calls `engine.InsertBlocks(ctx, blocksBatch, bals, true)`.
- No `BALFetcher` is injected into Caplin and no eth/71 round-trip happens on the CL path.

Safety: if the envelope ever does *not* carry the BAL bytes, `balByHash` stays empty and the path
silently degrades to today's behavior (execution recomputes the BAL) — no correctness risk either
way. **To confirm:** the kurtosis caplin+glamsterdam run in `testing.md` should show
`kv.BlockAccessList` populated on the internalcl node.

### Step 6 — Wire the shared fetcher; remove the ticker

`node/eth/backend.go`:

- Construct one `execution/p2p.BALFetcher` from the execution-p2p providers
  (`ExecutionP2PMessageSender/Listener/PeerTracker/PeerPenalizer`) next to where `bbd` is built.
- Pass it into `NewBackwardBlockDownloader(...)`. (Caplin needs no fetcher — see Step 5(b); it
  reads the BAL out of the payload envelope.)
- **Delete** the ticker wiring (`backend.go:690-700`, the `go NewBALDownloader(...).Run(...)`
  block gated on `AmsterdamTime != nil`).

### Step 7 — Remove / relocate old BAL client code

- Delete `p2p/sentry/sentry_multi_client/bal_downloader.go` (+ `bal_downloader_test.go`).
- Delete `p2p/sentry/sentry_multi_client/bal_fetcher.go` (+ `bal_fetcher_test.go`) after porting
  its validation/round-trip to `execution/p2p/bal_fetcher.go` and its tests to `execution/p2p`.
- **Keep** `p2p/protocols/eth/protocol.go` (packet types/message ids) and
  `p2p/protocols/eth/handlers.go` (`AnswerGetBlockAccessListsQuery` serving side) — per the
  code-move scope decision.

### Step 8 — Lint / regen / build

`make gen` (mock), then `make lint && make erigon integration` (CLAUDE.md). Lint is
non-deterministic — run until clean.

---

## 4. Best-effort semantics (precise)

- **Trigger:** only headers with `BlockAccessListHash != nil` (Amsterdam+). Pre-fork blocks never
  generate a request — matches the old `collectMissingBALs` short-circuit.
- **Peer:** `bbd` uses the body peer (no extra peer selection). Caplin picks an eth/71 peer from
  the EL `PeerTracker`.
- **Timeout:** `balBatchFetchTimeout`, default = body timeout (30s).
- **Miss handling:** `0x80` / timeout / error ⇒ absent from the map ⇒ block inserted without BAL
  ⇒ execution recomputes. Never fatal.
- **Misbehavior:** hash mismatch or `0xc0` where the header's hash is not the empty-BAL hash, or
  oversized/over-count responses ⇒ penalize peer, drop entry (ported behavior).
- **Alignment:** responses are positional per EIP-8159; `bbd`/Caplin project them to a `[][]byte`
  aligned with the inserted block batch (the fetcher may key by hash internally for the subset).

---

## 5. File-by-file change list

| File | Change |
|------|--------|
| `execution/p2p/bal_fetcher.go` | **New.** `BALFetcher` (struct + interface), `BALRequest`, `FetchFromPeer`, `Fetch`, EIP-8159 validation (ported). |
| `execution/p2p/message_sender.go` | Add `SendGetBlockAccessLists`. |
| `execution/p2p/message_listener.go` | Observe `MessageId_BLOCK_ACCESS_LISTS_71`. |
| `execution/p2p/bbd.go` | Add `balFetcher` field; fetch BALs from body peer in `downloadBlocksForHeaders`; `balRequestsForHeaders` helper. |
| `execution/p2p/bbd_result_feed.go` | Add `BALs` to `BlockBatchResult`; `Next` returns the batch; `consumeData` carries BALs. |
| `execution/p2p/bbd_options.go` | Add `balBatchFetchTimeout` (default = `bodiesBatchFetchTimeout`). |
| `execution/execmodule/chainreader/chain_reader.go` | **Collapse API:** delete `*WithAccessLists`; `InsertBlocks`/`InsertBlocksAndWait` take `bals [][]byte`; `InsertBlockAndWait` takes `bal []byte`; `blocksToRaw(blocks, bals)` positional. |
| `execution/engineapi/engine_block_downloader/block_downloader.go` | Consume `BlockBatchResult`; `InsertBlocksAndWait(…, batch.BALs)`; tip insert passes `nil`. |
| `execution/engineapi/engine_server.go` | Tip `NewPayload` insert (`:1008`) → `InsertBlocksAndWait(ctx, []*types.Block{block}, [][]byte{balBytes})`. |
| `cmd/utils/app/import_cmd.go` | `InsertBlocksAndWait(ctx, chain.Blocks, nil)`. |
| `execution/execmodule/execmoduletester/exec_module_tester.go` | `InsertBlocksAndWait(ctx, chain.Blocks, balsSlice)`. |
| `cl/phase1/execution_client/interface.go` | `InsertBlocks`/`InsertBlock` take `bals`/`bal`. |
| `cl/phase1/execution_client/execution_client_direct.go` | Thread `bals` into `chainRW.InsertBlocks(AndWait)`; no-BAL callers pass `nil`. |
| `cl/phase1/execution_client/execution_client_engine.go` | Thread `bals` into `chainRW.InsertBlocks(AndWait)`. |
| `cl/phase1/execution_client/engine_api_rpc_client.go` | Accept `bals`, ignore (tip via `newPayload`). |
| `cl/phase1/execution_client/execution_engine_mock.go` | Regenerate (`make gen`). |
| `cl/phase1/execution_client/block_collector/interface.go` | `insertBatch`/Flush carry BALs; inject `BALFetcher`. |
| `cl/phase1/execution_client/block_collector/persistent_block_collector.go` | Bounded-parallel `Fetch` at Flush; project to `bals`; `engine.InsertBlocks(ctx, batch, bals, true)`. |
| `node/eth/backend.go` | Build shared `BALFetcher`; inject into `bbd` only; **remove** ticker (`:690-700`). |
| `p2p/sentry/sentry_multi_client/bal_downloader.go` (+ test) | **Delete.** |
| `p2p/sentry/sentry_multi_client/bal_fetcher.go` (+ test) | **Delete** after porting. |
| `p2p/protocols/eth/protocol.go`, `handlers.go` | **Keep** (wire defs + serving). |

---

## 6. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| Lower BAL hit-rate vs. patient ticker | BAL is best-effort; recompute is the existing fallback. Add bounded fan-out to other eth/71 peers within the batch window if metrics show poor hit-rate. |
| Caplin catch-up missing the BAL | Not possible for a valid envelope — the BAL is a field of the SSZ payload (§1.5), so a downloaded envelope carries it; `decodeBlock` extracts it. No fetch, no stall. If an envelope is unavailable at all, that's a serving-side gap (out of scope), not a missing-BAL case. |
| eth/71 not negotiated on the execution-p2p sentry | BAL request is a no-op miss when the peer/sentry lacks eth/71 — same graceful degradation as today. Verify the execution-p2p sentry advertises eth/71 on Amsterdam chains. |
| `Next` signature change ripples | Single consumer (`block_downloader.go:226`); update in the same change. |
| Insert-API collapse touches many callers | Mechanical: ~7 `ChainReaderWriterEth1` + Caplin call sites pass `bals`/`nil` (table in Step 4). Compiler catches every miss; polygon/gRPC `InsertBlocks` are a different interface and untouched. |
| Consensus-code churn in Caplin | Keep the primary design to the collector + interface (no transition/forkchoice changes); follow `cl/CLAUDE.md` review. |
| Mock drift | `make gen` regenerates `execution_engine_mock.go`; CI lint guards mod tidy. |

---

## 7. Non-goals

- No separate persistence of execution-recomputed BALs (per decision: `InsertBlocks` persistence
  covers the cases that matter).
- No move of eth/71 wire definitions or the serving handler out of `p2p/protocols/eth`.
- No new BAL gossip/announcement protocol; this is request/response over eth/71 only.
- **No EL 3533-epoch BAL retention** (§1.5). Out of scope; affects serving/re-execution-after-offline,
  not the download flow. Tracked separately.
- **No Caplin serving-side re-hydration** via `engine_getPayloadBodiesBy{Range,Hash}V2` (§1.5).
  Out of scope; the sync test uses a geth+lighthouse progressor as the BAL-serving peer instead.
- No `t.Skip` of any test to get CI green (CLAUDE.md, automated-agent rule).
