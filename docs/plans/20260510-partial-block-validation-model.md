# Partial-block commitment validation — model and edge cases

**Status**: implemented 2026-05-10 on `feat/snapshot-flow-app-integration`.
**Driver**: Phase 5 (2026-05-08) wedge — every consumer failed at exec
block 25,049,601 (`nonce too high: state: 8, tx: 9`) because a
partial-block commitment was loaded as if it were a snapshot tip.

This document explains the validation model for snapshot publication
and the edge case conditions the model handles. It also enumerates
the operational cost of the chosen "partial-block" design decision so
operators understand why a publisher might pause publication or a
consumer might refuse a manifest.

## Background — why partial-block commitments exist

Erigon's retire writes commitment files at STEP boundaries
(`txNum = step's last txnum`). Step boundaries are defined by step
size (~390k txnums) and are independent of block boundaries — so
step boundaries normally cut **mid-block**. The state hash recorded
in a commitment file at a step boundary is therefore the state
*after a prefix of block N's transactions*, not the state at any
canonical block boundary.

Block headers commit cryptographically to two pieces of state:
- `header[N].stateRoot` — state after ALL of block N's transactions
  (end-of-block).
- `header[N-1].stateRoot` — state at start-of-block-N.

Mid-block state has **no direct consensus anchor**. A partial-block
commitment file is therefore not directly verifiable against a single
header value.

## The cryptographic loops we close

The validator framework closes four cryptographic loops, one per
data type, that together tie every byte of every published file back
to consensus-committed block headers.

| Loop | What it verifies | Anchor |
|---|---|---|
| Header chain (foundation) | `header[N+1].parentHash ≡ hash(header[N])` | self-referential |
| State (block-aligned) | trie root from `domain/v2.0-{accounts,storage,...}.kv` ≡ `header.stateRoot` | verified header |
| State (partial-block) | publisher publishes block .seg covering `AtBlock` alongside the partial commitment so a consumer can replay-verify | bundled block data |
| Transactions | tx Merkle root from `v1.1-*-transactions.seg` per block ≡ `header.transactionsRoot` | verified header |
| Receipts | receipts root from `domain/v3.0-receipt.*.kv` ≡ `header.receiptsRoot` | verified header |
| Withdrawals | withdrawals root from snapshot ≡ `header.withdrawalsRoot` (post-Shanghai) | verified header |

For the **state loop** specifically, the publisher records the state
hash inside the commitment file via `KeyCommitmentState` ('state'
key), which carries `(rootHash, blockNum, txNum)`. Whether the
direct cryptographic check is possible depends on whether the
recorded `txNum` lands at a block boundary:

- **Block-aligned** (`txNum == blockMaxTxNum`): rootHash is
  end-of-block state. Direct check `rootHash ≡ header[blockNum].Root`
  closes the loop.
- **Partial-block** (`txNum < blockMaxTxNum`): rootHash is mid-block
  state. No header records this value. Closure requires either
  (a) replay from `header[blockNum-1].Root` forward to `txNum` and
  compare, or (b) bundling the block .seg covering `blockNum` so a
  consumer can replay forward to end-of-block and check against
  `header[blockNum].Root`.

The current model adopts **(b) for partial-block commitments**:
publisher refuses to advertise a partial-block commitment until the
matching block .seg is at `LifecycleAdvertisable`, and consumer
refuses to load a partial-block commitment whose `AtBlock` isn't
covered by some block .seg in the same manifest.

## Validation gate — publisher side

[`CommitmentDomainValidator.ValidateStep`](../../node/components/storage/commitment_validator.go)

```
decode KeyCommitmentState → (rootHash, blockNum, txNum)
verify txNum lies within blockNum's [minTxNum, maxTxNum] range
isPartialBlock = txNum < blockMaxTxNum

if !isPartialBlock:
    require header[blockNum].Root == rootHash       (cryptographic check)
else:
    require block .seg covering blockNum            (defensive pause)
        is at LifecycleAdvertisable

stamp anchors (rootHash, blockNum, txNum, isPartialBlock) on FileEntry
register (step, block) binding
```

Pause-on-partial-block is the publisher's defensive correctness
check: the publisher won't ship a partial-block commitment until the
block data is also Advertisable.

## Validation gate — consumer side

[`FindPartialBlockCommitmentsWithoutCoverage`](../../db/downloader/chaintoml_v2.go)

```
for each domain file with IsPartialBlock=true:
    require some block .seg in the manifest's [blocks] section
        whose range covers AtBlock
    if missing: flag — refuse to load as snapshot tip
```

The consumer mirrors the publisher's defensive logic against the
manifest it discovers from peers. A partial-block commitment is only
trusted as a usable snapshot tip when the SAME manifest carries the
block .seg covering its `AtBlock`. This protects against:

- Publisher mid-republish where the partial commitment leaked into
  the manifest before the matching block .seg.
- Adversarial or buggy publisher serving a partial commitment without
  the verifiable block coverage.

## Edge case conditions — when the gates fire

### Normal steady-state operation
- Block segments retire well ahead of state collation (state
  collation is gated on `FrozenBlocks() ≥ step-end-txNum`).
- Block segment lifecycle (Declared → Indexed → Advertisable)
  completes within seconds-to-low-minutes.
- By the time a partial-block commitment validation runs, the
  matching block .seg is at Advertisable.
- **Result**: pause does NOT fire. Both gates are no-ops.

### Mid-startup / fresh datadir
- Publisher freshly started; many block segments + state files
  arriving simultaneously.
- Lifecycle.Driver advances files in order; some commitment files
  may complete validation before their matching block .seg.
- **Result**: pause fires temporarily. Publisher holds the
  commitment at Indexed; next lifecycle sweep retries; advances
  cleanly once block .seg catches up.

### Race window (commitment vs block .seg lifecycle)
- Phase 5 forensic confirmed this case: commitment file written at
  16:18, transactions.seg of matching block .seg finished writing
  16:17, .torrent metadata at 16:18. Block .seg almost certainly
  not yet Advertisable when commitment validation would run.
- **Result**: pause fires for one or more retry cycles. Self-heals
  as block .seg lifecycle advances.

### Manifest mid-republish (consumer-side)
- Consumer pulls chain.toml manifest mid-publisher-republish.
- Partial-block commitment is in the new manifest but the
  corresponding block .seg lifecycle hasn't yet pushed it to the
  manifest.
- **Result**: consumer flags the partial-block commitment via
  `FindPartialBlockCommitmentsWithoutCoverage`. Refuses to use as
  snapshot tip. Re-checks on next manifest republish.

### Pathological — block .seg never reaches Advertisable
- Block .seg is corrupt, validators reject it permanently (e.g.
  format error, hash mismatch).
- Lifecycle quarantines the block .seg after N retries.
- **Result**: paired commitment file stays at Indexed indefinitely.
  This is the **operational signal** the model is designed to
  produce — something is wrong; don't publish until investigated.

### Catch-up burst
- Publisher has been offline; on restart, retire produces many
  block segments + state files in rapid succession.
- Multiple commitment files may sit in pause simultaneously while
  block segments work through lifecycle.
- **Result**: pause window may extend for minutes. Self-heals; no
  data loss; just publication latency.

## Operational cost of the partial-block design decision

### Cost: publication latency
- A partial-block commitment can sit at Indexed for as long as it
  takes the matching block .seg to reach Advertisable. In steady
  state this is sub-minute; under catch-up bursts it can extend
  to minutes.
- Direct measurement: Phase 5 forensic showed transactions.seg of
  the matching block .seg taking ~5 minutes to write (700+ MB at
  10k-block segment granularity) before its lifecycle even started
  advancing. Worst-case pause window is bounded by block-segment
  retire + lifecycle latency, not unbounded.

### Cost: operator alerting
- Pauses that persist beyond a threshold need to surface to
  operators as warnings. (Currently the validator returns a normal
  error on pause; if the lifecycle's quarantine policy increments
  on each retry, persistent pauses can erroneously quarantine the
  commitment file.)
- **Watch-point** for retest: if quarantine triggers prematurely,
  introduce a sentinel error type so quarantine-counter skips
  pause errors. Tracked in
  [memory/design-gap-partial-block-validation.md].

### Cost: consumer-side delay-to-tip
- Consumer that pulls a manifest mid-republish may hit a window
  where the partial-block commitment for the latest step is not
  yet covered by block segments. Consumer waits for next manifest
  refresh before treating that step as usable.
- Worst case: one publisher republish cycle of delay
  (publisher republishes whenever inventory advances; typical
  steady-state cadence is minutes).

### Cost: no static analysis of partial-block commitments
- A partial-block commitment cannot be cryptographically validated
  in isolation. Validation depends on the SAME publisher also
  serving the block .seg, which the consumer can replay forward
  against `header.stateRoot`.
- **Trust boundary**: consumer trusts that publisher's block .seg
  is internally consistent (or will fail consumer-side block-loop
  validation if not). Without this trust the partial-block
  commitment can't be verified without external execution.

## Long-term fix candidates

Listed in increasing complexity. Tracked in
[memory/design-gap-partial-block-validation.md].

1. **Publisher-side replay-verify**: replay txns from
   `header[blockNum-1].Root` forward to `txNum` on every
   partial-block commitment validation. Closes the loop fully on
   the publisher side. Cost: re-execution per retire. Heavy but
   bounded.

2. **Step boundaries always block-aligned**: change retire so step
   boundaries align with block boundaries. Eliminates partial-block
   commitments entirely. Cost: non-uniform step sizes; complicates
   merge alignment. Possibly infeasible given Erigon's step-size
   semantics tied to txNum count.

3. **Historical-audit process**: separate service walks the
   published manifest, replays the chain, signs a "validated"
   attestation per commitment file. Decouples from publish path
   but adds external dependency.

4. **Step header in V2 manifest with replay proof** (extension of
   the V2 step-header design): publisher produces succinct replay
   proof verifiable without full replay. Requires new
   cryptographic machinery.

## Implementation references

- Publisher gate: [`CommitmentDomainValidator.ValidateStep`](../../node/components/storage/commitment_validator.go)
  — block-aligned cryptographic check + partial-block defensive
  pause.
- Publisher-side helper: `blockSegAdvertisableForBlock` in same file.
- Consumer gate: [`FindPartialBlockCommitmentsWithoutCoverage`](../../db/downloader/chaintoml_v2.go)
  — manifest self-consistency.
- V2 manifest schema (carries IsPartialBlock + AtBlock anchors):
  [`DomainFileEntry`](../../db/downloader/chaintoml_v2.go).
- FileEntry inventory anchors:
  [`Inventory.SetAnchors`](../../node/components/storage/snapshot/inventory.go).
- Tests:
  - `TestCommitmentDomainValidator_*` (publisher unit tests; full
    path exercised end-to-end in operational retests).
  - `TestFindPartialBlockCommitmentsWithoutCoverage` (consumer
    manifest check).
  - `TestApplyV2AnchorsToInventory` (anchor plumbing).
