# Durable epoch-based lazy unwind for MDBX domains (design note)

**Status:** exploratory design. Extends the in-memory `(txNum, epoch)` lazy-unwind
from PR #21386 (StateCache / BranchCache) into the persistent domain layer. The
account/storage/code domains look implementable (pending a detailed design); the
**commitment domain is the open problem** and is treated separately in §8.

**Scope note:** the commitment domain is owned by a separate workstream. §8 frames
the constraint and the candidate directions; it is not a unilateral commitment
redesign and must be reconciled with that workstream (it touches the wrong-trie-root
bug class directly).

---

## 1. Background — the in-memory model (#21386)

The caches make unwind O(1) and lazy:

- Global, per-cache: `epoch atomic.Uint32`, `unwindFloor atomic.Uint64`.
- Per entry: the value plus `(txNum, epoch)`.
- `Unwind(toTxN)` = `epoch++; floor = min(floor, toTxN)`. No scan, no delete.
- `Get` is stale iff `entry.epoch != epoch && entry.txNum >= floor`; stale entries
  are dropped lazily on the read that touches them (and reclaimed by LRU otherwise).
- Survives iff `txNum < floor` (committed below the unwind point) **or**
  `epoch == current` (written by the post-unwind re-execution). The epoch is what
  distinguishes a stale leftover from a fresh same-`txNum` fork write.

On a cache miss the caller falls through to the DB, which today has been eagerly
unwound — so the cache can afford to be lazy because something correct sits beneath it.

## 2. The idea — push the same trick into the DB

Today a domain unwind is **eager changeset replay**, `O(number of changed keys)`:
per key, delete the current `^step||value` from the values table and `Put` back the
prior value from the `DomainEntryDiff`, then prune history above the txNum
(`db/state/domain.go:1231-1314`). The "latest" values table is a *materialized*
newest-version overlay; the multi-version store underneath is history
(`Tbl*HistoryVals`) + the inverted index (`Tbl*Idx`).

"Epochs in the DB" means: **stop eagerly restoring on unwind. Mark post-floor latest
entries as a superseded epoch (O(1)) and let reads fall through to history.**

## 3. Encoding (the corrected version)

The key correction: **`txNum` subsumes `step`.** Today a latest entry carries `^step`
(8 bytes); `step = txNum/stepSize` is derivable, so store `^txNum` in the slot the
`^step` occupies instead. Consequences:

- The dupsort "latest-first" ordering is preserved (`^txNum` monotone like `^step`,
  just finer).
- The DB-validity check `lastTxNumOfStep(step) >= files.EndTxNum()` collapses to
  `txNum >= files.EndTxNum()` — more precise, no step conversion.
- Unwind comparison is natively tx-precise — the per-key `txNum` is right there (no
  #21752-style "which txN inside the step" reconstruction).
- Collation's `[txFrom,txTo)` filter works directly on `txNum`.

So the **net on-disk add is just the epoch** — `uint16` (~2 bytes) is enough:
`^txNum || epoch || value` (and `key||^txNum` for the large-value Code domain).

Durable global state: one metadata key per aggregator holding `(epoch, unwindFloor)`,
written in the unwind's rwtx so it is crash-consistent.

**Two encoding caveats:**

1. **Epoch order must use RFC 1982 serial-number arithmetic.** The epoch is a
   fixed-width wrapping counter, so "is this entry's epoch older than the current
   one" cannot be a naive `<` — after wrap a small number can be *newer* than a large
   one. Compare with RFC 1982 serial arithmetic (`a` precedes `b` iff
   `0 < (b − a) mod 2^N < 2^(N−1)`), which is correct as long as the two epochs being
   compared are never more than half the range apart. The staleness test stays
   `is-current` (equality) for the common path, but any ordering decision (and the
   guard against a wrapped epoch aliasing a live one) uses serial comparison.
2. **2-byte epoch is then safe because collation bounds the live window.** A durable
   epoch never resets on its own and increments once per reorg; `uint16` wraps at
   65,536. Once a step is collated nothing below the frozen boundary can be unwound
   (§7), so no surviving entry carries a pre-collation epoch — collation can reset
   `epoch→0` (or just advance the floor). That keeps all simultaneously-live epochs
   within RFC 1982's safe half-range, so 2 bytes covers "reorgs between two
   collations." Without the reset, use `uint32`.
2. **Keep single-slot-overwrite semantics.** Today the values table appears to keep
   one entry per key *per uncollated step* (collate filters by `v[:8]==stepVal`).
   Keying by the finer `^txNum` must not turn that into one dup per write for a hot
   key. The latest table must store **only the newest `txNum||epoch||value` per key**
   (overwrite), pushing priors to history (which already records every txNum change);
   collation then sources a step's frozen set from history/inverted-index, not from
   lingering latest dups. This is the main write/collate-path change to confirm.

## 4. Read path

- **Single `GetLatest`** (`domain.go:1621-1662`): after decoding, add
  `if epoch != cur && txNum >= floor` → the latest slot is not the answer → fall
  through to **`GetAsOf(key, floorTxNum)` = a history read** (inverted-index probe +
  history-value fetch). The correct post-unwind value of a non-re-written key is its
  value *as of the unwind target*, which only history holds. Cost: a stale hit
  degrades a single dupsort seek into a history lookup — concentrated on the
  most-recently-written (hottest) keys, until those slots are overwritten or collated.
  Files stay epoch-free: unwind can never go below the frozen boundary
  (`floor >= files.EndTxNum()` always), so stale entries only ever live in MDBX.
- **Cursors / range scans** (`DomainRangeLatest`, RPC storage-range, collation,
  commitment traversal): the clean "latest table = authoritative sorted latest"
  walk becomes a 3-way merge (live-latest + history-as-of-for-stale + files). This is
  the heaviest structural cost and touches every range consumer.

## 5. Write path

- `DomainPut` (`domain.go:351-505`) already takes `txNum`; it additionally reads the
  current global epoch and stamps `txNum||epoch` into the latest encoding. One
  encode/decode site set, flowing through `DomainBufferedWriter`.
- **Unwind flips from write-heavy to write-free:** `epoch++; floor = min(floor,txN)`
  on the durable meta key. O(1). That is the prize.
- **Prune semantics change:** history above the floor can no longer be pruned on
  unwind — it is the read fall-through. History retention grows; the changeset/
  diffset machinery is no longer needed *for unwind*.

### Code simplification: history replaces exec-side changesets

A second, arguably bigger, advantage of driving unwind from **history** rather than
**changesets** is that it removes the exec path's involvement entirely.

Changesets require execution to *cooperate*: every `DomainPut` threads a per-block
`DomainDiff` accumulator (`SetDiff` / `DomainUpdate`), the parallel executor swaps the
`currentChangesAccumulator` pointer per block, and that swap has to be serialized
against concurrent writes (`changesetMu`) so block N+1's writes don't land in block N's
changeset. That coupling — exec maintaining unwind-side state on the hot path — has
been a recurring source of parallel-exec races and off-by-one changeset-attribution
bugs.

History needs none of that. Prior values are captured by the domain write path itself
at **flush/commit** (`AddPrevValue`), as a natural byproduct of writing the domain —
no per-block accumulator, no pointer swap, no exec cooperation. So a history-driven
unwind lets the whole changeset apparatus be **removed from execution and folded into
the SharedDomains flush/commit**: one prior-value capture point, owned by the layer
that already owns the write. This is the same "derive the prior state post-hoc at
flush, not in the exec path" direction that motivates the lock-free-execution work —
fewer moving parts and an entire race class eliminated.

The backoff retention (§8) is what makes this affordable for commitment specifically:
without it, history-for-unwind would mean full commitment history (huge); with it, the
retained set is bounded and the exec-side changeset coupling still goes away.

## 6. Tables affected

- **Latest-values tables** (`TblAccountVals`, `TblStorageVals`, `TblCodeVals`,
  `TblCommitmentVals`): value/key encoding extended with the 2-byte epoch and
  `step→txNum`. Largest blast radius (every read/write/collate/range decode site).
- **New meta key** for durable `(epoch, unwindFloor)`.
- **History + inverted-index tables**: structurally unchanged (already txNum-keyed);
  retention changes (no prune above floor); they become the read fall-through.

## 7. Collation = the GC point (no epochs in snapshots)

Collation (`domain.go:647-813`) reads a step's latest entries, strips `^step`, writes
`key||value` into `.kv` — no step/txNum/epoch survives (provenance is only the
filename's txNum range). Under this model:

- **Stripping `txNum||epoch` at collate is safe** — exactly like stripping step today
  — because the frozen step is below the unwind horizon and is never unwound again
  ("unwind beyond data in snapshots is not allowed"). Epoch bookkeeping is therefore
  bounded to the hot MDBX region.
- **Collation becomes the GC + epoch-reset point:** it must skip epoch-stale entries
  and freeze the as-of-floor value instead (same merge as §4's cursor path), then it
  can reset `epoch→0` and raise the floor to the new frozen boundary.
- **New concurrency constraint:** collation of a step range must be serialized
  against any unwind mutating `epoch/floor` in that range.

### Research: the `KeyCommitmentState` snapshot record is itself an ad-hoc history

There is a pre-existing mechanism worth folding into this analysis. The commitment
domain stores its serialized trie state under the reserved key
`commitmentdb.KeyCommitmentState` — a regular domain entry that is collated into every
step's snapshot (special-cased in `merge.go`: "no replacement for state key") and read
back by `SeekCommitment` (`aggregator.go:2184`) to restore the trie on restart. So
each frozen step carries a point-in-time commitment-state record: **effectively a
coarse commitment history already embedded in the snapshots**, by a bespoke path
separate from the domain's (disabled) history.

This is an anachronism next to the partial-history scheme above: it's a second,
ad-hoc checkpoint mechanism. The persistence-change analysis should consider whether
the **backoff partial-history checkpoints subsume it** — one checkpoint/state-capture
mechanism (the retained commitment checkpoints) instead of two (partial history +
per-snapshot `KeyCommitmentState`). Needs research: what `KeyCommitmentState` records
vs what a partial-history checkpoint would record, and whether restart-restore
(`SeekCommitment`) can read the latter.

### Transport, sub-file access, and the canonical roll-up

Snapshots are immutable and distributed over transports that support **partial
reads** — HTTP range requests and BitTorrent pieces. The torrent format additionally
carries a hash structure that **rolls partial reads up into the verified whole**
(piece hashes, and v2's per-file Merkle tree), so a fetched byte range can be verified
without the whole file. Pulling that roll-up out as a first-class primitive enables
**verified sub-file access**: a consumer fetches and verifies only the regions a query
touches — the `.bt`/`.kvi`-located pages of a `.kv`, a specific partial-history
checkpoint, or a commitment-state record — instead of the multi-GB file. This matters
for the persistence design: partial-history checkpoints (§8) and any commitment-state
capture become individually retrievable, and cold file reads (§4's fall-through) can
be served as verified ranges. The persisted layout should therefore be designed for
range/checkpoint locality and alignment to the transport's verifiable units.

**The roll-up must be part of the canonical chain definition**, not out-of-band
torrent metadata. Today the piece/Merkle hashes live in the `.torrent` / downloader
metadata — fine for "did I download the right bytes", but not a consensus object. For
sub-file access to be *trustless* (a light or partial node verifying a fetched range
against what the chain canonically commits to), the snapshot content's Merkle roll-up
must be anchored in the canonical chain definition — the chain commits to the snapshot
roots the way a header commits to a state root. Otherwise verified-sub-file-access
degrades to "trust the torrent metadata." This is a consensus-surface change and the
heaviest open item here; it must be scoped with whoever owns the chain/snapshot
definition.

**In practice this points at BitTorrent v2 (BEP 52):** a SHA-256 per-file Merkle tree
over 16 KiB leaf blocks with a per-file `pieces root`, which natively verifies
arbitrary sub-ranges via Merkle proofs. Erigon is on v1 today (`downloader.go:1185`
explicitly opts out of v2), but `anacrolix/torrent` already supports v2, so this is a
switch, not a new implementation. Caveats: v2's hash is SHA-256, not the chain's
Keccak-256, so a canonical `pieces root` is a SHA-256 commitment object; the 16 KiB
leaf sets the sub-file granularity (align the layout to it); migrate via hybrid v1+v2
torrents.

**Web-standards alignment (the standardizing opportunity).** The same SHA-256 Merkle
roll-up can be made verifiable over plain HTTP/CDN, not torrent-only, by aligning with
web standards: HTTP Range (RFC 9110) for the partial read, `Content-Digest`/
`Repr-Digest` (RFC 9530) for content digests, and a Merkle-over-blocks content
encoding — e.g. MICE (`mi-sha256`, the scheme behind Signed HTTP Exchanges / Web
Packaging) — as the HTTP analogue of v2's roll-up (incremental per-block verification
as content streams). Choosing SHA-256 plus one documented Merkle construction lets a
single canonical root be verified via torrent-v2 proofs *and* web-standard HTTP, so any
CDN or web client can do trustless sub-file fetches — a portable, standards-aligned
distribution primitive rather than a bespoke one. Caveat: the concrete trees
(BitTorrent v2 vs MICE vs RFC 6962/9162 CT) differ in leaf size, padding, and node
domain-separation, so a shared root needs a deliberate construction choice (or
per-transport proofs over the same SHA-256 leaves).

**Hash choice — decided, not open.** Ethereum's Keccak-256 is itself non-standard
(original pre-NIST Keccak padding, not finalized SHA3-256), so it was never a web
standard — there is no single root that is both Ethereum-native *and*
web/torrent-standard. These are therefore two distinct commitments, and the design
keeps them separate:
- **State commitment stays Keccak-256** — unchanged consensus (MPT root, code hashes).
- **The snapshot-distribution roll-up is SHA-256** — it commits to snapshot *bytes for
  transport integrity*, a different object than state. Anchoring it canonically adds a
  "snapshot manifest root" alongside (not replacing) the Keccak state root.

SHA-256 is **EVM-verifiable via the `0x02` precompile**, so a light client or on-chain
verifier can check the roll-up / Merkle proofs cheaply — SHA-256 is not alien to
Ethereum, just not the trie hash. The trap to avoid is forcing one hash onto both
roles: putting the trie on SHA-256 is a consensus rewrite; putting the roll-up on
Keccak throws away the web/torrent standardization.

## 8. The commitment problem (the open issue)

Commitment is the decisive case. Confirmed mechanics:

- Commitment latest = trie branch nodes keyed by nibble prefix (`TblCommitmentVals`).
- **`HistoryDisabled: true`** (`statecfg/state_schema.go:274`). This forces
  `discardHistory` in the domain writer (`domain.go:385`), and `AddPrevValue` returns
  at `if w.discard { return nil }` *before* touching the inverted index — so
  commitment populates **neither history values nor the inverted index**
  (`TblCommitmentIdx` stays empty). §4's "stale → `GetAsOf(floor)`" does not exist for
  commitment, and the touched set is *not* recoverable from a commitment index.
- **Commitment unwind today is changeset replay, not recompute.** Domain writes feed
  `w.diff.DomainUpdate(k, step, preval)` (`domain.go:360-377`), gated only on
  `w.diff != nil` — independent of history. Unwind iterates `domainDiffs` and restores
  prior branch values (`domain.go:1269`). So commitment already keeps a **bounded,
  per-block prior-value store (the `DomainDiff` changeset)** for the unwind window.
  (`RebuildCommitmentFiles` in `squeeze.go` is a separate offline rebuild, not normal
  unwind.)
- Commitment is **derived state**: a branch node is a pure function of the
  accounts/storage beneath its prefix. Its value at the floor is recomputable from the
  state at the floor.

Because commitment is derived, its unwind is a **space/time choice**, and the prior
values are the largest, highest-churn stream in the system (commitment is ~187 GB
latest):

1. **Store prior values (today: changesets; alternatively bounded history).** The
   `DomainDiff` changeset *is* the bounded prior-value store; "commitment history"
   would be the same prior values **plus an inverted index** for point lookup — only
   worth the extra index if you want a lazy read-time fall-through. Either way you pay
   storage for commitment's prior-value stream, the most expensive in the system. Fast
   unwind (direct restore), heavy storage.
2. **Recompute from the state diff (recommended).** Store ~nothing for commitment. On
   unwind, take `S` = the account/storage keys changed in the unwound window — already
   kept for the state domains (their changesets/history) — revert those leaves, and
   recompute commitment over `S`. This is just `ComputeCommitment` over a touched-key
   set with reverted leaves: the operation the node already runs forward, now applied
   to the revert delta. Light storage (commitment keeps no priors; stop producing its
   `DomainDiff`), CPU at unwind. Unwinds are rare (reorgs, a few blocks), so this
   trades rare CPU for eliminating commitment's prior-value storage entirely.
3. **Frozen `.kv`** — gives a branch's value at the frozen boundary (`≤ floor`), not at
   the floor; wrong whenever the branch changed between boundary and floor. ✗.

**Recommended shape — hybrid, with commitment recomputed:**

- **Accounts / Storage / Code** → durable lazy-epoch unwind (the real O(1) win; they
  have history to fall through to per §4).
- **Commitment** → no stored priors. The epoch on commitment branches is only a
  **staleness guard** (mark old-epoch branches so a re-exec never serves one before
  it is recomputed — exactly the in-memory `BranchCache` role, mirrored in the DB
  latest table). Correct values come from recomputing over `S` with the reverted
  leaves.

This eliminates commitment's prior-value storage (the dominant cost) rather than
merely making its restore lazy.

**The "partial commitment history" dial** (i.e. keeping *some* commitment state to
bound the recompute): full recompute over `S` is self-contained only if `S` is
complete and every touched path is recomputed top-to-bottom. A sparse trie checkpoint
lets you (a) cap recompute depth and (b) cover the awkward case of a stale sibling
branch that is touched-above-floor but not on a re-touched path. That case is the
**recompute-before-read ordering** hazard — the wrong-trie-root bug class — and is the
real open problem.

The recompute must cover the **union** of the new fork's touched keys (recomputed
naturally) and the *old tip's* above-floor touched keys that the fork does not
re-touch (their leaves revert; their branches must revert too). That union comes from
the **state** changesets/history (`Tbl{Account,Storage}*`), not from a commitment
index. How much partial commitment state to keep — to make this recompute cheap and
ordering-safe — is the dial to settle with the commitment workstream.

### Backoff retention for commitment history

The "partial" dial is best realised as a **backoff retention**: dense near the tip,
geometrically sparser as a block ages, capped at a maximum depth. This keeps the
common case exact and cheap while bounding both storage and the worst-case recompute
distance for deep reorgs.

**The dense window should be ~96 blocks** — in normal operation unwinds beyond ~96
blocks don't happen, and most are far shorter. 96 is also already the codebase's
`maxReorgDepth` (it sizes the state domains' changeset window in
`changesetWindowStart`). So aligning the dense commitment window to the same 96 means
that for **every realistic reorg, full per-block commitment data is present and the
unwind is an exact restore — the recompute path is never exercised**. Recompute (which
touches the wrong-trie-root bug class) becomes a *rare deep-reorg safety net*, not a
common-case cost — a meaningful risk reduction.

Example schedule (tunable): keep **every block for the most recent 96**, then 1-in-128,
1-in-256, …, doubling the stride per tier, up to a max retained depth of ~4096 (beyond
which the data is below the unwind horizon / collated and is dropped; an unwind past
the cap isn't supported, consistent with "no unwind beyond snapshots"). Concretely, a
retained block at height `h` survives aging into a tier of stride `s` iff `h % s == 0`
— so pruning is a single modular test as a block crosses each tier boundary: drop it
unless it is the aligned "threshold block" for the coarser stride. Round (power-of-two)
strides make the alignment nest cleanly, so a kept block stays kept as it ages from one
tier into the next.

Properties:
- **Storage** is `~96 + Σ log-spaced checkpoints` ≈ `O(window + log(maxDepth))`
  retained blocks, instead of `O(maxDepth)` full history — bounded and small.
- **Recompute distance** is zero for any realistic reorg (≤ 96, every block retained →
  exact restore). Only a >96-block reorg anchors on the nearest retained checkpoint and
  recomputes forward over the touched set, bounded by the stride at that depth.
- **Prune cost** is the modular test above, run as blocks age — no scan.

Open sub-questions: what each retained entry holds (the block's branch *deltas* to
replay-restore, vs a sparser trie *checkpoint* to anchor recompute), and whether the
schedule is shared with — or distinct from — the dense-window changesets the state
domains already keep for their own unwind.

## 9. Open questions / next steps

1. Confirm and, if needed, change the latest table to **single-slot overwrite** so the
   `step→txNum` switch costs only the 2-byte epoch (not a dup-per-write blowup) — §3.
2. Detailed design of the **3-way range merge** (live-latest / history-as-of / files)
   and an audit of every cursor consumer — §4. This is the largest non-commitment cost.
3. Durable `(epoch, floor)` meta key + crash-consistency + the **epoch reset at
   collation** and its serialization against unwind — §7.
4. History **retention** policy once it can no longer be pruned above the floor — §5.
5. Commitment: settle the hybrid (lazy state + recompute commitment) with the
   commitment workstream and pin down the **recompute-before-read ordering over the
   old∪new touched set** — §8.
6. Tune the **backoff retention** schedule (dense window, tier strides, max depth) and
   decide what each retained entry holds (branch deltas vs trie checkpoint) and whether
   it shares the state domains' dense-window changesets — §8.
7. Research whether the **backoff checkpoints can subsume `KeyCommitmentState`** — the
   ad-hoc per-snapshot commitment-state record — so there is one state-capture
   mechanism, not two; include the restart-restore (`SeekCommitment`) path — §7.
8. Design the persisted layout for **verified sub-file access** over partial-read
   transports: range/checkpoint locality and **16 KiB-leaf alignment** so fetches map
   to whole BitTorrent-v2 Merkle blocks; assess switching the downloader from v1 to v2
   (`anacrolix/torrent` supports it; `downloader.go:1185` currently opts out) — §7.
9. **Anchor the snapshot roll-up in the canonical chain definition** (not out-of-band
   torrent metadata) so sub-file access is trustless. Consensus-surface change — scope
   with the chain/snapshot-definition owners — §7.
10. Evaluate a **web-standards-aligned** Merkle construction (SHA-256; verifiable via
    both torrent-v2 proofs and HTTP RFC 9110 Range + RFC 9530 digests + a
    Merkle-over-blocks encoding à la MICE) so one canonical root serves torrent and
    CDN/web clients alike — a portable standardizing primitive — §7.
