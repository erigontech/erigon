# Real validation flow — publisher and consumer

Pre-PR feature-completeness item raised 2026-05-04. The lifecycle's
`OnValidation` slot was previously wired with a `nil` chain (accept
all) — the framework existed but no checks ran. This plan defines
the real validation flow, splits the chain into switchable tiers by
performance cost, and documents the publisher vs consumer
expectations.

## Tiers

Validation is a layered chain. Each tier adds cost and catches a
different class of problem. Operators pick the floor that matches
their tolerance for the cost / coverage tradeoff.

### Tier 1 — Metadata shape (always-on)

Cheap checks against `FileEntry` fields, no I/O:

  - `NameNotEmpty` — empty Name is a producer bug.
  - `RangeOrdering` — `FromStep < ToStep` for stepped files.
  - `KindConsistencyFromName` — Kind agrees with the name's pattern.

Cost: ~µs per file. No flag — always runs.

### Tier 2 — Disk shape (default-on)

Reads file metadata + a byte or two. Catches truncation, zero-byte
producer failures, mismatched torrent length:

  - `ContentNotEmpty` — opens file, reads 1 byte, rejects if EOF.
  - `SizeMatchesTorrent` — opens `<file>.torrent` sidecar, reads
    full file, compares byte count.

Cost: O(file size) bytes copied (the SizeMatches read), but linear
and disk-cache-friendly. Hundreds of MB/s on SSD; Erigon's snapshot
files are 100s of MB to 1 GB each. Per-file: <1 s.

Default-on for both publisher and consumer. Disabling tier 2 is
opt-out via `--snap.validate-disk=false` (escape hatch for
debugging; not a production posture).

### Tier 3 — Format integrity (opt-in, slow)

Parses the file's content according to its format and verifies
internal structure. The validators wrapping `db/integrity/`:

  - `BlocksValidator` — header chain consistency, body hash check,
    canonical-headers gap detection (header.seg / bodies.seg).
  - `InvertedIndexValidator` — index structure round-trip, key
    density sanity, no missing references.
  - Future: `CommitmentIntegrityValidator`, `EFFilesValidator`,
    `RCacheNoDuplicatesValidator` — wrap remaining `db/integrity`
    checks as Validator types.

Cost: full file read + format parse. Per-file: seconds for index
files, tens of seconds for large `.kv` / `.v` segments. On a fresh
hoodi sync (~22 indexed files) this adds a non-trivial chunk to
time-to-tip — projected ~30 s to 2 min depending on hardware.

**Switchable** via `--snap.validate-format` (default false on the
consumer side, default true on the publisher side):

  - Consumer (no `--snap.bootstrap-from-preverified`): off by
    default. The trust-anchor model says consumers verify the
    publisher's signature on the manifest, then trust the
    publisher's per-file attestations. Format-validation is
    redundant work the publisher has already done.
  - Publisher (`--snap.bootstrap-from-preverified` set): on by
    default. The publisher is the source of truth; running format
    validators before advertising means the swarm's published files
    have already been parsed and structurally checked by *someone*.
    Skipping it on the publisher side means a corrupt file may be
    advertised before any consumer notices.

### Tier 4 — Cross-file consistency (opt-in, very slow, future)

Multi-file structural checks. Live at the storage layer (need read
context across files within a domain):

  - Commitment-chain consistency.
  - Merge equivalence (the merged file equals the constituents
    semantically).
  - History/KV alignment.

Out of scope for this PR; placeholder so the plan covers the full
shape. See `feature-pluggable-validation-phase.md` (memory) for the
broader analysis.

## Failure recovery

Validation failure ≠ permanent failure. Most failures are
recoverable; the recovery itself has a cost that the system has to
weigh against the value of recovering.

### Recovery categories

  - **Free** (skip): non-critical file (e.g. an optional accessory
    index for a domain not in this node's prune mode). Mark
    skipped, log, move on.
  - **Cheap — local rebuild** (CPU + disk I/O): rebuild a derived
    file from its source. Examples:
      - `.idx` / `.efi` / `.kvi` / `.bt` accessor missing or
        corrupt → `BuildMissedIndices` regenerates from the source
        segment. This is what the existing build path does today.
      - `.ef` history index built from existing inverted-index
        domain → re-derive locally.
    Cost: seconds to minutes per file depending on size. No
    network. Same as the normal indexing path, just triggered by a
    failure rather than a missing-deps gap.
  - **Medium — re-download from network**: the file's source bytes
    are wrong (truncated, hash mismatch, format-corrupt). Mark
    Missing, let the downloader fetch a fresh copy. Cost:
    bandwidth + wall-clock proportional to file size.
  - **Expensive — full re-sync**: the corruption is structural and
    propagates across files (e.g. commitment chain inconsistency
    that requires re-deriving from genesis). Should be very rare —
    if the only recovery is full re-sync, advertising the file in
    the first place was the bug.

### Validator → recovery mapping

| Validator failure          | Default recovery       | Why |
|----------------------------|------------------------|-----|
| `NameNotEmpty`             | quarantine + alert     | producer bug; can't recover automatically |
| `RangeOrdering`            | quarantine + alert     | as above |
| `KindConsistencyFromName`  | quarantine + alert     | as above |
| `ContentNotEmpty`          | re-download            | zero-byte file is bad source bytes |
| `SizeMatchesTorrent`       | re-download            | byte count wrong → bad source |
| `BlocksValidator` (tier 3) | re-download segment    | block.seg structure broken → source corruption |
| `InvertedIndexValidator`   | local rebuild (tier 2) → re-download (tier 3) | for `.ef`-derived `.efi`, rebuild; for the source `.ef`, re-download |
| Future: `CommitmentIntegrity` | local rebuild (cheapest) → re-derive from prior commitment | depends on which file failed |

The mapping lives next to each validator (a method on the
`Validator` interface, or an out-of-band lookup table on the
chain). Implementation choice: extend the interface so each
validator declares its recovery, vs put the table in the lifecycle
driver. Lean: extend the interface — keeps the validator and its
recovery in the same package, follows the extension-point model
from `app-integration-review-items.md`.

### Cost-aware recovery throttling

Recovery attempts have to be bounded:

  - **Per-file recovery counter.** Already implemented as the
    quarantine counter (5 consecutive failures → quarantine). Used
    for indexing failures; should extend to validation failures.
  - **Network budget.** Re-download is bandwidth-bound. A burst of
    validation failures shouldn't trigger N concurrent
    re-downloads — bound by the existing downloader's parallelism
    settings, but worth a sanity check.
  - **Operator escalation path.** Persistent quarantine = "this
    file is broken and the system can't fix it"; needs a clear log
    line and (eventually) a metric that operations dashboards can
    alert on.

### Recovery-vs-validation cost tradeoff

This is the meta-question that ties the tiers together:

  - **Tier 1/2 failures are cheap to detect AND cheap to recover**
    (re-download is the same as the original download). Always-on
    is the right default.
  - **Tier 3 failures are expensive to detect but the failure
    surfaces are LIKELY ALSO expensive to recover from** (re-download
    a 1 GB segment + rebuild its indexes). The publisher running
    Tier 3 means consumers don't have to — the Tier 3 cost is paid
    once at publish time, amortised across all consumers.
  - **Consumer-side Tier 3** is only useful when consumer doesn't
    trust the publisher (no DID, untrusted root). The DID/UCAN PR's
    trust posture controls this — once trust is established, Tier
    3 on the consumer is redundant work.

This argues for the publisher / consumer default split already in
the flag-interface section above: publisher defaults to Tier 3 on
(spending CPU once at publish), consumer defaults to Tier 3 off
(trusting the publisher's attestation).

## Flag interface

  - `--snap.validate-format` (bool, default false; see publisher
    note above) — enables Tier 3.
  - `--snap.validate-disk` (bool, default true) — escape hatch for
    Tier 2; disabling is operator-debugging only.
  - `--snap.validate-cross-file` (bool, default false) —
    placeholder for Tier 4 (no-op until implemented).

Tier 1 has no flag (always-on, free).

## Performance profile (to measure)

Hoodi minimal-mode benchmark, ~22 files at LifecycleIndexed →
LifecycleAdvertisable:

| Tier | Validators                         | Cost / file (target) | Total contribution |
|------|------------------------------------|----------------------|--------------------|
| 1    | name/range/kind                    | < 100 µs             | ~2 ms              |
| 2    | content+empty / size-vs-torrent    | 50–500 ms            | ~1–10 s            |
| 3    | blocks / inverted-index            | 1–30 s               | ~30 s – 2 min      |

Time-to-tip post-§5c without Tier 3: should track the ~10 min hoodi
baseline. With Tier 3 (publisher path): expect +30 s to +2 min.
Mainnet-archive scaling is the next unknown — the file count is
~100x and the per-file cost likely scales sub-linearly with size,
but we measure rather than project.

## Documentation deliverables

For the PR:

  1. **Operator guide** (extend `20260504-v2-operational-guide.md`):
     a "Validation tiers" section with the table above + flag table
     + the publisher-vs-consumer default rationale.
  2. **Failure-mode appendix**: what each validator catches, what
     the log lines look like, what the operator should do on
     failure (typical: file goes back to LifecycleDownloaded; next
     sweep retries; persistent failures quarantine).
  3. **Performance benchmark table** in
     `20260502-min-time-to-tip-target.md` — hoodi numbers for each
     tier on/off, baseline → with-tier-2 → with-tier-3.

## Implementation sequence

  1. Wire DefaultStage1ChainWithDisk (Tier 1 + Tier 2) into
     OnValidation. **DONE.**
  2. Add `--snap.validate-format` flag + ethconfig field.
  3. Convert remaining `db/integrity` checks to Validator types
     (incremental — start with the highest-value ones:
     CommitmentIntegrity, EFFiles).
  4. Construct the Tier 3 chain from the converted validators when
     `--snap.validate-format` is set; append to the default chain.
  5. Set publisher default true / consumer default false based on
     `--snap.bootstrap-from-preverified`.
  6. Hoodi measurement run (publisher with Tier 3 on; consumer
     with Tier 3 off) → fill in the perf table.
  7. Update operational guide.

Items 2–7 land as additional commits before the draft PR opens.
Tier 4 stays out-of-scope.

## Open questions

  - **Validation cache.** Tier 3 results don't change unless the
    file changes. A cache (file hash → last-validation-result)
    would let consumers skip re-validation on restart. Worth doing
    in this PR or follow-up? Lean: follow-up — cache shape needs
    its own design pass.
  - **Quarantine wiring.** Tier 3 failure today returns an error
    that bumps the quarantine counter. Persistent failures
    quarantine the file. Should Tier 3 failures be more aggressive
    (immediate quarantine, no retries) given their severity? Lean:
    keep current behaviour — the existing 5-failure threshold
    already filters transient errors.
  - **Producer-side sign-of-validation.** Pairs with the DID/UCAN
    work in `20260504-publisher-did-ucan.md` — the publisher should
    sign its manifest only after Tier 3 validation passes. Lands
    in the DID PR; mention here as the gating relationship.
