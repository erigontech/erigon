# Objective: Snapshot Flow — Continuous Validated Publication

Branch: `feat/snapshot-flow-v1`. This document is the design context for
the work landing on this branch — the trust model, validation phase,
view/transaction model, and integration plan that together replace
today's "stop the node to publish" ceremony with a continuous
automated pipeline running on every publishing node.

This is a long objective deliberately. The work spans seven independent
components and three architectural concerns; reviewers walking the
diff need the unifying narrative or the individual commits read as
unrelated additions.

## Problem

Today's snapshot subsystem expresses missing infrastructure as
**three sequential phases each with their own world-stop boundary**:

1. **Download → Operate.** A node either downloads snapshots OR
   operates against them; never both. Sync-from-cold takes hours
   even when the network has the data ready, because the runtime
   cannot operate while still backfilling older ranges.

2. **Operate → Stop → Publish.** Publication is a separate ceremonial
   phase. Operators stop the node, run `erigon seg integrity` (the
   external publish-time integrity tool — see
   `db/integrity/integrity_action_type.go`), wait for the
   `Publishable` check to pass, then push. The node must be down
   because there is no view-coherency layer; running integrity
   against a live mutating snapshot set produces false failures.

These boundaries are operational artefacts, not requirements. The
gap between what the network has and what nodes can use is the
direct cost. Worse, publication is **manual + per-host**: only
specific publish-host nodes run integrity + push, on specific
publication windows. Most nodes serving snapshots from peers never
validate what they're advertising — a producer-side bug propagates
across the swarm before anyone catches it.

## Goal

A single continuous automated pipeline:

> A file finishes downloading (consumer side) or finishes building
> (producer side) → validator chain runs on a held view-snapshot →
> verdict flips Advertisable → next manifest publish includes the
> file → peers fetch — all without stopping the node, all while
> operations continue at the tip.

Three architectural concerns are load-bearing:

- **Trust** (UCAN selection). Producers attest to what they advertise
  via signed UCAN delegations rooted in operator-configured trust
  roots (DID, ENR, or bootnode-derived). Consumers verify the chain
  before treating a peer's manifest as a data source.
- **Validation** (the producer/consumer gate). A pluggable validator
  chain runs at the moment a file becomes a candidate for inclusion
  in the trusted view. Failure keeps the file out of the published
  manifest (producer side) or rejects the peer's claim (consumer
  side). The existing `db/integrity` checks become the deep
  validators registered into this chain.
- **View model** (visible vs published vs disk). External consumers
  hold transactional views over the inventory; transitions are atomic
  and emit change-sets; the visible and published sets can transition
  on independent cadences. This is what makes "operate while
  backfilling" possible without false failures.

## Success criteria — the merge gate

Before the snapshot-flow work merges into Erigon's mainline and
application-level (execution, RPC, CL) testing begins, the scenario
suite must demonstrate:

1. **Latest-state download**: latest step's full state + commitment +
   blocks/headers/bodies covering up to manifest-tip.
2. **Backfill loop**: older step ranges fill in, marked available as
   each validates.
3. **Trust + validation, both sides**:
   - Consumer-side: orchestrator delivers bytes → validation chain
     runs → store marks files trusted + range available → only then
     visible to execution / RPC / CL.
   - Producer-side: local files (initial sync output, retirement /
     merge results) run through the same validator chain BEFORE the
     inventory marks them `advertisable=true`. The swarm invariant
     becomes "manifest content == validated content," not "manifest
     content == bytes-on-disk-somewhere."
4. **Producer scenarios**: a node generates a file, validator runs,
   only validated files appear in the next published manifest.
   Negative variant: inject a deliberately corrupt file → assert it
   stays `advertisable=false` and never enters the manifest.
5. **External-reference tests**: scenarios driven from
   preverified.toml / real fixture dirs, not test-synthesised
   assertions matching test-synthesised expectations.

The gate is a *scenario* gate, not a *production-runnable* gate. The
storage-component integration (next section) is downstream.

## Non-goals (this branch)

- **Production startup wiring.** `node/eth/backend.go` does not yet
  construct the snapshot.Inventory or run the trust loop. That work
  lives in storage-component integration: `storage.Provider` gains
  the canonical Inventory + a walk-the-files initialiser; the
  validation chain + UCAN trust loop wire on top. This branch ships
  the scenario-tested infrastructure; the production hookup is a
  follow-up branch.
- **Stage 2 cross-file validation.** The framework exists; the
  cross-file checks (commitment chaining, merge equivalence,
  history/kv alignment, block continuity) need the View interface +
  store-context plumbing that come with the storage integration.
- **Operate-while-backfill at the runtime level.** The view model
  documented here is the prerequisite, but actually running
  operations forward at the tip while backfilling older ranges
  requires execution-layer coordination that is outside this
  branch's scope.
- **Application-level testing.** Engine API, RPC, CL — all gated
  behind the merge gate above.

## Architecture

### Three views, one inventory

The `snapshot.Inventory` is the **visible files set** — the published,
transaction-safe view consumers read from. Snap-dir on disk is a
superset that includes in-flight downloads, temp files, and partial
artefacts. The split mirrors the existing `RoSnapshots.visibleFiles`
vs raw-on-disk distinction the freezeblocks layer already maintains.

In Phase 2 (when operate-while-backfill lands), the model expands to
four views with independent transition cadences:

```
   Disk             Manifest         Visible          Published
   ────             ────────         ───────          ─────────
   All bytes        Network's        What active      What this node
   on snap-dir      canonical        operations can   advertises to
   (incl. temp,     claim of what    read safely      peers in the
   in-flight,       exists           (held views)     next manifest
   etc.)
```

These can disagree on disk during dynamic transitions (a backfilled
range entering Published before the runtime needs it visible; a
merged file entering both while constituents stay visible to held
queries). The validation phase + change-set publication keep the
semantics coherent.

**Phase 1 simplification (this branch's scope).** Validators see only
the canonical inventory — the published / advertisable set. No
disk-only / manifest-only / visible-vs-published reasoning. Correct
+ complete for today's download-then-operate runtime; the richer
View interface is Phase 2 when storage gains the dynamic-transition
machinery.

### Trust loop: UCAN B / C / D

Operators authorise nodes via signed UCAN delegations. The code
treats UCANs as a transport-and-storage artefact (`chain.ucan.<seq>.bin`)
paired with each `chain.v2.<seq>.toml` manifest generation.

Three components, lettered to match the design memo:

- **UCAN B** — manifest filter (consumer side, `manifest_exchange/`).
  When a peer connects, fetch its V2 manifest and the sibling UCAN
  artefact, verify the delegation chain against configured trust
  roots, gate publication of `flow.PeerManifestReceived` on the
  result. Failure → warn-log + time-bounded blacklist.
  - Trust roots: DID first → ENR → bootnode-derived. Operator-
    configurable per deployment.
  - `--ucan-reverify-on-reconnect` knob defends against node-takeover
    (re-verify on every reconnect rather than honouring cached
    UCAN until expiry).
  - Blacklist duration configurable; default 1h.

- **UCAN C** — peer selection (orchestrator gap-fill, `flow/`).
  `TrustFilter` interface (`Trusted(peerID) bool`) gates which peers'
  manifests drive `DownloadRequested`. Manifest_exchange's trust
  state is the production filter; nil filter = trust-everyone. Gates
  apply at request entry AND at queue-drain time (defensive
  re-check for peers whose trust expired between queue and drain).

- **UCAN D** — delegation tool (`erigon snapshots delegate`).
  Operator runs locally, takes a target ENR, signs a delegation
  scoped to specified capabilities + expiry + depth-cap. CBOR on
  disk; JSON-only for inspection. Subcommand of the existing
  snapshots tool family.

Plus the producer-side wiring:

- **Loader**: JWT-style default-path resolution
  (`snapshotauth.LoadOrGenerateDelegation`). `--snapshot.delegation`
  flag wins; otherwise `<datadir>/snapshot.ucan` read or auto-
  generated as a self-signed bootstrap on first run.
- **RollingV2Publisher** writes the paired `chain.ucan.<seq>.bin`
  alongside each generation; `V2.UCANHash` is stamped with the
  UCAN torrent infohash so consumers know what to fetch.
- **Finalization (deferred)**: chain config gains built-in
  "Erigon snapshots" ENR roots per network (per-chain like
  bootnodes); `--trust=none` becomes a required explicit opt-out.

### Validation phase

A pluggable validator chain that gates promotion of a file into the
trusted view. Lives **inside Storage** (per the validation-phase
memo): cross-file consistency checks need the store's read context +
domain knowledge, which is naturally storage-side. The orchestrator
stays transport-only.

Two stages:

- **Stage 1 (file-level)**: per-file checks runnable on a single file.
  Hash matches torrent infohash, file is well-formed for its kind,
  content shape parses. The framework + four basic-level validators
  ship in this branch:
  - `NameNotEmpty`, `RangeOrdering`, `KindConsistencyFromName` —
    metadata-shape sanity.
  - `SizeMatchesTorrent` — catches truncated / inflated downloads
    via `<file>.torrent` length comparison.
  - `ContentNotEmpty` — catches the index-empty bug class
    (zero-byte .ef and friends that hash to a "valid" payload but
    crash downstream readers).
- **Stage 2 (cross-file / range / system-level)**: commitment chaining,
  merge equivalence, history/kv alignment, block continuity.
  Framework only in this branch; concrete validators land with the
  storage-integration follow-up.

**Both consumer and producer sides run the same validator chain.**
Producer side gains an additional user-defined plugin slot for
deployment-specific gates (workflow signoffs, embargo rules, custom
data-quality checks). Consumers ignore plugins entirely — those
are producer-only concerns. The `validation.Producer` type runs
the built-in chain then the plugin chain; failure attribution is
"producer built-in" vs "producer plugin" so triage doesn't need
to unwrap.

The `Inventory.MarkAdvertisable(name)` method is the producer-side
verdict: validation-pass flips a `FileEntry.Advertisable` flag from
false to true. The flag is set but **not yet read** by `GenerateV2`
in this branch — the behaviour flip lands when the storage component
owns the canonical Inventory and the retire/build pipeline drives
`MarkAdvertisable` explicitly. Existing tests are unaffected by the
field's introduction.

### Why structural validators belong in storage, not the downloader

The downloader is unaware of file structure — it sees opaque bytes
against a torrent infohash and verifies one against the other.
That's all it CAN do. Failure shapes the downloader cannot catch by
design:

- **Empty index files** (`.ef`, `.vi`, `.efi`, `.kvi`, `.bt`).
  Zero-byte payload hashes to a "valid" payload; torrent agrees;
  every byte-level integrity check passes. Downstream readers open
  the file and dereference pointers into nothing → crash. The bug
  class `ContentNotEmpty` catches at the basic level; the deeper
  catch ("the index opens AND yields records") needs the index-
  reader code from `db/recsplit` / `db/datastruct/btindex` and
  belongs in storage code.
- **Same-size byte tampering** (single byte flipped, file size
  unchanged). `SizeMatchesTorrent` misses this. The piece-level
  catch already exists as `db/integrity/torrent_verify.go`'s
  `VerifyTorrentFiles` — we wrap, we don't reinvent.
- **Format-magic-bytes mismatch** (`.seg` lacks compression header,
  `.kv` lacks btree header). Storage-side validators that import
  the format readers.

The validation framework lives in
`node/components/storage/validation/` and is format-agnostic.
Format-specific validators are registered FROM storage code that
already imports the structural readers. **The validation package
itself NEVER imports erigon's format-aware readers** — that would
collapse the layering.

### Cost vs coverage — exhaustive vs sampling

For deeper "index opens and yields records" checks, full iteration
is the gold standard but impractical for large files (TB-scale
snapshots, multi-GB `.ef` files). Two complementary modes:

- **Exhaustive**: walk every record. Used when file size ≤ threshold
  (small index, iteration cost acceptable) OR when periodic
  scrubbing is running (CI, manual integrity audit, post-incident).
- **Sampling**: probe N random offsets, parse each, verify they yield
  expected structure. Used on every promotion-to-trusted moment
  when the file is too large for full iteration. `db/integrity/sampler.go`
  already implements this with seeded PCG RNG for reproducibility.

The validator picks the mode internally based on file size and
configured threshold. Producer side defaults exhaustive (warm cache,
just built); consumer side defaults sampling (just downloaded, may
have many to validate).

### Existing integrity package — the integration target

`db/integrity/` is not a parallel system to design alongside; it's
the system whose checks become validators. The package has:

- Structured `Check` enum: `Blocks`, `HeaderNoGaps`, `BlocksTxnID`,
  `InvertedIndex`, `StateProgress`, `HistoryNoSystemTxs`,
  `CommitmentKvi`, `ReceiptsNoDups`, `RCacheNoDups`,
  `CommitmentRoot`, `CommitmentHistVal`,
  `StateRootVerifyByHistory`, `StateVerify`, `Publishable`.
- `Sampler` with seed + ratio, the canonical exhaustive-vs-sample
  implementation.
- `IntegrityCache` for incremental re-runs.
- `VerifyTorrentFiles` — full piece-level SHA1 against `.torrent`.
  The `HashAgainstTorrent` validator I had on the todo list is
  exactly this code; we wrap, not reinvent.

**Today's invocation site**: `erigon seg integrity` external CLI on
a stopped node. **Tomorrow's invocation site**: per-file, per-publish,
on the live producer-side gate, on every publishing node.

The bridge architecture is integrity-check-as-Validator adapter,
landing in storage code (NOT the validation package — keeps the
layering). Storage registers wrapped checks at startup; the chain
runs them at MarkAdvertisable time; the format knowledge stays in
`db/integrity`; the validation framework stays format-agnostic;
storage glues them together.

### Operational consolidation

The three world-stop boundaries collapse to zero:

| Today | After |
|-------|-------|
| Download → Operate (sequential) | Operate-while-backfill (continuous) |
| Operate → Stop → Run integrity | Validators run on held views during operation |
| Operators push manually from publish-host | Producer-side gate publishes continuously on every publishing node |

The merge gate's "scenarios passing" requirement is fundamentally a
demonstration that this consolidation works. Until the scenarios
pass, the existing world-stop boundaries are still load-bearing
and the merge into Erigon would prematurely commit to
infrastructure that hasn't been proven.

## Components shipped on this branch

In commit order on `feat/snapshot-flow-v1`:

| Commit | What |
|--------|------|
| `c7cddcf583` | downloader V2 infohash-collision fix; multi-peer mid-merge test |
| `761f5f7f30` | flow: per-peer file attribution in `peerFiles` + tighter eviction |
| `9175b5dc70` | snapshotauth + `erigon snapshots delegate` CLI |
| `281506b714` | snapshotauth: chain Verifier (DID/ENR/bootnode roots, attenuation) |
| `7cb6f1faa9` | UCAN B: manifest filter + sibling `chain.ucan.<seq>.bin` fetch |
| `494d5b4dba` | UCAN C: orchestrator peer selection in gap-fill |
| `99254f9a2c` | RollingV2Publisher writes paired UCAN sidecar |
| `65f211495a` | JWT-style delegation loader + `--snapshot.delegation` flag |
| `27a017666d` | validation: stage-1 framework + flow.Storage integration |
| `47d671aef2` | validation: KindConsistencyFromName |
| `e1cdeb3f52` | validation: producer-side gate framework (Advertisable + Producer) |
| `5724ae1c7d` | validation: SizeMatchesTorrent + corruption test mechanics |
| `67b7c4e424` | validation: ContentNotEmpty + index-empty bug-class capture |

Plus this design doc, completing the architectural narrative for
review.

## Open questions / deferred work

These are explicitly NOT shipped on this branch and have memory
captures or todos:

- **Storage-component integration.** `storage.Provider` gains
  canonical `*snapshot.Inventory`; walk-the-files initialiser
  populates from existing on-disk state at startup; OnFilesChange
  callbacks evolve into atomic-transition ChangeSet events.
- **Integrity-to-Validator bridge.** Storage-side adapter wrapping
  `integrity.Check` functions as `validation.Validator`. The next
  code-bearing item; design is converged but the implementation
  needs the storage-component integration to land first.
- **Stage 2 validators.** Cross-file consistency checks
  (commitment chaining, merge equivalence). Need the View interface
  + store-context plumbing that come with the storage integration.
- **Production startup wiring.** Per
  `snapshot-flow-startup-wiring.md` (memory) — covers the
  `--snapshot.delegation` flag → loader → BindAutoPublish thread,
  `--trust-roots` flag → TrustConfig → manifest_exchange + flow
  thread, bootnode-trust augmentation, finalization (default-on
  trust).
- **Latest-download + backfill scenario**: the gate-blocking
  end-to-end test. Multi-step manifest, exercises Phase 0
  (state-tip + commitment alignment + block backfill to manifest-
  tip) AND Phase 1 (older ranges marked available as they validate).

## References

Implementation context lives across these source files:

- `node/components/snapshotauth/` — UCAN envelope, Verifier, loader.
- `node/components/manifest_exchange/` — UCAN B (consumer-side trust
  gate).
- `node/components/storage/flow/` — orchestrator, UCAN C, per-peer
  attribution.
- `node/components/storage/validation/` — validator framework + builtins.
- `node/components/storage/snapshot/inventory.go` — Advertisable +
  MarkAdvertisable.
- `node/components/downloader/` — `BindAutoPublish`,
  `FetchPeerUCAN`, paired sidecar plumbing.
- `db/downloader/chaintoml_v2_rolling.go` — paired-publish, eviction.
- `db/integrity/` — the existing integrity checks the bridge will
  consume.
- `cmd/utils/app/snapshots_delegate_cmd.go` — `erigon snapshots
  delegate` CLI.
