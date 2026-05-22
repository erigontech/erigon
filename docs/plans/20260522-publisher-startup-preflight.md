# Publisher startup pre-flight — design

**Status**: implemented, 2026-05-22. Guarantee 3 reshaped from the original
"separate background re-validation task" into "an infohash validator in the
lifecycle driver chain + a settle-watcher" — see the Guarantee 3 section.

**Related**:
- `docs/plans/20260520-chaintoml-ucan-flow-spec.md` — the chain.toml + UCAN
  distribution spec. This doc refines *when* a publisher emits its L3
  advertisement.
- Parked issue "inventory/disk drift" (a file seen on disk without a
  `DownloadComplete` is in an unknown state, not `Local`).
- `db/integrity/` — existing integrity primitives (`VerifyTorrentFiles`,
  `Sampler`).

## Motivation

A live two-publisher sepolia run (2026-05-21) surfaced three problems with
the auto-publisher's trigger:

1. **Empty manifest at startup.** A `--snap.bootstrap-from-preverified`
   publisher has `preverified.toml` but no downloaded files yet. The
   auto-publisher fired on the startup tick and published a `version = 2`
   manifest with **zero file entries**. That empty generation is then
   *retained forever* — the validity rule keeps a generation while its
   name-set ⊆ current inventory, and the empty set is a subset of
   everything.
2. **Generation churn during sync.** The auto-publisher republished on
   every debounced batch of download completions. During a growing
   inventory nothing is ever evicted (every older, smaller name-set stays
   a subset), so generations accumulate one-per-batch.
3. **No guarantee the first manifest is correct.** Nothing checks that the
   files a fresh publisher is about to advertise are actually present and
   valid on disk — inventory rows carried over from a prior session assert
   `Local`/`Verified` without re-verification.

## Principle — the first manifest must be known-good

A publisher's first L3 advertisement after startup must be a **known-good
manifest**. "Known-good" is three guarantees:

| # | Guarantee | Meaning |
|---|---|---|
| 1 | **Complete** | every file the initial sync set out to fetch is downloaded |
| 2 | **Matches canonical** | the advertised (name, hash) set agrees with the canonical/quorum set |
| 3 | **Matches disk** | each advertised file is genuinely present and content-valid on disk |

Subsequent generations (backfill, retire output) are *not* held to the
startup pre-flight — a running node already validates new files through the
lifecycle before they reach `Advertisable`. The pre-flight is specifically
the **first publish after process start**.

Gating applies to **publication only**. Execution, staged sync, and serving
proceed normally — a download completing or a re-validation finishing never
gates the node process, only the chain.v2 advertisement.

## Guarantee 1 — Completeness gate

New event `flow.InitialDownloadsComplete{}` — fired once per orchestrator
lifetime, after `InitialStateReady`, when the orchestrator's in-flight
`pending` set has fully drained (phase 1 — state, blocks, meta, salt — and
phase 2 — caplin — all downloaded and verified into the inventory).

- Orchestrator fires it from the two points where `pending` can reach empty
  with `stateReadyFired` already true: the tail of `onDownloadComplete`, and
  the tail of `fireInitialStateReady` (covers the no-caplin / all-local
  case). A fire-once flag (`initialDownloadsCompleteFired`) guards it.
- This subsumes both problem 1 and problem 2: with the first publish gated to
  after the initial sync completes, **no generation is published during
  initial sync at all** — no empty generation, no per-batch churn. After the
  first, generations only come from genuine post-sync inventory changes
  (backfill, retire).

The event is purely observational — no orchestrator or execution path
consumes it.

## Guarantee 2 — Matches canonical (already implemented)

`RollingV2Publisher.selfCheck` runs `snapshotsync.CheckOwnAdvertisement`
against the live canonical view; it is wired in `node/eth/backend.go` and
runs on **every** `Publish`, the first included. A failing self-check aborts
that publish.

Nothing new to build. One action item: confirm the self-check is a hard
abort (not a warn-and-continue) on the first post-startup publish, and that
it is wired even when no genesis is pinned falls back to the static
preverified set as canonical.

## Guarantee 3 — Matches disk: infohash validation

The gap: a file on disk — a bootstrap-local file carried over from a prior
session, or any file since corrupted/truncated/bitrotten — may not match the
info-hash the publisher is about to advertise it under.

**Design (reshaped 2026-05-22).** An earlier draft built a standalone
background re-validation task. That was collapsed: the lifecycle driver
*already* runs the Stage 1 / Stage 2 validator chain on every file before it
reaches `Advertisable`, and the driver already runs asynchronously off the
foreground exec/serve path. So Guarantee 3 is two pieces, both reusing the
driver rather than duplicating it:

### 1. The infohash check is a validator

`InfoHashValidator` (`node/components/storage/infohash_validator.go`) is a
`validation.StepValidator` in the lifecycle driver's `batchChain`, ordered
ahead of the semantic validators (header-chain, commitment, receipt) so a
byte-corrupt file is caught before the heavier structural checks run. It
piece-hashes each `Local` file against its sibling `.torrent` via the new
`integrity.VerifyFileAgainstTorrent` primitive (built on `db/integrity`'s
existing `verifyFileFromTorrent`).

A file whose content does not match its `.torrent` never reaches
`Advertisable`, so the publisher never advertises bytes it does not hold.

- **Meta / salt / caplin files** take the driver's meta dispatch path, not
  `batchChain`. They are covered by an `OnMetaReady` handler that runs the
  same `checkFileInfoHash` before advancing them to `Advertisable`.
- **`.torrent` availability.** The seeder (`provider.scanAndSeed`) writes
  `.torrent` files asynchronously, concurrent with validation. When the
  `.torrent` is not on disk yet the validator returns
  `validation.ErrPause` — the driver retries on the next sweep without
  ticking the quarantine counter. `scanAndSeed` was extended to seed caplin
  files too (it previously only seeded them on the `Advertisable`-triggered
  ChangeSet path, which would have deadlocked caplin's infohash validation).
- The torrent client piece-verifies a file at download time; the validator
  re-runs the check uniformly on every file rather than tracking per-file
  provenance.

### 2. The settle-watcher gates the first manifest

The first publish must not happen before the pre-advertise validations have
run. `Provider.watchInitialValidation`
(`node/components/storage/initial_validation_watcher.go`) is a small
background goroutine that:

- waits for `flow.InitialDownloadsComplete`;
- polls the inventory until every `Local` file has settled out of the
  lifecycle's pre-`Advertisable` states — each is either `Advertisable`
  (passed the chain, infohash check included) or quarantined by the driver
  after repeated failure;
- publishes the new one-shot event `flow.InitialValidationComplete`.

**On a quarantined file — operator policy.** `--snapshot.revalidation-policy`
(`snapshotsync.RevalidationPolicy`, parsed like `ParseAdoptionPolicy`):

- **`redownload` (default)** — the watcher calls
  `Orchestrator.RequeueForDownload(name)`; the downloader re-adds the
  torrent and the torrent client piece-verifies the on-disk data and
  re-fetches the corrupt pieces, healing the file in place. A file no peer
  offers cannot be re-fetched and is excluded from the first manifest.
- **`stop`** — the watcher logs an error and returns without publishing
  `InitialValidationComplete`; the first-publish gate never opens. A corrupt
  local archive is surfaced loudly rather than silently healed.
- **`warn`** — the watcher logs and continues; the quarantined file is
  simply not `Advertisable`, so it is naturally excluded from the manifest.

## How the guarantees compose

The first publish waits on the conjunction of `InitialDownloadsComplete`
**and** `InitialValidationComplete` — `flow.FirstPublishGateChannel` closes
only when both have fired. `node/eth/backend.go` wires the production
publisher's `Downloader` V2-publish gate to that channel;
`downloader.BindAutoPublish` (harness) gates on the same channel when
`AutoPublishOpts.GateFirstPublish` is set.

Guarantee 2 (canonical self-check) runs inside `Publish` itself and needs no
gate wiring — it is enforced on the first publish like any other.

## Implementation — as built

1. `flow.InitialDownloadsComplete` event + orchestrator fire-points.
2. `flow.InitialValidationComplete` event; `InitialValidationCompleteChannel`
   and `FirstPublishGateChannel` helpers.
3. `integrity.VerifyFileAgainstTorrent` per-file primitive.
4. `InfoHashValidator` + `checkFileInfoHash`; wired into `batchChain` and the
   driver's `OnMetaReady`; `scanAndSeed` extended to seed caplin.
5. `db/snapshotsync.RevalidationPolicy` + `ParseRevalidationPolicy`;
   `--snapshot.revalidation-policy` flag.
6. `lifecycle.Driver.IsQuarantined`; `Orchestrator.RequeueForDownload`.
7. `Provider.watchInitialValidation` settle-watcher, spawned from
   `Provider.Initialize` under `LifecycleDrivenByStorage`.
8. Gate composition in `backend.go` and `auto_publish.go`.

## Open questions / follow-ups

- **A `redownload` re-download that never completes or fails** leaves the
  settle-watcher waiting and the first-publish gate shut. This mirrors the
  orchestrator's own handling of a wedged download; not specially handled.
- **genID / convergence framing** — *resolved 2026-05-22.* The L3
  advertisement is unique per node by definition; `<genID>`
  content-addressing is a per-node property; only the canonical chain.toml
  converges swarm-wide, via the consumer-computed quorum view.
