# Publisher startup pre-flight — design

**Status**: design, 2026-05-22. Not yet implemented. Sequencing in
"Implementation" below.

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
- `BindAutoPublish` holds the **first** publish until it observes the event;
  subsequent generations stay `TrustPromoted`-driven.
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

## Guarantee 3 — Matches disk: startup re-validation

The gap: inventory rows marked `Local`/`Verified` carried over from a prior
session are *unverified in fact*. The file on disk may be truncated,
corrupt, or stale.

Re-validation is **two checks**, applied to different scopes (refinement
2026-05-22):

1. **Infohash verification — every file.** Every file the publisher will
   advertise has its content hashed and checked against the infohash it is
   advertised under — *not* a subset. An earlier draft skipped files
   downloaded in the current session on the grounds the torrent client
   already piece-verified them at download time; that exception is dropped.
   The publisher is the authority for what it seeds, the check is
   comparatively cheap, and a uniform pass needs no per-file provenance
   tracking. `db/integrity.VerifyTorrentFiles` is the existing primitive.
   No sampling.
2. **Local validator set — the local files.** Infohash match proves the
   bytes are intact; it does *not* prove the file is a *correct* snapshot.
   The local files additionally run the full local validator chain — the
   Stage 1 (per-file) + Stage 2 (cross-file) validators and their
   `db/integrity` equivalents (`InvertedIndex`, `CommitmentKvi`,
   commitment-chain, header-chain, trie-root-vs-header, receipt-root,
   `no_gaps_in_canonical_headers`, …). These are the deep structural and
   semantic catches — a file can hash to a valid infohash and still be a
   wrong snapshot if the infohash itself was recorded from a bad prior run.

**Execution**: both checks run as a **low-priority background task**. They
must not steal performance from foreground activity (execution, staged
sync, serving peers) — bounded worker concurrency, throttled I/O, yields to
foreground. Because the first publish may wait arbitrarily and later
publishes have no timing constraint, re-validation has no deadline; it
trades wall-clock for zero foreground impact.

**On failure — operator policy.** When re-validation finds a file with an
incorrect infohash or that fails the validator set, the response is
operator-configurable, mirroring the existing `snapshotsync.AdoptionPolicy`
(auto/stage/warn) + `--snapshot.adoption-policy` model:

- **`redownload` (default)** — the minority-client behaviour. The bad file
  is demoted (dropped from `Verified`, excluded from the manifest) and
  re-queued for download; the re-download re-enters the orchestrator's
  `pending` set, so the completeness gate (Guarantee 1) naturally waits for
  it. A file that cannot be re-fetched (no peer offers it) is excluded from
  the first manifest — the L3 advertisement is "what this node holds and can
  vouch for", so advertising a strict subset is correct.
- **`stop`** — halt on the first re-validation failure. For operators who
  want a corrupt local archive surfaced loudly rather than silently healed.
- **`warn`** — log the failure and continue without re-downloading; the
  operator has accepted the risk. (Whether a `warn`-mode node still
  advertises the suspect file is an open question — default to *excluding*
  it, consistent with "only advertise what we can vouch for".)

Surfaced via a new flag, e.g. `--snapshot.revalidation-policy`
(`redownload` | `stop` | `warn`), parsed the same way as
`ParseAdoptionPolicy`.

**Open**: whether the Stage 1/2 validator set runs on the *entire* local
set every startup, or only on files not vouched by a `DownloadComplete`
this session, is a cost decision — the validator chain is far heavier than
an infohash check. Default for now: run it on every local file (matches the
"full check, no shortcuts" decision); revisit if startup wall-clock proves
a problem even as a background task.

## How the guarantees compose

The first publish must wait for **both**: all initial downloads complete
(Guarantee 1) **and** startup re-validation has finished — every advertised
file infohash-checked and the local validator set run on the local files
(Guarantee 3). The completeness gate alone is insufficient — it could fire
before re-validation has even looked at a stale file.

Re-validation therefore emits its own one-shot signal,
`StartupRevalidationComplete` (fires immediately for a fresh node with no
startup-local files). `BindAutoPublish` gates the first publish on the
conjunction of `InitialDownloadsComplete` AND `StartupRevalidationComplete`.
A file re-validation fails and re-queues re-enters `pending`, so
`InitialDownloadsComplete` then also waits for its re-download — the two
signals interlock without extra coordination.

Guarantee 2 (canonical self-check) runs inside `Publish` itself and needs no
gate wiring — it is enforced on the first publish like any other.

## Implementation sequencing

1. `flow.InitialDownloadsComplete` event + `InitialDownloadsCompleteChannel`
   helper; orchestrator fire-points + fire-once flag.
2. `BindAutoPublish` first-publish gate — opt-in via `AutoPublishOpts`
   (production-on; harness/tests default off, preserving current behaviour).
3. Startup re-validation background task, low-priority worker: infohash
   verification (`VerifyTorrentFiles`-equivalent) on every advertised file,
   plus the Stage 1/2 + `db/integrity` validator set on the local files.
   Demote + re-queue failures. Emit `StartupRevalidationComplete`.
4. Compose: `BindAutoPublish` gates on both signals.
5. Wire the gate on in `node/eth/backend.go` for the production publisher.
6. Tests — orchestrator event, gate, re-validation demote/re-queue — then a
   fresh two-publisher sepolia live run to confirm: no empty generation, one
   generation after sync completes, a planted-corrupt file is caught and
   re-fetched before the first publish.

Steps 1–2 are contained and could land first; 3–4 are the larger piece.

## Open questions

- **Where the re-validation task lives.** Candidate: the storage `Provider`
  startup path (it owns the inventory + the disk scan). It must run after
  the inventory is loaded and before — or concurrently with, gating only
  publication — the orchestrator's initial work.
- **Throttling mechanism.** Worker-pool size vs. I/O niceness vs. an
  explicit rate limit. Whatever is chosen must demonstrably yield to
  foreground exec/sync.
- **Interaction with `--snap.bootstrap-from-preverified`.** A bootstrap
  publisher's startup-local set may be large (a full prior archive); confirm
  the background task handles that volume without unbounded memory.
- **genID / convergence framing** — *resolved 2026-05-22.* The
  `genIDFromContent` comment, the `Publish` comment, the spec's Layer 3
  section and `TestV2SerializationIsCanonical` previously overstated
  cross-publisher convergence. Corrected: the L3 advertisement is unique per
  node by definition (it embeds the node's own Authority UCAN and
  `<enr-fp>`); `<genID>` content-addressing is a *per-node* property
  (no-op-republish dedup + restart stability); only the canonical chain.toml
  converges swarm-wide, via the consumer-computed quorum view.
