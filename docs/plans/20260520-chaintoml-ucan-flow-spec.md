# chain.toml + UCAN distribution flow — spec

**Status**: agreed 2026-05-20. This is the authoritative spec for the
snapshot-flow chain.toml distribution and manifest authentication.

**Supersedes**:
- `20260515-three-layer-snapshot-distribution.md` — the three-layer model;
  retained as historical context. Layer 1 (swarm agreement), deferred there,
  is specified here.
- `20260516-two-ucan-shape.md` — the two-UCAN design; its 7 open questions are
  resolved here.

Implementation sequencing lives in the plan file
`.claude/plans/time-to-get-back-generic-mist.md`; this document is the design
contract.

## Why this exists

The snapshot-flow work conflated three concerns that must be separate:
how the swarm agrees on what is canonical, what the canonical content is, and
what any one node holds. The implementation drifted from the two prior design
docs — `VerifyAdvertisement` has zero callers, the consumer UCAN gate is
disabled by default, the per-node manifest was never ENR-keyed, and no producer
publishes or extends a canonical. Consuming nodes do **zero** manifest
authentication in production today. This spec is the single coherent design
that closes those gaps.

## The three layers

| Layer | Question it answers | Artefact |
|---|---|---|
| L1 — Swarm agreement | how does the swarm converge on canonical? | mechanism, not a file |
| L2 — Canonical chain.toml | what files are in the chain, at what info-hash? | consumer-computed view |
| L3 — Per-node advertisement | what does THIS node hold and seed? | `chain.v2.<enr-fp>.<seq>.toml` |

Layers are strictly independent in code: no path reads more than one layer's
input and persists a fused artefact. Cross-layer decisions happen in
consumer-side fetch planning, which queries each layer and combines locally.

## Layer 1 — Deterministic Quorum Promotion

Snapshot retire is deterministic: same chain state + same retire algorithm +
same compression parameters → byte-identical files → identical BitTorrent
info-hashes. Honest publishers converge naturally; this is the foundation the
agreement mechanism rests on.

**Canonical is a consumer-computed view, not a published authority file.** An
entry `(name, hash)` is promoted to canonical when a node has observed it in
`≥ Q` distinct **Authority-UCAN-verified** publisher advertisements. A published
`canonical.toml` would re-introduce a single signer to compromise and a target
for equivocation; a derived view has no signer — every consumer recomputes the
same set from the same deterministic inputs (the same principle as
`DeriveManifestTips`: derive, don't add).

**Sybil boundary = UCAN audience pubkey**, not ENR or peer-id. Minting ENRs is
free; minting UCAN audiences that chain to the configured trust root is not.

**Quorum is population-relative**: `Q = max(Q_floor, ceil(F · N_authorised))`,
where `N_authorised` is the count of distinct UCAN audiences the trust root has
delegated advertise capability to (knowable because delegations chain to a
configured root), `F` is a configured fraction (default `0.5`), and `Q_floor`
is a configured absolute minimum (default `2` — one buggy node can never
self-promote). `F` and `Q_floor` ship in `snapcfg` next to `preverified.toml`.
This degrades gracefully: small chains use the floor, large chains the fraction.

**Monotonic growth**: the view only ever adds. Layer 1 never demotes — a file
leaving a publisher's inventory is the L3 validity rule's concern, not L1's. A
long-superseded merge form is GC'd from the view once zero verified publishers
have advertised it for a configured window (~24h).

**Merge transitions are tolerated natively**: pre-merge (`X.0-1024` +
`X.1024-2048`) and merged (`X.0-2048`) are different names with different
hashes — they never collide in the name→hash map. Both accumulate quorum
independently; both stay canonical simultaneously. `ValidateAdvertisement` /
`CheckOwnAdvertisement` already accept `[]PreverifiedItems`, so a consumer
fetches from peers on either side of the transition without disruption.

**Optional signed checkpoints** (`canonical.<enr-fp>.toml`): any publisher may
publish a snapshot of its derived view, signed via the same per-node mechanism
as its advertisement. A cold-start consumer may *seed* its initial view from a
checkpoint but **must still independently re-verify every entry to Q** before
trusting it. The checkpoint is a performance hint — an index into work already
done — never an authority.

## Layer 2 — Canonical chain.toml

Layer 2 is the *content* of the Layer-1-computed view: the name→canonical-hash
map. It is what `ValidateAdvertisement` filters peer advertisements against.
Today that filter uses the static embedded `snapcfg.Preverified.Items`; after
this spec it uses the live computed view, anchored at the pinned genesis.

### Canonical genesis & versioning

Canonical is versioned with a monotonic counter so the whole swarm shares one
vocabulary ("are you on canonical v5?").

- **Genesis (v0) is a pinned `preverified.toml` snapshot** — specifically the
  `preverified.toml` content present when the first authoritative publisher for
  the chain launched. NOT "whatever the running binary embeds": erigon releases
  ship different `preverified.toml`, so a binary-relative genesis would give
  publishers on different releases different v0 and incompatible numbering.
  Pinning v0 to one point-in-time snapshot makes every version number globally
  meaningful.
- **Genesis is recorded as a definite, verifiable artefact** — content + its
  hash — committed to `snapcfg` (next to where `preverified.toml` loads today)
  or distributed as `canonical.v0.toml`. A node joining later whose embedded
  `preverified.toml` is newer than genesis MUST anchor on the pinned v0 and
  treat the extra entries as later promoted versions — never adopt its own
  binary's `preverified.toml` as genesis.
- **Each version `v<N>` = genesis ∪ entries quorum-promoted through the N-th
  promotion batch.** Because the promotion set is deterministic, `v<N>` has
  definite content; any party recomputes and verifies it. The counter is a
  deterministic function of promotion history, not an authority's assertion.
- **`canonical.v<N>.toml` checkpoints** are named by this version; they are
  independently re-verifiable cold-start hints.

Open sub-question: who performs the one-time genesis-pinning act, and how a
second-generation chain (no prior authoritative publisher) bootstraps. Likely
the release that first ships authoritative-publisher support also ships the
pinned `canonical.v0.toml` for each supported chain.

## Layer 3 — Per-node advertisement

- **Filename**: `chain.v2.<enr-fp>.<seq>.toml`, where `<enr-fp>` is the first
  16 hex chars (8 bytes) of `sha256(enr-bytes)` — constant-width, not the raw
  long/noisy ENR. The literal `chain.v2.` prefix and `.toml` suffix mirror the
  canonical name; `<enr-fp>` is the only addition.
- **`<seq>` is mandatory and explicit.** Multiple advertisement generations from
  the *same* node legitimately co-exist on the swarm — a node republishes while
  peers still seed the prior generation, and the validity-eviction rule keeps an
  older generation alive while its set stays a subset of inventory. A bare
  `chain.<enr>.toml` would alias across generations; `<seq>` makes each a
  distinct torrent, cache entry, and UCAN sidecar.
- **Content**: lists ONLY the files this node holds locally, with the
  info-hashes it seeds. Sparse by construction — no implicit ranges, no merging
  across entries.
- **Validity rule** (already implemented): a retained generation is valid iff
  every name it lists is present in the publisher's current inventory. An
  invalid generation is removed from disk and unseeded.
- **No canonical-seq in the advertisement name.** The advertisement→canonical
  binding is per-entry by content (`ValidateAdvertisement`), not by version
  number — a name-embedded canonical-seq is a brittle second channel.

The canonical name stays `chain.v2.<canonical-seq>.toml`. The legacy unversioned
`chain.toml` is the V1 artefact, untouched.

## Manifest authentication — two UCANs

The interim `.sig` sidecar is replaced entirely. Authentication flows through
two UCANs at two cadences:

- **Authority UCAN** — `chain.ucan.authority.<enr-fp>.<rev>.bin`. Root authority
  → operator pubkey; capability `snapshot.publish:<chain>`; long-lived (months);
  `<rev>` is its own revision. Loaded via
  `snapshotauth.LoadOrGenerateDelegation`. Its info-hash is carried in the
  manifest field `AuthorityUCANHash`.
- **Content UCAN** — `chain.v2.<enr-fp>.<seq>.ucan`. Operator → self; capability
  `chain.v2:hash:<sha256_of_toml>`; short-lived (per-generation, expires at the
  next regeneration); `ParentHash` references the Authority UCAN. Discovered by
  name pattern (no manifest field).

### Resolved design questions (from `20260516-two-ucan-shape.md`)

- **(a) By-hash parent.** `snapshotauth.Delegation.Parent []byte` becomes
  `ParentHash []byte` (sha256 of parent canonical CBOR). `Delegation`
  `CurrentVersion` bumps 1→2; v1 is rejected (nothing in production).
  `Verifier.Verify` gains a `ParentResolver func(hash) ([]byte, error)` so the
  Authority UCAN is fetched/cached by hash — one cache entry shared across all
  of an operator's generations.
- **(b) BitTorrent distribution** for the Authority UCAN — the same seedable
  sidecar plumbing as the Content UCAN. No second trust channel.
- **(c) Self-audience** for the Content UCAN (issuer == audience): "I attest
  this manifest is mine and current."
- **(d)** `ManifestSignerFn` becomes
  `ContentUCANMinterFn func([]byte) ([]byte, error)`. `SignAdvertisement` /
  `VerifyAdvertisement` are deleted — `Delegation.Sign` / `VerifySignature`
  already provide the secp256k1 primitive. A capability constant
  `CapContentHash` (prefix `chain.v2:hash:`) is added.
- **(e)** `ChainTomlV2.UCANHash` is renamed `AuthorityUCANHash`; the Content
  UCAN is name-derived, no manifest field.
- **(f)** `TrustConfig` / `trustState` stay; the `gateOnUCAN` flow expands to
  walk both UCANs.
- **(g) Hard cutover** — `.sig` is removed in the same commit the Content UCAN
  lands. No migration window (`.sig` is unwired on the consumer and has no
  production users).

### Producer

`RollingV2Publisher` replaces `signer`/`SetSigner` with
`contentMinter`/`SetContentUCANMinter`. On each `Publish`, after the manifest
bytes are written, it mints a Content UCAN over those bytes, persists
`chain.v2.<enr-fp>.<seq>.ucan`, builds the torrent, and seeds it.
`DelegationSource` yields the Authority UCAN. All `.sig` paths are deleted.
`backend.go` wires `SetContentUCANMinter` (same secp256k1 key as discv5/sentry).

### Consumer

On a received peer manifest, `gateOnUCAN` runs the full chain:

1. Fetch `chain.v2.<enr-fp>.<seq>.toml`.
2. Fetch the Content UCAN by name-derived info-hash.
3. Verify the Content UCAN's signature against its issuer.
4. Check the `chain.v2:hash:<H>` capability: `H == sha256(manifest bytes)`.
5. Check the Content UCAN's `nbf`/`exp` cover now.
6. Resolve the Authority UCAN by `ParentHash`; cache by hash.
7. Verify the Authority UCAN traces to a configured trust root.
8. Check the Authority UCAN's `nbf`/`exp` and capabilities.
9. Check the Authority UCAN's audience ≡ the Content UCAN's issuer.

`SetTrust` must be wired in `backend.go` (today never called → gate disabled).
The per-peer manifest cache TTL is bounded by
`min(Content.Expires, Authority.Expires)` — no arbitrary wall-clock TTL.

## Minority-publisher transition — Staged Canonical Adoption

A publisher advertises the last canonical it has agreed to. A *minority*
publisher — one whose retired bytes lost the quorum race (an older retire
algorithm, or a bug) — is non-authoritative and must adopt the agreed canonical.

- **Detection** splits the `CheckOwnAdvertisement` verdict:
  - *divergence* — a name already canonical, own hash differs → fatal, as today.
  - *minority* — own hash for a name failed to reach quorum past a grace window
    while a *different* hash for that name did reach canonical → non-fatal,
    triggers adoption. The publisher runs the Layer-1 view itself (it observes
    peer advertisements via `manifest_exchange` regardless).
- **Staged download-and-replace**: compute the delta (canonical entries held at
  a non-canonical hash, plus canonical entries missing), fetch each by canonical
  info-hash into `<snapDir>/.staging-<canonical-gen>/`, never overwriting live
  files. Each staged file runs the full validator chain; any failure aborts the
  whole batch.
- **Atomic cutover** — under one inventory-write lock, after the whole batch
  validates: quiesce block-reader opens for the affected ranges; `rename(2)`
  each staged file over its live counterpart; rewrite inventory hashes and reset
  `LifecycleState` to `Downloaded`; invalidate (drop old torrents, evict stale
  `RollingV2Publisher` generations, drop affected per-peer manifest and segment
  caches); publish a fresh generation advertising only canonical hashes; resume
  readers.
- **In-flight consumers** of the superseded files have their torrents dropped;
  their download of the non-canonical hash fails (correct — those bytes never
  reached canonical) and the downloader retries the canonical info-hash from
  another peer.

Batching matters because the local node is itself an execution client reading
these snapshot files; a piecemeal swap would leave the state domain internally
inconsistent for the duration.

## Validation: strictly downward

Information flows down the stack; validation flows with it. There is no upward
validation — a canonical view is never revised based on peer advertisements;
disagreeing advertisements are dropped, not promoted. Canonical is the trust
anchor; everything below it is constrained by it.

## Out of scope

- Virtualization (sparse-archive distribution across many partial holders) —
  `HeldRanges` exists for it; consumer fetch-planning is later work.
- Forking (multiple chain heads) — the multi-canonical mechanism is the
  foundation; fork-support code is later.
- Operator observability UI for "where my advertisement sits in the swarm".
