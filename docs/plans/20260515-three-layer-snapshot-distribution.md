# Three-layer snapshot distribution — design

## Why this document exists

The snapshot-flow work to date conflated three concerns that need to be
separate for the system to scale, virtualize, and survive merge
transitions:

1. How the swarm agrees on what's canonical
2. What the canonical content is at any given moment
3. What any specific node holds and serves

The conflation surfaced concretely during initial-publication debugging
on the publisher6 retest cycle (2026-05-15): a single `chain.toml` was
being asked to simultaneously be the consensus document, the node's
inventory advertisement, and the source of truth for the swarm
agreement mechanism. Each of those concerns has different update
cadence, different validation rules, and different failure modes;
fusing them produced bugs that looked like one thing and turned out to
be another.

This document records the model the project is moving to. It is the
contract for upcoming code changes (`ManifestTips`,
`validateAdvertisement`, per-peer chain.toml caching, the Caplin
destination fix, and follow-on work) and the reference for future user
documentation.

## The three layers

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 1 — Swarm agreement                                            │
│                                                                      │
│   "How does the swarm converge on what's canonical?"                │
│                                                                      │
│   Today: trust upstream registry (R2/Github chain.toml fetch).       │
│   Later: quorum across trusted publishers, signature schemes, or     │
│          chain-consensus-anchored derivation.                        │
│                                                                      │
│   Output: a set of accepted canonical chain.toml documents           │
│           (size 1 normally, size ≥2 during merge transitions).       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 2 — Canonical truth (chain.toml)                               │
│                                                                      │
│   "What files are in the chain, with what canonical info-hashes?"   │
│                                                                      │
│   - Single TOML document (per accepted version) listing every file  │
│     name and its canonical info-hash.                                │
│   - Same content for every party reading the same version. Swarm-   │
│     wide consensus, not per-node.                                    │
│   - Deterministic retire is a prerequisite for participation as a   │
│     seeder: a node whose retire produces different bytes will       │
│     advertise a non-canonical hash and have those entries dropped.  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 3 — Per-node advertisement (chain.<enr>.toml)                  │
│                                                                      │
│   "What does THIS specific node serve?"                              │
│                                                                      │
│   - One file per node, named by the node's ENR.                      │
│   - Lists ONLY the files this node holds locally, with the          │
│     info-hashes IT seeds.                                            │
│   - Sparse by construction: no implicit ranges, no merging across   │
│     entries. Each line is independent.                               │
│   - Served by the BitTorrent layer alongside snapshot files.        │
│   - Cached by peers on receipt; cache key is the ENR.               │
└─────────────────────────────────────────────────────────────────────┘
```

The three layers are **strictly independent** in code:

- Layer 1 is unaware of any specific node's holdings.
- Layer 2 is unaware of how the swarm agreed on its content.
- Layer 3 is unaware of both — it just enumerates what's on disk.

No code path reads more than one layer's input and produces a "merged"
artifact. Decisions across layers happen in consumer-side fetch
planning, which queries each layer independently and combines the
answers locally without persisting a fused view.

## Validation: strictly downward

Information flows down the stack. Validation rules flow with it:

| Step | What's validated | How |
|---|---|---|
| Layer 1 → Layer 2 | The accepted canonical set is well-formed | (deferred design) |
| Layer 3 authenticity (consumer-side) | Peer advertisement was produced by the ENR it claims to be from, and hasn't been tampered with in transit | secp256k1 signature in `chain.<peer_enr>.sig` (sidecar) verified against the public key in `peer_enr` over the bytes of `chain.<peer_enr>.toml`. Fail ⇒ drop entire file |
| Layer 2 → Layer 3 (consumer-side) | An advertisement entry matches canonical | After authenticity check passes: entry's `(name, hash)` must appear in at least one accepted canonical version; non-matching entries are silently dropped |
| Layer 2 → Layer 3 (producer self-check) | This node's outgoing advertisement is well-formed | Every entry in the node's own `chain.<self_enr>.toml` must match canonical at publish time; mismatch is a producer bug, fail loud |
| Layer 3 → downloaded file | The downloaded bytes match what was advertised | BitTorrent piece hashing during download; final info-hash check on completion |

There is **no upward validation**. A canonical chain.toml is not
revised based on peer advertisements; peer advertisements that
disagree are dropped, not promoted. The asymmetry is deliberate —
canonical is the trust anchor; everything else is constrained by it.

## Threat model: man-in-the-middle publishers

The swarm by design contains redistributor nodes — peer A's manifest
is cached and re-served by peer B. This is a resilience feature: if
A goes offline, B still has A's chain.<A_enr>.toml and serves it to
other peers. But it means **every peer in the swarm is a potential
MITM with respect to every other peer's manifest**. The threat is
modification-in-transit, not impersonation (the ENR-keyed naming
prevents the latter).

| Scenario | Without signature | With signature |
|---|---|---|
| Peer B re-serves A's manifest unmodified | Works (no attack) | Works |
| Peer B modifies A's manifest before re-serving (drops entries, swaps hashes, adds fake info-hashes pointing at malicious files) | **Silent corruption**: consumer trusts B's claim it's from A; downloads malicious info-hashes; only discovers bad content after a full piece-hash check or canonical-hash check, having already wasted bandwidth and disk | **Loud reject**: signature fails immediately on receipt; the entire file is dropped, B is identifiable as misbehaving |
| Peer C produces its own manifest claiming to be A | Already impossible — ENR-keyed lookup wouldn't route a fetch for A's manifest to C | Signature fails (C doesn't hold A's private key); double protection |
| Peer A offline, B has cached signed copy | Works | Works — signature is over content, no live key check needed |

The canonical chain.toml does not need an analogous signature *today*
because it comes via HTTPS from the upstream registry, and TLS
handles the in-transit integrity. Once the swarm-agreement layer
(Layer 1) moves away from a single trusted upstream — e.g., to
quorum-signed canonicals — Layer 1 will need its own signature
mechanism, distinct from Layer 3's per-node signatures.

## Signature mechanism for chain.<enr>.toml

**Scheme**: secp256k1 ECDSA over the SHA256 of `chain.<enr>.toml`
content bytes. Same curve and key material as the ENR — no new key
management.

**Placement**: sidecar file `chain.<enr>.sig` next to the data file.
Reasons:

- Signing is over verbatim bytes — no canonicalization required for
  field order, whitespace, comments, etc.
- Matches the existing UCAN delegation sidecar pattern
  (`chain.ucan.NNN.toml` paired with `chain.v2.NNN.toml`), so the
  seeding/transport plumbing is well-trodden.
- A signature scheme change later doesn't perturb the TOML schema.

**Producer**: signs `chain.<self_enr>.toml` immediately after
regeneration. Writes `chain.<self_enr>.sig` alongside. Both files
are added as seedable to the BitTorrent layer.

**Consumer**: on receipt of a peer's advertisement, fetches both
`chain.<peer_enr>.toml` and `chain.<peer_enr>.sig` (the sidecar
discovery follows the same mechanism as UCAN — name pattern). Verifies
signature before any other check. Failure ⇒ both files discarded;
peer logged with reason; per-peer cache miss.

**Verification cost**: one secp256k1 verify per cached peer manifest.
At ~100 connected peers each pushing a refresh weekly, that's ~100
verifies per week — negligible.

**Test invariants**:
- Sign a synthetic chain.<enr>.toml + verify with matching pubkey → accept
- Tamper with byte n of the data file + verify → reject
- Sign with wrong key (not matching ENR) → reject
- Missing sidecar → reject (no implicit "unsigned is OK" path)
- Two valid signatures over the same file by same key → both accept (idempotent)

## What this means in practice

These sections describe operationally what the design implies. They
are intended as the source material for future user-facing
documentation.

### For node operators

**You will see two TOML files in your snapshot dir.**

- `chain.toml` (or numbered `chain.v2.NNN.toml` snapshots during a
  retire cycle) is the canonical view of the chain — the same content
  every Erigon node on the same chain agrees on. You don't edit it
  directly; it's fetched from upstream at startup and updated as the
  swarm-agreement layer decides new entries are canonical.

- `chain.<your_enr>.toml` is YOUR node's advertisement of the files
  it actually has and is willing to seed. It's regenerated whenever
  your inventory changes (after a download, retire, or merge). You
  don't edit it directly either; it's derived from your local disk
  state.

You will also see `chain.<peer_enr>.toml` files for each connected
peer you've fetched a manifest from, each paired with a
`chain.<peer_enr>.sig` sidecar containing the peer's signature over
its data file. These are cached on receipt — only after the signature
verifies — and seeded back out so peers who lose connectivity to a
specific node can still discover what that node serves. They're
garbage-collected after a staleness threshold (default: 7 days since
last refresh).

Your own outgoing files are similarly paired: `chain.<your_enr>.toml`
and `chain.<your_enr>.sig`. The signature uses the same secp256k1 key
your node uses for discv5 and sentry handshakes — no separate key
management. Operators don't interact with the signing process; it's
automatic at every regeneration.

**Your node will refuse to advertise entries it can't justify against
canonical.** If your local retire produces a file with an info-hash
that doesn't match canonical, the producer self-check catches it at
publish time and logs a loud error. This is intentional: rather than
silently advertise bad data and have downstream consumers waste
bandwidth, your node tells you about the problem at the source.

**Sparse holdings are first-class.** A minimal-mode publisher seeds
only the recent files it has. A specialist node could seed only blob
sidecars for the last 30 days. A future virtualized archive could
seed individual ranges across hundreds of nodes. None of these are
"degraded full nodes" — they're explicitly-advertised partial
participants, and consumers planning fetches know exactly who serves
what.

### For consumers (nodes downloading a snapshot)

**You learn what to download from canonical.** The chain.toml fetched
from upstream tells you what files exist on the chain and what their
correct hashes are. Your bootstrap reads it and decides what subset
your prune mode requires.

**You learn where to download from peers' advertisements.** As your
node connects to peers, you receive each peer's `chain.<enr>.toml`
and cache it locally. To download a specific file, you look across
cached advertisements to find peers that serve it.

**Mismatched advertisements get dropped.** If a peer claims to seed a
file with an info-hash that doesn't match canonical, your node
silently drops that entry on receipt. You don't waste bandwidth
fetching from them. The peer might still be useful for other files
where their hashes do match; only the bad entries are filtered.

**Tampered advertisements get rejected wholesale.** Before any
per-entry filtering, your node checks the signature on each peer's
`chain.<peer_enr>.toml` against the public key in their ENR. Failure
means either the file was modified in transit (the seeder/MITM model:
some redistributor peer changed it before re-serving) or the peer
itself is buggy/malicious. Either way the whole file is rejected —
not just the changed bits — because partial trust isn't meaningful
once authenticity is broken. The peer's other manifests (if you've
seen them) are unaffected; only this specific manifest gets dropped.

**During merge transitions, multiple canonicals may be in play.** The
swarm-agreement layer will signal when canonical chain.toml is in
transition (e.g., during a merge from `X.0-1024.kv` +
`X.1024-2048.kv` into `X.0-2048.kv`). Your node validates against the
union of currently-accepted canonicals: a peer's advertisement is
valid if it matches any of them. This means you can fetch from peers
on either side of the transition without disruption.

### For publishers (nodes that retire new files)

**Your retire output must be deterministic.** Same input chain state
+ same retire algorithm + same compression parameters must produce
byte-identical files. BitTorrent info-hashes are content-derived; if
your retire produces different bytes than other publishers, your
files get different info-hashes and your advertisement entries are
filtered out by every consumer's `validateAdvertisement` check.

**You participate by carrying canonical forward.** Every retire cycle
your node produces new files. Once the swarm-agreement layer
accepts them into canonical (specifics deferred), the canonical
chain.toml grows monotonically. Your `chain.<self_enr>.toml`
naturally tracks what you've retired locally, and as you upload to
peers they propagate the new advertisement through the swarm.

**You can be wrong without breaking anything else.** If your retire
has a bug and you produce a file with the wrong content, your
advertisement carries the wrong hash. Consumers filter it out
automatically; no canonical state gets corrupted; no other publisher
is affected; your operator sees the producer self-check error and
investigates. The blast radius of a buggy publisher is bounded to
its own data.

### For developers writing tests

**Test inputs are fixture chain.toml files, not live fetches.**
Capture the canonical chain.toml at known dates under
`testdata/snapshot-flow/` and reference them deterministically. Tests
that fetch from upstream are not deterministic.

**`ManifestTips` is the canonical helper.** Same function for
canonical chain.toml and per-node advertisements; different inputs,
different semantics for the result. Pin `ManifestTips(fixture)
==expected_tips` per fixture.

**Validation tests assert subset behavior, not specific filtering
rules.**

- Synthetic advertisement with bad hash + canonical fixture →
  validateAdvertisement output drops bad entry, keeps good ones.
- Multi-canonical fixture set (simulating merge transition) +
  advertisement valid under one version → validateAdvertisement
  keeps the entry, signalling cross-version tolerance.

**Producer determinism tests use the same fixture pattern.**

- Empty datadir + fixture canonical + fixed prune mode + fixed flags
  → bootstrap-synthesis output is byte-identical across test runs.
- Pin entry counts per (chain × prune mode × file kind).

## Implications for current code

The model maps onto existing infrastructure with two specific gaps,
identified by the manifest_exchange audit:

| Existing | Status | Action |
|---|---|---|
| `chain.toml` fetched from R2/Github at startup | Functional | Treat as canonical Layer 2 input; no schema change |
| `chain.v2.NNN.toml` produced by `RollingV2Publisher` | Functional | This IS Layer 3 producer output. Will alias / symlink as `chain.<self_enr>.toml` for the stable-name view |
| Peer manifest received via `manifest_exchange.fetchAndPublish` | **Gap** | Transient — published as event, not cached to disk |
| Signature on per-node advertisements | **Gap** | No `chain.<enr>.sig` sidecar produced or verified; MITM-publisher tampering would be silent |
| `validateAdvertisement(adv, canonicals)` filter on receipt | **Gap** | Doesn't exist; peer entries trusted as-is today |
| `ManifestTips(items, *chain.Config)` derivation helper | **Gap** | Tips are inferred ad-hoc; need single canonical helper |
| Producer self-check at publish time | **Gap** | Publisher trusts its own retire blindly |

The corresponding work items, ordered:

1. **`ManifestTips` helper** — single-source-of-truth derivation. Used by everything downstream. Doesn't commit to schema changes.
2. **`HeldRanges` helper** — sparse-aware advertisement range enumeration. Used by consumer fetch planning.
3. **`validateAdvertisement(adv, canonicals []ChainToml)`** — multi-canonical signature from day one (size 1 today, size ≥2 once merge support lands).
4. **`signAdvertisement(data []byte, privKey)`** and **`verifyAdvertisement(data []byte, sig []byte, peerPubKey)`** — secp256k1 over SHA256. Producer-side and consumer-side respectively.
5. **Canonical chain.toml test fixture** — captured under `testdata/snapshot-flow/`, with pinned tip values.
6. **Producer self-check** — at `RollingV2Publisher.Publish` time, assert every entry in the outgoing manifest has a canonical match. Fail loud on mismatch.
7. **Producer signing** — at `RollingV2Publisher.Publish` time, also write `chain.<self_enr>.sig` next to the data file; add to seedables.
8. **Consumer-side disk cache + signature verify** — `fetchAndPublish` fetches both data and sidecar via the manifest-exchange protocol, verifies signature using the peer's ENR public key, then runs `validateAdvertisement`, then writes both files to `datadir/snapshots/chain.<peer_enr>.{toml,sig}` and registers them as seedable. GC by staleness.
9. **Caplin destination fix** — use canonical chain.toml's block-tip via `ManifestTips`, not EL's `FrozenBlocks()` which collapses to state-tip.
10. **Bug Z** — minimal-mode bootstrap drops `transactions.seg` below `canonical.block_tip - 100K`, computed via `ManifestTips`.

## Deferred work (explicitly out of scope for current round)

- **Swarm-agreement layer redesign** (Layer 1): replacing upstream-registry trust with quorum/signature/chain-anchored agreement. The model accommodates it; the change is deferred.
- **Virtualization** (sparse-archive distribution across many partial holders): the chain.<enr>.toml format supports it natively; the consumer fetch-planning code to take advantage is later work.
- **Forking** (multiple chain heads with overlap): the multi-canonical mechanism is the foundation; the actual fork support code is later.
- **Client introspection / agency UI** (operator observability of "where my advertisement sits in the swarm"): the data is computable once layers exist; the surfacing is later.

These are all enabled by the layering. None of them are required for
the current round of stability + repeatable initial publication.

## Release sequencing (per direction of 2026-05-15)

The variability-introducing features (forking, virtualization) land
on a stable base. Current round = stabilize the current model.
Subsequent rounds add one variability vector at a time. The three-
layer split is the prerequisite that makes the sequence possible
without rewriting at each stage:

1. **Round A (current)**: stabilize the producer/consumer baseline.
   Land Layers 2 and 3 with single-canonical semantics. Pin tests.
2. **Round B**: forking — multiple canonical heads. Multi-canonical
   validation already in place (validateAdvertisement signature
   accepts a slice); swarm-agreement layer learns to track multiple
   accepted chains.
3. **Round C**: virtualization — sparse-archive distribution. Per-
   node advertisements already sparse-by-construction; consumer
   fetch-planning extends to cross-peer range coverage and
   on-demand piece-level lookup.

Each round assumes the previous is stable. Test contracts are
additive: round B's tests don't replace round A's invariants, they
extend them. Round C does the same to A+B.
