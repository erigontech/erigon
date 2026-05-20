# Two-UCAN shape — design questions

> **Superseded (2026-05-20)** by
> `20260520-chaintoml-ucan-flow-spec.md`, which resolves all 7 open
> questions below and is the authoritative two-UCAN design. Retained
> for historical context.

**Status**: design pending. Recording the questions now so when we
return to it after the current re-test cycle, the framing is in
place.

## Why this document exists

The session that produced
`20260515-three-layer-snapshot-distribution.md` added a `.sig` sidecar
(`chain.v2.<seq>.sig`) carrying a secp256k1 signature over the
manifest bytes — to defend against in-transit modification by MITM
redistributor peers. That signature lives **outside** the UCAN
delegation chain, which means we now have TWO authentication
artefacts per published manifest:

  - **UCAN sidecar** (`chain.ucan.<seq>.bin`): proves the operator
    has been delegated authority to publish on this chain. Carries
    `nbf`/`exp` liveness, capabilities, and a delegation chain back
    to a trust root.
  - **Signature sidecar** (`chain.v2.<seq>.sig`): proves the
    manifest content was signed by the ENR-holder and hasn't been
    tampered with.

These overlap conceptually. Both are "authenticate this manifest
artefact"; they answer different sub-questions (content integrity
vs. authority delegation). Fragmenting them across two unrelated
mechanisms means the permissions model UCAN was designed for stops
covering all the relevant assertions.

The goal of this document: replace `.sig` with a UCAN-integrated
mechanism so all authentication assertions flow through a single
delegation chain, with consistent liveness, consistent verification,
and consistent operator UX.

**Principle**: permissions stay integrated with UCAN. Don't
introduce parallel signing systems that fragment the trust model.

## Lifecycle constraint

Three distinct cadences must be respected:

| Concern | Cadence | Bound to |
|---|---|---|
| Manifest content | Slow — minutes to hours | Inventory change (retire output, merges) |
| Operator authority | Long — months | Operator key rotation, authority changes |
| Manifest integrity attestation | Same as manifest content | Manifest regeneration only |

Embedding everything in a single UCAN that rebinds on the fastest
cadence forces the slow concerns (operator authority, in particular)
to churn unnecessarily. We want UCANs that match the natural
cadence of what they attest.

## Proposed shape: two UCANs

| UCAN | Purpose | Issuer → Audience | Liveness | Capabilities |
|---|---|---|---|---|
| **Authority UCAN** | "Operator X is delegated by root R to publish on this chain" | Root authority → operator pubkey | Long (months); rotates on key/authority change | `snapshot.publish:<chain>` |
| **Content UCAN** | "Manifest with hash H is current, attested by operator X" | Operator pubkey → itself | Short — per-generation, expires on next regeneration | `chain.v2:hash:<sha256_of_toml>` |

Two physical files, two cadences:

```
chain.ucan.authority.<rev>.bin   long-lived; refreshes when authority/operator rotates
chain.v2.<seq>.ucan              co-generated with chain.v2.<seq>.toml; content-bound
```

The Content UCAN's `Parent` field references the Authority UCAN —
either inline (current `Parent []byte`) or by-hash. Open question (a)
below.

## Verification chain at the consumer

1. Fetch `chain.v2.<seq>.toml`
2. Fetch `chain.v2.<seq>.ucan` (Content UCAN)
3. Verify Content UCAN's signature using the operator pubkey
4. Check Content UCAN's hash-claim ≡ `sha256(chain.v2.<seq>.toml)` —
   *this is what `.sig` was doing*
5. Check Content UCAN's nbf/exp covers now
6. Walk to the Authority UCAN it references; fetch if not cached
7. Verify Authority UCAN's signature traces to a configured trust root
8. Check Authority UCAN's nbf/exp covers now; capabilities include
   the chain we're on
9. Verify Authority UCAN's audience pubkey ≡ Content UCAN's issuer
   pubkey

Three concerns covered through one verification chain, with
delegation, capabilities, and liveness handled by UCAN throughout:

  - Content integrity (step 4) — what `.sig` did
  - Operator authenticity (step 3, key from Authority UCAN)
  - Authority delegation (steps 6-9)

## Open design questions

### (a) Parent reference: inline vs. by-hash

Today `Delegation.Parent []byte` stores the parent UCAN inline. For
the two-UCAN model, every Content UCAN would carry a full copy of
the (slowly-changing) Authority UCAN — adds maybe 200-500 bytes per
generation × 64 retained generations = up to 30 KB. Not large, but
duplicated.

Alternative: `Delegation.ParentHash []byte` references the parent's
content hash. Authority UCAN becomes a separate fetch (matches
today's chain.ucan.<seq>.bin pattern). Consumer caches Authority
UCANs by hash; cache hit on the same authority across generations.

**Lean**: by-hash. The cache-friendliness matters more than the
duplication cost. But this is a schema change to the `Delegation`
struct.

### (b) Authority UCAN distribution

Same BitTorrent path as today's UCAN (a sidecar info-hash in the
manifest, fetched on demand)? Or out-of-band (chain config,
trusted endpoint)?

**Lean**: BitTorrent path, same plumbing. Decoupling from
BitTorrent introduces a different trust mechanism for the most
trust-critical layer — counterproductive.

### (c) Content UCAN audience

Self (operator → operator) is the cleanest semantically — "I
attest that this manifest is mine and current." But UCAN spec
typically requires a distinct audience. Audience-less / broadcast
might need a spec extension.

**Lean**: self-audience. Most consistent with the "I attest" semantic.
Verify our UCAN library supports it.

### (d) What replaces `chain.v2.<seq>.sig` and `ManifestSignerFn`

The signing callback becomes `ContentUCANMinterFn`:

```go
type ContentUCANMinterFn func(manifestBytes []byte) (ucanBytes []byte, error)
```

Same shape: bytes in, bytes out. Internally the implementation
computes `sha256(manifestBytes)`, builds a Content UCAN with that
hash as a capability claim, signs with the operator's key, encodes
to CBOR. The publisher persists the result as `chain.v2.<seq>.ucan`.

The `.sig` file goes away entirely. The `SignAdvertisement` /
`VerifyAdvertisement` helpers in `db/snapshotsync` could survive as
the underlying primitive (UCAN minting uses them internally) or be
inlined into the UCAN minter implementation.

### (e) Manifest schema: `UCANHash` field

Today `ChainTomlV2.UCANHash` carries the Authority UCAN info-hash.
With two UCANs:

  - Drop `UCANHash`; both files are discoverable by name pattern
  - Or rename to `AuthorityUCANHash` and add `ContentUCANHash`
  - Or use a separate field for each so the manifest is
    self-describing

**Lean**: name pattern based discovery for the Content UCAN (its
filename is derivable from the manifest's seq); keep `UCANHash` (or
`AuthorityUCANHash`) for the Authority UCAN since its revision
doesn't align with manifest seq.

### (f) Trust-config interaction

Today `manifest_exchange/trust.go` configures the UCAN verification
gate. With two UCANs the gate logic changes:

  - Single trust check → verify Content UCAN end-to-end through
    Authority UCAN to trust root
  - Per-entry capability check: Content UCAN's
    `chain.v2:hash:<H>` claim against the fetched manifest's hash

The trust struct + TrustConfig stay; the verification flow expands
to walk both UCANs.

### (g) Migration

For nodes that have not adopted the two-UCAN model yet:

  - Producer: emits both `.sig` AND `.ucan` content sidecar until
    `.sig` is removed in a follow-up release
  - Consumer: accepts EITHER `.sig` matching the legacy path OR
    Content UCAN with matching hash claim
  - One release of overlap, then `.sig` removed

OR:

  - Hard cutover when the design is settled and tests are in place
  - `.sig` removed in the same commit that adds Content UCAN

**Lean**: hard cutover. The `.sig` work is new and not yet in
production use; no upgrade compatibility to preserve. Net deletion
of `.sig` + `ManifestSignerFn` + sidecar plumbing + tests.

## Cache TTL is bound by UCAN expiry

A consequence of moving content attestation into the UCAN: the
TTL of cached peer manifests
(`datadir/snapshots/peer-manifests/chain.<peer_id>.toml`) is
**bounded by the associated Content UCAN's `exp` claim**, not by an
arbitrary wall-clock staleness threshold.

Why this is correct:

  - The Content UCAN's `exp` is the operator's explicit statement
    "I attest this manifest is current until time T". After T, the
    operator has not re-attested. The manifest may still be
    *byte-correct* (content hasn't changed) but it's no longer
    *current* per the operator's own attestation.
  - A consumer holding a cached manifest past its UCAN's `exp` is
    holding state the operator has implicitly retracted. Continuing
    to advertise it (re-seed) propagates a claim the issuer is no
    longer making.
  - The Authority UCAN's `exp` is the longer-cadence bound. If
    Authority UCAN expires, every Content UCAN signed under it is
    transitively invalid regardless of its own `exp`.

So the cache GC rule becomes:

```
Evict chain.<peer_id>.toml when:
  Content UCAN.exp < now()
  OR Authority UCAN.exp < now()
  OR the peer republishes a newer manifest (existing replace-on-receive)
```

No arbitrary TTL. No 7-day default. The numbers come from the
attestation chain itself.

Implementation notes:

  - The Content UCAN's expiry must be stored alongside the cached
    manifest (or re-derivable on read). Simplest: cache the
    `.ucan` file next to the `.toml`; GC reads the UCAN's `exp`
    field directly.
  - Eviction can be lazy (check on access) or periodic (sweep
    every N minutes). Periodic is cheaper at scale but may serve
    expired manifests transiently. Lazy guarantees expiry but
    incurs a UCAN-decode cost per cache hit.
  - When evicting due to expiry, the corresponding `.torrent`
    registration must also be torn down so we don't continue
    seeding a retracted attestation.

This also means **the deferred "GC of stale per-peer cache files
with 7-day TTL" todo is obsolete** — the UCAN-bound rule
supersedes it.

## What this displaces from the current implementation

| Current (post-82fc20bd1b) | After two-UCAN design |
|---|---|
| `RollingV2Publisher.SetSigner(ManifestSignerFn)` | `RollingV2Publisher.SetContentUCANMinter(ContentUCANMinterFn)` |
| `chain.v2.<seq>.sig` sidecar | `chain.v2.<seq>.ucan` sidecar |
| `ChainV2SigFileNameForSeq` helper | `ChainV2ContentUCANFileNameForSeq` helper |
| `SignAdvertisement` / `VerifyAdvertisement` standalone helpers | Inlined into UCAN minter / verifier OR retained as primitives |
| Producer signing wiring in `backend.go` (P2P.PrivateKey closure) | Wired to the operator's UCAN minter using same key |
| Future `FetchPeerSig` method + `SignatureVerifierFn` | Replaced by Content UCAN fetch through existing `UCANFetcher` interface |

The .sig path was deliberately built parallel to UCAN — same
sidecar pattern, same eviction handling, same callback shape. That
parallelism makes the consolidation tractable.

## When to do this

After the current re-test cycle confirms the existing pieces work
end-to-end. Order:

  1. Re-test current branch (Bug X/Y/Z + producer self-check +
     producer signing + consumer filter + Caplin block_tip)
  2. Settle the open questions above (each one is small; can
     answer over a single sitting)
  3. Implement the two-UCAN consolidation
  4. Drop `.sig` path

Listed in `MEMORY.md` and the session todo list as a deferred
work item.
