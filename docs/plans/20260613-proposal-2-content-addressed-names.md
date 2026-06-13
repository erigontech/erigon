# Proposal 2 — Content-addressed snapshot filenames

**Status**: draft, iteration-2 work item. Must be resolved before
production adoption of the experimental snapshot-flow + mode-B
unwind implementation.

**Depends on**: [Proposal 1](20260613-proposal-1-txnum-boundaries.md)
— filenames must carry canonical (txNum-based) metadata before a
content hash committed to that metadata is meaningful.

**Sibling of**: [Proposal 3](20260613-proposal-3-chain-definition-transports-split.md)
— P3 separates "what the file is" from "how the file gets here";
content-addressed identity is the cleanest hinge for that split.

## TL;DR

Today a snapshot file's identity is its filename — a string that
encodes domain, range, and format version. There is no in-name
binding to the file's bytes; if a publisher serves a corrupted file
under the right name, consumers accept it (subject to whatever
domain-specific validation runs after download). This proposal makes
the file's content hash part of its name and surface, so identity
verification reduces to a single byte-comparison the transport
layer can perform.

The content hash is **transport-independent** — the same value
appears in the filename, the manifest, the W3C SRI tag, the RFC 9530
HTTP header, the BT v2 metainfo. Whether the file came over
BitTorrent, HTTP, IPFS, or a USB stick, the verification is the
same.

This proposal lays out the encoding scheme, the keccak/SHA-256
separation, the relationship to BitTorrent v2 (BEP 52), the W3C
SRI / RFC 9530 layering, and the migration path.

## Motivation

### 1. Untrustworthy transports must be a wasted-resources risk, not an integrity risk

Today's transport (`chain.toml` manifest + BitTorrent v1 infohash) is
trusted: the infohash commits to the file's bytes (BT v1 piece
hashes are SHA-1 over piece-sized chunks of the concatenated
torrent files), and `chain.toml` is itself signed via the UCAN
delegation chain. Tampering at any single hop is detectable.

A future world where transports diversify — IPFS, HTTP webseeds
without signed manifests, sneakernet — needs an identity that
travels with the file independently of its delivery channel. A
content hash IS that identity. With it, an attacker controlling
the transport can waste a consumer's bandwidth (serving wrong
content) but cannot trick the consumer into accepting it as
canonical.

Without a hash that travels with the file, every new transport
mechanism has to re-derive trust — through its own signing scheme,
its own infohash convention, its own metadata table. Content
hashes collapse all of that into one universal coordinate.

### 2. Hash-versioned filenames distinguish good and bad files

If a publisher mis-merges a file (the truncated
`v2.0-commitment.272-274.kv` from the failed soak that wedged boot
this week is the canonical example), the consumer ends up with the
wrong bytes under a name that LOOKS right. The good and bad files
share an identity; collisions silently overwrite.

With the hash in the name, a wrong-bytes file gets a wrong name.
Two files with the same `(domain, fromTxNum, toTxNum)` but
different bytes have different filenames; both can coexist on
disk; the consumer picks the one whose hash matches the manifest's
expectation, or rejects both and quarantines.

### 3. Sub-range verification (HTTP byte ranges)

A consumer fetching a 130 MB commitment file over HTTP with a
broken Wi-Fi connection wants to resume. With one whole-file hash,
resume requires re-downloading and re-hashing everything up to
the resume point. With a per-chunk Merkle structure (Proposal 2
Part B), individual chunks can be hash-checked against the file's
Merkle root and the rest of the file's content trusted by
construction.

This generalises: BitTorrent v2 (BEP 52) does this at piece level
with a per-file SHA-256 Merkle tree over 16 KiB leaves. MICE
(`mi-sha256`) does it over arbitrary HTTP byte ranges. The
structure is the same; the wire is different. P2 picks **16 KiB
leaves of SHA-256** to match BT v2's choice exactly, so the same
Merkle tree serves both transports natively.

### 4. Content hash, not transport hash

A BitTorrent infohash commits to the *torrent metainfo*, not just
the file contents. Two semantically-identical files in two
different torrent batches have different infohashes. The infohash
is a transport coordinate; we want a content coordinate.

W3C SRI (`sha256-<base64>`) and RFC 9530 Repr-Digest
(`sha-256=:<base64>:`) are content coordinates. They name the
file's bytes regardless of how those bytes are delivered. They
also already exist in standards, with existing tooling support.
We adopt them.

## Proposed grammar

```
old (Proposal 1):  v<format-version>-<domain>.<fromTxNum>-<toTxNum>.<kind>
new (Proposal 2):  v<format-version>-<domain>.<fromTxNum>-<toTxNum>.<hashprefix>.<kind>
```

- `format-version`, `domain`, `fromTxNum`, `toTxNum`, `kind` — as in
  Proposal 1.
- `hashprefix` — first 64 bits (8 bytes, 11 base64-url chars) of the
  file's SHA-256 Merkle root. Truncated for filename length; the
  full 256-bit hash lives in the manifest.

Example:

```
v2.0-commitment.103125000-104297500.AbCdEfGhIjK.kv
```

The 64-bit prefix gives `2^-64` probability of accidental collision
across a single chain's file set — negligible at any realistic
chain size. Adversarial collisions require a 64-bit preimage attack
on SHA-256 (still ~`2^64` work) — strong enough for filename
disambiguation; not strong enough to admit a wrong file (the
manifest still verifies the full 256-bit hash).

The infix `.<hashprefix>.` sits between the `txNum` boundary and the
`kind` suffix so a consumer can recover `(domain, fromTxNum, toTxNum)`
from the filename for routing purposes, then verify integrity by
comparing the prefix against the manifest's full hash.

## The hash function: SHA-256, not Keccak-256

Erigon already uses **Keccak-256** as its state-trie hash function
(matches Ethereum consensus rules). It is tempting to reuse Keccak
here too, but they commit to different things and should stay
separate:

| Purpose | Hash | Why |
|---|---|---|
| State commitment | Keccak-256 | Consensus rule. The block header's `state_root` is the Keccak hash of the state trie. Any consumer reading the state file MUST recompute the state root with Keccak; using SHA-256 here would break the consensus check. |
| File-content commitment | SHA-256 | Distribution-layer rule. W3C SRI, RFC 9530, BT v2, MICE, IPFS CIDs (`sha2-256` variant), CDN edge-cache validators all canonically use SHA-256. Picking it here means we get all of those standards' tooling for free. |

These two commitments are **independent**. The state root binds the
state's *content*; the file hash binds the file's *bytes*. Both
must verify, and they verify against different things, with
different tooling.

This separation is the same one HTTP/HTTPS makes between the
TLS-layer integrity (which protects the bytes in transit) and the
SRI-layer integrity (which proves the bytes are the ones the
publisher signed). They overlap but do not substitute.

## Merkle structure: BitTorrent v2 layout

The file's hash is the **root of a SHA-256 Merkle tree** over 16 KiB
leaves. This matches BitTorrent v2 (BEP 52) exactly:

- Each 16 KiB block of the file is one leaf.
- Leaves are pair-hashed up the tree to a single root.
- Final unpaired leaves are duplicated to balance the tree (BEP 52's
  rule).
- A torrent's metainfo carries the per-file root; consumers verify
  each downloaded piece (also 16 KiB on v2) against its Merkle
  branch.

By picking the SAME leaf size and the SAME tree shape, our
content hash IS the BitTorrent v2 file root. No translation; no
two-source-of-truth problem.

For HTTP transports, the same Merkle tree underlies MICE
(`mi-sha256`) sub-range verification: each 16 KiB MICE block hash
chains to the root, so partial-download resume verifies cheaply.

For sneakernet / sneakernet-like transports, the root alone is
enough: hash the file in 16 KiB chunks, build the tree, compare.

## RFC 9530 Repr-Digest header

For HTTP delivery the file's content hash is advertised in the
**`Repr-Digest`** response header (RFC 9530):

```
Repr-Digest: sha-256=:<base64-encoded-256-bit-root>:
```

This is the standardised "the hash guarantee" the user
identified as the web's equivalent of our manifest commitment.
It binds the response body to a hash the client can verify
end-to-end; intermediate caches and CDNs don't have to be
trusted. Any HTTPS-fronted snapshot store gets this nearly
for free — most web servers and CDNs can emit `Repr-Digest`
with a config flag.

## W3C SRI tags

W3C Subresource Integrity (SRI) attributes — `sha256-<base64>` —
are the canonical form when the hash needs to ride alongside a
URL. Manifest entries can carry an SRI tag per file so a third
party with just the manifest fragment can verify any file from
any transport:

```toml
[[files]]
name = "v2.0-commitment.103125000-104297500.AbCdEfGhIjK.kv"
integrity = "sha256-<full-base64-root>"
size = 129048107
```

## Migration path

Same Option B as Proposal 1: dual-encoding read, single-encoding
write.

- Read path accepts both old (`.kv`) and new (`.<hashprefix>.kv`)
  filename forms.
- Write path always emits the new form.
- Manifest entries gain an optional `integrity = "sha256-..."`
  field; absence on existing entries means "no SRI check —
  trust the BT v1 infohash as today."

Once the merge cadence supersedes all old files, the dual path
removes.

## What this proposal does NOT change

- Consensus state commitments (Keccak-256 stays on the state
  trie — required by Ethereum consensus rules).
- File contents (bytes-on-disk are identical to today; we are
  just adding a hash-based identity layer).
- The BT v1 infohash mechanism (continues to work alongside;
  consumers can verify via SRI/BT v2 root if they prefer).
- `chain.toml`'s UCAN delegation chain (still the authority for
  who can publish what; the content hash is orthogonal).

## Open questions

### 1. How does this interact with chain.toml's signed authority?

Today `chain.toml`'s entries are authoritative via the UCAN chain.
Adding `integrity = "sha256-..."` gives a second authority for the
file's bytes; if they disagree, who wins?

Answer: the manifest hash is what the consumer trusts (UCAN-signed).
The file-on-disk's hash MUST equal the manifest's hash. If they
disagree, the file is rejected. The manifest is still the root of
trust; the integrity field is just a typed, standards-compliant
binding from the manifest's authority to the file's bytes.

### 2. Should the hash prefix be base64-url or hex?

Base64-url is more compact (11 chars vs 16 chars for 64 bits) and
URL-safe. Hex is more readable. Both work; this proposal picks
base64-url for compactness — filenames are already long.

### 3. Block snapshots (`v1.1-*`)

Same question as Proposal 1. Block snapshots are already named by
raw block numbers; adding a hash prefix produces e.g.
`v1.1-002900-003000-AbCdEfGhIjK-headers.seg`. Lower urgency than
state files because retire reproduction is cheap, but consistent
with the state-file treatment is preferable. **Recommendation**:
include block snapshots in the migration, same Option B path.

### 4. How does the Merkle tree get computed without re-reading the file every time?

Build once at file-close time (post-retire/merge), persist the
root in a sidecar (`<file>.merkle` or similar) or write it into
the manifest at the same moment the file becomes Advertisable.
The tree itself doesn't need to be kept after the root is
extracted — re-derivable from the file when sub-range
verification is needed.

### 5. What happens to seeding when filenames change?

A publisher upgrading from old-format filenames to new-format
filenames produces new `.torrent` files. Old peers still serving
the old name continue to do so; the dual-encoding read path
accepts both. No coordination needed.

## Recommendation

**Adopt the four-layer integrity story**:

1. **In-name**: 64-bit truncated SHA-256 Merkle root in filename.
2. **In manifest**: full 256-bit root as `integrity = "sha256-..."`
   (W3C SRI form), authoritative via UCAN.
3. **In HTTP**: `Repr-Digest: sha-256=:...:` on HTTPS delivery.
4. **In BitTorrent**: BT v2 metainfo carries the same Merkle root
   natively (no separate computation needed).

The 16 KiB leaf size + SHA-256 hash function are picked to match
BT v2 exactly so the same Merkle tree serves both transports
natively.

## Sequencing

This proposal depends on Proposal 1: filename metadata must be
canonical (txNum-based, not derived) before a hash committing to
that metadata makes sense. The hash should commit to the file's
bytes only, not to its derived chain-config-dependent metadata.

Proposal 3 (chain-definition / transports split) sits downstream:
once files have a content-addressed identity, the "chain
definition" artifact lists files by content hash, and the
"transports" artifact maps those hashes to specific delivery
URLs / infohashes. Without the content hash, the split has no
canonical pivot.
