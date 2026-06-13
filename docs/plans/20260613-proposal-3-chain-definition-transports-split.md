# Proposal 3 — Chain definition / transports split

**Status**: draft, iteration-2 work item. Must be resolved before
production adoption of the experimental snapshot-flow + mode-B
unwind implementation.

**Depends on**: [Proposal 1](20260613-proposal-1-txnum-boundaries.md)
(canonical txNum boundaries) and
[Proposal 2](20260613-proposal-2-content-addressed-names.md)
(content-addressed filenames). Both must land before P3 has a
canonical pivot to split against.

## TL;DR

Today's `chain.toml` mixes two concerns:

- **What the chain IS** — canonical file set, content hashes, step
  boundaries, fork schedule, genesis. Authoritative; signed via the
  UCAN delegation chain; identical across all peers.
- **How the chain GETS HERE** — torrent infohashes, webseed URLs,
  CDN endpoints, IPFS gateways, peer-set hints. Operational;
  transport-specific; varies per deployment / peer / region.

Mixing them couples authority to transport: changing a webseed URL
re-signs the chain. Adding a new transport mechanism requires a
schema migration on the trust artifact. Distributing the trust
artifact ITSELF over an untrusted transport requires the trust
artifact to validate itself, which loops.

This proposal splits the two into separate artifacts:

- **`chain.toml`** (renamed `chain.<chainID>.def.toml` or similar) —
  pure chain definition. File hashes, ranges, fork timing, genesis,
  canonical commitments. Signed; rarely changes.
- **`transports.toml`** (or `<chainID>.transports.toml`) — advisory
  transport map. Hash → list of `{type, url}`. Unsigned (or signed
  by a different, looser key); changes often; can be augmented per
  peer.

The split is what makes "untrusted transports" safe: adding IPFS as
a transport doesn't change the trust model, it just adds entries to
the (separately signed, advisory) transports artifact. A
mis-publishing IPFS gateway can waste resources but cannot trick
consumers into accepting wrong bytes.

## Motivation

### 1. Authority vs availability are different problems

Today `chain.toml` answers BOTH:
- "Is this the right chain?" (authority — UCAN signature chain)
- "Where do I get this file?" (availability — webseed URLs,
  torrent infohashes, peer hints)

These have different change cadences and different trust models.

Authority is rarely-changing — a new state-snapshot file every step
boundary; a new genesis only when forking. Once signed, the answer
is durable.

Availability is constantly-changing — webseed URLs go down, CDN
endpoints move, peers join and leave, transports get added (HTTP →
BitTorrent → IPFS → ...). Each availability change today requires
re-signing the trust artifact.

That coupling makes the trust artifact slower than it should be,
and it makes adding new transports a coordination ceremony rather
than an operational change.

### 2. Untrustworthy transports must not be in the trust artifact

Once content-addressed identity exists (Proposal 2), wrong bytes
are detectable regardless of how they arrived. That makes
untrusted transports SAFE — they can only waste bandwidth, not
break integrity. To get the safety, the transports must NOT live
in the trust artifact (otherwise adding a sketchy IPFS gateway
requires re-signing the trust artifact, which defeats the point).

Split them so transports are advisory and the trust artifact only
commits to content hashes.

### 3. Per-peer transport diversity

A consumer in a high-bandwidth datacenter may prefer HTTPS
webseeds; a consumer behind an asymmetric residential link may
prefer BitTorrent; a consumer on a censored network may prefer
Tor-fronted IPFS. The chain definition is the same for all of
them; the transport map should be configurable per consumer.

With the split, a consumer can ignore the published transports
file and use their own. Or augment it with private mirrors.
The chain definition stays canonical.

### 4. Federated history-network sits naturally on top

The federated history-network design (erigon-documents
`erigon3/history-network/`) wants a substrate that lists files
by content hash and lets different clients agree on the
"canonical file set" while disagreeing on transport. The chain
definition IS that substrate. The transports file is the federation
layer.

Today's `chain.toml` mixes them, so a Geth-side consumer of the
history network would need to ingest erigon-specific transport
hints. With the split, Geth (or Reth, or Nimbus) consumes the
chain definition AS IS and brings its own transports.

## Proposed structure

```
chain.<chainID>.def.toml      # "the chain definition"
chain.<chainID>.transports.toml  # "advisory transports"
```

### Chain definition (`chain.<chainID>.def.toml`)

Authoritative. Signed via the UCAN delegation chain. Lists every
canonical file by content hash:

```toml
[chain]
id = 560048              # hoodi
name = "hoodi"
genesis_hash = "0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b"

[fork_schedule]
shapella = "1970-01-01T00:00:00Z"
dencun   = "1970-01-01T00:00:00Z"
pectra   = "2025-03-26T14:37:12Z"
fusaka   = "2025-10-28T18:53:12Z"
bpo1     = "2025-11-05T18:02:00Z"
bpo2     = "2025-11-12T13:52:24Z"

[[files]]
name      = "v2.0-commitment.103125000-104297500.AbCdEfGhIjK.kv"
domain    = "commitment"
from_tx   = 103125000
to_tx     = 104297500
integrity = "sha256-<base64-full-256-bit-merkle-root>"
size      = 129048107

[[files]]
name      = "v1.1-002990-003000.LmNoPqRsTuV.headers.seg"
from_block = 2990000
to_block   = 3000000
integrity = "sha256-<base64-full-256-bit-merkle-root>"
size      = 41032
```

Notice what's missing: no `infohash`, no `webseed`, no URLs at all.
This file commits ONLY to the content's identity. It can be
distributed via any transport; consumers verify integrity against
the embedded hashes.

### Transport map (`chain.<chainID>.transports.toml`)

Advisory. Keyed by content hash (NOT filename). Lists transports
the operator knows about:

```toml
[[transport]]
hash    = "sha256-<base64>"        # matches a `[[files]].integrity` in the definition
type    = "bittorrent-v2"
infohash = "<v2-infohash-hex>"

[[transport]]
hash    = "sha256-<base64>"
type    = "https"
url     = "https://snapshots.erigon.example/hoodi/v2.0-commitment.103125000-104297500.AbCdEfGhIjK.kv"

[[transport]]
hash    = "sha256-<base64>"
type    = "https"
url     = "https://mirror.example/erigon/hoodi/<same-file>"

[[transport]]
hash    = "sha256-<base64>"
type    = "ipfs"
cid     = "bafy..."
```

A consumer:
1. Reads the chain definition. Gets the canonical set of file
   hashes + integrity tags.
2. Reads the transports file. Gets a map of hash → ways-to-fetch.
3. Picks any transport, fetches, verifies the resulting bytes
   against the integrity tag from the definition.
4. If the bytes match, ingest. If not, try another transport.

The trust artifact (`def.toml`) is what binds the chain to its
content. The advisory artifact (`transports.toml`) is just a
delivery hint table.

## Migration path

### Phase 0 — coexistence

Both old (`chain.toml`) and new (`chain.<id>.def.toml` +
`chain.<id>.transports.toml`) coexist. Consumers read the new
artifacts when present, fall back to old when not. Publishers
produce both. No behavior change.

### Phase 1 — new artifacts authoritative

New consumers prefer the new artifacts. UCAN delegation chains
sign the new `def.toml`; the old `chain.toml` is generated
(possibly auto-signed) FROM the new artifacts for back-compat
during the window.

### Phase 2 — old retired

`chain.toml` removed. Only the split artifacts remain. Consumers
that haven't upgraded fail at the chain.toml lookup with a clear
"this network has migrated to <new path>" error.

## Open questions

### 1. Who signs the transports file?

Options:
- **Operator only** — each publisher signs their own
  transports file with their own key. Consumers trust the publisher
  they pulled it from (TOFU model). Simple; matches today's webseed
  trust model.
- **Federation key** — a separate UCAN delegation specifically for
  transport publication. Looser than the chain-definition delegation;
  intentionally low-stakes (worst case is wasted bandwidth).
- **Unsigned** — anyone can publish a transports file; consumers
  treat it as advisory. They still verify integrity against the
  signed def.toml so trust isn't required.

**Recommendation**: unsigned (Option 3). Once content-addressed
identity exists, transport publication is genuinely low-stakes.
Signing introduces ceremony without adding integrity (the
def.toml's integrity tag is the only integrity the consumer needs).

### 2. How does the transports file get discovered?

For consumers bootstrapping from scratch, they need to find SOME
transports file to fetch the def.toml from. Three options:
- Compiled-in defaults per chainID (the bootstrap entry point).
- DNS TXT records (one per chain, points at a transports file URL).
- Decentralised discovery (DHT, peer gossip).

**Recommendation**: start with compiled-in defaults (simplest;
matches today's preverified.toml URL embedding). DNS TXT for
flexibility. Decentralised discovery as iteration-3 work.

### 3. What about caplin (beacon) snapshots?

Caplin snapshots are listed in today's `chain.toml` alongside EL
snapshots. They're a different file type but the same trust /
transport story. The split applies equally; caplin entries move
into `def.toml` and `transports.toml` like any other file.

### 4. What about Erigon-version-specific metadata?

Things like `step_size` (P1's open question 5) are
publisher-specific operational metadata, not chain-definition
properties. They belong in the transports file (or a separate
operational file), not in the chain definition.

### 5. Per-chain or one-pair-fits-all?

This proposal assumes one `def.toml` + one `transports.toml` per
chain. An alternative is one mega-file for all chains the operator
knows about — keep all hoodi/sepolia/mainnet in a single pair.

**Recommendation**: per-chain. Each chain has its own UCAN
authority, its own genesis, its own fork schedule. Per-chain
files mirror the natural authority boundary.

## What this proposal does NOT change

- The content hashes computed in Proposal 2 (they're the same
  values, just split across two files).
- The UCAN delegation chain (still the root of trust for chain
  definitions; transports file may or may not use it).
- File contents (bytes-on-disk are identical).
- The aggregator's reading path (consumers internally just see
  "files I want, by hash" — they don't care which artifact the
  hash came from).

## Recommendation

**Adopt the split with the unsigned advisory transports option**.
This is the minimum-ceremony path that gets the safety benefit
(integrity travels with the content; transports can be untrusted
without compromise) AND the operational benefit (transport changes
don't touch the trust artifact). Phase 0 / 1 / 2 migration
preserves existing tooling during the rollout window.

## Sequencing

Proposal 3 sits downstream of Proposals 1 and 2 because:

- Without canonical filenames (P1), the file identity isn't stable
  enough to split.
- Without content-addressed identity (P2), the transports file has
  no integrity-independent key to use as its hash table coordinate.

The implementation order is: P1 → P2 → P3. Each can be implemented
behind a `Phase 0 / coexistence` window so consumers and publishers
can upgrade independently. None of the proposals require a
flag-day forced migration.
