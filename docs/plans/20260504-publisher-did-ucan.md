# Publisher DID + embedded trust root

Follow-up scope item identified during the V2 delivery review. The
V2 mechanism distributes the *what* (chain.toml manifests) — this
adds the *who*: a verifiable identity for Erigon's internal
snapshotter and an embedded trust anchor so consumers default-trust
it without operator configuration.

## Why we need it

Today V2 nodes accept any peer's chain.toml that arrives over devp2p.
That is fine for the initial rollout (one well-known publisher, the
Erigon snapshotter), but it is not a model that scales: any node
can advertise a chain.toml entry; consumers have no way to
distinguish "the snapshotter we trust" from "any random peer".

A publisher DID solves the targeting problem: the publisher signs
its chain.toml advertisement with a long-lived key whose
public-half is bound to a DID. Consumers verify the signature against
the embedded DID before applying the manifest. Untrusted manifests
are dropped early (before they pollute the local registry, before
torrent metadata is fetched, before download decisions are made).

The infrastructure already exists in `plugins/auth/` (DID + UCAN
capability + signer) and `node/components/snapshotauth/` (chain,
delegation, loader). What is missing is:

  1. A canonical Erigon-snapshotter DID and the corresponding key
     held by Erigon Tech operations.
  2. Embed the DID's public material in the Erigon executable so
     every node already trusts it by default.
  3. Wire chain.toml verification through the existing UCAN chain
     resolver before applying discovered entries.

## Scope of the follow-up PR

In:

  - Generate the publisher's keypair (operations side, out-of-tree).
  - Add the DID document (or the public-key hash + algorithm
    identifier) as a build-time embedded constant in the binary.
    Equivalent to how the executable embeds preverified.toml today.
  - Wire `ApplyDiscoveredChainToml` (and its UCAN-aware variant in
    `chaintoml_v2.go`) to verify the discovered manifest's signature
    against the embedded publisher DID before merging into the local
    registry. Reject + log on mismatch.
  - `--snap.publisher-trust-roots` CLI flag for operators who want
    to extend or override the embedded set (e.g. additional in-house
    publishers, alternate testnet operators).
  - Tests: the `chaintoml_v2_ucan_test.go` suite already exercises
    UCAN-chain validation; extend to cover the embedded-default-trust
    path and the operator-override path.

Out:

  - Full UCAN delegation tooling for operators creating sub-publishers
    (parked per `feature-ucan-peer-selection.md`; lands when the
    delegation CLI is built).
  - Multi-publisher consensus / quorum verification.
  - Key rotation procedure for the publisher (operations playbook,
    not code).

## Documentation deliverable: "be your own DID source"

The same PR that lands the publisher DID + embedded trust root must
also document the procedure for **third parties operating their own
publisher**. Use cases:

  - Testnet operators running a chain.toml publisher for a
    pre-production fork.
  - In-house mirrors at organisations running their own snapshotter
    fleet.
  - Forked-chain operators (Polygon, etc.) running V2 publishing for
    their chain's manifest.
  - Researchers / community publishers running parallel snapshot
    distributions.

The doc is layered: simplest setup first, public-distribution setup
second. Operators pick the tier that matches their deployment.

### Tier 0 — Webpeers (distribution-only, no identity)

Before the trust tiers, note the existing role: any V2 node that
holds files can serve them via BitTorrent. These **webpeers** are
distribution-only — they speed up downloads by adding seed
capacity, but they do NOT mint chain.toml manifests or get a DID.
A webpeer's chain.toml is whatever it received from a trusted
publisher; it cannot rewrite that manifest.

You do **not** need a DID, a keypair, or operator setup to run a
webpeer. Just run an Erigon V2 node with `--snap.p2p-manifest`. It
will download what it's told to download, then serve those files to
peers automatically. This is the lowest-effort way to contribute to
snapshot distribution.

The DID-based trust tiers below apply only to operators who want to
**mint authoritative chain.toml entries** — i.e. tell consumers
"these are the files I attest to". Distribution and trust are
orthogonal: a webpeer redistributes a publisher's signed manifest
without itself being a publisher.

### Tier 1 — Closed deployment (you control all your nodes)

Skip DIDs. Generate an Ed25519 keypair, distribute the public-key
fingerprint to your fleet via your existing configuration
management. No web hosting needed.

  1. `erigon snap publisher-key generate` (one command, writes
     `publisher.key` private + `publisher.pub` public).
  2. Compute fingerprint: `erigon snap publisher-key fingerprint
     publisher.pub` → emits a hex SHA-256 line.
  3. Distribute the fingerprint to your fleet:
     `--snap.publisher-trust-roots=/path/to/fingerprints.txt` on each
     consumer node. Same file shape as Erigon's embedded constant —
     one fingerprint per line.
  4. Run your publisher: `--snap.bootstrap-from-preverified
     --snap.publisher-signing-key=publisher.key`.

This is the **happy path for in-house operators**. Trust is rooted in
whatever already deploys configuration to your hosts; the
publisher's identity never leaves your network.

### Tier 2 — Public publisher with did:web

For operators distributing snapshots to nodes outside their direct
control (testnets, mirrors, community publishers). Adds a
DID document hosted on your domain so consumers verify against a
human-readable, rotation-friendly identifier.

  1. Mint the keypair (same as Tier 1).
  2. Author a `did.json` document containing your
     `#bootstrap-publisher` verification method; example template
     ships with the doc. Optionally include `#root` (cold,
     delegation) and `#release` (reserved) following the Erigon
     pattern.
  3. Host at `https://<your-domain>/.well-known/did.json` over HTTPS
     with a valid certificate.
  4. Compute the fingerprint of `#bootstrap-publisher`. Distribute
     it via `--snap.publisher-trust-roots` (same flag as Tier 1) OR
     publish your DID identifier on a registry / website your
     consumers' operators read.
  5. Document the rotation procedure: replace the
     verificationMethod, publish updated did.json, communicate the
     new fingerprint to your consumer-operators.

This is the **recommended path for public publishers**. Web-based
DIDs are the simplest publishable identity (no DID-method-specific
infrastructure beyond an HTTPS endpoint).

### Tier 3 — Cloud publish (managed deployment)

For operators running the publisher on cloud infrastructure
(AWS / GCP / Azure / etc.) where key custody is delegated to a
cloud-managed key service. Builds on Tier 2 (public did:web doc); the
difference is *where* the signing key lives and *how* the publisher
binary signs.

  - **Cloud KMS for the hot key.** The `#bootstrap-publisher`
    private key never leaves the cloud KMS — neither on disk on the
    publisher host nor in any config file. The publisher binary
    calls the KMS sign API with the manifest digest; the KMS
    returns the signature. Compromise of the publisher host does
    not leak the key.
  - **Provider integration.** `--snap.publisher-signing-key` accepts
    a URI (e.g. `kms://aws/us-east-1/key-id`,
    `kms://gcp/projects/<p>/locations/global/keyRings/<r>/cryptoKeys/<k>`)
    in addition to the local-file form. Provider auth comes from
    the host's IAM role (no embedded credentials).
  - **DID document hosted on the same cloud.** Static-site hosting
    on S3 / GCS behind CloudFront / Cloud CDN with the
    `.well-known/did.json` mapping. ACME-issued cert via the
    provider's certificate manager.
  - **Multi-region failover.** Run multiple publisher instances
    using the same KMS-backed key across regions. A consumer that
    can't reach one publisher's torrent endpoint falls back to
    another; chain.toml content is identical (same key signs).
  - **Audit + rotation.** KMS rotation rotates the underlying key
    material; the DID-doc verificationMethod is updated to point at
    the new key reference. Audit trail is the KMS access log.

Cloud publish is the **operationally hardest of the three tiers**
but the right shape for high-availability public publishers (e.g.
the Erigon-Tech snapshotter at full production scale, or a paid-SLA
mirror service). It's optional — Tier 2 with a local key file works
for most public publishers.

### Why three tiers

The DID document is overhead for closed deployments. An in-house
operator already knows their own publisher's public key — they don't
need a self-resolved DID document to discover it. Conversely a
public operator needs the human-readable identifier so consumers can
configure trust without out-of-band fingerprint exchange. Cloud
custody adds operational hardening (KMS, multi-region) on top of the
public-publisher shape; not every public publisher needs it. Tiering
the doc respects that — operators pick the tier that matches their
deployment surface.

This makes DID-based publisher identity a documented, reusable
pattern rather than an Erigon-Tech-only mechanism — aligning with
the V2 architecture's decentralisation goal.

## Sequencing relative to the in-flight delivery PR

The in-flight V2 PR ships without this. Default-trust posture for
that PR: any peer's chain.toml is accepted (matches today's behaviour;
no regression). The DID-trust gate becomes default-on in the next
release cycle once the publisher key is generated, embedded, and
deployed to the snapshotter.

Operator instructions for the interim period are in
`20260504-v2-operational-guide.md` under "Bootstrap publisher". This
follow-up PR amends those instructions when it lands.

## DID design decisions (2026-05-04)

**DID method:** `did:web:erigon.tech`. The DID document lives at
`https://erigon.tech/.well-known/did.json` and is HTTPS-resolvable
(but see runtime-resolution caveat below).

**Verification methods (keys in the DID document):**

  - `#bootstrap-publisher` — Ed25519 signing key in
    `verificationMethod`. Hot key, held by the operational
    snapshotter, used to sign chain.toml manifests on publish.
  - `#root` — Ed25519 in `capabilityDelegation`. Cold key (HSM /
    offline), used to sign UCAN delegations that promote secondary
    publishers (testnet operators, in-house mirrors). Rotated
    rarely.
  - `#release` — Ed25519 in `assertionMethod`. Reserved slot;
    intended for future binary-release artifact signing. Not used
    by V2; including it now avoids a DID-doc revision later.

Three roles separate hot-key rotation (bootstrap-publisher) from
delegation-root rotation (root). If the snapshotter's hot key leaks,
the root key signs a DID-doc update that revokes the compromised
verificationMethod; consumers refresh on the next release cycle (see
runtime-resolution caveat).

**Signing model:** sign the manifest digest, not the full payload.
On `PublishChainToml`, the snapshotter computes the SHA-256 of the
canonicalised chain.toml content + serves the (digest, signature,
key-id) tuple alongside the ENR entry. Consumers re-compute the
digest and verify the signature against the trust root.

**Trust-root distribution — embedded, not runtime-resolved:**

  - At build time, embed the SHA-256 fingerprint of the current
    `#bootstrap-publisher` public key as a binary constant. Same
    shape as preverified.toml's embedding today — a build-time
    constant the consumer trusts implicitly.
  - At runtime, the consumer verifies signatures against the
    embedded fingerprint. The DID document at erigon.tech is the
    human-readable record; consumers do NOT fetch it on the V2
    sync path.
  - **Why not runtime did:web fetch:** introduces a hard HTTPS
    dependency on erigon.tech availability. An erigon.tech outage
    would break V2 sync everywhere. Embedded fingerprint keeps
    consumers air-gapped from the operations side; key rotation is
    a release-cadence concern.
  - **Operator override:** `--snap.publisher-trust-roots=<path>`
    accepts additional fingerprints (testnets, in-house publishers,
    forked-chain operators) without a rebuild.

**Publisher-side wiring:**

  - `--snap.publisher-signing-key=<path>` — points to the Ed25519
    private key for `#bootstrap-publisher`. Only meaningful when
    `--snap.bootstrap-from-preverified` is set (regular nodes do
    not publish manifests they didn't author).
  - On startup the publisher loads the key, derives the fingerprint,
    and verifies it matches the local `#bootstrap-publisher` entry
    in the operator-supplied DID doc (or the embedded constant).
    Mismatch is a fatal startup error — refusing to advertise with
    an unrecognised key is the right default.
  - On publish: sign the canonical digest, attach to the
    advertisement.

**Key generation ceremony (operations, out of tree):**

  - Generate the Ed25519 keypair on an air-gapped host.
  - Publish the DID document at erigon.tech with the public key.
  - Compute the SHA-256 fingerprint and check it into the Erigon
    repo as the embedded trust-root constant.
  - Distribute the private key to the snapshotter host(s) via the
    standard ops channel.
  - Document the rotation procedure (new key → new DID-doc revision
    → new fingerprint constant → next Erigon release).

## Notes

  - The reason this is a separate PR: it touches release/operations
    flow (key generation + embed). Combining with the V2 mechanism PR
    would block the V2 mechanism on a key-generation ceremony that
    has its own review path.
  - The embedded-fingerprint approach mirrors how preverified.toml
    ships today; the trust anchor is identical in shape (build-time
    constant, operator-overridable).
  - Consumer side is largely existing code paths
    (`plugins/auth/verify.go` + `node/components/snapshotauth/chain.go`);
    the producer side (snapshotter signing its chain.toml on publish)
    needs the small addition of a sign-on-publish hook in
    `db/downloader/chaintoml.go`.
