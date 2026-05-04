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

## Sequencing relative to the in-flight delivery PR

The in-flight V2 PR ships without this. Default-trust posture for
that PR: any peer's chain.toml is accepted (matches today's behaviour;
no regression). The DID-trust gate becomes default-on in the next
release cycle once the publisher key is generated, embedded, and
deployed to the snapshotter.

Operator instructions for the interim period are in
`20260504-v2-operational-guide.md` under "Bootstrap publisher". This
follow-up PR amends those instructions when it lands.

## Notes

  - The reason this is a separate PR: it touches release/operations
    flow (key generation + embed). Combining with the V2 mechanism PR
    would block the V2 mechanism on a key-generation ceremony that
    has its own review path.
  - The embedded-DID approach mirrors how preverified.toml ships
    today; the trust anchor is identical in shape (build-time
    constant, operator-overridable).
  - Consumer side is largely existing code paths; the producer side
    (snapshotter signing its chain.toml on publish) needs the small
    addition of a sign-on-publish hook in `chaintoml.go`.
