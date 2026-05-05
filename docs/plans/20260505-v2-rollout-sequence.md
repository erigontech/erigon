# V2 mainnet rollout — post-merge sequence

What needs to happen, in what order, to take V2 from "this PR
merged" to "V2 manifest discovery is the default mainnet path".

The operational mechanics (flag combinations, role boundaries,
verification log lines) are in
`docs/plans/20260504-v2-operational-guide.md`. This doc is purely
the temporal sequence — the chain of events, who triggers each,
and what unblocks next.

## Phase 0 — Prerequisites (done or in flight)

  - `feat/snapshot-flow-v1` (PR #20933) merges to main. Lays
    the trust + validation + producer-gate scaffolding this PR
    builds on. Independent of mainnet measurement.
  - Mainnet measurement on this branch (in flight): bootstrap
    publisher + V2 node both fresh, time-to-tip captured for the
    PR description's test plan.
  - This PR merges to main. Default-flag posture preserves
    pre-V2 behaviour; opt-in only.

After Phase 0: V2 code lives in main, but no production node uses
the V2 path. Default-flag combinations preserve preverified-
driven sync. Operators who set the V2 flags get the new behaviour;
nobody else sees a change.

## Phase 1 — Erigon Tech snapshotter becomes bootstrap publisher

**Trigger**: this PR merged + a release containing the V2 code
deployed to Erigon Tech's snapshotter infrastructure.

**Operations action** (Erigon Tech side, out of tree):

  - Restart the snapshotter with these flags added to its
    startup:
    ```
    --snap.p2p-manifest
    --snap.bootstrap-from-preverified
    --snap.lifecycle-driven-by-storage
    ```
  - Verify in the snapshotter's log:
    `[chaintoml] re-published existing chain.toml ENR entry`
  - Verify any V2 node connecting via devp2p sees the chain.toml
    ENR entry on the snapshotter's enode.

**Effect**: Erigon Tech's snapshotter (the node currently
publishing preverified.toml) becomes the **first V2 bootstrap
publisher** on mainnet. Any V2 node that opts in via
`--snap.p2p-manifest` can now discover the manifest and download
from V2 without preverified.toml being the floor.

**Operator-side immediate**: V2 nodes can opt in. Defaults still
unchanged — preverified is what an unmodified node uses.

## Phase 2 — Soak the V2 path against real consumers

**Trigger**: Phase 1 complete, snapshotter advertising stably.

**What happens**:
  - Operators that want V2's time-to-tip benefit add
    `--snap.p2p-manifest` to their nodes.
  - Erigon team monitors:
    - Snapshotter log for `[chaintoml]` activity, peer
      discovery success rates.
    - Reports from operators running V2-on (lifecycle-driver
      counts, time-to-tip, quarantine triggers).
    - Any unexpected `Adding torrents from disk` lines (would
      indicate the §5e gate isn't holding).
  - Bug fixes land via separate small PRs as issues surface.

**Soak duration target**: a release cycle (1–2 months). Long enough
for diverse hardware / network conditions to surface anything the
hoodi + mainnet measurement runs missed.

## Phase 3 — Default-flag flip

**Trigger**: Phase 2 soak shows no regressions.

**What flips, in this order**:

  1. **`--snap.p2p-manifest`**: default false → true. End-users
     see V2 manifest discovery automatically; preverified
     remains the fallback if no V2 peer is reachable within
     the 5-min `manifestReady` timeout.
  2. **`--snap.lifecycle-driven-by-storage`**: default false →
     true (separate release after p2p-manifest stabilises).
     Storage component owns the import lifecycle for everyone;
     stage-driven path becomes opt-out (`=false`) until removed.

**Effect**: A node started with default flags on a fresh
deployment downloads via V2 by default and runs the storage-owned
lifecycle. Preverified.toml is fallback-only.

## Phase 4 — Third-party publishers

**Trigger**: Phase 3 stable. Could happen earlier in parallel for
operators who already run their own snapshotter.

**What happens**:
  - Third parties (testnet operators, in-house mirrors,
    forked-chain operators) deploy their own bootstrap
    publishers using the same V2 flags.
  - Per-deployment trust: consumer operators add the third-party's
    public-key fingerprint via `--snap.publisher-trust-roots` —
    awaits the DID/UCAN PR
    (`docs/plans/20260504-publisher-did-ucan.md`) for the
    canonical trust-root format.
  - Three documentation tiers in
    `20260504-publisher-did-ucan.md` cover closed deployments,
    public did:web publishers, and cloud-KMS-backed publishers.

**Effect**: V2 stops being Erigon-Tech-only. Multiple authoritative
publishers can serve the same chain; consumers pick (or trust
several).

## Phase 5 — Cleanup

**Trigger**: Phase 4 stable. V2-only is the operational norm.

**What lands**:
  - `AddTorrentsFromDisk` removed entirely (today gated; §5e
    full removal in completion plan).
  - Stage-driven path removed (after
    `--snap.lifecycle-driven-by-storage` has been default-on
    long enough).
  - `--snap.p2p-manifest` becomes a no-op flag (V2 is the only
    path); removed in a subsequent release.
  - preverified.toml drops out of the codebase entirely once no
    code path consumes it.

**Effect**: V2 is the single, simple snapshot mechanism in Erigon.
The pre-V2 code paths and their flags retire.

## Critical-path dependencies

  - **#20933 → this PR**: this PR's diff base is `feat/snapshot-flow-v1`.
    #20933 has to merge before this raises against main.
  - **This PR → snapshotter update**: no operator action lands
    until V2 code is in a release.
  - **Snapshotter update → V2-on-mainnet measurement**: the
    measurement we're capturing on this PR is bootstrap-publisher
    fresh + V2-node-fresh paired locally; the *production* V2
    measurement requires Phase 1.
  - **Phase 2 soak → default flag flip**: real-world reports
    have to be clean for at least one release cycle.

## What this doc deliberately does NOT cover

  - Rollback: each phase is opt-in until the default flips, so
    rollback is "stop adding the flag." Phase 3 introduces
    rollback-meaningful behavior; the corresponding rollback
    is "set the flag back to false explicitly."
  - Capacity planning for the snapshotter under load (operations
    concern; depends on observed peer counts).
  - Mainnet vs testnet sequencing: testnets can adopt V2 ahead
    of mainnet at operator discretion since they're independently
    operated; not a critical path.
