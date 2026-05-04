# V2 snapshot-flow — operational guide

This document describes how to run Erigon nodes against the V2
snapshot-distribution mechanism that landed in this PR. It's
intended for operators (validators, RPC providers, archive
operators, test infra) and the Erigon team running internal
snapshotter infrastructure.

## What V2 changes for operators

Pre-V2: every node downloads the full preverified.toml file set
(grows with chain age, currently ~hundreds of GB on mainnet).
Time-to-tip scales with the size of preverified.

Post-V2: nodes discover what to download via peer-published
chain.toml manifests. Most nodes only fetch what's needed to
operate at tip (latest state slice + recent blocks). Time-to-tip
is bounded by how fast that smaller set arrives, regardless of
overall chain age. Hoodi: 10 min validated. Mainnet: target same.

## Two operator roles

V2 introduces a two-role model:

### Bootstrap publisher

The node that originates the chain.toml advertisement. Uses
preverified.toml to know which files exist; advertises a chain.toml
that includes preverified ∪ its-own-local files; serves files to
peers via BitTorrent.

Run with:
```
--snap.p2p-manifest \
--snap.bootstrap-from-preverified
```

Plus normal node flags. The bootstrap publisher must have either
the full preverified set or a meaningfully complete coherent set
(no internal step gaps that downstream consumers would hit).
Archive mode is the safe default for bootstrap publishers.

### Regular V2 node

Any other node. Uses chain.toml from peers (typically the bootstrap
publisher) as its sole source of truth. preverified is invisible.

Run with:
```
--snap.p2p-manifest
```

Plus normal node flags. NO `--snap.bootstrap-from-preverified` —
that flag promotes the node to bootstrap-publisher role.

### Storage-driven lifecycle (recommended)

Both roles benefit from running with the storage-driven import
lifecycle:

```
--snap.lifecycle-driven-by-storage
```

This activates the V2-only-mode optimisations:
- Inventory-driven file management
- Latest-first download ordering
- Per-file quarantine on persistent failures (avoid log floods)
- Skipping the legacy `AddTorrentsFromDisk` disk-walk

## Erigon's internal snapshotter as bootstrap

After this PR lands, Erigon's internal snapshotter infrastructure
(the node that publishes preverified.toml today) becomes the
bootstrap publisher for the V2 mechanism. Configuration:

  - Add `--snap.p2p-manifest --snap.bootstrap-from-preverified`
    to the snapshotter's startup flags.
  - Verify it's advertising chain.toml via ENR (look for
    `[chaintoml] re-published existing chain.toml ENR entry` in
    its log).
  - Verify peers can discover it: any V2 node connecting via
    devp2p should see the chain.toml ENR entry on the snapshotter's
    enode.

Until this runs, V2 nodes have no peer to discover from on
mainnet — they'll wait the manifestReady timeout (5 min) then fall
back to preverified (today's existing behaviour), which is safe
but defeats the time-to-tip benefit.

## Verification — is V2 actually active?

For a node you're running, look for these in the log:

  - `[storage-lifecycle] driver started` (Info) — lifecycle driver
    is running. Indicates `--snap.lifecycle-driven-by-storage`
    took effect.
  - `[1/6 OtterSync] Waiting for P2P manifest discovery (timeout 5m0s)...`
    (Info) — manifestReady gate active. Indicates
    `--snap.p2p-manifest` took effect.
  - `[chaintoml] discovered peer chain.toml ...` (Info) — V2
    discovery succeeded. Followed by `[chaintoml] applied
    discovered entries new=N` showing how many new entries the
    peer's manifest added vs the local registry.
  - `[1/6 OtterSync] P2P manifest ready, proceeding with download`
    (Info) — gate cleared. Snapshot stage proceeds against the
    discovered manifest (and ONLY that, when bootstrap flag is
    off).
  - `[storage-lifecycle] BuildMissedIndices start/done` (Info) —
    lifecycle driver is running storage-driven index builds.

If you instead see `Adding torrents from disk` (no `[storage-lifecycle]`
prefix), the legacy path is active. Confirm
`--snap.lifecycle-driven-by-storage` is set.

## Failure modes + debugging

  - **manifestReady times out** (5 min, falls back to preverified).
    Cause: no peer with chain.toml ENR found. Confirm bootstrap
    publisher is reachable + advertising. With static peers, use
    `--staticpeers=<bootstrap-enode>` to ensure connection.

  - **Test node can't connect to publisher.** Check P2P port
    reachability, ENR validity, firewall. The snapshot-flow harness
    in `node/components/integration/snapshot/scenarios/` has
    deterministic tests if you need to verify the wiring without
    real peers.

  - **Lifecycle driver quarantining many files.** The 2026-05-03
    test surfaced this when the publisher's manifest had gaps.
    Look for `[storage-lifecycle] quarantining file after repeated
    failures`. The publisher's chain.toml is incomplete; either
    find a more complete publisher or run the bootstrap from a
    fully-archived datadir.

  - **chain.toml collision crash.** Predates V2 and was fixed in
    this PR via the §5e gate (`AddTorrentsFromDisk` skipped when
    `--snap.lifecycle-driven-by-storage` is set). If you hit this
    crash, confirm the lifecycle flag is set.

## Operational watch points

The lifecycle introduces new log lines and metrics. Things worth
monitoring:

  - **`advanced to=Indexed` / `advanced to=Advertisable`** — files
    transitioning through the lifecycle. Healthy nodes show steady
    rate after retire fires.

  - **`quarantining file`** — should be rare. Persistent quarantines
    indicate a real problem (broken manifest, missing dependency,
    corrupted file).

  - **`discovered new on-disk files`** — the disk-scan picking up
    newly-retired files. Should fire after each retire batch.

  - **`P2P manifest discovery timed out`** — fallback to preverified.
    Indicates V2 mechanism didn't find a publisher; investigate
    peer connectivity.

## Default-flag posture

For this PR's release:

  - `--snap.lifecycle-driven-by-storage`: opt-in. Default false.
    Becomes default-on after a release cycle of soak-testing.
  - `--snap.p2p-manifest`: opt-in. Default false until Erigon's
    internal snapshotter is configured as bootstrap publisher.
    Then becomes default-on so end-users get the time-to-tip
    benefit automatically.
  - `--snap.bootstrap-from-preverified`: opt-in. Operators
    explicitly take on bootstrap-publisher role; default false.

A node started with the defaults today operates exactly as
pre-PR — preverified-driven, stage-managed. Nothing changes for
operators who don't opt in.

## References

  - Spec: `docs/plans/20260501-storage-lifecycle-spec.md`
  - Time-to-tip target + measurements: `docs/plans/20260502-min-time-to-tip-target.md`
  - Completion plan: `docs/plans/20260502-app-integration-completion.md`
