# Force discv5 bonding for known endpoints — design + implementation

**Status**: Implemented in `p2p/sentry/sentry_grpc_server.go`'s
`forceDiscv5Bonding` goroutine (started from `startP2PServer` after
`srv.Start()` succeeds). Part of the snapshot-flow PR.
**Scope**: Keep known endpoints (connected devp2p peers, configured
static peers, bootnodes) in the discv5 routing table with their latest
ENR. Detect ENR-seq updates cheaply via Ping; refresh full record via
Resolve+AddKnownNode when seq has advanced. No new protocol, no parallel
trust path — bridges a missing wire between three existing inputs
(devp2p peer set, static-peer config, bootnodes config) and the discv5
routing table.

## Problem statement

The 2026-05-07 Phase 2 test surfaced a 4-minute startup hang on a fresh
consumer with a known publisher (`--staticpeers=<publisher-enode>`,
`--bootnodes=<publisher-enode>`). The consumer's devp2p connection
established in seconds; chain-toml acquisition stayed at "0 peers with
chain-toml" for 4 minutes.

Why: chain-toml is one entry on the publisher's ENR record. The
consumer's `ResolvingPeerNodeSource` calls `dv5.Resolve(peer)` for each
devp2p peer on every chain-toml scan tick — but `Resolve` falls through
to a slow recursive `Lookup` when no discv5 session exists with the
peer. For two single-host nodes that haven't found each other through
the discv5 random-walk yet, no session exists, so `Resolve` takes
minutes.

The architectural gap: **devp2p peer-connect doesn't trigger anything in
discv5**. Devp2p and discv5 are decoupled. Owning a devp2p connection
to peer X tells you nothing about X via discv5 unless discv5 happens to
already know about X through its routing-table dance.

## Why the earlier "btsync subprotocol" idea was wrong

An earlier design proposed a new devp2p subprotocol (btsync) that would
exchange peers' signed ENR records over RLPx. That's discv5 over a
different transport — duplicate protocol, duplicate trust verification,
duplicate state. Architecturally unjustified.

The right answer doesn't add a protocol. It connects two existing
protocols at the right point: when devp2p establishes a peer
connection, force discv5 to *also* notice that peer.

## Approach: ticker-driven Resolve+AddKnownNode for known endpoints

The first design (a peer-connect-only hook with `dv5.Ping`) was too
narrow. Multi-consumer testing (Phase 4 / 2026-05-08) surfaced two
issues that hook would not have addressed:

1. **Static peers may never become connected devp2p peers.** A
   snapshot publisher whose peer slots fill with random discovery peers
   refuses inbound static dials with `DiscTooManyPeers`. The consumer's
   `srv.Peers()` then never lists the publisher, so a peer-only hook
   would never target it. Bootnodes are the same case and more
   load-bearing — in the snapshot-distribution model they are often
   root publishers of chain-toml.
2. **`dv5.Ping` alone doesn't reliably populate the routing table.**
   When discv5's table is still completing its initial seeding,
   `Table.handleAddNode` rejects inbound additions
   (`isInitDone()` gate). Ping bonds at the wire layer but the table
   never ingests the ENR.

The implementation therefore runs as an independent goroutine started
after `srv.Start()` succeeds:

- Iterate the union of `srv.Peers()`, `srv.StaticNodes`,
  `srv.BootstrapNodesV5`, and `srv.BootstrapNodes` every 2 seconds.
- For each unique node ID, spawn a worker that:
  1. Calls `dv5.Ping(n)` — single round-trip; the returned `Pong`
     carries the peer's current `ENRSeq`.
  2. If we already have an entry for this ID with the same seq, we're
     done (cheapest path; no metadata update needed).
  3. Otherwise call `dv5.Resolve(n)` to fetch the rich ENR over the
     wire.
  4. Call `dv5.AddKnownNode(resolved)` to inject it via the
     non-inbound path (bypassing the `isInitDone` gate).
  5. Cache `(id, seq)` so subsequent ticks short-circuit at step 2.

Concurrency: a `sync.Mutex` guards the cache; an `inFlight` set
prevents overlapping bond-attempts for the same ID across ticks.

Why `Resolve+AddKnownNode` rather than `Ping` alone:

- `dv5.Ping` bonds at the wire layer but the table-side add (via
  `addInboundNode`) is dropped while `isInitDone()` is false. After
  init the inbound path works, but during early startup it silently
  drops bonded peers — and the cached "we already pinged this peer"
  state then prevents retry. `Resolve+AddKnownNode` injects the rich
  ENR via the non-inbound path that bypasses the gate.
- `AddKnownNode` returns `false` both when the node can't be admitted
  AND when it was already in the bucket (in which case `bumpInBucket`
  updates the record in place). The implementation treats either as
  successful refresh — the table state is consistent with the resolved
  record either way.

Effective behaviour change:
- Before (multi-consumer test): consumer's `chaintoml` scan reports
  `withChainToml=0` indefinitely (4+ min observed; never recovered).
- After (validated 2026-05-08): chain-toml ENR discovered on the
  first scan, ~3 seconds after process start. Subsequent ENR-seq
  bumps from publisher republishing chain-toml propagate within the
  next bond tick (≤ 2 s).

## What this does NOT replace

- **Discovery** of unknown peers — that's still discv5's normal walks
  + the PR-C fast crawler. This patch only helps once you already have
  a devp2p connection to a peer.
- **Trust verification** — unchanged. ENR signature verified against
  node key; UCAN delegation chain via existing snapshotauth verifier.
  This patch just delivers the ENR faster, doesn't change what's done
  with it.
- **The chain-toml content fetch** — unchanged. ENR carries the info-
  hash; `manifest_exchange` still fetches the manifest body via
  existing RLPx flow.

## Backwards compatibility

- Old peers without discv5: the Ping fails silently (logged at Debug),
  no harm done. The existing slow path still works.
- Self-hosted / single-host nodes without external bootnodes: the
  forced ping bypasses the routing-table-walk requirement entirely.
  These work for the first time at the speed users expect.

## Implementation surface

### Modified code

- `p2p/sentry/sentry_grpc_server.go` — `forceDiscv5Bonding(ctx, srv,
  logger)` goroutine launched from `startP2PServer` after
  `srv.Start()` returns. ~70 LOC. Reads `srv.Peers()`,
  `srv.StaticNodes`, `srv.BootstrapNodesV5`, `srv.BootstrapNodes`;
  uses `dv5.Ping` / `dv5.Resolve` / `dv5.AddKnownNode` (already
  exported on `UDPv5`).
- No new public API. `Ping` already returns the peer's `ENRSeq` via
  `*v5wire.Pong`, which is what enables the cheap seq-check.

### Tests

- Integration test in `node/components/integration/snapshot/scenarios/`:
  two in-process nodes, one connects to the other via staticpeer,
  assert the second node's chain-toml entry is visible within 1 s of
  devp2p connect.
- Multi-consumer regression: a fresh consumer joining a publisher
  whose devp2p slots are full reaches `withChainToml >= 1` on its
  first chaintoml scan (validated by manual mainnet test cycle on the
  feat/snapshot-flow-app-integration branch, 2026-05-08).

## Acceptance criteria

- **Multi-consumer regression**: peer B with `--staticpeers=A` reaches
  `withChainToml >= 1` in `acquireChainToml` within ~3 s of process
  start, even when A's devp2p peer slots are full and refusing static
  dials with `DiscTooManyPeers`. Validated 2026-05-08 (C6 / C8 vs the
  C3-C5 baseline that never reached `withChainToml >= 1`).
- **Sequence-bump propagation**: after A's ENR sequence advances
  (e.g. publisher republishing chain-toml every ~2-3 minutes), B
  detects the bump via Ping's returned `ENRSeq` on the next bond
  tick (≤ 2 s) and refreshes its routing-table entry via
  Resolve+AddKnownNode. The dedup criterion is (id, ENR-seq), so
  metadata updates always propagate.
- **Ping failure is non-fatal**: peers without discv5 (or unreachable
  endpoints) log a Debug line; nothing else is affected. Worker
  goroutines exit cleanly without leaking state.
- **Backwards compatible**: routes through existing `Ping`,
  `Resolve`, and `AddKnownNode` exported by `UDPv5`. No protocol
  change.
- `make lint` clean.

## Sequencing

This patch is independent of the planned discv5 fast crawler and the
inventory subscription cleanup. They compose:

- PR-B fixes the **known-peer ENR delivery latency** (peer-connect →
  ENR available in < 1 s).
- PR-C fixes the **unknown-peer discovery latency** (no routing-table
  entry → routing-table entry in seconds via fast-crawl, instead of
  minutes via random walk).
- PR-D restructures the **internal subscription model** (snapshot +
  event bus) — orthogonal to the wire protocols.

After PR-A + PR-B + PR-C: a fresh consumer with no operator config
finds publishers via PR-C, connects, and learns their ENR via PR-B's
forced handshake — total bootstrap latency from start to "downloading"
is bounded by (discv5 fast crawl: 5-10 s) + (devp2p connect: ms) +
(PR-B forced ping: < 1 s) ≈ ~10 s.

## Open questions

- **Scope of the peer-connect hook**: does it fire only on outbound
  staticpeer connects, or on all (inbound + outbound) peers? Recommend
  ALL peers — every devp2p connection should drive the discv5 session
  establishment. The Ping is cheap; cost is negligible.
- **Does it conflict with discv5's own session-establishment timing?**
  No — discv5's normal handshake state machine handles concurrent /
  redundant Pings idempotently. A forced Ping where one is already in
  flight just no-ops or returns the existing session.
- **Should we Ping repeatedly to refresh ENRs over time?** Not in this
  patch. Discv5's normal session-keepalive handles that. This patch
  only addresses the connect-time bootstrap.
