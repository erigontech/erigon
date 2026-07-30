# Peer Distribution Soak — Design

**Purpose:** stand up a continuous live multi-erigon soak that exercises
the chain.toml + manifest + UCAN peer distribution stack under a 2 → 5
→ 10 peer swarm-size ladder, mirroring the shape of the unwind soak
(`scripts/unwind-fresh-sync-then-soak.sh` / `soak-until-stopped.sh`) and
the fork soak (`scripts/fork-soak-until-stopped.sh`).

Third pillar of the "three concurrent continuous soaks" goal:

1. Base change distribution — the 2/5/10 peer sync cycle **(this doc)**
2. Unwinds — `scripts/unwind-soak.sh` (already running)
3. Forks — `scripts/fork-soak-until-stopped.sh` (paused)

The three must eventually run concurrently on the same machine without
breaking each other.

## What the soak is testing

Cross-peer agreement + propagation of:

- Chain.toml (v2 today, v3 envelope form when Phase 3 lands)
- Per-peer manifest (chain.<enr>.toml) advertisement
- UCAN signature validation
- Snapshot infohash agreement + BitTorrent transfer
- Delayed-merge propagation window (`ERIGON_MERGE_MIN_AGE_STEPS`) —
  publisher holds per-step files long enough for peers to catch up.

Explicitly NOT testing here (has its own soak): unwind correctness,
fork transitions.

## Cycle shape

One driver parameterised by `N_PEERS` (default 10). Each cycle runs a
**fixed-size** swarm for the cycle's duration — swarm size doesn't
grow within a cycle. Different sizes are captured by running the
driver with different `N_PEERS` values.

```
Cycle = start publisher + (N_PEERS - 1) followers → hold with churn → wipe
```

Sizes to exercise:

- `N_PEERS=2` — smallest exchange. Chain.toml agreement is trivially
  between 2 nodes; no fan-out. Catches bugs in the direct publisher
  → follower path.
- `N_PEERS=5` — fan-out begins. Discovery via non-publisher peers
  becomes possible; concurrent manifest-fetches start hitting the
  dedup path.
- `N_PEERS=10` — primary soak target. Full swarm dynamics; multiple
  discovery paths; more concurrent downloads.

**Primary continuous soak: `N_PEERS=10`**. Smaller sizes run
occasionally as smoke tests (or when a bug is suspected of being
size-specific). Note (user, 2026-07-30): running 10 alone may miss
inconsistencies visible only at 2 or 5 — track any size-specific
signal in the rare-issues tracker.

Within a cycle:

- **Phase 1 (bring-up)**: start publisher, then stagger followers with
  60s intervals for the first two and 30s for the rest. Wait for
  chain.toml exchange + first snapshot download to complete.
- **Phase 2 (steady-state)**: hold for STEADY_MIN minutes (default
  10). Publisher continues its retire/republish cycle; followers
  observe and keep chain.toml in sync.
- **Phase 3 (churn)**: iterate — pick a random follower, disconnect
  at DevP2P, wait DWELL seconds, reconnect. Repeat for CHURN_ITERS
  (default 10). Mirrors the in-process `TestP2P_SoakChurn`
  ([p2p_soak_test.go](../../node/components/integration/snapshot/scenarios/p2p_soak_test.go))
  but at live-erigon scale.
- **Phase 4 (final assert)**: cross-peer chain.toml agreement across
  every current swarm member; no size-of-swarm regression from
  bring-up.

At end of cycle:

- **Default**: destroy all followers, keep publisher's datadir; new
  cycle starts fresh followers against the same publisher (tests
  publisher's ability to serve wave after wave).
- **KEEP_PUBLISHER=false**: destroy publisher too; new cycle rebuilds
  entire swarm from scratch.

## Assertions per phase

For each new follower joining:

1. Chain.toml discovered via peer ENR within 60s.
2. Chain.toml content matches publisher byte-for-byte (per version).
3. Manifest fan-out: follower's own chain.<enr>.toml is discoverable
   by the publisher.
4. Snapshot download completes within phase-specific timeout.
5. Post-download: follower's on-disk file infohashes match
   publisher's.
6. Cross-peer chain.toml agreement across all current swarm members
   (any pair produces identical bytes for the current version).

Assertions match the shape of the in-process
`TestP2P_Swarm_StaggeredEntry` at [p2p_staggered_entry_test.go](../../node/components/integration/snapshot/scenarios/p2p_staggered_entry_test.go)
but graduated to live erigon on hoodi.

## Chain choice

**hoodi** — matches the unwind soak, so both can share a fresh-sync
datadir template. Cheap to sync from preverified. Enough real
chain.toml surface (the publisher hosts real files, peers actually
download real snapshots) to catch propagation bugs.

Alternative: bal-devnet-2. Rejected — narrower chain, less snapshot
volume, doesn't stress the manifest fan-out.

## Port allocation

10 erigons on one machine × ~12 listening ports each = 120 ports.
Systematic offset table:

| Node | port-offset | http | authrpc | private | torrent | p2p | caplin-udp | caplin-tcp | sentinel | beacon | mcp |
|---|---|---|---|---|---|---|---|---|---|---|---|
| publisher | +0    | 19545 | 19551 | 11590 | 43369 | 31503 | 4750 | 4751 | 8490 | 6260 | 9260 |
| follower1 | +100  | 19645 | 19651 | 11690 | 43469 | 31603 | 4850 | 4851 | 8590 | 6360 | 9360 |
| follower2 | +200  | 19745 | 19751 | 11790 | 43569 | 31703 | 4950 | 4951 | 8690 | 6460 | 9460 |
| ... | ... | ... |
| follower9 | +900  | 20445 | 20451 | 12490 | 44269 | 32403 | 5650 | 5651 | 9390 | 7160 | 10160 |

Same convention as the unwind + fork soaks already use. Publisher =
+0 so its ports match existing muscle memory + launcher script.

**Coexistence with other soaks:** the unwind soak already occupies the
+0 (publisher) slot. Peer distribution soak must live at a base offset
+1000+ so all three soaks fit. Deferred to the cross-soak concurrent
step (item 9 in current todos).

## Publisher provisioning

Same as fork-parent:

- Trust-root key generated once, persisted to
  `$DATADIR/peer-dist-trust-root.hex`.
- `--snapshot.trust-roots=<pub-hex>` on launch.
- `--snap.p2p-manifest` on.
- Chain.toml is emitted by the publisher's own retire/republish cycle
  — no bootstrap chain.toml file needed.

## Follower provisioning

- Fresh datadir per follower per cycle.
- No trust-root key. Just `--snap.p2p-manifest` on, and (once
  discovered) `--snapshot.trust-roots=<publisher-hex>` provisioned
  from a shared file the driver writes.
- `--staticpeers=<publisher-enode>` for the first two followers so
  the initial ENR-based discovery is guaranteed. Followers 3+ can
  discover via the existing swarm without static peers, testing
  organic fan-out.

## Directory + log layout

```
/erigon/tmp/erigon-hoodi-peerdist.publisher/
/erigon/tmp/erigon-hoodi-peerdist.follower{1..9}/
/tmp/peerdist-soak/
    runner.log
    index.tsv
    cycle0001-<ts>/
        publisher.log
        follower1.log
        ...
        cycle.csv          # per-phase pass/fail + timing
        cycle.summary.log  # driver's own narrative
```

Mirrors `/tmp/continuous-soak/` layout from the unwind soak.

## Driver script structure

Two new scripts, matching the unwind soak's separation:

- `scripts/peerdist-fresh-cycle.sh` — runs ONE cycle end-to-end
  (Phase A → B → C → D). Wipes follower datadirs at start; publisher
  optional-wipe per KEEP_PUBLISHER.
- `scripts/peerdist-soak-until-stopped.sh` — outer loop calling
  peerdist-fresh-cycle.sh, one per cycle, appending to
  `/tmp/peerdist-soak/index.tsv`, KEEP_GOING flag semantics matching
  the unwind driver.

Plus:

- `scripts/erigon-launch-hoodi-peerdist-publisher.sh` — launcher for
  the publisher role.
- `scripts/erigon-launch-hoodi-peerdist-follower.sh` — launcher for
  a follower, takes NODE_IDX env var to compute port offset.

## Assertions probes

The driver needs machine-readable probes for each assertion. Two
options:

1. **RPC-driven**: publisher exposes chain.toml bytes via a new
   `admin_getChainToml` RPC or similar; driver polls each node's
   endpoint and diffs.
2. **File-driven**: driver reads each node's `$DATADIR/snapshots/chain.toml`
   directly and diffs. Skips the RPC surface area.

Option 2 is faster to build (no new RPC needed) and equally valid for
correctness testing. Ship 2 first; add 1 later if we want the
production simulation to include the RPC surface.

Cross-peer chain.toml agreement:

```bash
for f in /erigon/tmp/erigon-hoodi-peerdist.follower*/snapshots/chain.toml; do
    sha256sum "$f"
done | awk '{print $1}' | sort -u
# Must produce exactly one hash after Phase A steady-state.
```

Similarly for manifest fan-out — every node has every other node's
chain.<enr>.toml under snapshots/, driver enumerates and diffs.

## Phase timings (initial values, adjust after first cycle)

- Phase A (size 2): publisher up, follower1 up, wait 5 min for
  chain.toml exchange + first snapshot download. Assert.
- Phase B (size 5): stagger followers 2/3/4 at 60s intervals, wait
  3 min after last one is up. Assert.
- Phase C (size 10): stagger followers 5–9 at 30s intervals, wait
  3 min. Assert.
- Phase D (churn): 10 iterations of disconnect/reconnect one random
  follower with 20s dwell between events. Assert steady-state at end.

Total per-cycle target: ~30 min. Faster than a full unwind soak cycle
(~2-4h).

## Success criteria (soak-level, not cycle-level)

Match the unwind soak's known-rare-issues bar
([20260729-continuous-soak-known-rare-issues.md](20260729-continuous-soak-known-rare-issues.md)):

- Every rare failure surfaces as a signature-tagged entry in a
  `20260730-peer-dist-soak-known-rare-issues.md` tracker.
- Soak is "green" only after every known issue is closed or
  explicitly downgraded.
- ≥3 consecutive cycles without recurrence to close an entry.

## Concurrent-run considerations

When all three soaks run together on one machine:

- Port ranges disjoint (see coexistence note above).
- CPU + RAM headroom: 10 peerdist erigons + 1 unwind erigon + 2
  fork erigons + concurrent Caplin instances ≈ 13 processes.
  Need to profile before assuming this fits on the current box.
- Disk: 10 peerdist datadirs × ~50 GB fresh hoodi = 500 GB.
  `/erigon` has ~7 TB but recent runs have been near-full. May need
  a leaner peerdist mode (share preverified downloads via symlinks,
  or use minimal-sync).
- Network: 10 erigons on hoodi = 10× the P2P bandwidth of one.
  Hoodi's discovery + gossip probably tolerates this; production
  simulation validates it.

## Rollout

1. Sketch approved → build launcher scripts + one-cycle driver.
2. Run one cycle manually end-to-end. Iterate on assertions +
   phase timings.
3. Wire the outer soak-until-stopped loop.
4. Run 3 consecutive cycles clean before opening the rare-issues
   tracker.
5. Concurrent-run test: peerdist + unwind (fork paused). Then all
   three.

## Open questions

- **Publisher restarts**: do we churn the publisher too, or keep it
  stable across the whole soak? Publisher restart is the higher-risk
  event (chain.toml republish on boot, salt reload, torrent state
  regen). Default: publisher is stable within a cycle; churn happens
  cycle-to-cycle when KEEP_PUBLISHER=false.
- **Merge boundary alignment**: should Phase D deliberately trigger a
  merge (via `ERIGON_MERGE_MIN_AGE_STEPS=0`) so churn happens across
  the merge event? Or keep merges rare (default)? Default: rare.
- **Delayed-merge visibility**: assert `ERIGON_MERGE_MIN_AGE_STEPS`
  propagation window actually kept the per-step file long enough for
  the slowest follower to complete download. Requires measuring the
  gap between publisher's file mtime and follower's download-complete.
  Deferred.

## References

- [docs/plans/20260504-v2-operational-guide.md](20260504-v2-operational-guide.md)
  — publisher / follower / discovery model.
- [docs/plans/20260515-three-layer-snapshot-distribution.md](20260515-three-layer-snapshot-distribution.md)
  — three-layer swarm agreement / canonical / advertisement model.
- [node/components/integration/snapshot/scenarios/p2p_staggered_entry_test.go](../../node/components/integration/snapshot/scenarios/p2p_staggered_entry_test.go)
  — in-process precursor test whose assertions this soak graduates.
- [node/components/integration/snapshot/scenarios/p2p_soak_test.go](../../node/components/integration/snapshot/scenarios/p2p_soak_test.go)
  — in-process churn soak (10 iterations, seconds); Phase D graduates
  its shape to live erigon.
- [scripts/fork-soak-until-stopped.sh](../../scripts/fork-soak-until-stopped.sh)
  — driver-script structure to mirror.
- [scripts/unwind-fresh-sync-then-soak.sh](../../scripts/unwind-fresh-sync-then-soak.sh)
  — per-cycle-driver structure to mirror.
