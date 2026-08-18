# Peer manifest supersession + quorum reconciliation

**Date:** 2026-08-18
**Trigger:** verify12 iter-5 stall — root cause traced to a stale peer-manifest cache the consumer keeps as authoritative long after the peer has rotated to a newer view.
**Related fixes shipped this session:** [[checkpoint-2026-08-17-retire-race-fix-verified]] (5 commits landed; G3+G4 and G5+G6 close the retire↔downloader-mmap coord races; verify11 iters 1–4 green; verify12 iter-5 exposed this orthogonal manifest-lifecycle gap).

---

## The observed failure (concrete)

Consumer verify12 (`/erigon/tmp/erigon-hoodi-soak.v4split-verify12.stuck-manifest-mismatch`, frozen) stalled on fresh sync for >60 min. Downloader status:

```
[Downloader] Syncing snapshot-flow  eta=∞ elapsed=1h0m0s
  metadata=1/1 files=0/1 data=48.26%,768.0KB/1.6MB
  hashing-rate=16.1MB/s peer-download=16.0MB/s peers=1 conns=1
```

Cycling the file `v2.0-003435-003436-transactions.idx` (and 13 similar single-step transactions.idx files) with the expected infohash `2532016e8813d599410c38de80fcc74412aa3909`. Two upstream sources for that file:

- **Master publisher's current chain.toml** — 0 entries for this file (or any of the 13). Master has retired these single-step files into wider merged (`v2.0-003430-003440-...`) siblings.
- **Master publisher's chain.v2 historical files on disk** — 10+ variants, none of them declare the hash `2532016e...`. Master rotated these versions and the version consumer cached is not among them anymore.
- **CDN webseed** (`https://erigon36-v1-snapshots-hoodi.erigon.network/`) — HAS the file, but at a *different* hash `0a752307edb5eee8b033305cd3a173713d5637f0`. CDN's .torrent sidecar advertises the same `0a752307...`. Consumer's expected infohash never matches actual bytes → infinite retry.

**Source of the stuck expectation** (found in the consumer's frozen datadir):

```
/erigon/tmp/erigon-hoodi-soak.v4split-verify12.stuck-manifest-mismatch/snapshots/
  .peers/dbe13762445c19c421ac9b12b3e29f5edad45e32bd2eb52a2acb999c3b543143/
    chain.v2.dbe13762445c19c4.81892dc3c9cb460d.toml       (77821 B, mtime 20:45)
```

That file contains:

```toml
[[blocks]]
name = 'v2.0-003435-003436-transactions.idx'
range = [3435000, 3436000]
hash = '2532016e8813d599410c38de80fcc74412aa3909'
```

and 12 more single-step transactions.idx entries. Master publisher never retained this specific chain.v2 version (`81892dc3c9cb460d`) — it was published and rotated out within some earlier window; consumer happened to fetch it before rotation.

`peer-manifests/chain.<trust-root>.toml` at the parent level contains the same entries — this is the aggregated per-peer cache used for restart recovery. Both caches are stale.

## Design gap

The consumer's peer-manifest lifecycle has two persistence layers:

1. **Anacrolix `.peers/<peerID>/chain.v2.<content-hash>.toml`** (`node/components/downloader/bus.go:250`) — the downloader writes one file per chain-toml torrent it fetches, named after the manifest's own content-hash. Files accumulate; old versions are never pruned when the peer rotates.
2. **`peer-manifests/chain.<trust-root>.toml`** (`node/components/manifest_exchange/provider.go:687` `writePeerManifestCache`) — atomic write-then-rename, single file per peer, overwritten on each fetch. This IS superseded correctly on subsequent fetches.

But the orchestrator's runtime state (`Orchestrator.pending`, `Orchestrator.peerFiles`) is populated by `PeerManifestReceived` events, and **there is no diff-vs-previous logic** — each PeerManifestReceived is treated as additive. A file that peer P used to advertise but now doesn't stays in `pending` (and stays in `peerFiles[name].peers` for peer P).

If the file's peers-set becomes empty via `onPeerDeparted` (peer left entirely), the existing code at [orchestrator.go:1141-1147](node/components/storage/flow/orchestrator.go#L1141-L1147) evicts the peerFiles entry — but ONLY if `not in pending`. In-flight downloads are kept indefinitely; there is no cancel path.

Result: a file the peer no longer advertises AND is in the downloader's active-torrent queue keeps retrying forever with "No metadata yet".

**Additionally**, the user has raised: "the logic to manage the manifest and the quorum may be missing or faulty" — the current model handles one peer's supersession poorly, and multi-peer quorum reconciliation (when peers P1 and P2 have different views of "current authoritative manifest") is not explicitly modelled.

## Test coverage gap

Multi-client download tests to date exercise chain-to-head sync (consumer catches up from a static publisher). Continuous operation with peer manifest rotation during a live consumer's run has not been tested. Concrete gaps:

- Peer publishes M1, consumer sees M1, peer publishes M2 (M1's file X now retired into M2's wider file Y). What does consumer do about X?
- Peer P and Q both advertise M1. P rotates to M2 first; Q rotates minutes later. What does consumer see during the mismatch window?
- Peer P advertises M1, consumer downloads to pending, P disappears. Consumer knows via PeerDeparted. What happens to the pending download?
- Peer P flaps: online → offline (with M1 pending) → back online (advertising M2). Do we resume M1's download, cancel it, or start fresh with M2?

## Expected behavior (from 2026-08-18 design review)

Design review clarifications from the user, capturing the intended model before implementation:

### The "canonical chain.toml" is a consumer-owned concept, distinct from any peer's manifest

Each peer publishes ITS OWN chain.toml (rotating as it retires+merges). The consumer computes a CANONICAL chain.toml locally by reconciling trusted peers' manifests via quorum. That canonical view is what the consumer treats as authoritative for its own state; a single peer's manifest change does not immediately shift the canonical view.

### Answers so far

1. **Files we've already downloaded, dropped by one publisher (still in another's view)**:
   Keep the local copy until the CANONICAL chain.toml makes its change. Decision at that point depends on whether we've locally merged in the meantime — if we already produced a wider replacement, we can drop the older narrower file. As an optimization, once enough agreeing peers say the wider file is authoritative, the consumer can choose to download it instead of merging locally (avoiding duplicate work).

2. **Publishers advertise DIFFERENT hashes for the same file name (content divergence)**:
   Reject both. No quorum on content = no authority = skip the file. Content divergence between trusted publishers signals a non-determinism bug worth surfacing, not silently resolving.

### Merge transitions are structural, not divergence

When peer P's manifest changes because it retired `A,B,C` into `ABC`, that's a MERGE TRANSITION — the wider file semantically SUBSUMES the narrower ones. This is a routine, expected shape change, not a "disagreement" between peers. Treat differently from GENUINE range mismatch (where peers advertise the same range with different hashes).

Concretely:

- Peer P1 advertises `{003430-003431, 003431-003432, ...}` (single-step).
- Peer P1 rotates to `{003430-003440}` (merged wider).
- This IS a merge transition. Range-wise `003430-003440` contains `003431-003432`.
- Consumer should recognize this: not "P1 removed 003431-003432", but "P1 replaced narrower with wider, semantically same coverage".

Detection: overlap comparison between removed-in-new and added-in-new. If a removed file's [from, to) fits inside an added file's [from, to) with matching kind — it's a merge transition. If not — it's a genuine range change.

### Canonical fallback: previous version is authoritative when no quorum

When quorum does not form around a NEW version:
- Consumer's current canonical chain.toml (the last-known-good) REMAINS authoritative.
- Downloader queue continues driven by that canonical.
- Consumer keeps trying to serve pending downloads from any peer that still has bytes matching the canonical view — even peers whose CURRENT advertisement has moved on but who may still hold the files on disk.

This raises the **publisher retention question**: to serve consumers whose canonical is one revision behind, publishers may need to RETAIN old files on disk past their current advertisement window. Trade-off: disk cost vs. consumer-catch-up capability. Not settled by consumer-side design alone.

### Quorum model: file-identity, unanimous among trusted peers

Same filename must be advertised by ALL trusted peers to be in the canonical. Coverage-based aggregation across granularities (e.g. narrower files summing to a wider file's range) does NOT form quorum — the FILENAMES must match. Consequences:

- P1 advertising `003430-003440` and P2 advertising the 10 single-step files that cover the same range: **NO quorum** on any file. Consumer stays on previous canonical.
- Once both peers agree on a new filename set (both rotated to `003430-003440`), quorum forms and canonical advances.
- Gap files (only one peer advertises them) implicitly get IGNORED — no quorum → never enter canonical → never fetched.

### Publisher retention: retain until quorum changes, bounded

Publisher keeps rotated-out files past its current advertisement so lagging consumers whose canonical is one revision behind can still fetch. Retention is BOUNDED — a natural bound is the merge-min-age window (existing `ERIGON_MERGE_MIN_AGE_STEPS`), which coincides with the delay before rotated files become eligible for physical deletion.

Practical implication: consumers should typically converge on new canonical within the retention window. Consumers that fall further behind risk missing bytes.

### Now resolvable given the above

- **Gap files** — resolved by file-identity quorum: gap files don't enter canonical, so consumer never fetches them. No separate gap-file policy needed.
- **Local production vs canonical**: local production takes precedence for the file we're producing (G3+G4's `IsProducing` filter stands). Canonical tells us the file should be authoritative once local production completes. If canonical would want a DIFFERENT hash than we produce, the mismatch surfaces at file-publish time (torrent hash comparison).

### Canonical re-evaluation: debounced

Wait N seconds after the last received peer manifest before re-evaluating canonical. Batches rotation-window churn cleanly (P1 rotates → wait → P2 rotates → wait → both settled → re-evaluate once). Debounce period should exceed typical publisher retire+publish cycles but be short enough that stall detection doesn't trip (seconds, not minutes).

### Publisher-side: quarantine premerge files (not delete)

Currently: publisher deletes premerge files on merge completion. Change: RETAIN premerge files, but move to a QUARANTINE location so local processing (aggregator, reader, retire) doesn't see them in the primary snapshots dir.

Quarantine dir requirements:
- Downloader can still serve files from it (anacrolix's per-torrent Storage can point at the quarantine location).
- Local Domain/History/InvertedIndex disk scans in the primary snapshots dir do NOT enumerate quarantine files.
- Quarantine files eventually deleted under disk pressure (per below).

Retention duration: naturally bounded by the time quorum takes to transition. Publisher keeps files quarantined as long as any consumer's canonical still references them; once quorum advances, quarantine can shrink. Under disk pressure, quarantine is evicted first (before primary snapshots). Absent disk pressure, quarantine remains available.

### Webseed CDN is a lagging peer, not an authority

The webseed CDN (`erigon36-v1-snapshots-hoodi.erigon.network` etc.) is a static snapshot from some past chain point. As the chain progresses and publishers rotate, the CDN falls behind — it holds files that were authoritative when the snapshot was taken but no longer are. In the quorum model the CDN is just another peer whose current advertisement is definitely stale relative to the live swarm.

Consequences:
- CDN failures (404 on rotated files, hash mismatches on merged files) are EXPECTED, not exceptional — the chain has passed the CDN's snapshot point.
- Downgrade CDN mismatch/404 messages from WARN to INFO (or DEBUG); they carry no operator action.
- CDN's contribution to canonical must be gated the same as any peer's: file-identity quorum. Files only-on-CDN don't enter canonical because the current publishers don't advertise them.
- Practical: don't add the CDN to trusted peers unless it's known-current. In the current setup, CDN's role is "seed rare bytes for a full history walk from genesis"; live-sync consumers should prefer publisher swarm over CDN.

### Consumer never falls behind retention

Retention is TIED to quorum. A consumer whose canonical is one revision behind will find files in publishers' quarantine (because quorum-based retention means publishers keep old files as long as any consumer references them). Once quorum advances (all consumers agree on the new view, via debounced re-eval), publishers' retention pressure eases and quarantine cleanup can happen naturally.

If a consumer catastrophically falls behind (chain is broken from consumer's perspective, canonical multiple revisions old), that's a separate concern — the current bug is about routine per-revision-lag which the retention window naturally handles.

### Still open — needs design review before implementation

- **Transition procedure when quorum forms around new canonical**:
  - Compute delta: files removed from OLD canonical, files added in NEW canonical.
  - For removed: cancel pending downloads, prune local seed set. Keep local files (we may still be using them) OR mark as GC-candidate if the delta was a merge-transition and we've locally merged to the wider replacement.
  - For added: request via existing `requestGapsFor` gate.
  - **Local-merged case**: if we already locally produced the wider file (because retire ran here too), canonical-add of the wider file is a no-op — we already have it, IsProducing→Local transition already fired. Add of narrower files removed by canonical: skip request (they're subsumed by our wider file).

- **Quarantine location + directory layout on publisher side**: where exactly does it live in the datadir? How does the anacrolix torrent Storage get configured to serve from both primary + quarantine?

- **Concrete debounce period**: 5s? 15s? Match to publisher retire cadence.

- **First-boot / no-canonical-yet**: what's the bootstrap flow? Consumer has no canonical, receives its first PeerManifestReceived — does quorum form on a single-peer set (trivial), or wait for ≥2 peers before setting initial canonical?

## Proposed design (SUPERSEDED PENDING REVIEW — flagged as open questions)

### Terminology

- **Peer's advertised view**: the current chain.v2 content-hash the peer's ENR carries (the "I'm authoritative for this specific view").
- **Cached view**: the chain.v2 content-hash the consumer last successfully fetched from this peer.
- **Superseded**: the cached view differs from the advertised view — the cached one is no longer authoritative for this peer.
- **Orphaned file**: a file no advertising peer's current view declares. Distinct from "peer disappeared" — the peer is still there, just with a different manifest.

### State to track

- `Orchestrator.peerManifests map[peerID]peerManifestState` where:
  ```go
  type peerManifestState struct {
      contentHash [32]byte     // the chain.v2 hash (from the manifest bytes)
      files       map[string]struct{} // union of all file names across Domains/Blocks/Caplin/Meta/Salt
      lastSeen    time.Time     // last successful fetch
  }
  ```
- Guarded by `peerMu` (same as `peerFiles` and `pending`).

### Events

Add `flow.DownloadSuperseded{FileName string, Reason string}` published by the orchestrator when a file's last advertising peer is superseded AND the file was in `pending`. Provider subscribes; calls `downloaderClient.Delete([]string{fileName})` to drop the torrent from anacrolix's active set. Provider also removes the `.peers/<peerID>/chain.v2.<old>.toml` file that declared it (if still on disk).

### Orchestrator: on PeerManifestReceived from peer P

1. Compute `newFiles`: union of all file names in the payload.
2. If `peerManifests[P]` exists, compute `removed = peerManifests[P].files \ newFiles` — files this peer used to advertise but no longer does.
3. For each `name` in `removed`:
   - Remove P from `peerFiles[name].peers`
   - If `peerFiles[name].peers` is now empty:
     - If `name` in `pending` → publish `DownloadSuperseded{FileName: name, Reason: "all peers superseded"}` and drop from `pending`, decrement `statePending` if it was counted.
     - Else → drop from `peerFiles`.
4. Process `newFiles` via existing `requestGapsFor` — additions still go through the same gate (haveLocally, IsProducing, coverage, etc.).
5. Update `peerManifests[P] = {contentHash, newFiles, now}`.

### Orchestrator: on PeerDeparted from peer P

Extend the existing handler: after removing peer from all `peerFiles[name].peers`, if a name became orphaned AND is in pending → publish `DownloadSuperseded` (same path as above). This closes the current gap where the code retains the entry indefinitely.

Also: `delete(peerManifests, P)`.

### Provider: on DownloadSuperseded

- `downloaderClient.Delete([]string{name})` — drops the torrent from anacrolix.
- Optional: sweep `.peers/<peerID>/chain.v2.*.toml` files whose contents include this file name and haven't been touched since the peer's `contentHash` last changed. This is best-effort cache hygiene; the important thing is that the torrent is cancelled.

### Quorum considerations

Two-peer scenario: P1 advertises {A, B, X}, P2 advertises {A, B, X}. Consumer downloads all three.

P1 rotates to {A, B, Y} (X was merged into Y). P2 still on old view.

Under this design:
- PeerManifestReceived from P1 with {A, B, Y}: removed={X}, but `peerFiles[X].peers` still contains P2 → not orphaned → download continues.
- Consumer starts downloading Y (via requestGapsFor).
- When P2 rotates too: `peerFiles[X].peers` becomes empty → orphaned → DownloadSuperseded fires → X cancelled.

**Quorum threshold**: this design implicitly requires ALL advertising peers to supersede a file before we cancel its download. That's the safest default: if any trusted peer still says "here's X", we keep trying. Alternative — a majority-based quorum (fire cancel when >50% of advertising peers have superseded) — is more aggressive and out of scope for this fix, but could be layered on later.

**Trust filter**: the existing `o.trust.Trusted(peerID)` gate in `requestGapsFor` filters at add time. `peerManifests[P]` should only track trusted peers to avoid manifests from untrusted peers holding files back from cancellation. Symmetric filter at the tracking step.

### Cache hygiene (secondary)

The `.peers/<peerID>/chain.v2.*.toml` accumulation is a slow disk leak but does not by itself cause the stall (the runtime state does). A separate periodic sweeper can prune files whose content-hash doesn't match the peer's current ENR advertisement — after `DownloadSuperseded` has fired for any file the old cache uniquely declared.

Not part of this fix's critical path.

## Explicit test cases

Written first, TDD, must fail on current code and pass on the fix.

1. **`TestOrchestrator_ManifestSupersedes_DropsRemovedFileWithNoOtherAdvertisers`** — single peer P1 sends manifest {A, X}, then manifest {A} only. Downloader receives 2 DownloadRequested events at first. After second manifest: no DownloadRequested for A again (haveLocally would gate it), no new for X (skipped), AND a DownloadSuperseded event fires for X.

2. **`TestOrchestrator_ManifestSupersedes_QuorumHoldsWhenAnotherPeerStillAdvertises`** — peers P1 and P2 both send {A, X}. P1 sends {A} only. Assert: NO DownloadSuperseded for X (still advertised by P2). After P2 also sends {A} only, DownloadSuperseded fires for X.

3. **`TestOrchestrator_PeerDeparted_OrphansPendingFile_FiresSuperseded`** — peer P sends {X}, orchestrator publishes DownloadRequested for X, P departs (never completing X). Assert: DownloadSuperseded fires for X (current behavior: entry retained indefinitely).

4. **`TestOrchestrator_ManifestFlap_ReAdvertisedFileCancelsSupersession`** — peer P sends {X}, then {} (X orphaned + cancelled), then {X} again. Assert: after third manifest, DownloadRequested fires for X again (re-request, not silently held cancelled).

5. **`TestProvider_OnDownloadSuperseded_CallsDownloaderDelete`** — Provider subscriber test with a mock downloader client, assert Delete called with the correct file name when DownloadSuperseded fires.

## Phased implementation (audit 2026-08-18 confirms nothing from this design exists)

**Current code state**: additive `peerFiles` union across all peers, first-advertiser-wins for content, no canonical concept, no per-peer manifest state, no debounce, no delta-cancel, no publisher quarantine. Trust filter and G3+G4's `IsProducing` are the only building blocks in place. Per [orchestrator.go:322-328](node/components/storage/flow/orchestrator.go#L322-L328) comment, content divergence handling was explicitly deferred.

### Phase 1 — Consumer per-peer manifest state (state-gathering only, no behavior change)

- Add `Orchestrator.peerManifests map[peerID]*peerManifestState`.
- `peerManifestState`: `{files map[string]*FileEntry, receivedAt time.Time}`.
- On `PeerManifestReceived`: record the peer's current view.
- On `PeerDeparted`: drop from `peerManifests`.
- Tests: assert state after single manifest, two manifests from same peer (replacement), peer departure.
- No effect on requestGapsFor yet — this is pure observation.

### Phase 2 — Canonical view computation (still no behavior change)

- Add `Orchestrator.canonical map[string]*FileEntry` — files agreed by all trusted peers with matching hash.
- `computeCanonical()` = intersection of `peerManifests[trusted-peers-only].files`, keyed by (name, hash) — a name with divergent hashes across peers is EXCLUDED (per divergence-rejection rule).
- Debounced re-eval: after N seconds of no new PeerManifestReceived, recompute canonical.
- Add `flow.CanonicalChanged{Added []*FileEntry, Removed []string}` event for downstream consumers to react.
- Tests: single peer → canonical = its manifest. Two peers matching → canonical is that set. Two peers different (subset) → canonical is intersection. Two peers same-name-different-hash → excluded from canonical.

### Phase 3 — requestGapsFor consults canonical (behavior change)

- Modify `requestGapsFor` to only request files that ARE in canonical (or, on first-boot when canonical is empty, the current additive-union behavior as bootstrap fallback).
- Add G3+G4-style rule: skip files marked `IsProducing`.
- Tests: request-when-in-canonical; skip-when-not; bootstrap-fallback-when-canonical-empty.

### Phase 4 — Cancel on canonical transitions (behavior change + new event)

- Add `flow.DownloadSuperseded{FileName, Reason}` event.
- On `CanonicalChanged`: for each removed file that is in `pending`, publish `DownloadSuperseded`.
- Provider subscribes; calls `downloaderClient.Delete([]string{name})`; drops from `pending`; decrements `statePending`.
- Tests: file in pending removed from canonical → DownloadSuperseded fires; not-in-pending removed → no event; add of new file → still uses requestGapsFor.

### Phase 5 — Publisher quarantine of premerge files

- Add `snapshots/quarantine/` subdirectory in publisher datadir.
- Modify `reclaimFiles` (aggregator.go:2921) → move-to-quarantine instead of `os.Remove`.
- Anacrolix per-torrent Storage: when serving a rotated file, look in quarantine.
- Local Domain/History/InvertedIndex scans: unchanged (they don't see quarantine).
- Quarantine eviction: on disk pressure (df% > threshold), oldest-first.
- Tests: retire+merge produces quarantined file; local scan doesn't see it; downloader can serve it.

### Phase 6 — Merge-transition detection (optimization)

- On canonical transition, detect merge-transitions (removed files' [from,to) subset of added file's [from,to) with matching kind).
- Log them differently (INFO not WARN).
- Optionally: if we've already locally merged, skip request for the merged replacement (subsumed by our local file).

### Comprehensive test scenarios (by phase)

Each phase gets a test suite covering the scenarios from the design review, not just verify12's specific stall. Every test asserts a design invariant.

**Phase 1 — per-peer state**:
- P1-1: single peer's manifest recorded
- P1-2: same peer's second manifest REPLACES the first (self-replacing semantics)
- P1-3: PeerDeparted clears the peer's state
- P1-4: two peers each have isolated state
- P1-5: manifest with multiple kinds (Domains + Blocks + Caplin + Meta + Salt) all captured
- P1-6: empty manifest recorded as empty
- P1-7: untrusted peer's manifest NOT captured (trust filter)

**Phase 2 — canonical computation**:
- P2-1: zero trusted peers → canonical empty
- P2-2: one trusted peer with manifest → canonical mirrors it (trivial quorum)
- P2-3: two peers with identical manifests → canonical is that set
- P2-4: two peers, one advertises a superset → canonical is the intersection
- P2-5: two peers advertise same filename with DIFFERENT hashes → filename excluded from canonical (divergence rejection)
- P2-6: peer joins (was 1 peer, now 2) → canonical shrinks to intersection; CanonicalChanged event fires
- P2-7: peer departs (was 2 peers, now 1) → canonical grows to remaining peer's set; CanonicalChanged fires
- P2-8: debounced re-eval: N peer manifests within debounce window → single canonical recompute
- P2-9: three peers, majority-2 disagreement → strict unanimity means file NOT in canonical (documents policy)
- P2-10: merge-transition (peer P retires narrower files into wider) does NOT affect other peers' canonical contributions
- P2-11: peer flaps (drop then re-appear with same manifest) → canonical bounces via prior-canonical fallback

**Phase 3 — requestGapsFor consults canonical**:
- P3-1: file in canonical + not local + not producing → requested
- P3-2: file in canonical + already local → NOT requested (haveLocally gate stands)
- P3-3: file in canonical + producing locally (G3+G4) → NOT requested
- P3-4: file NOT in canonical (only one peer advertises) → NOT requested even if peers advertise
- P3-5: bootstrap (canonical empty, no prior) → fallback to additive behavior for first-boot only
- P3-6: file in canonical, then canonical drops it (via next Phase 4 test) → not re-requested if peer re-advertises

**Phase 4 — cancel on canonical transitions**:
- P4-1: file in pending removed from canonical → DownloadSuperseded fires; Provider calls downloader.Delete; pending decremented
- P4-2: file NOT in pending (never requested) removed from canonical → no cancel event (nothing to cancel)
- P4-3: peer P departs, orphaning a pending file → DownloadSuperseded fires (extends current PeerDeparted behavior that retained entries indefinitely)
- P4-4: file cancelled, then peer re-advertises it → requestGapsFor picks it up cleanly (no phantom-cancel state)
- P4-5: merge-transition (narrower removed, wider added by same rotation) → cancel narrower, request wider — as a single atomic transition

**Phase 5 — publisher quarantine**:
- P5-1: retire+merge on publisher moves narrower .kv into snapshots/quarantine/, not deleted
- P5-2: local Domain/History/InvertedIndex scans do NOT enumerate quarantine files
- P5-3: downloader Storage can serve from quarantine dir
- P5-4: quarantine eviction on disk pressure (oldest-first)
- P5-5: process restart: quarantine files persist; still servable
- P5-6: quarantined file exists on disk with same infohash as pre-merge → torrent seed continues

**Phase 6 — merge-transition detection**:
- P6-1: narrower files' [from, to) fits inside added wider file's [from, to) with matching kind → merge-transition flagged
- P6-2: added file at different Kind (e.g. .kv vs .kvi) → NOT merge-transition
- P6-3: locally-produced wider file already present → merge-transition detection skips redundant download
- P6-4: partial merge (some narrower not covered by any wider) → NOT merge-transition; treated as regular removal

**End-to-end validation** (a separate integration test that runs a small orchestrator + provider + downloader):
- Reproduces verify12 stall: peer publishes M1 {A, B, X}, consumer starts downloading; peer publishes M2 {A, B, Y} (X → Y merge transition); consumer's canonical advances; X is cancelled; Y is requested; no stuck pending.

### Bootstrap and edge cases

- **Single-peer canonical**: first-boot with only one trusted peer. Accept the peer's manifest as canonical trivially (intersection of 1 set is itself). Second peer's arrival may shrink canonical; files that were fetched-during-single-peer stay via prior-canonical fallback.
- **Zero trusted peers**: canonical empty. No downloads. Progress requires trusted peer connect.
- **Only-one-trusted-peer scenario (test-hosts)**: canonical = that peer. All files single-authority. Works as today.

### Rollout scope

- No public API change. New event is internal to the flow package + Provider subscriber.
- New state field on Orchestrator. Existing tests should not regress — the diff-and-remove logic only fires on ≥2 manifests from same peer.
- No changes to `peer-manifests/chain.<peerID>.toml` writeback (already superseded correctly).
- No changes to `.peers/<peerID>/chain.v2.*.toml` accumulation policy (deferred to cache-hygiene follow-up).
- Provider must gain a bus subscriber for DownloadSuperseded; existing subscribers pattern at `provider.go:585+`.

## Follow-ups (out of scope for this fix, but recorded)

- **CDN publish-set**: the webseed CDN should eventually advertise its own current authoritative view (a chain.toml of what IT actually holds) so the consumer can treat it as a first-class lagging peer in the quorum model. Today the CDN is a bare HTTP endpoint at fixed paths; the consumer discovers what it has via 404s. Publishing its own view would let the orchestrator's file-identity filter cleanly exclude files the CDN has retired. TODO.

## Original follow-ups

- Cache hygiene periodic sweeper for `.peers/<peerID>/chain.v2.*.toml` accumulation.
- Majority-quorum policy for cancelling downloads while some peers still advertise.
- ENR-advertised chain-toml-hash vs cached hash reconciliation (currently we track via PeerManifestReceived; direct ENR comparison could preempt).
- Metrics: DownloadSuperseded counters per reason (peer_departed / all_peers_superseded / etc).
