# QMTree Prune Policy Design

## Context

QMTree stores a Merkle commitment over all execution state transitions. At mainnet
block 22M the data is projected to be:

| Data | Size | File pattern |
|------|------|-------------|
| Entry snapshots (.kv + .kvi) | ~237 GB | `v1.0-qmtree-entries.<from>-<to>.{kv,kvi}` |
| Root files (.v) | ~36 MB | `v1.0-qmtree-roots.<from>-<to>.v` |
| Hot entries (MDBX) | small | `QMTreeEntries`, `QMTreeMeta` tables |

Entry snapshots dominate storage. Root files are negligible.

## Tree Structure

```
Upper tree (levels 13+)     ← built entirely from twig roots
    │
Twig roots                  ← 32 bytes each, stored in .v files
    │
Intra-twig Merkle (2048 leaves per twig, levels 0-12)
    │
Leaves = hash(preState, stateChange, transition, prevLeaf)
    │
Entries (.kv files) = the 3 hash components per leaf (96 bytes)
```

The minimum addressable unit for proofs is a **twig** (2048 entries).
To generate a proof for any txNum, the 2048 entries in its twig must be
available to reconstruct the intra-twig Merkle tree.

## Prune Policy: Follow Existing `--prune.mode`

QMTree entries are treated the same as other snapshot domain history
(accounts, storage, code, commitment). The existing prune infrastructure
in `db/kv/prune/` applies:

```
--prune.mode=archive    → keep all qmtree entries (full proof history)
--prune.mode=full       → keep entries within DefaultPruneDistance (100,000 steps)
--prune.mode=minimal    → keep entries within DefaultPruneDistance
```

The `History` distance in `prune.Mode` controls how many steps of qmtree
entries to retain, just as it does for account/storage/code history.

**Root files (.v) are never pruned** regardless of mode — they're needed
for upper tree reconstruction and total ~36 MB for the entire chain.

### How it maps to existing modes

The `History` distance is in **blocks** (not steps). `DefaultPruneDistance` is
100,000 blocks, which at ~100 txns/block is ~10M entries (~25 steps).

| `--prune.mode` | Entry retention | Root retention | Proof capability |
|----------------|----------------|----------------|-----------------|
| `archive` | all | all | full historical proofs |
| `full` | last 100K blocks (~25 steps, ~2.5 GB) | all | proofs within window |
| `minimal` | last 100K blocks (~25 steps, ~2.5 GB) | all | proofs within window |

QMTree entry files are step-aligned, so the block-based distance is converted
to the nearest step boundary when deciding which entry files to retain/prune.
A custom `--prune.distance` adjusts the window.

### Step-based pruning in SnapshotManager

On new step collation:
1. Check if the total retained steps exceeds the configured History distance
2. Delete `.kv`/`.kvi` files for expired steps (oldest first)
3. Never delete `.v` root files
4. Startup validation: verify root files cover the full step range even when
   entries are pruned

## Data Distribution

### Torrent structure

Root files and entry files are published as separate torrent groups so that
nodes download only what their prune mode requires:

```
qmtree-roots/          ← always downloaded, ~36 MB total
  v1.0-qmtree-roots.0-2048.v
  v1.0-qmtree-roots.2048-3072.v
  v1.0-qmtree-roots.3072-4096.v
  ...

qmtree-entries/         ← downloaded per prune window, ~237 GB total (archive)
  v1.0-qmtree-entries.0-2048.kv
  v1.0-qmtree-entries.0-2048.kvi
  v1.0-qmtree-entries.2048-3072.kv
  v1.0-qmtree-entries.2048-3072.kvi
  ...
```

Archive nodes seed all files. Pruned nodes seed only the window they retain.

### Bootstrap sequence

1. Download root files (fast — ~36 MB)
2. Reconstruct upper tree from twig roots (`rebuildUpperTreeFromTwigRoots`)
3. Download entry files matching the node's prune distance (or all for archive)
4. Sync hot entries from chain tip via normal block execution

## Implementation Plan (post-generation)

### Phase 1: Generate and publish

1. Complete mainnet qmtree sync to chain tip (~Apr 15-16)
2. Merge root files into large ranges for efficient distribution
3. Publish root files + entry files as torrents via the existing downloader
4. Verify: download on a fresh node, reconstruct tree, validate root matches

### Phase 2: Integrate with prune policy

1. Register qmtree entries as a prunable domain in the snapshot manager
2. Honor `prune.Mode.History` distance for entry file retention
3. Root files excluded from pruning (always retained)
4. Collation/pruning cycle follows the same step boundary logic as other domains

### Phase 3: Test pruned node

1. Start a fresh node with `--prune.mode=full`
2. Download root files + entry files within the configured prune distance
3. Verify: node syncs to tip, generates proofs within window, returns appropriate
   error for out-of-window requests
4. Test: node continues syncing, new steps collated, old steps pruned

### Phase 4: Test minimal bootstrap

1. Start a fresh node, download only root files (~36 MB)
2. Node reconstructs upper tree, begins syncing from tip
3. Entry files download in background as the prune window fills
4. Proofs become available once entry files for the window are present

## Open Questions

- Should out-of-window proof requests trigger on-demand entry fetch from archive peers?
- How does the prune window interact with reorgs? Need to retain enough entries for max reorg depth.
- Should root files be embedded in the erigon binary for known chains (like genesis configs) for instant bootstrap?
