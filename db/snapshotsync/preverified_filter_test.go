// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain"
)

// TestFilterPreverifiedByPruneMode locks the bug-N policy: the
// bootstrap-from-preverified path filters preverified entries against
// the running prune.Mode so a --prune.mode=minimal publisher doesn't
// pull the full archive (state history) it would just prune anyway.
//
// Each case asserts the EXACT set of survivors so a change to the
// filter that drops a different set of files trips this test.
func TestFilterPreverifiedByPruneMode(t *testing.T) {
	t.Parallel()

	// Synthetic preverified covering every file shape the filter could
	// decide on:
	//   - state primary (.kv in domain/) — always kept
	//   - state history (.v in history/, .ef in idx/, .vi in accessor/)
	//     — dropped iff History pruning is enabled (every mode except
	//     Archive)
	//   - block snapshot files (top-level *.seg)
	//   - pre-merge transactions — dropped iff Blocks == DefaultBlocksPruneMode
	//     (Full mode only, by current prune.Mode constants)
	//   - chain config (salt-*.txt, erigondb.toml) — always kept
	//   - caplin — always kept
	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v1.0-accounts.0-1024.kv", Hash: "0001"},
		preverified.Item{Name: "domain/v1.0-storage.0-1024.kv", Hash: "0002"},
		preverified.Item{Name: "domain/v1.0-commitment.0-1024.kv", Hash: "0003"},
		preverified.Item{Name: "history/v1.0-accountsHistory.0-1024.v", Hash: "0004"},
		preverified.Item{Name: "history/v1.0-storageHistory.0-1024.v", Hash: "0005"},
		preverified.Item{Name: "idx/v1.0-accountsIdx.0-1024.ef", Hash: "0006"},
		preverified.Item{Name: "idx/v1.0-logAddrIdx.0-1024.ef", Hash: "0007"},
		preverified.Item{Name: "accessor/v1.0-history.0-1024.vi", Hash: "0008"},
		preverified.Item{Name: "v1.0-000000-000500-headers.seg", Hash: "0009"},
		preverified.Item{Name: "v1.0-000000-000500-bodies.seg", Hash: "000a"},
		preverified.Item{Name: "v1.0-000000-000500-transactions.seg", Hash: "000b"}, // pre-merge
		preverified.Item{Name: "v1.0-020000-020500-transactions.seg", Hash: "000c"}, // post-merge
		preverified.Item{Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Hash: "000d"},
		preverified.Item{Name: "salt-state.txt", Hash: "000e"},
		preverified.Item{Name: "erigondb.toml", Hash: "000f"},
	}

	// Chain config: merge at block 15M (mainnet-ish). Pre-merge =
	// transactions whose .From < 15_000_000. With ×1000 multiplier
	// embedded in the snaptype filename parser, the v1.0-000000-000500
	// segment covers block range [0, 500_000) — pre-merge. The
	// v1.0-020000-020500 segment covers [20_000_000, 20_500_000) —
	// post-merge.
	mergeHeight := uint64(15_000_000)
	cc := &chain.Config{MergeHeight: &mergeHeight}

	t.Run("archive keeps everything", func(t *testing.T) {
		got := FilterPreverifiedByPruneMode(items, cc, prune.ArchiveMode)
		require.Len(t, got, len(items),
			"archive mode must keep every preverified entry — pruning is disabled on every axis")
		// Compare by name set for clarity.
		gotNames := namesOf(got)
		for _, want := range items {
			require.Contains(t, gotNames, want.Name)
		}
	})

	t.Run("minimal drops state history AND caplin archive; keeps state primary + blocks + config", func(t *testing.T) {
		got := FilterPreverifiedByPruneMode(items, cc, prune.MinimalMode)
		gotNames := namesOf(got)

		// Kept under minimal:
		mustKeep := []string{
			"domain/v1.0-accounts.0-1024.kv",
			"domain/v1.0-storage.0-1024.kv",
			"domain/v1.0-commitment.0-1024.kv",
			"v1.0-000000-000500-headers.seg",
			"v1.0-000000-000500-bodies.seg",
			"v1.0-000000-000500-transactions.seg", // Minimal keeps tx — only Full filters pre-merge tx
			"v1.0-020000-020500-transactions.seg",
			"salt-state.txt",
			"erigondb.toml",
		}
		for _, name := range mustKeep {
			require.Contains(t, gotNames, name,
				"minimal must keep %s — it's state primary / blocks / config", name)
		}

		// Dropped under minimal (bug-N regression sentinel — these are
		// the ~1.3 TB of mainnet state history + ~150 GB of caplin
		// archive that filled the disk):
		mustDrop := []string{
			"history/v1.0-accountsHistory.0-1024.v",
			"history/v1.0-storageHistory.0-1024.v",
			"idx/v1.0-accountsIdx.0-1024.ef",
			"idx/v1.0-logAddrIdx.0-1024.ef",
			"accessor/v1.0-history.0-1024.vi",
			"caplin/v1.1-000000-000010-beaconblocks.seg",
		}
		for _, name := range mustDrop {
			require.NotContains(t, gotNames, name,
				"minimal must drop %s — state history (idx/, history/, accessor/) and caplin archive are not part of the minimal-mode publish set", name)
		}
	})

	t.Run("full mode drops state history AND pre-merge transactions", func(t *testing.T) {
		got := FilterPreverifiedByPruneMode(items, cc, prune.FullMode)
		gotNames := namesOf(got)

		// Full has History.Enabled() == true (DefaultPruneDistance) so
		// state-history drops. AND Full has Blocks == DefaultBlocksPruneMode,
		// triggering pre-merge transaction filtering via the existing
		// isTransactionsSegmentExpired predicate.
		require.NotContains(t, gotNames, "history/v1.0-accountsHistory.0-1024.v")
		require.NotContains(t, gotNames, "idx/v1.0-accountsIdx.0-1024.ef")
		require.NotContains(t, gotNames, "accessor/v1.0-history.0-1024.vi")
		require.NotContains(t, gotNames, "v1.0-000000-000500-transactions.seg",
			"full mode drops pre-merge transactions (this is the Blocks == DefaultBlocksPruneMode + IsPreMerge branch)")
		// Post-merge transactions kept.
		require.Contains(t, gotNames, "v1.0-020000-020500-transactions.seg")
	})

	t.Run("blocks mode drops state history + caplin; keeps all transactions", func(t *testing.T) {
		got := FilterPreverifiedByPruneMode(items, cc, prune.BlocksMode)
		gotNames := namesOf(got)
		require.NotContains(t, gotNames, "history/v1.0-accountsHistory.0-1024.v")
		require.NotContains(t, gotNames, "caplin/v1.1-000000-000010-beaconblocks.seg",
			"every non-archive mode drops caplin/ — coupled to prune.History.Enabled()")
		// Blocks mode has Blocks=KeepAllBlocksPruneMode, which is NOT
		// DefaultBlocksPruneMode, so pre-merge tx filter is a no-op.
		require.Contains(t, gotNames, "v1.0-000000-000500-transactions.seg")
		require.Contains(t, gotNames, "v1.0-020000-020500-transactions.seg")
	})

	t.Run("uninitialised mode is a defensive no-op pass-through", func(t *testing.T) {
		var zero prune.Mode // Initialised == false
		got := FilterPreverifiedByPruneMode(items, cc, zero)
		require.Len(t, got, len(items),
			"uninitialised mode must not silently behave as archive (or anything else) — callers must pass an initialised mode")
	})
}

func namesOf(items snapcfg.PreverifiedItems) []string {
	names := make([]string, len(items))
	for i, p := range items {
		names[i] = p.Name
	}
	return names
}

// TestFilterIsSubsetOfArchive pins the monotonicity invariant the
// bootstrap-side filter shares with SyncSnapshots' inline filter:
// no mode ever produces a larger kept set than archive (the no-op
// case). A regression that synthesizes entries — for instance a
// future "merge two adjacent items into one" optimisation — would
// trip this immediately. Caught here instead of in the live publisher.
//
// Also catches accidental rule inversions: if someone flips a
// `continue` check the wrong way, a prune mode could end up keeping
// MORE than archive, which is nonsensical.
func TestFilterIsSubsetOfArchive(t *testing.T) {
	t.Parallel()

	const sampleHash = "0123456789abcdef0123456789abcdef01234567"
	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v1.0-accounts.0-1024.kv", Hash: sampleHash},
		preverified.Item{Name: "history/v1.0-accountsHistory.0-1024.v", Hash: sampleHash},
		preverified.Item{Name: "idx/v1.0-accountsIdx.0-1024.ef", Hash: sampleHash},
		preverified.Item{Name: "accessor/v1.0-history.0-1024.vi", Hash: sampleHash},
		preverified.Item{Name: "v1.0-000000-000500-headers.seg", Hash: sampleHash},
		preverified.Item{Name: "v1.0-000000-000500-bodies.seg", Hash: sampleHash},
		preverified.Item{Name: "v1.0-000000-000500-transactions.seg", Hash: sampleHash},
		preverified.Item{Name: "v1.0-020000-020500-transactions.seg", Hash: sampleHash},
		preverified.Item{Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Hash: sampleHash},
		preverified.Item{Name: "salt-state.txt", Hash: sampleHash},
		preverified.Item{Name: "erigondb.toml", Hash: sampleHash},
	}
	mergeHeight := uint64(15_000_000)
	cc := &chain.Config{MergeHeight: &mergeHeight}

	archive := FilterPreverifiedByPruneMode(items, cc, prune.ArchiveMode)
	archiveSet := make(map[string]struct{}, len(archive))
	for _, p := range archive {
		archiveSet[p.Name] = struct{}{}
	}

	for _, mode := range []struct {
		name string
		m    prune.Mode
	}{
		{"full", prune.FullMode},
		{"blocks", prune.BlocksMode},
		{"minimal", prune.MinimalMode},
	} {
		t.Run(mode.name+" is subset of archive", func(t *testing.T) {
			got := FilterPreverifiedByPruneMode(items, cc, mode.m)
			require.LessOrEqual(t, len(got), len(archive),
				"%s mode must never keep more entries than archive", mode.name)
			for _, p := range got {
				_, inArchive := archiveSet[p.Name]
				require.Truef(t, inArchive,
					"%s mode kept %q but archive dropped it — this means a non-archive mode is producing entries archive doesn't, which is nonsensical (modes only restrict, never relax)",
					mode.name, p.Name)
			}
		})
	}
}
