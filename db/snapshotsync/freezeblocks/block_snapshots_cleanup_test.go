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

package freezeblocks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

// These tests pin the post-cancel orphan-cleanup contract that
// SetHead Mode-B relies on. The wedge: BlockRetire.CancelInFlight
// successfully drains the retire goroutine, but any .seg files
// retire had written before its .torrent build step was interrupted
// remain on disk as orphans (no .torrent companion, never announced
// through NotifyOnFilesChange, never landed in Inventory). The next
// step in Provider.Unwind is findInventoryOrphansPastBlock — which
// correctly refuses the unwind. Without an explicit cleanup, every
// Mode-B that lands while retire is mid-flight wedges at the
// preflight check (live-caught 2026-06-17 soak v12 iter 1 mode_b:
// orphans v1.1-003034-003035-{bodies,headers,transactions}.seg past
// target=3,030,235).

// writeFilePair creates a .seg file and (optionally) its .torrent
// companion under dir. Used to fabricate the post-cancel snapshot
// directory shape: some pairs complete (have .torrent), some orphan
// (.seg only).
func writeFilePair(t *testing.T, dir, segName string, withTorrent bool) string {
	t.Helper()
	segPath := filepath.Join(dir, segName)
	require.NoError(t, os.WriteFile(segPath, []byte("seg data"), 0o600))
	if withTorrent {
		require.NoError(t, os.WriteFile(segPath+".torrent", []byte("torrent meta"), 0o600))
	}
	return segPath
}

func TestBlockRetire_CleanOrphanSegsPastTarget_DeletesOrphans(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()

	// Past target (3,030,235) — orphan .seg without .torrent (retire
	// cancelled mid-torrent-build). MUST be cleaned.
	writeFilePair(t, snapDir, "v1.1-003034-003035-headers.seg", false)
	writeFilePair(t, snapDir, "v1.1-003034-003035-bodies.seg", false)
	writeFilePair(t, snapDir, "v1.1-003034-003035-transactions.seg", false)

	// Past target — complete .seg + .torrent (retire finished, was
	// announced via NotifyOnFilesChange). MUST be left alone (Inventory
	// has these; the dedicated past-target trim in Provider.Unwind
	// handles them via the standard trim path).
	writeFilePair(t, snapDir, "v1.1-003031-003032-headers.seg", true)
	writeFilePair(t, snapDir, "v1.1-003031-003032-bodies.seg", true)

	// Below target — orphan .seg without .torrent. MUST be left alone
	// (out of scope for the past-target cleanup; cleaned up via the
	// general FS reconciliation pass, not the per-SetHead path).
	writeFilePair(t, snapDir, "v1.1-003020-003021-headers.seg", false)

	// Below target — complete pair. MUST be left alone.
	writeFilePair(t, snapDir, "v1.1-003010-003011-headers.seg", true)

	br := &BlockRetire{dirs: datadir.Dirs{Snap: snapDir}}
	removed, err := br.CleanOrphanSegsPastTarget(3_030_235)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{
			"v1.1-003034-003035-bodies.seg",
			"v1.1-003034-003035-headers.seg",
			"v1.1-003034-003035-transactions.seg",
		},
		removed,
		"only orphan .seg files past target should be removed — complete pairs and pre-target orphans must be left alone")

	// Verify FS state.
	for _, name := range []string{
		"v1.1-003034-003035-bodies.seg",
		"v1.1-003034-003035-headers.seg",
		"v1.1-003034-003035-transactions.seg",
	} {
		_, err := os.Stat(filepath.Join(snapDir, name))
		require.True(t, os.IsNotExist(err), "%s must be deleted", name)
	}
	for _, name := range []string{
		"v1.1-003031-003032-headers.seg",
		"v1.1-003031-003032-bodies.seg",
		"v1.1-003020-003021-headers.seg",
		"v1.1-003010-003011-headers.seg",
	} {
		require.FileExists(t, filepath.Join(snapDir, name), "%s must remain", name)
	}
}

func TestBlockRetire_CleanOrphanSegsPastTarget_AlsoDeletesCompanionIdx(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()

	// Orphan .seg + matching .idx (retire built the index but not the
	// .torrent before cancel). Both should be deleted.
	writeFilePair(t, snapDir, "v1.1-003034-003035-headers.seg", false)
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "v1.1-003034-003035-headers.idx"), []byte("idx data"), 0o600))

	br := &BlockRetire{dirs: datadir.Dirs{Snap: snapDir}}
	removed, err := br.CleanOrphanSegsPastTarget(3_030_235)
	require.NoError(t, err)
	require.Contains(t, removed, "v1.1-003034-003035-headers.seg")

	_, err = os.Stat(filepath.Join(snapDir, "v1.1-003034-003035-headers.seg"))
	require.True(t, os.IsNotExist(err), ".seg must be deleted")
	_, err = os.Stat(filepath.Join(snapDir, "v1.1-003034-003035-headers.idx"))
	require.True(t, os.IsNotExist(err), "companion .idx must also be deleted")
}

func TestBlockRetire_CleanOrphanSegsPastTarget_NoOpOnCleanDir(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()

	// All pairs complete (retire finished cleanly before cancel had any
	// orphans to leave). Cleanup must report nothing removed.
	writeFilePair(t, snapDir, "v1.1-003020-003021-headers.seg", true)
	writeFilePair(t, snapDir, "v1.1-003031-003032-headers.seg", true)

	br := &BlockRetire{dirs: datadir.Dirs{Snap: snapDir}}
	removed, err := br.CleanOrphanSegsPastTarget(3_030_235)
	require.NoError(t, err)
	require.Empty(t, removed, "clean dir: nothing to remove")
}

func TestBlockRetire_CleanOrphanSegsPastTarget_NoSnapDir(t *testing.T) {
	t.Parallel()
	// CLI tool / harness path: BlockRetire constructed without dirs.Snap
	// set. Must be a clean no-op.
	br := &BlockRetire{}
	removed, err := br.CleanOrphanSegsPastTarget(3_030_235)
	require.NoError(t, err)
	require.Empty(t, removed)
}

func TestBlockRetire_CleanOrphanSegsPastTarget_NonBlockFilesIgnored(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()

	// State-aggregator files (v2.0-*) live in subdirs in production but
	// even at the top level we must not touch them. Random files must
	// also be left alone.
	writeFilePair(t, snapDir, "v2.0-stray-domain.kv", false)
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "chain.toml"), []byte("chain"), 0o600))

	// One orphan v1.1 file to confirm the scan filter only matched it.
	writeFilePair(t, snapDir, "v1.1-003034-003035-headers.seg", false)

	br := &BlockRetire{dirs: datadir.Dirs{Snap: snapDir}}
	removed, err := br.CleanOrphanSegsPastTarget(3_030_235)
	require.NoError(t, err)
	require.Equal(t, []string{"v1.1-003034-003035-headers.seg"}, removed,
		"only v1.1-*.seg block snapshots are in scope — v2.0-* state files and chain.toml must be ignored")
}
