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

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
	dlcomp "github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/ethconfig"
)

// writeHeadersSegment builds a minimal but valid headers .seg + .idx in
// dir for block range [from, to). word is the single compressed entry —
// callers pass distinct words so a live and a staged segment differ on
// disk and the cutover swap is observable.
func writeHeadersSegment(t *testing.T, dir string, from, to uint64, word []byte, logger log.Logger) {
	t.Helper()
	enum := snaptype2.Headers.Enum()
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test",
		filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, from, to, enum)),
		dir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord(word))
	require.NoError(t, c.Compress())

	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, enum.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	require.NoError(t, idx.AddKey(word, 0))
	require.NoError(t, idx.Build(t.Context()))
}

// TestCutoverStagedBatch_SwapsLiveFile drives the running-node cutover
// against a real state Aggregator and a real block-snapshots view: a
// staged headers segment is renamed over the live one under the commit
// barrier, both views are re-opened, and the inventory hash is
// re-stamped. It pins that the live file ends up holding the canonical
// (staged) bytes and the staging directory is removed.
func TestCutoverStagedBatch_SwapsLiveFile(t *testing.T) {
	logger := log.New()
	ctx := t.Context()

	dirs := datadir.New(t.TempDir())
	for _, d := range []string{dirs.Snap, dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors, dirs.SnapCaplin, dirs.Tmp} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}

	const from, to = uint64(0), uint64(1000)
	enum := snaptype2.Headers.Enum()
	segName := snaptype.SegmentFileName(version.V1_0, from, to, enum)
	idxName := snaptype.IdxFileName(version.V1_0, from, to, enum.String())

	// Live (minority) headers segment.
	writeHeadersSegment(t, dirs.Snap, from, to, []byte{0xAA}, logger)
	livePath := filepath.Join(dirs.Snap, segName)
	liveBefore, err := os.ReadFile(livePath)
	require.NoError(t, err)

	// Staged (canonical) replacement with deliberately different bytes.
	stagingDir := filepath.Join(dirs.Tmp, "adoption-test")
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))
	writeHeadersSegment(t, stagingDir, from, to, []byte{0xBB, 0xBB, 0xBB}, logger)
	stagedSeg := filepath.Join(stagingDir, segName)
	stagedIdx := filepath.Join(stagingDir, idxName)
	stagedSegBytes, err := os.ReadFile(stagedSeg)
	require.NoError(t, err)
	require.NotEqual(t, liveBefore, stagedSegBytes, "fixture: staged content must differ from live")

	snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, logger)
	require.NoError(t, snaps.OpenFolder())
	defer snaps.Close()

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	agg := dbstate.NewTest(dirs).MustOpen(ctx, db)
	defer agg.Close()

	inv := snapshot.NewInventory()
	// Seed the entry at LifecycleAdvertisable to mirror the production
	// starting point: the minority node held this file as validated and
	// advertisable. The cutover must reset that so downstream consumers
	// re-evaluate.
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{
		Name:         segName,
		Local:        true,
		Advertisable: true,
		Trust:        snapshot.TrustVerified,
	}))

	p := &Provider{Aggregator: agg, AllSnapshots: snaps, Inventory: inv, logger: logger}

	canonHash := [20]byte{0x99}
	batch := &dlcomp.StagedBatch{
		Dir: stagingDir,
		Files: []dlcomp.StagedFile{
			{Name: segName, InfoHash: canonHash, Path: stagedSeg},
			{Name: idxName, InfoHash: [20]byte{0x88}, Path: stagedIdx},
		},
	}

	require.NoError(t, p.cutoverStagedBatch(batch, nil))

	liveAfter, err := os.ReadFile(livePath)
	require.NoError(t, err)
	require.Equal(t, stagedSegBytes, liveAfter, "live file must hold the canonical content after cutover")

	_, statErr := os.Stat(stagingDir)
	require.True(t, os.IsNotExist(statErr), "staging dir must be removed after cutover")

	e, ok := inv.GetByName(segName)
	require.True(t, ok)
	require.Equal(t, canonHash, e.TorrentHash, "inventory must be re-stamped with the canonical hash")
	require.Equal(t, snapshot.LifecycleDownloaded, e.State,
		"cutover must reset LifecycleState to Downloaded so the driver re-emits Indexed → Advertisable")
	require.False(t, e.Advertisable,
		"Advertisable flag must clear so downstream consumers see the file as needing re-evaluation")
}
