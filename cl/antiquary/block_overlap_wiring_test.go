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

package antiquary

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Startup is the only removal a default node gets: the dump sites run under snapgen, which is off
// by default, and downloaded files overlap too.
func TestLoopRemovesCoveredBlockSegmentsAtStartup(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeStateFixture(t, dirs.Snap, "beaconblocks", 0, 20_000, logger)
	subSeg := writeStateFixture(t, dirs.Snap, "beaconblocks", 0, 10_000, logger)
	subIdx := strings.TrimSuffix(subSeg, ".seg") + ".idx"

	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
	t.Cleanup(sn.Close)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	// A cursor past the visible tip keeps the startup reindex from reading the fixture as blocks.
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 20_000)
	}))

	ctx, cancel := context.WithCancel(t.Context())
	d := &recordingDownloader{}
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, dirs, d, db, nil, sn, nil, nil, logger, false, true, false, false, nil)
	a.NotifyBackfilled()

	done := make(chan error, 1)
	go func() { done <- a.Loop() }()
	require.Eventually(t, func() bool {
		_, err := os.Stat(subSeg)
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 10*time.Millisecond, "the covered segment must be unlinked at startup")
	cancel()
	if err := <-done; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	require.ElementsMatch(t, []string{filepath.Base(subSeg), filepath.Base(subIdx)}, d.deletedNames())
	require.NoFileExists(t, subIdx)
}

func TestAntiquateMergesBeaconBlocksIntoTheNextTier(t *testing.T) {
	const tier = snaptype.Erigon2MergeLimit
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	var blobSegs []string
	for from := uint64(0); from < tier; from += snaptype.CaplinMergeLimit {
		f, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BlobSidecars.FileName(version.ZeroVersion, from, from+snaptype.CaplinMergeLimit))
		require.True(t, ok)
		c, err := seg.NewCompressor(t.Context(), "test", f.Path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, logger)
		require.NoError(t, err)
		require.NoError(t, c.AddWord(nil))
		require.NoError(t, c.Compress())
		c.Close()
		require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), f, 0, dirs.Tmp, &background.Progress{}, log.LvlDebug, logger))
		blobSegs = append(blobSegs, f.Path)
	}

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var prevRoot common.Hash
		for slot := uint64(0); slot < tier; slot += 500 {
			var root common.Hash
			binary.BigEndian.PutUint64(root[:], slot+1)
			if err := beacon_indicies.MarkRootCanonical(tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(tx, root, prevRoot); err != nil {
				return err
			}
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root), root[:]); err != nil {
				return err
			}
			prevRoot = root
		}
		return beacon_indicies.WriteHighestFinalized(tx, tier+safetyMargin)
	}))

	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	d := &recordingDownloader{}
	a := NewAntiquary(t.Context(), nil, nil, nil, &clparams.MainnetBeaconConfig, dirs, d, db, nil, sn, nil, nil, logger, false, true, false, true, nil)
	require.NoError(t, a.antiquate())

	var subFiles []string
	for from := uint64(0); from < tier; from += snaptype.CaplinMergeLimit {
		segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, from+snaptype.CaplinMergeLimit)
		subFiles = append(subFiles, segName, strings.TrimSuffix(segName, ".seg")+".idx")
	}
	require.ElementsMatch(t, subFiles, d.deletedNames())
	for _, f := range subFiles {
		require.NoFileExists(t, filepath.Join(dirs.Snap, f))
	}
	for _, f := range blobSegs {
		require.FileExists(t, f)
	}
	require.Contains(t, d.seededNames(), snaptype.BeaconBlocks.FileInfo(dirs.Snap, 0, tier).Name())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BlobSidecars(), len(blobSegs))
	blocks := view.BeaconBlocks()
	require.Len(t, blocks, 1)
	require.Equal(t, uint64(0), blocks[0].From())
	require.Equal(t, uint64(tier), blocks[0].To())
	word, err := blocks[0].Get(54_500)
	require.NoError(t, err)
	require.Equal(t, uint64(54_501), binary.BigEndian.Uint64(word))
}

func antiquaryWithBlockSegments(t *testing.T, to uint64) (*Antiquary, *recordingDownloader) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	for from := uint64(0); from < to; from += snaptype.CaplinMergeLimit {
		writeStateFixture(t, dirs.Snap, "beaconblocks", from, from+snaptype.CaplinMergeLimit, logger)
	}

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, to+safetyMargin)
	}))

	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	d := &recordingDownloader{}
	return NewAntiquary(t.Context(), nil, nil, nil, &clparams.MainnetBeaconConfig, dirs, d, db, nil, sn, nil, nil, logger, false, true, false, true, nil), d
}

func blockSegmentFiles(from, to uint64) []string {
	var files []string
	for ; from < to; from += snaptype.CaplinMergeLimit {
		segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, from+snaptype.CaplinMergeLimit)
		files = append(files, segName, strings.TrimSuffix(segName, ".seg")+".idx")
	}
	return files
}

func TestAntiquateMergesBeaconBlocksWithNothingToDump(t *testing.T) {
	const tier = snaptype.Erigon2MergeLimit
	a, d := antiquaryWithBlockSegments(t, tier)

	require.NoError(t, a.antiquate())

	require.FileExists(t, snaptype.BeaconBlocks.FileInfo(a.dirs.Snap, 0, tier).Path)
	subFiles := blockSegmentFiles(0, tier)
	require.ElementsMatch(t, subFiles, d.deletedNames())
	for _, f := range subFiles {
		require.NoFileExists(t, filepath.Join(a.dirs.Snap, f))
	}
}

func TestAntiquateMergesOneBeaconBlocksTierPerCall(t *testing.T) {
	const tier = snaptype.Erigon2MergeLimit
	a, d := antiquaryWithBlockSegments(t, 2*tier)
	lowerSubFiles, upperSubFiles := blockSegmentFiles(0, tier), blockSegmentFiles(tier, 2*tier)

	require.NoError(t, a.antiquate())

	require.FileExists(t, snaptype.BeaconBlocks.FileInfo(a.dirs.Snap, 0, tier).Path)
	require.NoFileExists(t, snaptype.BeaconBlocks.FileInfo(a.dirs.Snap, tier, 2*tier).Path)
	require.ElementsMatch(t, lowerSubFiles, d.deletedNames())
	for _, f := range upperSubFiles {
		require.FileExists(t, filepath.Join(a.dirs.Snap, f))
	}

	require.NoError(t, a.antiquate())

	require.FileExists(t, snaptype.BeaconBlocks.FileInfo(a.dirs.Snap, tier, 2*tier).Path)
	require.ElementsMatch(t, append(lowerSubFiles, upperSubFiles...), d.deletedNames())
	for _, f := range upperSubFiles {
		require.NoFileExists(t, filepath.Join(a.dirs.Snap, f))
	}
}
