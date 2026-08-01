// Copyright 2024 The Erigon Authors
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
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestDumpBeaconBlocksNoPanic is a higher-level regression test: the production
// panic occurred inside DumpBeaconBlocks when chooseSegmentEnd was called with
// nil snCfg. This test uses toSlot=CaplinMergeLimit so the loop body executes
// (doesn't break early), exercising the exact code path that panicked.
func TestDumpBeaconBlocksNoPanic(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	// toSlot == CaplinMergeLimit: toSlot-fromSlot is not < blocksPerFile,
	// so the loop reaches chooseSegmentEnd and dumpBeaconBlocksRange.
	// Before the fix this panicked; now it returns an error (no snap dir / empty db).
	var err error
	require.NotPanics(t, func() {
		err = DumpBeaconBlocks(t.Context(), db, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, log.LvlDebug, log.New())
	})
	require.ErrorContains(t, err, "skipped too many blocks in a row")
}

// dumpBeaconBlocksRange indexes the segment it just compressed, which is only safe because
// Compress fsyncs and renames before returning.
func TestDumpBeaconBlocksRangeBuildsSegAndIdx(t *testing.T) {
	ctx := t.Context()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		var prevRoot common.Hash
		// One block every 500 slots keeps skippedInARow below its 1000 limit.
		for slot := uint64(0); slot < snaptype.CaplinMergeLimit; slot += 500 {
			var root common.Hash
			binary.BigEndian.PutUint64(root[:], slot+1)
			if err := beacon_indicies.MarkRootCanonical(ctx, tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(ctx, tx, root, prevRoot); err != nil {
				return err
			}
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root), root[:]); err != nil {
				return err
			}
			prevRoot = root
		}
		return nil
	}))

	require.NoError(t, dumpBeaconBlocksRange(ctx, db, 0, snaptype.CaplinMergeLimit, 1, dirs, 1, log.LvlDebug, log.New()))

	segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, 0, snaptype.CaplinMergeLimit)
	f, _, ok := snaptype.ParseFileName(dirs.Snap, segName)
	require.True(t, ok)

	d, err := seg.NewDecompressor(f.Path)
	require.NoError(t, err)
	defer d.Close()
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), uint64(d.Count()))

	idxPaths, err := filepath.Glob(filepath.Join(dirs.Snap, "*.idx"))
	require.NoError(t, err)
	require.Len(t, idxPaths, 1)

	idx, err := recsplit.OpenIndex(idxPaths[0])
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), idx.KeyCount())
}

// Pre-Deneb chains never freeze blobs, so the answer must be 0 regardless of visible segments.
func TestFrozenBlobsZeroBeforeDeneb(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = math.MaxUint64

	dirs := datadir.New(t.TempDir())
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)

	require.Equal(t, uint64(0), sn.FrozenBlobs())
}

// FrozenBlobs must take visibleLock: recalcVisibleFiles (via OpenFolder)
// reassigns s.visible under the write lock. Meaningful under -race.
func TestFrozenBlobsVisibleLockRace(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			sn.FrozenBlobs()
		}
	}()
	var err error
	for range 100 {
		if err = sn.OpenFolder(); err != nil {
			break
		}
	}
	<-done
	require.NoError(t, err)
}
