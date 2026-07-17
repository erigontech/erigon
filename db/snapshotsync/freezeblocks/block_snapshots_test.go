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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

const testMergeLimit = snaptype.Erigon2MergeLimit

// blockFilesTxStub is a kv.Getter that also exposes a pinned block-files view,
// like a temporal tx does.
type blockFilesTxStub struct {
	kv.Getter
	view *blocksnapshots.View
}

func (s blockFilesTxStub) BlockFilesRoTx() *blocksnapshots.View { return s.view }

// TestBlockReaderPrefersTxBlockView proves step 3: when a tx exposes a pinned
// block-files view, the reader resolves segments through it — even one retired
// from the live set after the view was pinned. (The temporal tx that supplies
// this view in production is covered in the follow-up that enables it.)
func TestBlockReaderPrefersTxBlockView(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	ver := version.V1_0
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, testMergeLimit, typ.Enum(), dir, ver, logger)
		createTestSegmentFile(t, testMergeLimit, 2*testMergeLimit, typ.Enum(), dir, ver, logger)
	}
	require.NoError(t, snapshots.OpenFolder())

	blockReader := NewBlockReader(snapshots, nil)

	// Pin a view, then retire the [0, mergeLimit) tx segment from the live set.
	tx := blockFilesTxStub{view: snapshots.View()}
	defer tx.view.Close()
	_, err := snapshots.RetireFilesBelow(snaptype2.Transactions, testMergeLimit+1, nil)
	require.NoError(t, err)

	const blk = testMergeLimit / 2 // inside the retired [0, mergeLimit) segment

	// The live set no longer resolves it...
	_, okLive, relLive := snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, blk)
	relLive()
	require.False(t, okLive, "retired segment must be gone from the live set")

	// ...but a reader using the tx's pinned view still does.
	_, okTx, relTx := blockReader.viewSingleFile(tx, snaptype2.Transactions, blk)
	relTx()
	require.True(t, okTx, "reader must resolve the retired segment via the tx's pinned view")
}

// The minimal/full-node step: expire old transaction segments (handing their files to the
// seeder), keeping recent ones and the other block types.
func TestRetireMergedTransactionFilesBelow(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	ver := version.V1_0
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, testMergeLimit, typ.Enum(), dir, ver, logger)
		createTestSegmentFile(t, testMergeLimit, 2*testMergeLimit, typ.Enum(), dir, ver, logger)
	}
	require.NoError(t, snapshots.OpenFolder())

	var deleted []string
	retired, err := snapshots.RetireFilesBelow(snaptype2.Transactions, testMergeLimit+testMergeLimit/2, func(files []string) error {
		deleted = append(deleted, files...)
		return nil
	})
	require.NoError(t, err)
	require.True(t, retired)

	// The seeder is told about the [0, mergeLimit) tx segment: its .seg + both indexes.
	require.ElementsMatch(t, []string{
		snaptype.SegmentFileName(ver, 0, testMergeLimit, snaptype2.Transactions.Enum()),
		snaptype.IdxFileName(ver, 0, testMergeLimit, snaptype2.Transactions.Enum().String()),
		snaptype.IdxFileName(ver, 0, testMergeLimit, snaptype2.Indexes.TxnHash2BlockNum.Name),
	}, deleted)

	// Gone from the live set...
	_, ok, rel := snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, testMergeLimit/2)
	rel()
	require.False(t, ok, "retired tx segment must be gone from the live set")

	// ...the [mergeLimit, 2*mergeLimit) tx segment stays (its range ends at the cutoff)...
	_, ok, rel = snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, testMergeLimit)
	rel()
	require.True(t, ok, "tx segment at/above the cutoff must be kept")

	// ...and headers of the same range are untouched.
	_, ok, rel = snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Headers, testMergeLimit/2)
	rel()
	require.True(t, ok, "only transaction segments are retired")
}

func TestDumpRangeErrorsWhenRangeAlreadyClaimed(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	f := snaptype2.Headers.FileInfo(dir, 0, 1000)
	require.True(t, snapshots.TryAcquireRange(f.Type.Enum(), f.From, f.To))

	dumperCalled := false
	dumper := func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
		dumperCalled = true
		return 0, errors.New("dumper must not run on a claimed range")
	}

	_, err := dumpRange(t.Context(), f, dumper, nil, nil, nil, dir, 1, log.LvlInfo, logger, &snapshots.BaseRoSnapshots)
	require.ErrorIs(t, err, snapshotsync.ErrRangeBuildInProgress)
	require.False(t, dumperCalled)
}
