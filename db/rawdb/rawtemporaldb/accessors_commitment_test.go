package rawtemporaldb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execfinality"
)

type unwindFilesTx struct {
	kv.TemporalTx
	end uint64
}

func (tx unwindFilesTx) FreezeInfo() kv.FreezeInfo {
	return unwindFilesView{FreezeInfo: tx.TemporalTx.FreezeInfo(), end: tx.end}
}

type unwindFilesView struct {
	kv.FreezeInfo
	end uint64
}

func (view unwindFilesView) ValueFilesEndTxNum() uint64 { return view.end }

func TestMinimumUnwindKeepsImmutableTransactions(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	for block := range uint64(5) {
		require.NoError(t, rawdbv3.TxNums.Append(tx, block, 3*block+2))
		if block > 0 {
			require.NoError(t, changeset.WriteDiffSet(tx, block, common.Hash{byte(block)}, &changeset.StateChangeSet{}))
		}
	}
	for _, tc := range []struct{ end, want uint64 }{{0, 0}, {6, 1}, {7, 2}, {9, 2}, {10, 3}} {
		view := unwindFilesTx{TemporalTx: tx, end: tc.end}
		got, err := rawtemporaldb.CanUnwindToBlockNum(view, rawdbv3.TxNums)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "exclusive file end %d", tc.end)
		_, allowed, err := rawtemporaldb.CanUnwindBeforeBlockNum(got, view, rawdbv3.TxNums)
		require.NoError(t, err)
		require.True(t, allowed)
		if got > 0 {
			_, allowed, err = rawtemporaldb.CanUnwindBeforeBlockNum(got-1, view, rawdbv3.TxNums)
			require.NoError(t, err)
			require.False(t, allowed)
		}
	}
	_, err := rawtemporaldb.CanUnwindToBlockNum(unwindFilesTx{TemporalTx: tx, end: 100}, rawdbv3.TxNums)
	require.ErrorIs(t, err, commitmentdb.ErrBehindCommitment)
}

func TestMinimumUnwindUsesValueFilesAndRetainedChangesets(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	for block := range uint64(5) {
		require.NoError(t, rawdbv3.TxNums.Append(tx, block, 3*block+2))
	}
	for block := uint64(3); block <= 4; block++ {
		require.NoError(t, changeset.WriteDiffSet(tx, block, common.Hash{byte(block)}, &changeset.StateChangeSet{}))
	}
	// Changesets for blocks 3 and 4 allow a return to block 2. Only value
	// files that cover a later block can raise that minimum.
	for _, tc := range []struct{ valueEnd, wantBlock uint64 }{
		{6, 2},  // value files end in block 1
		{10, 3}, // value files end in block 3
	} {
		view := unwindFilesTx{TemporalTx: tx, end: tc.valueEnd}
		got, err := rawtemporaldb.CanUnwindToBlockNum(view, rawdbv3.TxNums)
		require.NoError(t, err)
		require.Equal(t, tc.wantBlock, got, "exclusive value file end %d", tc.valueEnd)
	}
}

func TestMinimumUnwindKeepsFilesHiddenByLaggingIndex(t *testing.T) {
	const stepSize = 10
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(stepSize))
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for block := range uint64(5) {
			require.NoError(t, rawdbv3.TxNums.Append(tx, block, 5*block+4))
			if block > 0 {
				require.NoError(t, changeset.WriteDiffSet(tx, block, common.Hash{byte(block)}, &changeset.StateChangeSet{}))
			}
		}
		return nil
	}))
	finality := execfinality.NewContext(4, 4, 0, false, rawdbv3.TxNums)
	require.NoError(t, db.BuildFiles2(t.Context(), 0, 2, finality, false))
	agg.WaitForFiles()
	checkBoundary := func(name string, visibleEnd uint64) {
		t.Run(name, func(t *testing.T) {
			tx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.Equal(t, visibleEnd, tx.Debug().TxNumsInFiles(kv.AccountsDomain))
			// Both steps are on disk. Their last transaction, 19, belongs to block 3.
			_, allowed, err := rawtemporaldb.CanUnwindBeforeBlockNum(2, tx, rawdbv3.TxNums)
			require.NoError(t, err)
			require.False(t, allowed, "the value file on disk must protect block 3")
			_, allowed, err = rawtemporaldb.CanUnwindBeforeBlockNum(3, tx, rawdbv3.TxNums)
			require.NoError(t, err)
			require.True(t, allowed)
		})
	}
	agg.CloseFilesNoReopen()
	checkBoundary("files_closed", 0)

	paths, err := filepath.Glob(filepath.Join(dirs.SnapIdx, "*-logaddrs.1-2.ef"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	index := paths[0]
	savedIndex := filepath.Join(t.TempDir(), filepath.Base(index))
	require.NoError(t, os.Rename(index, savedIndex))
	require.NoError(t, db.OpenStateSnapshots(t.Context()))
	checkBoundary("index_lagging", stepSize)
	require.NoError(t, os.Rename(savedIndex, index))
	require.NoError(t, db.OpenStateSnapshots(t.Context()))
	checkBoundary("index_complete", 2*stepSize)
}

type snapshotTxIndex struct {
	rawdbv3.TxBlockIndex
	t *testing.T
}

func (index snapshotTxIndex) BlockNumber(_ context.Context, _ kv.Tx, txNum uint64) (uint64, bool, error) {
	require.Equal(index.t, uint64(11), txNum)
	return 3, true, nil
}

func TestMinimumUnwindUsesSnapshotBlockIndex(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	require.NoError(t, changeset.WriteDiffSet(tx, 1, common.Hash{1}, &changeset.StateChangeSet{}))
	view := unwindFilesTx{TemporalTx: tx, end: 12}
	reader := rawdbv3.TxNums.WithCustomReadTxNumFunc(snapshotTxIndex{t: t})
	got, err := rawtemporaldb.CanUnwindToBlockNum(view, reader)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got)
}
