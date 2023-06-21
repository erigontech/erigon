package stagedsync

import (
	"context"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func genTestCallTraceSet(t *testing.T, tx kv.RwTx, to uint64) {
	v := [21]byte{}
	for i := uint64(0); i < to; i++ {
		v[19] = byte(i % 5)
		if i%2 == 0 {
			v[20] = 1
		}
		if i%2 == 1 {
			v[20] = 2
		}
		err := tx.Put(kv.CallTraceSet, hexutility.EncodeTs(i), v[:])
		require.NoError(t, err)
	}
}

func TestCallTrace(t *testing.T) {
	logger := log.New()
	ctx, require := context.Background(), require.New(t)
	histV3, db, _ := temporal.NewTestDB(t, datadir.New(t.TempDir()), nil)
	if histV3 {
		t.Skip()
	}
	tx, err := db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	genTestCallTraceSet(t, tx, 30)
	addr := [20]byte{}
	addr[19] = byte(1)
	froms := func() *roaring64.Bitmap {
		b, err := bitmapdb.Get64(tx, kv.CallFromIndex, addr[:], 0, 30)
		require.NoError(err)
		return b
	}
	tos := func() *roaring64.Bitmap {
		b, err := bitmapdb.Get64(tx, kv.CallToIndex, addr[:], 0, 30)
		require.NoError(err)
		return b
	}

	err = stages.SaveStageProgress(tx, stages.Execution, 30)
	require.NoError(err)

	// forward 0->20
	err = promoteCallTraces("test", tx, 0, 20, 0, time.Nanosecond, ctx.Done(), "", logger)
	require.NoError(err)
	require.Equal([]uint64{6, 16}, froms().ToArray())
	require.Equal([]uint64{1, 11}, tos().ToArray())

	// unwind 20->10
	err = DoUnwindCallTraces("test", tx, 20, 10, ctx, "", logger)
	require.NoError(err)
	require.Equal([]uint64{6}, froms().ToArray())
	require.Equal([]uint64{1}, tos().ToArray())

	// forward 10->30
	err = promoteCallTraces("test", tx, 10, 30, 0, time.Nanosecond, ctx.Done(), "", logger)
	require.NoError(err)
	require.Equal([]uint64{6, 16, 26}, froms().ToArray())
	require.Equal([]uint64{1, 11, 21}, tos().ToArray())

	// prune 0 -> 10
	err = pruneCallTraces(tx, "test", 10, ctx, "", logger)
	require.NoError(err)
}
