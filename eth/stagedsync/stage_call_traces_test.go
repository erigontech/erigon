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

package stagedsync

import (
	"context"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/eth/stagedsync/stages"
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
	t.Skip("this stage is disabled in E3")

	logger := log.New()
	ctx, require := context.Background(), require.New(t)
	db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
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
