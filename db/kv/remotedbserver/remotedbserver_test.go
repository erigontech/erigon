// Copyright 2021 The Erigon Authors
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

package remotedbserver

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestKvServer_renew(t *testing.T) {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require, ctx, db := require.New(t), context.Background(), memdb.NewTestDB(t, kv.ChainDB)
	require.NoError(db.Update(ctx, func(tx kv.RwTx) error {
		wc, err := tx.RwCursorDupSort(kv.TblAccountVals)
		require.NoError(err)
		require.NoError(wc.Append([]byte{1}, []byte{1}))
		require.NoError(wc.Append([]byte{1}, []byte{2}))
		require.NoError(wc.Append([]byte{2}, []byte{1}))
		require.NoError(wc.Append([]byte{3}, []byte{1}))
		return nil
	}))

	s := NewKvServer(ctx, db, nil, nil, nil, log.New())
	g, ctx := errgroup.WithContext(ctx)
	testCase := func() error {
		id, err := s.begin(ctx)
		if err != nil {
			return err
		}
		var c, c2 kv.Cursor
		if err = s.with(id, func(tx kv.Tx) error {
			c, err = tx.Cursor(kv.TblAccountVals)
			return err
		}); err != nil {
			return err
		}
		k, v, err := c.First()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		if err = s.renew(ctx, id); err != nil {
			return err
		}

		if err = s.with(id, func(tx kv.Tx) error {
			c, err = tx.Cursor(kv.TblAccountVals)
			if err != nil {
				return err
			}
			c2, err = tx.Cursor(kv.TblAccountVals)
			return err
		}); err != nil {
			return err
		}

		k, v, err = c.Next()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)
		k, v, err = c2.Next()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		s.rollback(id)
		return nil
	}
	for i := 0; i < 10; i++ {
		g.Go(testCase)
	}
	require.NoError(g.Wait())
}

func TestKVServerSnapshotsReturnsSnapshots(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	blockSnapshots := NewMockSnapshots(ctrl)
	blockSnapshots.EXPECT().Files().Return([]string{"headers.seg", "bodies.seg"}).Times(1)
	historySnapshots := NewMockSnapshots(ctrl)
	historySnapshots.EXPECT().Files().Return([]string{"history"}).Times(1)

	s := NewKvServer(ctx, nil, blockSnapshots, nil, historySnapshots, log.New())
	reply, err := s.Snapshots(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"headers.seg", "bodies.seg"}, reply.BlocksFiles)
	require.Equal(t, []string{"history"}, reply.HistoryFiles)
}

func TestKVServerSnapshotsReturnsBorSnapshots(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	blockSnapshots := NewMockSnapshots(ctrl)
	blockSnapshots.EXPECT().Files().Return([]string{"headers.seg", "bodies.seg"}).Times(1)
	borSnapshots := NewMockSnapshots(ctrl)
	borSnapshots.EXPECT().Files().Return([]string{"borevents.seg", "borspans.seg"}).Times(1)
	historySnapshots := NewMockSnapshots(ctrl)
	historySnapshots.EXPECT().Files().Return([]string{"history"}).Times(1)

	s := NewKvServer(ctx, nil, blockSnapshots, borSnapshots, historySnapshots, log.New())
	reply, err := s.Snapshots(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"headers.seg", "bodies.seg", "borevents.seg", "borspans.seg"}, reply.BlocksFiles)
	require.Equal(t, []string{"history"}, reply.HistoryFiles)
}

func TestKVServerSnapshotsReturnsEmptyIfNoBlockSnapshots(t *testing.T) {
	ctx := context.Background()
	s := NewKvServer(ctx, nil, nil, nil, nil, log.New())
	reply, err := s.Snapshots(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, reply.BlocksFiles)
	require.Empty(t, reply.HistoryFiles)
}
