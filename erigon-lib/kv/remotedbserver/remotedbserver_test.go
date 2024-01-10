/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package remotedbserver

import (
	"context"
	"runtime"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver/mock"
)

func TestKvServer_renew(t *testing.T) {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require, ctx, db := require.New(t), context.Background(), memdb.NewTestDB(t)
	require.NoError(db.Update(ctx, func(tx kv.RwTx) error {
		wc, err := tx.RwCursorDupSort(kv.PlainState)
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
			c, err = tx.Cursor(kv.PlainState)
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
			c, err = tx.Cursor(kv.PlainState)
			if err != nil {
				return err
			}
			c2, err = tx.Cursor(kv.PlainState)
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
	blockSnapshots := mock.NewMockSnapshots(ctrl)
	blockSnapshots.EXPECT().Files().Return([]string{"headers.seg", "bodies.seg"}).Times(1)
	historySnapshots := mock.NewMockSnapshots(ctrl)
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
	blockSnapshots := mock.NewMockSnapshots(ctrl)
	blockSnapshots.EXPECT().Files().Return([]string{"headers.seg", "bodies.seg"}).Times(1)
	borSnapshots := mock.NewMockSnapshots(ctrl)
	borSnapshots.EXPECT().Files().Return([]string{"borevents.seg", "borspans.seg"}).Times(1)
	historySnapshots := mock.NewMockSnapshots(ctrl)
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
