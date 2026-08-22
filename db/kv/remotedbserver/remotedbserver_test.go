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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type getLatestOptionsTx struct {
	kv.TemporalTx
	opts kv.GetLatestOptions
}

func (tx *getLatestOptionsTx) GetLatest(_ kv.Domain, _ []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.opts = opts
	return nil, 0, nil
}

func TestGetLatestForwardsMaxStep(t *testing.T) {
	tx := &getLatestOptionsTx{}
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	s.txs[1] = &threadSafeTx{TemporalTx: tx}
	maxStep := uint64(0)
	_, err := s.GetLatest(t.Context(), &remoteproto.GetLatestReq{TxId: 1, Table: kv.AccountsDomain.String(), Latest: true, MaxStep: &maxStep})
	require.NoError(t, err)
	require.Equal(t, kv.Step(0), tx.opts.MaxStep())
}

func TestGetLatestForwardsBranchCache(t *testing.T) {
	tx := &getLatestOptionsTx{}
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	s.txs[1] = &threadSafeTx{TemporalTx: tx}
	_, err := s.GetLatest(t.Context(), &remoteproto.GetLatestReq{TxId: 1, Table: kv.CommitmentDomain.String(), Latest: true, BranchCache: true})
	require.NoError(t, err)
	require.True(t, tx.opts.BranchCache())
}

func TestKvServer_renew(t *testing.T) {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	dirs := datadir.New(t.TempDir())
	require, ctx, db := require.New(t), t.Context(), temporaltest.NewTestDB(t, dirs)
	require.NoError(db.Update(ctx, func(tx kv.RwTx) error {
		wc, err := tx.RwCursorDupSort(kv.TblAccountVals)
		require.NoError(err)
		defer wc.Close()
		require.NoError(wc.Append([]byte{1}, []byte{1}))
		require.NoError(wc.Append([]byte{1}, []byte{2}))
		require.NoError(wc.Append([]byte{2}, []byte{1}))
		require.NoError(wc.Append([]byte{3}, []byte{1}))
		return nil
	}))

	s := NewKvServer(ctx, db, nil, nil, log.New())
	g, ctx := errgroup.WithContext(ctx)
	testCase := func() error {
		id, err := s.begin(ctx)
		if err != nil {
			return err
		}
		var c, c2 kv.Cursor
		if err := s.with(id, func(tx kv.TemporalTx) error {
			c, err = tx.Cursor(kv.TblAccountVals)
			return err
		}); err != nil {
			return err
		}
		k, v, err := c.First()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		if err := s.renew(ctx, id); err != nil {
			return err
		}

		if err := s.with(id, func(tx kv.TemporalTx) error {
			c, err = tx.Cursor(kv.TblAccountVals) //nolint:gocritic
			if err != nil {
				return err
			}
			c2, err = tx.Cursor(kv.TblAccountVals)
			return err
		}); err != nil {
			return err
		}
		defer c.Close()
		defer c2.Close()

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
	for range 10 {
		g.Go(testCase)
	}
	require.NoError(g.Wait())
}

func TestKVServerSnapshotsReturnsSnapshots(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	blockSnapshots := NewMockSnapshots(ctrl)
	blockSnapshots.EXPECT().Files().Return([]string{"headers.seg", "bodies.seg"}).Times(1)
	historySnapshots := NewMockSnapshots(ctrl)
	historySnapshots.EXPECT().Files().Return([]string{"history"}).Times(1)

	s := NewKvServer(ctx, nil, blockSnapshots, historySnapshots, log.New())
	reply, err := s.Snapshots(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"headers.seg", "bodies.seg"}, reply.BlocksFiles)
	require.Equal(t, []string{"history"}, reply.HistoryFiles)
}

func TestKVServerSnapshotsReturnsEmptyIfNoBlockSnapshots(t *testing.T) {
	ctx := t.Context()
	s := NewKvServer(ctx, nil, nil, nil, log.New())
	reply, err := s.Snapshots(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, reply.BlocksFiles)
	require.Empty(t, reply.HistoryFiles)
}
