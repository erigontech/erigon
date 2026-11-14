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
	"encoding/binary"
	"errors"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type getLatestOptionsTx struct {
	kv.TemporalTx
	opts kv.GetLatestOptions
}

type prunableStepsDB struct {
	kv.TemporalRoDB
	steps uint64
}

func (db *prunableStepsDB) MaxPrunableStepsBacklog() uint64 {
	return db.steps
}

func TestMaxPrunableStepsBacklog(t *testing.T) {
	db := &prunableStepsDB{steps: 123}
	s := NewKvServer(t.Context(), db, nil, nil, log.New())
	reply, err := s.MaxPrunableStepsBacklog(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(123), reply.Steps)
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

type readSequenceTx struct {
	kv.TemporalTx
	err error
}

func (tx *readSequenceTx) ReadSequence(string) (uint64, error) { return 0, tx.err }

func TestSequenceReturnsReadSequenceError(t *testing.T) {
	sentinel := errors.New("read sequence failed")
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	s.txs[1] = &threadSafeTx{TemporalTx: &readSequenceTx{err: sentinel}}
	_, err := s.Sequence(t.Context(), &remoteproto.SequenceReq{TxId: 1, Table: kv.HeaderNumber})
	require.ErrorIs(t, err, sentinel)
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

func TestKvServerPagination(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require, ctx, db := require.New(t), t.Context(), temporaltest.NewTestDB(t, dirs)
	key := []byte{0xaa}
	require.NoError(db.Update(ctx, func(tx kv.RwTx) error {
		for i := uint64(1); i <= 5; i++ {
			v := binary.BigEndian.AppendUint64(nil, i)
			if err := tx.Put(kv.TblTracesToIdx, key, v); err != nil {
				return err
			}
		}
		return nil
	}))

	s := NewKvServer(ctx, db, nil, nil, log.New())
	id, err := s.begin(ctx)
	require.NoError(err)
	defer s.rollback(id)

	var txNums []uint64
	for token, pages := "", 0; pages == 0 || token != ""; pages++ {
		reply, err := s.IndexRange(ctx, &remoteproto.IndexRangeReq{TxId: id, Table: "tracesto", K: key, FromTs: 0, ToTs: -1, OrderAscend: true, Limit: -1, PageSize: 2, PageToken: token})
		require.NoError(err)
		require.LessOrEqual(len(reply.Timestamps), 2)
		txNums, token = append(txNums, reply.Timestamps...), reply.NextPageToken
	}
	require.Equal([]uint64{1, 2, 3, 4, 5}, txNums)
}

func TestKvServerRangeAsOfPagination(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require, ctx, db := require.New(t), t.Context(), temporaltest.NewTestDB(t, dirs)
	require.NoError(db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		d, err := execctx.NewSharedDomains(ctx, tx, log.New())
		if err != nil {
			return err
		}
		defer d.Close()
		d.SetDisableInlineTouchKey(true)
		for i := byte(1); i <= 5; i++ {
			if err := d.DomainPut(kv.AccountsDomain, tx, []byte{i}, []byte{i}, 1, nil); err != nil {
				return err
			}
		}
		return d.Flush(ctx, tx)
	}))

	s := NewKvServer(ctx, db, nil, nil, log.New())
	id, err := s.begin(ctx)
	require.NoError(err)
	defer s.rollback(id)

	pages := func(limit int64) (keys []string) {
		for token, n := "", 0; n == 0 || token != ""; n++ {
			reply, err := s.RangeAsOf(ctx, &remoteproto.RangeAsOfReq{TxId: id, Table: "accounts", Ts: 10, OrderAscend: true, Limit: limit, PageSize: 2, PageToken: token})
			require.NoError(err)
			require.LessOrEqual(len(reply.Keys), 2)
			for i, k := range reply.Keys {
				require.Equal(k, reply.Values[i])
				keys = append(keys, string(k))
			}
			token = reply.NextPageToken
		}
		return keys
	}
	require.Equal([]string{"\x01", "\x02", "\x03", "\x04", "\x05"}, pages(-1))
	require.Equal([]string{"\x01", "\x02", "\x03"}, pages(3))
}

// indexJoinTx serves in-memory inverted indexes with the range semantics of TemporalTx.IndexRange.
type indexJoinTx struct {
	kv.TemporalTx
	index map[kv.InvertedIdx]map[string][]uint64
}

func (tx *indexJoinTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, _ int) (stream.U64, error) {
	var res []uint64
	for _, ts := range tx.index[name][string(k)] {
		if asc && (fromTs < 0 || ts >= uint64(fromTs)) && (toTs < 0 || ts < uint64(toTs)) ||
			!asc && (fromTs < 0 || ts <= uint64(fromTs)) && (toTs < 0 || ts > uint64(toTs)) {
			res = append(res, ts)
		}
	}
	if !asc {
		slices.Reverse(res)
	}
	return stream.Array(res), nil
}

func TestIndexJoinPaginates(t *testing.T) {
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	s.txs[1] = &threadSafeTx{TemporalTx: &indexJoinTx{index: map[kv.InvertedIdx]map[string][]uint64{
		kv.LogTopicIdx: {"topic": {1, 3, 4, 5, 7, 8, 9, 11}},
		kv.LogAddrIdx:  {"a1": {3, 5, 9}, "a2": {4, 7, 8, 11}},
	}}}
	clauses := []*remoteproto.IndexJoinClause{
		{Keys: []*remoteproto.IndexJoinKey{{Table: kv.LogTopicIdx.String(), K: []byte("topic")}}},
		{Keys: []*remoteproto.IndexJoinKey{{Table: kv.LogAddrIdx.String(), K: []byte("a1")}, {Table: kv.LogAddrIdx.String(), K: []byte("a2")}}},
	}
	for _, tc := range []struct {
		name         string
		fromTs, toTs int64
		asc          bool
		want         []uint64
	}{
		{"asc", 2, 11, true, []uint64{3, 4, 5, 7, 8, 9}},
		{"desc", 11, 3, false, []uint64{11, 9, 8, 7, 5, 4}},
		{"unbounded", -1, -1, true, []uint64{3, 4, 5, 7, 8, 9, 11}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &remoteproto.IndexJoinReq{TxId: 1, Clauses: clauses, FromTs: tc.fromTs, ToTs: tc.toTs, OrderAscend: tc.asc, PageSize: 2}
			var got []uint64
			pages := 0
			for {
				reply, err := s.IndexJoin(t.Context(), req)
				require.NoError(t, err)
				require.LessOrEqual(t, len(reply.Timestamps), 2)
				got = append(got, reply.Timestamps...)
				pages++
				if reply.NextPageToken == "" {
					break
				}
				req.PageToken = reply.NextPageToken
			}
			require.Equal(t, tc.want, got)
			require.Equal(t, (len(tc.want)+1)/2, pages)
		})
	}
}

// cancelingTx cancels the request context once its index streams have been read a given number of times.
type cancelingTx struct {
	*indexJoinTx
	cancel      context.CancelFunc
	readsBefore int
}

type cancelingStream struct {
	stream.U64
	tx *cancelingTx
}

func (it cancelingStream) Next() (uint64, error) {
	if it.tx.readsBefore--; it.tx.readsBefore == 0 {
		it.tx.cancel()
	}
	return it.U64.Next()
}

func (tx *cancelingTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (stream.U64, error) {
	it, err := tx.indexJoinTx.IndexRange(name, k, fromTs, toTs, asc, limit)
	return cancelingStream{it, tx}, err
}

func TestIndexJoinStopsScanningOnCancel(t *testing.T) {
	var even, odd []uint64
	for ts := range uint64(1000) {
		if ts%2 == 0 {
			even = append(even, ts)
		} else {
			odd = append(odd, ts)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	s.txs[1] = &threadSafeTx{TemporalTx: &cancelingTx{
		indexJoinTx: &indexJoinTx{index: map[kv.InvertedIdx]map[string][]uint64{kv.LogAddrIdx: {"even": even, "odd": odd}}},
		cancel:      cancel,
		readsBefore: 10,
	}}
	req := &remoteproto.IndexJoinReq{TxId: 1, FromTs: -1, ToTs: -1, OrderAscend: true, Clauses: []*remoteproto.IndexJoinClause{
		{Keys: []*remoteproto.IndexJoinKey{{Table: kv.LogAddrIdx.String(), K: []byte("even")}}},
		{Keys: []*remoteproto.IndexJoinKey{{Table: kv.LogAddrIdx.String(), K: []byte("odd")}}},
	}}
	_, err := s.IndexJoin(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
}

// reusedBufKV yields n pairs from one reused buffer, like an MDBX cursor, then fails with err if set.
type reusedBufKV struct {
	buf    []byte
	n      int
	err    error
	closed bool
}

func (it *reusedBufKV) HasNext() bool { return it.n > 0 || it.err != nil }
func (it *reusedBufKV) Close()        { it.closed = true }
func (it *reusedBufKV) Next() ([]byte, []byte, error) {
	if it.n == 0 {
		return nil, nil, it.err
	}
	it.n--
	it.buf[0] = byte(it.n)
	return it.buf, it.buf, nil
}

type rangeTx struct {
	kv.TemporalTx
	it *reusedBufKV
}

func (tx *rangeTx) Range(string, []byte, []byte, order.By, int) (stream.KV, error) { return tx.it, nil }

func TestRangeCopiesPairsAndClosesIterator(t *testing.T) {
	s := NewKvServer(t.Context(), nil, nil, nil, log.New())
	it := &reusedBufKV{buf: make([]byte, 1), n: 3}
	s.txs[1] = &threadSafeTx{TemporalTx: &rangeTx{it: it}}
	reply, err := s.Range(t.Context(), &remoteproto.RangeReq{TxId: 1, Limit: -1})
	require.NoError(t, err)
	require.Equal(t, [][]byte{{2}, {1}, {0}}, reply.Keys)
	require.Equal(t, [][]byte{{2}, {1}, {0}}, reply.Values)
	require.True(t, it.closed)

	errNext := errors.New("next failed")
	it = &reusedBufKV{buf: make([]byte, 1), n: 1, err: errNext}
	s.txs[1] = &threadSafeTx{TemporalTx: &rangeTx{it: it}}
	_, err = s.Range(t.Context(), &remoteproto.RangeReq{TxId: 1, Limit: -1})
	require.ErrorIs(t, err, errNext)
	require.True(t, it.closed)
}
