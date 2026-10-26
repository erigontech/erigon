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

package kv_test

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// memIndexes is an in-memory set of inverted indexes with the range semantics of TemporalTx.IndexRange.
type memIndexes map[kv.InvertedIdx]map[string][]uint64

func (m memIndexes) indexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (stream.U64, error) {
	var res []uint64
	for _, ts := range m[name][string(k)] {
		if inRange(ts, fromTs, toTs, asc) {
			res = append(res, ts)
		}
	}
	slices.Sort(res)
	if !asc {
		slices.Reverse(res)
	}
	if limit >= 0 && len(res) > limit {
		res = res[:limit]
	}
	return stream.Array(res), nil
}

func inRange(ts uint64, fromTs, toTs int, asc order.By) bool {
	if asc {
		return (fromTs < 0 || ts >= uint64(fromTs)) && (toTs < 0 || ts < uint64(toTs))
	}
	return (fromTs < 0 || ts <= uint64(fromTs)) && (toTs < 0 || ts > uint64(toTs))
}

// expected computes the join with set operations over the whole index.
func (m memIndexes) expected(q kv.IndexQuery) []uint64 {
	var res map[uint64]struct{}
	for _, clause := range q.Clauses {
		union := map[uint64]struct{}{}
		for _, k := range clause {
			for _, ts := range m[k.Idx][string(k.Key)] {
				if inRange(ts, q.FromTs, q.ToTs, q.Asc) {
					union[ts] = struct{}{}
				}
			}
		}
		if res == nil {
			res = union
			continue
		}
		for ts := range res {
			if _, ok := union[ts]; !ok {
				delete(res, ts)
			}
		}
	}
	var out []uint64
	for ts := range res {
		out = append(out, ts)
	}
	slices.Sort(out)
	if !q.Asc {
		slices.Reverse(out)
	}
	return out
}

func joinToArray(t *testing.T, m memIndexes, q kv.IndexQuery) []uint64 {
	t.Helper()
	it, err := kv.JoinIndexRanges(q, m.indexRange)
	require.NoError(t, err)
	res, err := stream.ToArrayU64(it)
	require.NoError(t, err)
	return res
}

var (
	addrA, addrB, addrC = []byte("addrA"), []byte("addrB"), []byte("addrC")
	topicX, topicY      = []byte("topicX"), []byte("topicY")
)

func testIndexes() memIndexes {
	return memIndexes{
		kv.LogAddrIdx: {
			string(addrA): {1, 3, 5, 7, 9, 11},
			string(addrB): {2, 3, 8, 11},
			string(addrC): {4, 6},
		},
		kv.LogTopicIdx: {
			string(topicX): {3, 4, 5, 6, 11},
			string(topicY): {1, 2, 3, 9},
		},
		kv.TracesFromIdx: {string(addrA): {10, 12}},
		kv.TracesToIdx:   {string(addrA): {12, 13}},
	}
}

func TestJoinIndexRangesMissingKey(t *testing.T) {
	m := testIndexes()
	q := kv.IndexQuery{Clauses: [][]kv.IndexKey{{{kv.LogAddrIdx, []byte("missing")}}, {{kv.LogTopicIdx, topicX}}}, FromTs: -1, ToTs: -1, Asc: order.Asc}
	require.Empty(t, joinToArray(t, m, q))
}

func TestJoinIndexRangesRejectsEmptyQuery(t *testing.T) {
	m := testIndexes()
	_, err := kv.JoinIndexRanges(kv.IndexQuery{FromTs: -1, ToTs: -1, Asc: order.Asc}, m.indexRange)
	require.ErrorIs(t, err, kv.ErrEmptyIndexQuery)
	_, err = kv.JoinIndexRanges(kv.IndexQuery{Clauses: [][]kv.IndexKey{{{kv.LogAddrIdx, addrA}}, {}}, FromTs: -1, ToTs: -1, Asc: order.Asc}, m.indexRange)
	require.ErrorIs(t, err, kv.ErrEmptyIndexQuery)
}

func TestJoinIndexRangesMatchesSetOperations(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	idxs := []kv.InvertedIdx{kv.LogAddrIdx, kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx}
	keys := [][]byte{[]byte("k0"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}
	m := memIndexes{}
	for _, idx := range idxs {
		m[idx] = map[string][]uint64{}
		for _, k := range keys {
			var tss []uint64
			for ts := range uint64(200) {
				if rnd.IntN(4) == 0 {
					tss = append(tss, ts)
				}
			}
			m[idx][string(k)] = tss
		}
	}
	for range 500 {
		clauses := make([][]kv.IndexKey, 1+rnd.IntN(3))
		for i := range clauses {
			clauses[i] = make([]kv.IndexKey, 1+rnd.IntN(4))
			for j := range clauses[i] {
				clauses[i][j] = kv.IndexKey{Idx: idxs[rnd.IntN(len(idxs))], Key: keys[rnd.IntN(len(keys))]}
			}
		}
		q := kv.IndexQuery{Clauses: clauses, FromTs: rnd.IntN(220) - 10, ToTs: rnd.IntN(220) - 10, Asc: order.By(rnd.IntN(2) == 0)}
		if q.FromTs < 0 {
			q.FromTs = -1
		}
		if q.ToTs < 0 {
			q.ToTs = -1
		}
		require.Equal(t, m.expected(q), joinToArray(t, m, q), "query %+v", q)
	}
}

type closeTracker struct {
	stream.U64
	closed *int
}

func (c closeTracker) Close() { *c.closed++; c.U64.Close() }

func TestJoinIndexRangesClosesOpenedStreamsOnError(t *testing.T) {
	m := testIndexes()
	errRange := errors.New("range failed")
	var opened, closed int
	failThird := func(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (stream.U64, error) {
		if opened == 2 {
			return nil, errRange
		}
		opened++
		it, err := m.indexRange(name, k, fromTs, toTs, asc, limit)
		return closeTracker{it, &closed}, err
	}
	q := kv.IndexQuery{Clauses: [][]kv.IndexKey{{{kv.LogAddrIdx, addrA}}, {{kv.LogTopicIdx, topicX}, {kv.LogTopicIdx, topicY}}}, FromTs: -1, ToTs: -1, Asc: order.Asc}
	_, err := kv.JoinIndexRanges(q, failThird)
	require.ErrorIs(t, err, errRange)
	require.Equal(t, 2, closed)
}
