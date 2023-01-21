package iter_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

func TestIntersect(t *testing.T) {
	s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
	s2 := iter.Array[uint64]([]uint64{2, 3, 7})
	s3 := iter.Intersect[uint64](s1, s2)

	res, err := iter.ToArr[uint64](s3)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 7}, res)
}

// Contraversial use-cases:
// - peek merge unlimited iterators until result>=PageSize. It require "Stop" primitive and better to be less eager.
// - get precise range/limit, maybe big
// Then we have 2 API's: Dual (rename Dual to Dual in kv_interface.go) and Cursor.
// But we want have Cursor+requestBatch operation - to avoid too many network ping-pong.
// invIdx.Dual() and invIdx.Paginate() (c.NextPage())
//

// Cockroach:
// 1. Iterate performs a paginated scan and applying the function f to every page.
// The semantics of retrieval and ordering are the same as for Scan. Note that
// Txn auto-retries the transaction if necessary. Hence, the paginated data
// must not be used for side-effects before the txn has committed.
// 2. []Pair; where Pair{k,v []byte}

// Vitess:
// 1. Exec/Dual methods
// But has separated class for each RealtionalAlgebra operator: InMemSort.run()/Intersect.run()
// then operator implementation use hardcoded Exec/Dual method.
// 2. Pairs has struct:
//  // A length of -1 means that the field is NULL. While
//  // reading values, you have to accummulate the length
//  // to know the offset where the next value begins in values.
//  repeated sint64 lengths = 1;
//  // values contains a concatenation of all values in the row.
//  bytes values = 2;

// Request(from, limit) -> (N, nextFrom),  Request(nextFrom, limit) -> (N, nil)
// Request(from, limit) -> (N, hasNext=true),  Request(nextFrom, limit) -> (N, hasNext=false): server just need internally read Limit+1
//
// Request(from, limit) -> InteractiveStream(N, nextFrom),  Request(nextFrom, limit) -> (N, nil))

// TODO: remotedbserver - txs leak... when to close them? insider rollback check non-locked tx.ttl? When do we renew?
// stream will not help

// no Limit in request
// request (from, N) - merge on client
// stream (all) - push from server - merge on client

func TestUnion(t *testing.T) {
	t.Run("arrays", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := iter.Union[uint64](s1, s2)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, res)
	})
	t.Run("empty left", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{})
		s2 := iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := iter.Union[uint64](s1, s2)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 3, 7, 8}, res)
	})
	t.Run("empty right", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := iter.Array[uint64]([]uint64{})
		s3 := iter.Union[uint64](s1, s2)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{})
		s2 := iter.Array[uint64]([]uint64{})
		s3 := iter.Union[uint64](s1, s2)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{}, res)
	})
}
func TestUnionPairs(t *testing.T) {
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{4}, []byte{1})
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, values, err := iter.UnionPairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {3}, {4}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {1}}, values)
	})
	t.Run("empty 1st", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, _, err := iter.UnionPairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.AccountsHistory, []byte{4}, []byte{1})
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		keys, _, err := iter.UnionPairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Stream(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Stream(kv.PlainState, nil, nil)
		m := iter.UnionPairs(it, it2)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := iter.PairsWithError(10)
		it2 := iter.PairsWithError(12)
		keys, _, err := iter.UnionPairs(it, it2).ToArray()
		require.Equal("expected error at iteration: 10", err.Error())
		require.Equal(10, len(keys))
	})
}
