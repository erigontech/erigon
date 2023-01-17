package stream_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/stream"
	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
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
		it, _ := tx.Range(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, values, err := stream.MergePairs(it, it2).ToArray()
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
		it, _ := tx.Range(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
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
		it, _ := tx.Range(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Range(kv.AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		m := stream.MergePairs(it, it2)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := stream.PairsWithError(10)
		it2 := stream.PairsWithError(12)
		keys, _, err := stream.MergePairs(it, it2).ToArray()
		require.Equal("expected error at iteration: 10", err.Error())
		require.Equal(10, len(keys))
	})
}
