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

package iter_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/stretchr/testify/require"
)

func TestUnion(t *testing.T) {
	t.Run("arrays", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 6, 7})
		s2 := iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := iter.Union[uint64](s1, s2, order.Asc, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 6, 7, 8}, res)

		s1 = iter.ReverseArray[uint64]([]uint64{1, 3, 6, 7})
		s2 = iter.ReverseArray[uint64]([]uint64{2, 3, 7, 8})
		s3 = iter.Union[uint64](s1, s2, order.Desc, -1)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{8, 7, 6, 3, 2, 1}, res)

		s1 = iter.ReverseArray[uint64]([]uint64{1, 3, 6, 7})
		s2 = iter.ReverseArray[uint64]([]uint64{2, 3, 7, 8})
		s3 = iter.Union[uint64](s1, s2, order.Desc, 2)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{8, 7}, res)

	})
	t.Run("empty left", func(t *testing.T) {
		s1 := iter.EmptyU64
		s2 := iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := iter.Union[uint64](s1, s2, order.Asc, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 3, 7, 8}, res)
	})
	t.Run("empty right", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := iter.EmptyU64
		s3 := iter.Union[uint64](s1, s2, order.Asc, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.EmptyU64
		s2 := iter.EmptyU64
		s3 := iter.Union[uint64](s1, s2, order.Asc, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}
func TestUnionPairs(t *testing.T) {
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.E2AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{4}, []byte{1})
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, values, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
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
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.E2AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{4}, []byte{1})
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		m := iter.UnionKV(it, it2, -1)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := iter.PairsWithError(10)
		it2 := iter.PairsWithError(12)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.Equal("expected error at iteration: 10", err.Error())
		require.Equal(10, len(keys))
	})
}

func TestIntersect(t *testing.T) {
	t.Run("intersect", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := iter.Array[uint64]([]uint64{2, 3, 7})
		s3 := iter.Intersect[uint64](s1, s2, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{3, 7}, res)

		s1 = iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 = iter.Array[uint64]([]uint64{2, 3, 7})
		s3 = iter.Intersect[uint64](s1, s2, 1)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{3}, res)
	})
	t.Run("empty left", func(t *testing.T) {
		s1 := iter.EmptyU64
		s2 := iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := iter.Intersect[uint64](s1, s2, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)

		s2 = iter.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 = iter.Intersect[uint64](nil, s2, -1)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
	t.Run("empty right", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := iter.EmptyU64
		s3 := iter.Intersect[uint64](s1, s2, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, nil, res)

		s1 = iter.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s3 = iter.Intersect[uint64](s1, nil, -1)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.EmptyU64
		s2 := iter.EmptyU64
		s3 := iter.Intersect[uint64](s1, s2, -1)
		res, err := iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)

		s3 = iter.Intersect[uint64](nil, nil, -1)
		res, err = iter.ToArr[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}

func TestRange(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		s1 := iter.Range[uint64](1, 4)
		res, err := iter.ToArr[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3}, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.Range[uint64](1, 1)
		res, err := iter.ToArr[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, res)
	})
}

func TestPaginated(t *testing.T) {
	t.Run("paginated", func(t *testing.T) {
		i := 0
		s1 := iter.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return []uint64{1, 2, 3}, "test", nil
			case 2:
				return []uint64{4, 5, 6}, "test", nil
			case 3:
				return []uint64{7}, "", nil
			case 4:
				panic("must not happen")
			}
			return
		})
		res, err := iter.ToArr[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, res)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
	t.Run("error", func(t *testing.T) {
		i := 0
		testErr := fmt.Errorf("test")
		s1 := iter.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return []uint64{1, 2, 3}, "test", nil
			case 2:
				return nil, "test", testErr
			case 3:
				panic("must not happen")
			}
			return
		})
		res, err := iter.ToArr[uint64](s1)
		require.ErrorIs(t, err, testErr)
		require.Equal(t, []uint64{1, 2, 3}, res)

		//idempotency
		require.True(t, s1.HasNext())
		require.True(t, s1.HasNext())
		_, err = s1.Next()
		require.ErrorIs(t, err, testErr)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			return []uint64{}, "", nil
		})
		res, err := iter.ToArr[uint64](s1)
		require.NoError(t, err)
		require.Nil(t, res)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
}

func TestPaginatedDual(t *testing.T) {
	t.Run("paginated", func(t *testing.T) {
		i := 0
		s1 := iter.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return [][]byte{{1}, {2}, {3}}, [][]byte{{1}, {2}, {3}}, "test", nil
			case 2:
				return [][]byte{{4}, {5}, {6}}, [][]byte{{4}, {5}, {6}}, "test", nil
			case 3:
				return [][]byte{{7}}, [][]byte{{7}}, "", nil
			case 4:
				panic("must not happen")
			}
			return
		})

		keys, values, err := iter.ToKVArray(s1)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}}, keys)
		require.Equal(t, [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}}, values)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
	t.Run("error", func(t *testing.T) {
		i := 0
		testErr := fmt.Errorf("test")
		s1 := iter.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return [][]byte{{1}, {2}, {3}}, [][]byte{{1}, {2}, {3}}, "test", nil
			case 2:
				return nil, nil, "test", testErr
			case 3:
				panic("must not happen")
			}
			return
		})
		keys, values, err := iter.ToKVArray(s1)
		require.ErrorIs(t, err, testErr)
		require.Equal(t, [][]byte{{1}, {2}, {3}}, keys)
		require.Equal(t, [][]byte{{1}, {2}, {3}}, values)

		//idempotency
		require.True(t, s1.HasNext())
		require.True(t, s1.HasNext())
		_, _, err = s1.Next()
		require.ErrorIs(t, err, testErr)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := iter.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			return [][]byte{}, [][]byte{}, "", nil
		})
		keys, values, err := iter.ToKVArray(s1)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
}

func TestFiler(t *testing.T) {
	createKVIter := func() iter.KV {
		i := 0
		return iter.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return [][]byte{{1}, {2}, {3}}, [][]byte{{1}, {2}, {3}}, "test", nil
			case 2:
				return nil, nil, "", nil
			}
			return
		})

	}
	t.Run("dual", func(t *testing.T) {
		s2 := iter.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{1}) })
		keys, values, err := iter.ToKVArray(s2)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}}, keys)
		require.Equal(t, [][]byte{{1}}, values)

		s2 = iter.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{3}) })
		keys, values, err = iter.ToKVArray(s2)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{3}}, keys)
		require.Equal(t, [][]byte{{3}}, values)

		s2 = iter.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{4}) })
		keys, values, err = iter.ToKVArray(s2)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)

		s2 = iter.FilterKV(iter.EmptyKV, func(k, v []byte) bool { return bytes.Equal(k, []byte{4}) })
		keys, values, err = iter.ToKVArray(s2)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)
	})
	t.Run("unary", func(t *testing.T) {
		s1 := iter.Array[uint64]([]uint64{1, 2, 3})
		s2 := iter.FilterU64(s1, func(k uint64) bool { return k == 1 })
		res, err := iter.ToU64Arr(s2)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, res)

		s1 = iter.Array[uint64]([]uint64{1, 2, 3})
		s2 = iter.FilterU64(s1, func(k uint64) bool { return k == 3 })
		res, err = iter.ToU64Arr(s2)
		require.NoError(t, err)
		require.Equal(t, []uint64{3}, res)

		s1 = iter.Array[uint64]([]uint64{1, 2, 3})
		s2 = iter.FilterU64(s1, func(k uint64) bool { return k == 4 })
		res, err = iter.ToU64Arr(s2)
		require.NoError(t, err)
		require.Nil(t, res)

		s2 = iter.FilterU64(iter.EmptyU64, func(k uint64) bool { return k == 4 })
		res, err = iter.ToU64Arr(s2)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}
