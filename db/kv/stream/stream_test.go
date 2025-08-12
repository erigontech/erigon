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

package stream_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func TestUnion(t *testing.T) {
	t.Run("arrays", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{1, 3, 6, 7})
		s2 := stream.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := stream.Union[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 6, 7, 8}, res)

		s1 = stream.ReverseArray[uint64]([]uint64{1, 3, 6, 7})
		s2 = stream.ReverseArray[uint64]([]uint64{2, 3, 7, 8})
		s3 = stream.Union[uint64](s1, s2, order.Desc, -1)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{8, 7, 6, 3, 2, 1}, res)

		s1 = stream.ReverseArray[uint64]([]uint64{1, 3, 6, 7})
		s2 = stream.ReverseArray[uint64]([]uint64{2, 3, 7, 8})
		s3 = stream.Union[uint64](s1, s2, order.Desc, 2)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{8, 7}, res)

	})
	t.Run("empty left", func(t *testing.T) {
		s1 := stream.EmptyU64
		s2 := stream.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := stream.Union[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 3, 7, 8}, res)
	})
	t.Run("empty right", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := stream.EmptyU64
		s3 := stream.Union[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := stream.EmptyU64
		s2 := stream.EmptyU64
		s3 := stream.Union[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}
func TestUnionPairs(t *testing.T) {
	db := memdb.NewTestDB(t, kv.ChainDB)
	ctx := context.Background()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.HeaderNumber, []byte{1}, []byte{1})
		_ = tx.Put(kv.HeaderNumber, []byte{3}, []byte{1})
		_ = tx.Put(kv.HeaderNumber, []byte{4}, []byte{1})
		_ = tx.Put(kv.TblAccountVals, []byte{2}, []byte{9})
		_ = tx.Put(kv.TblAccountVals, []byte{3}, []byte{9})
		it, _ := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		it2, _ := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		keys, values, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {3}, {4}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {1}}, values)
	})
	t.Run("empty 1st", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.TblAccountVals, []byte{2}, []byte{9})
		_ = tx.Put(kv.TblAccountVals, []byte{3}, []byte{9})
		it, _ := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		it2, _ := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.HeaderNumber, []byte{1}, []byte{1})
		_ = tx.Put(kv.HeaderNumber, []byte{3}, []byte{1})
		_ = tx.Put(kv.HeaderNumber, []byte{4}, []byte{1})
		it, _ := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		it2, _ := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		it2, _ := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		m := stream.UnionKV(it, it2, -1)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := stream.PairsWithError(10)
		it2 := stream.PairsWithError(12)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.Equal("expected error at iteration: 10", err.Error())
		require.Len(keys, 10)
	})
}

func TestIntersect(t *testing.T) {
	t.Run("intersect", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := stream.Array[uint64]([]uint64{2, 3, 7})
		s3 := stream.Intersect[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{3, 7}, res)

		s1 = stream.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 = stream.Array[uint64]([]uint64{2, 3, 7})
		s3 = stream.Intersect[uint64](s1, s2, order.Asc, 1)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{3}, res)
	})

	t.Run("intersect Desc", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{7, 6, 5, 4, 3, 1})
		s2 := stream.Array[uint64]([]uint64{7, 3, 2})
		s3 := stream.Intersect[uint64](s1, s2, order.Desc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Equal(t, []uint64{7, 3}, res)
	})

	t.Run("empty left", func(t *testing.T) {
		s1 := stream.EmptyU64
		s2 := stream.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 := stream.Intersect[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)

		s2 = stream.Array[uint64]([]uint64{2, 3, 7, 8})
		s3 = stream.Intersect[uint64](nil, s2, order.Asc, -1)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
	t.Run("empty right", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s2 := stream.EmptyU64
		s3 := stream.Intersect[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)

		s1 = stream.Array[uint64]([]uint64{1, 3, 4, 5, 6, 7})
		s3 = stream.Intersect[uint64](s1, nil, order.Asc, -1)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := stream.EmptyU64
		s2 := stream.EmptyU64
		s3 := stream.Intersect[uint64](s1, s2, order.Asc, -1)
		res, err := stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)

		s3 = stream.Intersect[uint64](nil, nil, order.Asc, -1)
		res, err = stream.ToArray[uint64](s3)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}

func TestRange(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		s1 := stream.Range[uint64](1, 4)
		res, err := stream.ToArray[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3}, res)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := stream.Range[uint64](1, 1)
		res, err := stream.ToArray[uint64](s1)
		require.NoError(t, err)
		require.Empty(t, res)
	})
}

func TestPaginated(t *testing.T) {
	t.Run("paginated", func(t *testing.T) {
		i := 0
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
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
		res, err := stream.ToArray[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, res)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
	t.Run("error", func(t *testing.T) {
		i := 0
		testErr := errors.New("test")
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
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
		res, err := stream.ToArray[uint64](s1)
		require.ErrorIs(t, err, testErr)
		require.Equal(t, []uint64{1, 2, 3}, res)

		//idempotency
		require.True(t, s1.HasNext())
		require.True(t, s1.HasNext())
		_, err = s1.Next()
		require.ErrorIs(t, err, testErr)
	})
	t.Run("empty", func(t *testing.T) {
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			return []uint64{}, "", nil
		})
		res, err := stream.ToArray[uint64](s1)
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
		s1 := stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
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

		keys, values, err := stream.ToArrayKV(s1)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}}, keys)
		require.Equal(t, [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}}, values)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
	t.Run("error", func(t *testing.T) {
		i := 0
		testErr := errors.New("test")
		s1 := stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
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
		keys, values, err := stream.ToArrayKV(s1)
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
		s1 := stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			return [][]byte{}, [][]byte{}, "", nil
		})
		keys, values, err := stream.ToArrayKV(s1)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)

		//idempotency
		require.False(t, s1.HasNext())
		require.False(t, s1.HasNext())
	})
}

func TestFiler(t *testing.T) {
	createKVIter := func() stream.KV {
		i := 0
		return stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
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
		s2 := stream.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{1}) })
		keys, values, err := stream.ToArrayKV(s2)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}}, keys)
		require.Equal(t, [][]byte{{1}}, values)

		s2 = stream.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{3}) })
		keys, values, err = stream.ToArrayKV(s2)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{3}}, keys)
		require.Equal(t, [][]byte{{3}}, values)

		s2 = stream.FilterKV(createKVIter(), func(k, v []byte) bool { return bytes.Equal(k, []byte{4}) })
		keys, values, err = stream.ToArrayKV(s2)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)

		s2 = stream.FilterKV(stream.EmptyKV, func(k, v []byte) bool { return bytes.Equal(k, []byte{4}) })
		keys, values, err = stream.ToArrayKV(s2)
		require.NoError(t, err)
		require.Nil(t, keys)
		require.Nil(t, values)
	})
	t.Run("unary", func(t *testing.T) {
		s1 := stream.Array[uint64]([]uint64{1, 2, 3})
		s2 := stream.FilterU64(s1, func(k uint64) bool { return k == 1 })
		res, err := stream.ToArrayU64(s2)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, res)

		s1 = stream.Array[uint64]([]uint64{1, 2, 3})
		s2 = stream.FilterU64(s1, func(k uint64) bool { return k == 3 })
		res, err = stream.ToArrayU64(s2)
		require.NoError(t, err)
		require.Equal(t, []uint64{3}, res)

		s1 = stream.Array[uint64]([]uint64{1, 2, 3})
		s2 = stream.FilterU64(s1, func(k uint64) bool { return k == 4 })
		res, err = stream.ToArrayU64(s2)
		require.NoError(t, err)
		require.Nil(t, res)

		s2 = stream.FilterU64(stream.EmptyU64, func(k uint64) bool { return k == 4 })
		res, err = stream.ToArrayU64(s2)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}
