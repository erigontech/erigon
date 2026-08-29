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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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
	t.Run("limit applies when other side is nil", func(t *testing.T) {
		s := stream.Union[uint64](stream.Array([]uint64{1, 2, 3, 4, 5}), nil, order.Asc, 2)
		res, err := stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)

		s = stream.Union[uint64](nil, stream.Array([]uint64{1, 2, 3, 4, 5}), order.Asc, 2)
		res, err = stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)
	})
	t.Run("limit applies when other side is empty", func(t *testing.T) {
		s := stream.Union[uint64](stream.Array([]uint64{1, 2, 3, 4, 5}), stream.EmptyU64, order.Asc, 2)
		res, err := stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)

		s = stream.Union[uint64](stream.EmptyU64, stream.Array([]uint64{1, 2, 3, 4, 5}), order.Asc, 2)
		res, err = stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)
	})
	t.Run("limit zero yields nothing", func(t *testing.T) {
		s := stream.Union[uint64](stream.Array([]uint64{1, 2, 3}), nil, order.Asc, 0)
		res, err := stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Empty(t, res)

		s = stream.Union[uint64](stream.Array([]uint64{1, 2, 3}), stream.Array([]uint64{4, 5}), order.Asc, 0)
		res, err = stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Empty(t, res)
	})
	t.Run("closes the discarded side", func(t *testing.T) {
		empty, full := &countingU64{}, &countingU64{arr: []uint64{1, 2}}
		stream.Union[uint64](empty, full, order.Asc, -1)
		require.Equal(t, 1, empty.closed)

		empty, full = &countingU64{}, &countingU64{arr: []uint64{1, 2}}
		stream.Union[uint64](full, empty, order.Asc, -1)
		require.Equal(t, 1, empty.closed)
	})
}

func TestUnion2(t *testing.T) {
	t.Run("limit applies when other side is nil", func(t *testing.T) {
		s := stream.Union2[uint64, uint64](nil, &countingDuo{arr: []uint64{7, 8, 9}}, order.Asc, 1)
		keys, _, err := stream.ToArrayDuo[uint64, uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{7}, keys)
	})
	t.Run("closes the discarded side", func(t *testing.T) {
		empty, full := &countingDuo{}, &countingDuo{arr: []uint64{1, 2}}
		stream.Union2[uint64, uint64](empty, full, order.Asc, -1)
		require.Equal(t, 1, empty.closed)
	})
}

func TestUnionPairs(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		t.Cleanup(tx.Rollback)
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{3}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{4}, []byte{1}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{3}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, values, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {3}, {4}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {1}}, values)
	})
	t.Run("empty 1st", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		t.Cleanup(tx.Rollback)
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{3}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		t.Cleanup(tx.Rollback)
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{3}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{4}, []byte{1}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		m := stream.UnionKV(it, it2, -1)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(PairsWithError(10), PairsWithError(12), -1))
		require.Equal("expected error at iteration: 10", err.Error())
		require.Len(keys, 10)

		// the error of whichever side fails first wins, regardless of position
		_, _, err = stream.ToArrayKV(stream.UnionKV(PairsWithError(12), PairsWithError(10), -1))
		require.Equal("expected error at iteration: 10", err.Error())
	})
	t.Run("limit applies when other side is empty", func(t *testing.T) {
		require := require.New(t)
		keys, _, err := stream.ToArrayKV(stream.UnionKV(&countingKV{keys: [][]byte{{1}, {2}, {3}}}, nil, 2))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}}, keys)

		keys, _, err = stream.ToArrayKV(stream.UnionKV(nil, &countingKV{keys: [][]byte{{1}, {2}, {3}}}, 2))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}}, keys)
	})
}

func TestMultisetKV(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	t.Run("preserves duplicates", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{3}, []byte{1}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{3}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, values, err := stream.ToArrayKV(stream.MultisetKV(it, it2, -1))
		require.NoError(err)
		// Key {3} appears twice (from both streams), unlike UnionKV which deduplicates
		require.Equal([][]byte{{1}, {2}, {3}, {3}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {9}}, values)
	})
	t.Run("sorted merge", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{4}, []byte{1}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{5}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.MultisetKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {4}, {5}}, keys)
	})
	t.Run("empty left", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{3}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.MultisetKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty right", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{3}, []byte{1}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.MultisetKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}}, keys)
	})
	t.Run("both empty", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		m := stream.MultisetKV(it, it2, -1)
		require.False(m.HasNext())
	})
	t.Run("limit", func(t *testing.T) {
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		require.NoError(tx.Put(kv.HeaderNumber, []byte{1}, []byte{1}))
		require.NoError(tx.Put(kv.HeaderNumber, []byte{3}, []byte{1}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{2}, []byte{9}))
		require.NoError(tx.Put(kv.TblAccountVals, []byte{3}, []byte{9}))
		it, err := tx.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		it2, err := tx.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
		require.NoError(err)
		keys, _, err := stream.ToArrayKV(stream.MultisetKV(it, it2, 2))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}}, keys)
	})
	t.Run("limit applies when other side is empty", func(t *testing.T) {
		require := require.New(t)
		keys, _, err := stream.ToArrayKV(stream.MultisetKV(&countingKV{keys: [][]byte{{1}, {2}, {3}}}, nil, 2))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}}, keys)

		keys, _, err = stream.ToArrayKV(stream.MultisetKV(nil, &countingKV{keys: [][]byte{{1}, {2}, {3}}}, 2))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}}, keys)
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
	t.Run("closes both sides when result is empty", func(t *testing.T) {
		full, empty := &countingU64{arr: []uint64{1, 2}}, &countingU64{}
		stream.Intersect[uint64](full, empty, order.Asc, -1)
		require.Equal(t, 1, full.closed)
		require.Equal(t, 1, empty.closed)

		full = &countingU64{arr: []uint64{1, 2}}
		stream.Intersect[uint64](full, nil, order.Asc, -1)
		require.Equal(t, 1, full.closed)

		full = &countingU64{arr: []uint64{1, 2}}
		stream.Intersect[uint64](nil, full, order.Asc, -1)
		require.Equal(t, 1, full.closed)
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
	t.Run("reverse is [from, to) descending", func(t *testing.T) {
		res, err := stream.ToArray[uint64](stream.ReverseRange[uint64](4, 1))
		require.NoError(t, err)
		require.Equal(t, []uint64{4, 3, 2}, res)

		res, err = stream.ToArray[uint64](stream.ReverseRange[uint64](1, 1))
		require.NoError(t, err)
		require.Empty(t, res)
	})
	t.Run("exhausted", func(t *testing.T) {
		s1 := stream.Range[uint64](1, 1)
		_, err := s1.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)

		s2 := stream.ReverseRange[uint64](1, 1)
		_, err = s2.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
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
	t.Run("empty page in the middle does not end the stream", func(t *testing.T) {
		i := 0
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return []uint64{1, 2}, "more", nil
			case 2:
				return nil, "more", nil
			case 3:
				return []uint64{3, 4}, "", nil
			}
			panic("must not happen")
		})
		res, err := stream.ToArray[uint64](s1)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4}, res)
	})
	t.Run("Next before HasNext returns ErrIteratorExhausted", func(t *testing.T) {
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			return []uint64{1}, "", nil
		})
		_, err := s1.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("repeated HasNext does not skip a page", func(t *testing.T) {
		pages := 0
		s1 := stream.Paginate[uint64](func(pageToken string) (arr []uint64, nextPageToken string, err error) {
			pages++
			if pages == 1 {
				return []uint64{1}, "more", nil
			}
			return []uint64{2}, "", nil
		})
		var res []uint64
		for s1.HasNext() {
			require.True(t, s1.HasNext()) // invariant 1: must not consume or skip a page
			v, err := s1.Next()
			require.NoError(t, err)
			res = append(res, v)
		}
		require.Equal(t, []uint64{1, 2}, res)
		require.Equal(t, 2, pages)
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
	t.Run("empty page in the middle does not end the stream", func(t *testing.T) {
		i := 0
		s1 := stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			i++
			switch i {
			case 1:
				return [][]byte{{1}}, [][]byte{{1}}, "more", nil
			case 2:
				return nil, nil, "more", nil
			case 3:
				return [][]byte{{2}}, [][]byte{{2}}, "", nil
			}
			panic("must not happen")
		})
		keys, _, err := stream.ToArrayKV(s1)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}}, keys)
	})
	t.Run("Next before HasNext returns ErrIteratorExhausted", func(t *testing.T) {
		s1 := stream.PaginateKV(func(pageToken string) (keys, values [][]byte, nextPageToken string, err error) {
			return [][]byte{{1}}, [][]byte{{1}}, "", nil
		})
		_, _, err := s1.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
}

func TestFilter(t *testing.T) {
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

// PairsWithErrorIter - return N, keys and then error
type PairsWithErrorIter struct {
	errorAt, i int
}

func PairsWithError(errorAt int) *PairsWithErrorIter {
	return &PairsWithErrorIter{errorAt: errorAt}
}
func (m *PairsWithErrorIter) Close()        {}
func (m *PairsWithErrorIter) HasNext() bool { return true }
func (m *PairsWithErrorIter) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return fmt.Appendf(nil, "%x", m.i), fmt.Appendf(nil, "%x", m.i), nil
}

func TestExhaustedCombinators(t *testing.T) {
	// invariant 4: Next() past the end must not hand back a stale value with a nil error
	t.Run("union uno", func(t *testing.T) {
		s := stream.Union[uint64](stream.Array([]uint64{1}), stream.Array([]uint64{2}), order.Asc, kv.Unlim)
		_, err := stream.ToArray[uint64](s)
		require.NoError(t, err)
		_, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("union duo", func(t *testing.T) {
		s := stream.Union2[uint64, uint64](&countingDuo{arr: []uint64{1}}, &countingDuo{arr: []uint64{2}}, order.Asc, kv.Unlim)
		_, _, err := stream.ToArrayDuo[uint64, uint64](s)
		require.NoError(t, err)
		_, _, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("union kv", func(t *testing.T) {
		s := stream.UnionKV(&countingKV{keys: [][]byte{{1}}}, &countingKV{keys: [][]byte{{2}}}, kv.Unlim)
		_, _, err := stream.ToArrayKV(s)
		require.NoError(t, err)
		_, _, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("multiset", func(t *testing.T) {
		s := stream.MultisetKV(&countingKV{keys: [][]byte{{1}}}, &countingKV{keys: [][]byte{{2}}}, kv.Unlim)
		_, _, err := stream.ToArrayKV(s)
		require.NoError(t, err)
		_, _, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("intersect", func(t *testing.T) {
		s := stream.Intersect[uint64](stream.Array([]uint64{1, 2}), stream.Array([]uint64{2}), order.Asc, kv.Unlim)
		res, err := stream.ToArray[uint64](s)
		require.NoError(t, err)
		require.Equal(t, []uint64{2}, res)
		_, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("limit", func(t *testing.T) {
		s := stream.Limit[uint64](stream.Array([]uint64{1, 2, 3}), 1)
		_, err := s.Next()
		require.NoError(t, err)
		require.False(t, s.HasNext())
		// past the cap: must not hand out the inner stream's next element
		_, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("limit does not spend budget on a failed Next", func(t *testing.T) {
		s := stream.LimitDuo[[]byte, []byte](PairsWithError(0), 2)
		_, _, err := s.Next()
		require.Error(t, err)
		require.True(t, s.HasNext()) // the error is terminal and repeatable, not swallowed by the cap
		_, _, err = s.Next()
		require.Error(t, err)
	})
}

func TestExhausted(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, err := (&stream.Empty[uint64]{}).Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)

		_, _, err = (&stream.EmptyDuo[[]byte, []byte]{}).Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)

		_, _, _, err = (&stream.EmptyTrio[[]byte, []byte, uint64]{}).Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("array", func(t *testing.T) {
		s := stream.Array[uint64](nil)
		_, err := s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
	t.Run("single duo", func(t *testing.T) {
		s := stream.NewSingleDuo[uint64, uint64](1, 2)
		k, v, err := s.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1), k)
		require.Equal(t, uint64(2), v)

		_, _, err = s.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
	})
}

func TestTraceUsesProvidedLogger(t *testing.T) {
	t.Run("uno", func(t *testing.T) {
		h := &recordingHandler{}
		l := log.New()
		l.SetHandler(h)
		s := stream.Trace[uint64](stream.Array([]uint64{1}), l, "pfx")
		require.True(t, s.HasNext())
		_, err := s.Next()
		require.NoError(t, err)
		require.NotEmpty(t, h.msgs)
	})
	t.Run("duo", func(t *testing.T) {
		h := &recordingHandler{}
		l := log.New()
		l.SetHandler(h)
		s := stream.TraceDuo[[]byte, []byte](stream.EmptyKV, l, "pfx")
		require.False(t, s.HasNext())
		require.NotEmpty(t, h.msgs)
	})
	t.Run("nil logger does not panic", func(t *testing.T) {
		s := stream.Trace[uint64](stream.Array([]uint64{1}), nil, "pfx")
		require.True(t, s.HasNext())
	})
}

type recordingHandler struct{ msgs []string }

func (h *recordingHandler) Log(r *log.Record) error               { h.msgs = append(h.msgs, r.Msg); return nil }
func (h *recordingHandler) Enabled(context.Context, log.Lvl) bool { return true }

// countingU64 counts Close calls, to pin that combinators release the streams they discard.
type countingU64 struct {
	arr    []uint64
	i      int
	closed int
}

func (s *countingU64) HasNext() bool { return s.i < len(s.arr) }
func (s *countingU64) Close()        { s.closed++ }
func (s *countingU64) Next() (uint64, error) {
	if s.i >= len(s.arr) {
		return 0, stream.ErrIteratorExhausted
	}
	v := s.arr[s.i]
	s.i++
	return v, nil
}

type countingDuo struct {
	arr    []uint64
	i      int
	closed int
}

func (s *countingDuo) HasNext() bool { return s.i < len(s.arr) }
func (s *countingDuo) Close()        { s.closed++ }
func (s *countingDuo) Next() (uint64, uint64, error) {
	if s.i >= len(s.arr) {
		return 0, 0, stream.ErrIteratorExhausted
	}
	v := s.arr[s.i]
	s.i++
	return v, v, nil
}

type countingKV struct {
	keys   [][]byte
	i      int
	closed int
}

func (s *countingKV) HasNext() bool { return s.i < len(s.keys) }
func (s *countingKV) Close()        { s.closed++ }
func (s *countingKV) Next() ([]byte, []byte, error) {
	if s.i >= len(s.keys) {
		return nil, nil, stream.ErrIteratorExhausted
	}
	k := s.keys[s.i]
	s.i++
	return k, k, nil
}

func TestTransform(t *testing.T) {
	t.Run("duo", func(t *testing.T) {
		s := stream.TransformKV(&countingKV{keys: [][]byte{{1}, {2}}}, func(k, v []byte) ([]byte, []byte, error) {
			return append([]byte{9}, k...), v, nil
		})
		keys, values, err := stream.ToArrayKV(s)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{9, 1}, {9, 2}}, keys)
		require.Equal(t, [][]byte{{1}, {2}}, values)
	})
	t.Run("duo propagates the transform error", func(t *testing.T) {
		testErr := errors.New("test")
		s := stream.TransformKV(&countingKV{keys: [][]byte{{1}}}, func(k, v []byte) ([]byte, []byte, error) {
			return nil, nil, testErr
		})
		_, _, err := stream.ToArrayKV(s)
		require.ErrorIs(t, err, testErr)
	})
	t.Run("duo propagates the source error", func(t *testing.T) {
		s := stream.TransformKV(PairsWithError(1), func(k, v []byte) ([]byte, []byte, error) { return k, v, nil })
		_, _, err := stream.ToArrayKV(s)
		require.Error(t, err)
	})
	t.Run("changes the value type", func(t *testing.T) {
		s := stream.TransformDuoV[[]byte, []byte, uint64](&countingKV{keys: [][]byte{{1}, {2}}},
			func(k, v []byte) ([]byte, uint64, error) { return k, uint64(v[0]) * 10, nil })
		keys, values, err := stream.ToArrayDuo[[]byte, uint64](s)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}}, keys)
		require.Equal(t, []uint64{10, 20}, values)
	})
	t.Run("kv to u64", func(t *testing.T) {
		s := stream.TransformKV2U64(&countingKV{keys: [][]byte{{1}, {2}}},
			func(k, v []byte) (uint64, error) { return uint64(k[0]), nil })
		res, err := stream.ToArrayU64(s)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)
	})
	t.Run("kv to u64 propagates the source error", func(t *testing.T) {
		s := stream.TransformKV2U64(PairsWithError(0), func(k, v []byte) (uint64, error) { return 0, nil })
		_, err := stream.ToArrayU64(s)
		require.Error(t, err)
	})
}

func TestCount(t *testing.T) {
	t.Run("counts", func(t *testing.T) {
		n, err := stream.CountU64(stream.Array([]uint64{1, 2, 3}))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		n, err = stream.CountKV(&countingKV{keys: [][]byte{{1}, {2}}})
		require.NoError(t, err)
		require.Equal(t, 2, n)

		n, err = stream.CountU64(stream.EmptyU64)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("returns the count reached before the error", func(t *testing.T) {
		n, err := stream.CountKV(PairsWithError(3))
		require.Error(t, err)
		require.Equal(t, 3, n)
	})
}

func TestToArrMust(t *testing.T) {
	t.Run("returns", func(t *testing.T) {
		require.Equal(t, []uint64{1, 2}, stream.ToArrU64Must(stream.Array([]uint64{1, 2})))
		keys, values := stream.ToArrKVMust(&countingKV{keys: [][]byte{{1}}})
		require.Equal(t, [][]byte{{1}}, keys)
		require.Equal(t, [][]byte{{1}}, values)
	})
	t.Run("panics on error", func(t *testing.T) {
		require.Panics(t, func() { stream.ToArrKVMust(PairsWithError(0)) })
	})
}

func TestLimit(t *testing.T) {
	t.Run("caps", func(t *testing.T) {
		res, err := stream.ToArray[uint64](stream.Limit[uint64](stream.Array([]uint64{1, 2, 3}), 2))
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)
	})
	t.Run("zero yields nothing", func(t *testing.T) {
		res, err := stream.ToArray[uint64](stream.Limit[uint64](stream.Array([]uint64{1, 2, 3}), 0))
		require.NoError(t, err)
		require.Empty(t, res)
	})
	t.Run("unlimited returns the stream unwrapped", func(t *testing.T) {
		in := stream.Array([]uint64{1, 2, 3})
		require.Same(t, in, stream.Limit[uint64](in, kv.Unlim))
	})
	t.Run("duo caps and forwards Close", func(t *testing.T) {
		in := &countingKV{keys: [][]byte{{1}, {2}, {3}}}
		s := stream.LimitDuo[[]byte, []byte](in, 1)
		keys, _, err := stream.ToArrayKV(s)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}}, keys)

		s.Close()
		require.Equal(t, 1, in.closed)
	})
}

func TestMultisetKU64(t *testing.T) {
	t.Run("preserves duplicates and merges sorted", func(t *testing.T) {
		x := &countingKU64{keys: [][]byte{{1}, {3}}}
		y := &countingKU64{keys: [][]byte{{2}, {3}}}
		keys, _, err := stream.ToArrayDuo[[]byte, uint64](stream.MultisetKU64(x, y, -1))
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}, {3}, {3}}, keys)
	})
	t.Run("limit", func(t *testing.T) {
		x := &countingKU64{keys: [][]byte{{1}, {3}}}
		y := &countingKU64{keys: [][]byte{{2}}}
		keys, _, err := stream.ToArrayDuo[[]byte, uint64](stream.MultisetKU64(x, y, 2))
		require.NoError(t, err)
		require.Equal(t, [][]byte{{1}, {2}}, keys)
	})
}

func TestFilterError(t *testing.T) {
	t.Run("duo propagates the source error", func(t *testing.T) {
		_, _, err := stream.ToArrayKV(stream.FilterKV(PairsWithError(2), func(k, v []byte) bool { return true }))
		require.Error(t, err)
	})
	t.Run("uno propagates the source error", func(t *testing.T) {
		_, err := stream.ToArrayU64(stream.FilterU64(&erroringU64{}, func(uint64) bool { return true }))
		require.Error(t, err)
	})
	t.Run("filtered-out elements still surface a later error", func(t *testing.T) {
		_, _, err := stream.ToArrayKV(stream.FilterKV(PairsWithError(2), func(k, v []byte) bool { return false }))
		require.Error(t, err)
	})
}

func TestIntersectError(t *testing.T) {
	t.Run("propagates the source error", func(t *testing.T) {
		s := stream.Intersect[uint64](&erroringU64{arr: []uint64{1, 2}}, stream.Array([]uint64{9}), order.Asc, -1)
		_, err := stream.ToArray[uint64](s)
		require.Error(t, err)
	})
	t.Run("desc with limit", func(t *testing.T) {
		x := stream.Array([]uint64{7, 6, 5, 3, 1})
		y := stream.Array([]uint64{7, 5, 3})
		res, err := stream.ToArray[uint64](stream.Intersect[uint64](x, y, order.Desc, 2))
		require.NoError(t, err)
		require.Equal(t, []uint64{7, 5}, res)
	})
}

func TestArrStream(t *testing.T) {
	t.Run("next batch drains the rest", func(t *testing.T) {
		s := stream.Array([]uint64{1, 2, 3})
		v, err := s.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1), v)

		batch, err := s.NextBatch()
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 3}, batch)
		require.False(t, s.HasNext())
	})
	t.Run("reverse does not mutate the caller's slice", func(t *testing.T) {
		in := []uint64{1, 2, 3}
		res, err := stream.ToArray[uint64](stream.ReverseArray(in))
		require.NoError(t, err)
		require.Equal(t, []uint64{3, 2, 1}, res)
		require.Equal(t, []uint64{1, 2, 3}, in)
	})
}

// erroringU64 yields arr, then fails - the Uno counterpart of PairsWithErrorIter.
type erroringU64 struct {
	arr []uint64
	i   int
}

func (s *erroringU64) HasNext() bool { return true }
func (s *erroringU64) Close()        {}
func (s *erroringU64) Next() (uint64, error) {
	if s.i >= len(s.arr) {
		return 0, errors.New("expected error")
	}
	v := s.arr[s.i]
	s.i++
	return v, nil
}

type countingKU64 struct {
	keys   [][]byte
	i      int
	closed int
}

func (s *countingKU64) HasNext() bool { return s.i < len(s.keys) }
func (s *countingKU64) Close()        { s.closed++ }
func (s *countingKU64) Next() ([]byte, uint64, error) {
	if s.i >= len(s.keys) {
		return nil, 0, stream.ErrIteratorExhausted
	}
	k := s.keys[s.i]
	s.i++
	return k, uint64(k[0]), nil
}

func benchU64(n int, step uint64) []uint64 {
	arr := make([]uint64, n)
	for i := range arr {
		arr[i] = uint64(i) * step
	}
	return arr
}

// benchKeys - `step` controls overlap between the two sides: coprime steps give the
// mostly-disjoint merge that history/domain ranges actually perform, rather than two
// identical key sets, which would send every element down the equal-key branch.
func benchKeys(n int, step uint64) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint64(nil, uint64(i)*step)
	}
	return keys
}

func BenchmarkUnionUno(b *testing.B) {
	x, y := benchU64(2048, 2), benchU64(2048, 3)
	b.ReportAllocs()
	for b.Loop() {
		s := stream.Union[uint64](stream.Array(x), stream.Array(y), order.Asc, kv.Unlim)
		for s.HasNext() {
			if _, err := s.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkUnionKV(b *testing.B) {
	x, y := benchKeys(2048, 2), benchKeys(2048, 3)
	b.ReportAllocs()
	for b.Loop() {
		s := stream.UnionKV(&countingKV{keys: x}, &countingKV{keys: y}, kv.Unlim)
		for s.HasNext() {
			if _, _, err := s.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMultisetKV(b *testing.B) {
	x, y := benchKeys(2048, 2), benchKeys(2048, 3)
	b.ReportAllocs()
	for b.Loop() {
		s := stream.MultisetKV(&countingKV{keys: x}, &countingKV{keys: y}, kv.Unlim)
		for s.HasNext() {
			if _, _, err := s.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
