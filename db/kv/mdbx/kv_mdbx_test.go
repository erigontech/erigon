// Copyright 2022 The Erigon Authors
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

package mdbx

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func BaseCaseDB(t *testing.T) kv.RwDB {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	table := "Table"
	db := New(kv.ChainDB, logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table:       kv.TableCfgItem{Flags: kv.DupSort},
			kv.Sequence: kv.TableCfgItem{},
		}
	}).MapSize(128 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)
	return db
}

func BaseCaseDBForBenchmark(b *testing.B) kv.RwDB {
	b.Helper()
	path := b.TempDir()
	logger := log.New()
	table := "Table"
	db := New(kv.ChainDB, logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table:       kv.TableCfgItem{Flags: kv.DupSort},
			kv.Sequence: kv.TableCfgItem{},
		}
	}).MapSize(128 * datasize.MB).MustOpen()
	b.Cleanup(db.Close)
	return db
}

func BaseCase(t *testing.T) (kv.RwDB, kv.RwTx, kv.RwCursorDupSort) {
	t.Helper()
	db := BaseCaseDB(t)
	table := "Table"

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.3")))

	return db, tx, c
}

func iteration(t *testing.T, c kv.RwCursorDupSort, start []byte, val []byte) ([]string, []string) {
	t.Helper()
	var keys []string
	var values []string
	var err error
	i := 0
	for k, v, err := start, val, err; k != nil; k, v, err = c.Next() {
		require.NoError(t, err)
		keys = append(keys, string(k))
		values = append(values, string(v))
		i += 1
	}
	for ind := i; ind > 1; ind-- {
		c.Prev()
	}

	return keys, values
}

func TestSeekBothRange(t *testing.T) {
	_, _, c := BaseCase(t)

	v, err := c.SeekBothRange([]byte("key2"), []byte("value1.2"))
	require.NoError(t, err)
	// SeekBothRange does exact match of the key, but range match of the value, so we get nil here
	require.Nil(t, v)

	v, err = c.SeekBothRange([]byte("key3"), []byte("value3.2"))
	require.NoError(t, err)
	require.Equal(t, "value3.3", string(v))
}

func TestRange(t *testing.T) {
	t.Run("Asc", func(t *testing.T) {
		_, tx, _ := BaseCase(t)

		//[from, to)
		it, err := tx.Range("Table", []byte("key1"), []byte("key3"), order.Asc, kv.Unlim)
		require.NoError(t, err)
		require.True(t, it.HasNext())
		k, v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.1", string(v))

		require.True(t, it.HasNext())
		k, v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.3", string(v))

		require.False(t, it.HasNext())
		require.False(t, it.HasNext())

		// [from, nil) means [from, INF)
		it, err = tx.Range("Table", []byte("key1"), nil, order.Asc, kv.Unlim)
		require.NoError(t, err)
		cnt := 0
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(t, err)
			cnt++
		}
		require.Equal(t, 4, cnt)
	})
	t.Run("Desc", func(t *testing.T) {
		_, tx, _ := BaseCase(t)

		//[from, to)
		it, err := tx.Range("Table", []byte("key3"), []byte("key1"), order.Desc, kv.Unlim)
		require.NoError(t, err)
		require.True(t, it.HasNext())
		k, v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, "key3", string(k))
		require.Equal(t, "value3.3", string(v))

		require.True(t, it.HasNext())
		k, v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, "key3", string(k))
		require.Equal(t, "value3.1", string(v))

		require.False(t, it.HasNext())

		it, err = tx.Range("Table", nil, nil, order.Desc, 2)
		require.NoError(t, err)

		cnt := 0
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(t, err)
			cnt++
		}
		require.Equal(t, 2, cnt)
	})
}

func TestRangeDupSort(t *testing.T) {
	t.Run("Asc", func(t *testing.T) {
		_, tx, _ := BaseCase(t)

		//[from, to)
		it, err := tx.RangeDupSort("Table", []byte("key1"), nil, nil, order.Asc, -1)
		require.NoError(t, err)
		defer it.Close()
		require.True(t, it.HasNext())
		k, v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.1", string(v))

		require.True(t, it.HasNext())
		k, v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.3", string(v))

		require.False(t, it.HasNext())
		require.False(t, it.HasNext())

		// [from, nil) means [from, INF)
		it, err = tx.RangeDupSort("Table", []byte("key1"), []byte("value1"), nil, order.Asc, -1)
		require.NoError(t, err)
		_, vals, err := stream.ToArrayKV(it)
		require.NoError(t, err)
		require.Len(t, vals, 2)

		it, err = tx.RangeDupSort("Table", []byte("key1"), []byte("value1"), []byte("value1.3"), order.Asc, -1)
		require.NoError(t, err)
		_, vals, err = stream.ToArrayKV(it)
		require.NoError(t, err)
		require.Len(t, vals, 1)
	})
	t.Run("Desc", func(t *testing.T) {
		_, tx, _ := BaseCase(t)

		//[from, to)
		it, err := tx.RangeDupSort("Table", []byte("key1"), nil, nil, order.Desc, -1)
		require.NoError(t, err)
		require.True(t, it.HasNext())
		k, v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.3", string(v))

		require.True(t, it.HasNext())
		k, v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, "key1", string(k))
		require.Equal(t, "value1.1", string(v))

		require.False(t, it.HasNext())

		it, err = tx.RangeDupSort("Table", []byte("key1"), []byte("value1"), []byte("value0"), order.Desc, -1)
		require.NoError(t, err)
		_, vals, err := stream.ToArrayKV(it)
		require.NoError(t, err)
		require.Len(t, vals, 2)

		it, err = tx.RangeDupSort("Table", []byte("key1"), []byte("value1.3"), []byte("value1.1"), order.Desc, -1)
		require.NoError(t, err)
		_, vals, err = stream.ToArrayKV(it)
		require.NoError(t, err)
		require.Len(t, vals, 1)
	})
}

func TestLastDup(t *testing.T) {
	db, tx, _ := BaseCase(t)

	err := tx.Commit()
	require.NoError(t, err)
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	roC, err := roTx.CursorDupSort("Table")
	require.NoError(t, err)
	defer roC.Close()

	var keys, vals []string
	var k, v []byte
	for k, _, err = roC.First(); err == nil && k != nil; k, _, err = roC.NextNoDup() {
		v, err = roC.LastDup()
		require.NoError(t, err)
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	}
	require.NoError(t, err)
	require.Equal(t, []string{"key1", "key3"}, keys)
	require.Equal(t, []string{"value1.3", "value3.3"}, vals)
}

func TestPutGet(t *testing.T) {
	_, tx, c := BaseCase(t)

	require.NoError(t, c.Put([]byte(""), []byte("value1.1")))

	var v []byte
	v, err := tx.GetOne("Table", []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, v, []byte("value1.1"))

	v, err = tx.GetOne("RANDOM", []byte("key1"))
	require.Error(t, err) // Error from non-existent bucket returns error
	require.Nil(t, v)
}

func TestIncrementRead(t *testing.T) {
	_, tx, _ := BaseCase(t)

	table := "Table"

	_, err := tx.IncrementSequence(table, uint64(12))
	require.NoError(t, err)
	chaV, err := tx.ReadSequence(table)
	require.NoError(t, err)
	require.Equal(t, uint64(12), chaV)
	_, err = tx.IncrementSequence(table, uint64(240))
	require.NoError(t, err)
	chaV, err = tx.ReadSequence(table)
	require.NoError(t, err)
	require.Equal(t, uint64(252), chaV)
}

func TestHasDelete(t *testing.T) {
	_, tx, _ := BaseCase(t)

	table := "Table"

	require.NoError(t, tx.Put(table, []byte("key2"), []byte("value2.1")))
	require.NoError(t, tx.Put(table, []byte("key4"), []byte("value4.1")))
	require.NoError(t, tx.Put(table, []byte("key5"), []byte("value5.1")))

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1"))) //valid but already deleted
	require.NoError(t, c.DeleteExact([]byte("key2"), []byte("value1.1"))) //valid key but wrong value

	res, err := tx.Has(table, []byte("key1"))
	require.NoError(t, err)
	require.False(t, res)

	res, err = tx.Has(table, []byte("key2"))
	require.NoError(t, err)
	require.True(t, res)

	res, err = tx.Has(table, []byte("key3"))
	require.NoError(t, err)
	require.True(t, res) //There is another key3 left

	res, err = tx.Has(table, []byte("k"))
	require.NoError(t, err)
	require.False(t, res)
}

func TestForAmount(t *testing.T) {
	_, tx, _ := BaseCase(t)

	table := "Table"

	require.NoError(t, tx.Put(table, []byte("key2"), []byte("value2.1")))
	require.NoError(t, tx.Put(table, []byte("key4"), []byte("value4.1")))
	require.NoError(t, tx.Put(table, []byte("key5"), []byte("value5.1")))

	var keys []string

	err := tx.ForAmount(table, []byte("key3"), uint32(2), func(k, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"key3", "key3"}, keys)

	var keys1 []string

	err1 := tx.ForAmount(table, []byte("key1"), 100, func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		return nil
	})
	require.NoError(t, err1)
	require.Equal(t, []string{"key1", "key1", "key2", "key3", "key3", "key4", "key5"}, keys1)

	var keys2 []string

	err2 := tx.ForAmount(table, []byte("value"), 100, func(k, v []byte) error {
		keys2 = append(keys2, string(k))
		return nil
	})
	require.NoError(t, err2)
	require.Nil(t, keys2)

	var keys3 []string

	err3 := tx.ForAmount(table, []byte("key1"), 0, func(k, v []byte) error {
		keys3 = append(keys3, string(k))
		return nil
	})
	require.NoError(t, err3)
	require.Nil(t, keys3)
}

func TestPrefix(t *testing.T) {
	_, tx, _ := BaseCase(t)

	table := "Table"
	var keys, keys1, keys2 []string
	kvs1, err := tx.Prefix(table, []byte("key"))
	require.NoError(t, err)
	defer kvs1.Close()
	for kvs1.HasNext() {
		k1, _, err := kvs1.Next()
		require.NoError(t, err)
		keys = append(keys, string(k1))
	}
	require.Equal(t, []string{"key1", "key1", "key3", "key3"}, keys)

	kvs2, err := tx.Prefix(table, []byte("key1"))
	require.NoError(t, err)
	defer kvs2.Close()
	for kvs2.HasNext() {
		k1, _, err := kvs2.Next()
		require.NoError(t, err)
		keys1 = append(keys1, string(k1))
	}
	require.Equal(t, []string{"key1", "key1"}, keys1)

	kvs3, err := tx.Prefix(table, []byte("e"))
	require.NoError(t, err)
	defer kvs3.Close()
	for kvs3.HasNext() {
		k1, _, err := kvs3.Next()
		require.NoError(t, err)
		keys2 = append(keys2, string(k1))
	}
	require.Nil(t, keys2)
}

func TestAppendFirstLast(t *testing.T) {
	_, tx, c := BaseCase(t)

	table := "Table"

	require.Error(t, tx.Append(table, []byte("key2"), []byte("value2.1")))
	require.NoError(t, tx.Append(table, []byte("key6"), []byte("value6.1")))
	require.Error(t, tx.Append(table, []byte("key4"), []byte("value4.1")))
	require.NoError(t, tx.AppendDup(table, []byte("key2"), []byte("value1.11")))

	k, v, err := c.First()
	require.NoError(t, err)
	require.Equal(t, k, []byte("key1"))
	require.Equal(t, v, []byte("value1.1"))

	keys, values := iteration(t, c, k, v)
	require.Equal(t, []string{"key1", "key1", "key2", "key3", "key3", "key6"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3", "value1.11", "value3.1", "value3.3", "value6.1"}, values)

	k, v, err = c.Last()
	require.NoError(t, err)
	require.Equal(t, []byte("key6"), k)
	require.Equal(t, []byte("value6.1"), v)

	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key6"}, keys)
	require.Equal(t, []string{"value6.1"}, values)
}

func TestSeek(t *testing.T) {
	_, _, c := BaseCase(t)

	k, v, err := c.Seek([]byte("k"))
	require.NoError(t, err)
	keys, values := iteration(t, c, k, v)
	require.Equal(t, []string{"key1", "key1", "key3", "key3"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3", "value3.1", "value3.3"}, values)

	k, v, err = c.Seek([]byte("key3"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key3", "key3"}, keys)
	require.Equal(t, []string{"value3.1", "value3.3"}, values)

	k, v, err = c.Seek([]byte("xyz"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Nil(t, keys)
	require.Nil(t, values)
}

func TestSeekExact(t *testing.T) {
	_, _, c := BaseCase(t)

	k, v, err := c.SeekExact([]byte("key3"))
	require.NoError(t, err)
	keys, values := iteration(t, c, k, v)
	require.Equal(t, []string{"key3", "key3"}, keys)
	require.Equal(t, []string{"value3.1", "value3.3"}, values)

	k, v, err = c.SeekExact([]byte("key"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Nil(t, keys)
	require.Nil(t, values)
}

func TestSeekBothExact(t *testing.T) {
	_, _, c := BaseCase(t)

	k, v, err := c.SeekBothExact([]byte("key1"), []byte("value1.2"))
	require.NoError(t, err)
	keys, values := iteration(t, c, k, v)
	require.Nil(t, keys)
	require.Nil(t, values)

	k, v, err = c.SeekBothExact([]byte("key2"), []byte("value1.1"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Nil(t, keys)
	require.Nil(t, values)

	k, v, err = c.SeekBothExact([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key1", "key1", "key3", "key3"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3", "value3.1", "value3.3"}, values)

	k, v, err = c.SeekBothExact([]byte("key3"), []byte("value3.3"))
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key3"}, keys)
	require.Equal(t, []string{"value3.3"}, values)
}

func TestNextDups(t *testing.T) {
	_, tx, _ := BaseCase(t)

	table := "Table"

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.DeleteExact([]byte("key3"), []byte("value3.1"))) //valid but already deleted
	require.NoError(t, c.DeleteExact([]byte("key3"), []byte("value3.3"))) //valid key but wrong value

	require.NoError(t, tx.Put(table, []byte("key2"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key2"), []byte("value1.2")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value1.6")))
	require.NoError(t, c.Put([]byte("key"), []byte("value1.7")))

	k, v, err := c.Current()
	require.NoError(t, err)
	keys, values := iteration(t, c, k, v)
	require.Equal(t, []string{"key", "key2", "key2", "key3"}, keys)
	require.Equal(t, []string{"value1.7", "value1.1", "value1.2", "value1.6"}, values)

	v, err = c.FirstDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key", "key2", "key2", "key3"}, keys)
	require.Equal(t, []string{"value1.7", "value1.1", "value1.2", "value1.6"}, values)

	k, v, err = c.NextNoDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key2", "key2", "key3"}, keys)
	require.Equal(t, []string{"value1.1", "value1.2", "value1.6"}, values)

	k, v, err = c.NextDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key2", "key3"}, keys)
	require.Equal(t, []string{"value1.2", "value1.6"}, values)

	v, err = c.LastDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key2", "key3"}, keys)
	require.Equal(t, []string{"value1.2", "value1.6"}, values)

	k, v, err = c.NextDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Nil(t, keys)
	require.Nil(t, values)

	k, v, err = c.NextNoDup()
	require.NoError(t, err)
	keys, values = iteration(t, c, k, v)
	require.Equal(t, []string{"key3"}, keys)
	require.Equal(t, []string{"value1.6"}, values)
}

func TestCurrentDup(t *testing.T) {
	_, _, c := BaseCase(t)

	count, err := c.CountDuplicates()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)

	require.Error(t, c.PutNoDupData([]byte("key3"), []byte("value3.3")))
	require.NoError(t, c.DeleteCurrentDuplicates())

	k, v, err := c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	keys, values := iteration(t, c, k, v)
	require.Equal(t, []string{"key1", "key1"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3"}, values)

	require.Equal(t, []string{"key1", "key1"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3"}, values)
}

func TestDupDelete(t *testing.T) {
	_, tx, c := BaseCase(t)

	k, _, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)

	err = c.DeleteCurrentDuplicates()
	require.NoError(t, err)

	err = c.Delete([]byte("key1"))
	require.NoError(t, err)

	//TODO: find better way
	count, err := tx.Count("Table")
	require.NoError(t, err)
	assert.Zero(t, count)
}

func TestBeginRoAfterClose(t *testing.T) {
	db := New(kv.ChainDB, log.New()).InMem(t.TempDir()).MustOpen()
	db.Close()
	_, err := db.BeginRo(context.Background())
	require.ErrorContains(t, err, "closed")
}

func TestBeginRwAfterClose(t *testing.T) {
	db := New(kv.ChainDB, log.New()).InMem(t.TempDir()).MustOpen()
	db.Close()
	_, err := db.BeginRw(context.Background())
	require.ErrorContains(t, err, "closed")
}

func TestBeginRoWithDoneContext(t *testing.T) {
	db := New(kv.ChainDB, log.New()).InMem(t.TempDir()).MustOpen()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.BeginRo(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBeginRwWithDoneContext(t *testing.T) {
	db := New(kv.ChainDB, log.New()).InMem(t.TempDir()).MustOpen()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.BeginRw(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func testCloseWaitsAfterTxBegin(
	t *testing.T,
	count int,
	txBeginFunc func(kv.RwDB) (kv.Getter, error),
	txEndFunc func(kv.Getter) error,
) {
	t.Helper()
	db := New(kv.ChainDB, log.New()).InMem(t.TempDir()).MustOpen()
	var txs []kv.Getter
	for i := 0; i < count; i++ {
		tx, err := txBeginFunc(db)
		require.NoError(t, err)
		txs = append(txs, tx)
	}

	isClosed := &atomic.Bool{}
	closeDone := make(chan struct{})

	go func() {
		db.Close()
		isClosed.Store(true)
		close(closeDone)
	}()

	for _, tx := range txs {
		// arbitrary delay to give db.Close() a chance to exit prematurely
		time.Sleep(time.Millisecond * 20)
		assert.False(t, isClosed.Load())

		err := txEndFunc(tx)
		require.NoError(t, err)
	}

	<-closeDone
	assert.True(t, isClosed.Load())
}

func TestCloseWaitsAfterTxBegin(t *testing.T) {
	ctx := context.Background()
	t.Run("BeginRoAndCommit", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRo(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRoAndCommit3", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			3,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRo(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRoAndRollback", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRo(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRoAndRollback3", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			3,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRo(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRwAndCommit", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRw(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRwAndRollback", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.Getter, error) { return db.BeginRw(ctx) },
			func(tx kv.Getter) error { tx.Rollback(); return nil },
		)
	})
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Ensure two functions can perform updates in a single batch.
func TestDB_Batch(t *testing.T) {
	_db := BaseCaseDB(t)
	table := "Table"
	db := _db.(*MdbxKV)

	// Iterate over multiple updates in separate goroutines.
	n := 2
	ch := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ch <- db.Batch(func(tx kv.RwTx) error {
				return tx.Put(table, u64tob(uint64(i)), []byte{})
			})
		}(i)
	}

	// Check all responses to make sure there's no error.
	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	// Ensure data is correct.
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		for i := 0; i < n; i++ {
			v, err := tx.GetOne(table, u64tob(uint64(i)))
			if err != nil {
				panic(err)
			}
			if v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Batch_Panic(t *testing.T) {
	_db := BaseCaseDB(t)
	db := _db.(*MdbxKV)

	var sentinel int
	var bork = &sentinel
	var problem interface{}
	var err error

	// Execute a function inside a batch that panics.
	func() {
		defer func() {
			if p := recover(); p != nil {
				problem = p
			}
		}()
		err = db.Batch(func(tx kv.RwTx) error {
			panic(bork)
		})
	}()

	// Verify there is no error.
	if g, e := err, error(nil); !errors.Is(g, e) {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
	// Verify the panic was captured.
	if g, e := problem, bork; g != e {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
}

func TestDB_BatchFull(t *testing.T) {
	_db := BaseCaseDB(t)
	table := "Table"
	db := _db.(*MdbxKV)

	const size = 3
	// buffered so we never leak goroutines
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx kv.RwTx) error {
			return tx.Put(table, u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = size
	// high enough to never trigger here
	db.MaxBatchDelay = 1 * time.Hour

	go put(1)
	go put(2)

	// Give the batch a chance to exhibit bugs.
	time.Sleep(10 * time.Millisecond)

	// not triggered yet
	select {
	case <-ch:
		t.Fatalf("batch triggered too early")
	default:
	}

	go put(3)

	// Check all responses to make sure there's no error.
	for i := 0; i < size; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	// Ensure data is correct.
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		for i := 1; i <= size; i++ {
			v, err := tx.GetOne(table, u64tob(uint64(i)))
			if err != nil {
				panic(err)
			}
			if v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_BatchTime(t *testing.T) {
	_db := BaseCaseDB(t)
	table := "Table"
	db := _db.(*MdbxKV)

	const size = 1
	// buffered so we never leak goroutines
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx kv.RwTx) error {
			return tx.Put(table, u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = 1000
	db.MaxBatchDelay = 0

	go put(1)

	// Batch must trigger by time alone.

	// Check all responses to make sure there's no error.
	for i := 0; i < size; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	// Ensure data is correct.
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		for i := 1; i <= size; i++ {
			v, err := tx.GetOne(table, u64tob(uint64(i)))
			if err != nil {
				return err
			}
			if v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*MdbxKV)

	// buffered so we never leak goroutines
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(table, u64tob(uint64(1)), u64tob(uint64(1)))
	})
	if err != nil {
		b.Fatal(err)
	}

	// Ensure data is correct.
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		key := u64tob(uint64(1))
		b.ResetTimer()
		for i := 1; i <= b.N; i++ {
			v, err := tx.GetOne(table, key)
			if err != nil {
				return err
			}
			if v == nil {
				b.Errorf("key not found: %d", 1)
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_Put(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*MdbxKV)

	// Ensure data is correct.
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		keys := make([][]byte, b.N)
		for i := 1; i <= b.N; i++ {
			keys[i-1] = u64tob(uint64(i))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := tx.Put(table, keys[i], keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_PutRandom(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*MdbxKV)

	// Ensure data is correct.
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		keys := make(map[string]struct{}, b.N)
		for len(keys) < b.N {
			keys[string(u64tob(uint64(rand.Intn(1e10))))] = struct{}{}
		}
		b.ResetTimer()
		for key := range keys {
			err := tx.Put(table, []byte(key), []byte(key))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*MdbxKV)

	keys := make([][]byte, b.N)
	for i := 1; i <= b.N; i++ {
		keys[i-1] = u64tob(uint64(i))
	}

	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		for i := 0; i < b.N; i++ {
			err := tx.Put(table, keys[i], keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	// Ensure data is correct.
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := tx.Delete(table, keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func TestSequenceOps(t *testing.T) {
	table1 := []byte("Table132323")
	table2 := []byte("Table232232")
	t.Run("empty read", func(t *testing.T) {
		_, tx, _ := BaseCase(t)
		_, err := tx.ReadSequence(string(table1))
		require.NoError(t, err)
	})

	t.Run("putting things and reading back", func(t *testing.T) {
		_, tx, _ := BaseCase(t)
		t1, err := tx.IncrementSequence(string(table1), 5)
		require.NoError(t, err)
		require.Equal(t, uint64(0), t1)

		t2, err := tx.IncrementSequence(string(table2), 10)
		require.NoError(t, err)
		require.Equal(t, uint64(0), t2)

		t1, err = tx.ReadSequence(string(table1))
		require.NoError(t, err)
		require.Equal(t, uint64(5), t1)

		t2, err = tx.ReadSequence(string(table2))
		require.NoError(t, err)
		require.Equal(t, uint64(10), t2)
	})

	t.Run("reset sequence", func(t *testing.T) {
		_, tx, _ := BaseCase(t)
		t1, err := tx.ReadSequence(string(table1))
		require.NoError(t, err)
		require.Equal(t, uint64(0), t1)

		// increment sequence and then reset and test value
		// start
		t1, err = tx.IncrementSequence(string(table1), 5)
		require.NoError(t, err)
		require.Equal(t, uint64(0), t1)

		err = tx.ResetSequence(string(table1), 3)
		require.NoError(t, err)

		t1, err = tx.ReadSequence(string(table1))
		require.NoError(t, err)
		require.Equal(t, uint64(3), t1)
	})
}

func BenchmarkDB_ResetSequence(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	//db := _db.(*MdbxKV)
	ctx := context.Background()

	tx, err := _db.BeginRw(ctx)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = tx.ResetSequence(table, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
	}
	tx.Rollback()
}

func TestMdbxWithSyncBytes(t *testing.T) {
	db, err := New(kv.TemporaryDB, log.Root()).
		Path(t.TempDir()).
		MapSize(8 * datasize.GB).
		GrowthStep(16 * datasize.MB).
		Flags(func(f uint) uint { return f&^mdbxgo.Durable | mdbxgo.SafeNoSync }).
		SyncPeriod(2 * time.Second).
		SyncBytes(20_000).
		DirtySpace(uint64(64 * datasize.MB)).
		// WithMetrics().
		Open(context.Background())
	if err != nil {
		t.Fatalf("failed to open mdbx")
	}
	t.Cleanup(db.Close)
}
