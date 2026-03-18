// Copyright 2024 The Erigon Authors
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

package membatchwithdb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// --- memStore unit tests (behavior parity with MDBX) ---

func TestMemStore_PutGetDelete(t *testing.T) {
	s := newMemStore()

	// Empty table returns nil
	v, err := s.GetOne("test", []byte("key1"))
	require.NoError(t, err)
	require.Nil(t, v)

	has, err := s.Has("test", []byte("key1"))
	require.NoError(t, err)
	require.False(t, has)

	// Put and get
	require.NoError(t, s.Put("test", []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put("test", []byte("key2"), []byte("val2")))
	require.NoError(t, s.Put("test", []byte("key3"), []byte("val3")))

	v, err = s.GetOne("test", []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), v)

	has, err = s.Has("test", []byte("key2"))
	require.NoError(t, err)
	require.True(t, has)

	// Overwrite
	require.NoError(t, s.Put("test", []byte("key1"), []byte("val1-updated")))
	v, err = s.GetOne("test", []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("val1-updated"), v)

	// Delete
	require.NoError(t, s.Delete("test", []byte("key2")))
	v, err = s.GetOne("test", []byte("key2"))
	require.NoError(t, err)
	require.Nil(t, v)

	// Delete non-existent key (no error)
	require.NoError(t, s.Delete("test", []byte("nonexistent")))
}

func TestMemStore_CursorIteration(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("B"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("D"), []byte("4")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))

	c := s.newCursor("test")
	defer c.Close()

	// First
	k, v, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), k)
	assert.Equal(t, []byte("1"), v)

	// Next
	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("B"), k)
	assert.Equal(t, []byte("2"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("D"), k)
	assert.Equal(t, []byte("4"), v)

	// End
	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)

	// Last
	k, v, err = c.Last()
	require.NoError(t, err)
	assert.Equal(t, []byte("D"), k)
	assert.Equal(t, []byte("4"), v)

	// Prev
	k, v, err = c.Prev()
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)
}

func TestMemStore_CursorSeek(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))
	require.NoError(t, s.Put("test", []byte("E"), []byte("5")))

	c := s.newCursor("test")
	defer c.Close()

	// Seek exact match
	k, v, err := c.Seek([]byte("C"))
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)

	// Seek between keys — returns next key
	k, v, err = c.Seek([]byte("B"))
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)

	// Seek past end
	k, v, err = c.Seek([]byte("Z"))
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)

	// SeekExact
	k, v, err = c.SeekExact([]byte("C"))
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)

	// SeekExact miss
	k, v, err = c.SeekExact([]byte("B"))
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)
}

func TestMemStore_CursorWriteDuringIteration(t *testing.T) {
	// This is the key test: writing via cursor while iterating must not deadlock.
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))

	c := s.newCursor("test")
	defer c.Close()

	k, _, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), k)

	// Write via cursor (AppendDup calls Put internally)
	require.NoError(t, c.Put([]byte("B"), []byte("2")))

	// Next should see the newly inserted entry
	k, v, err := c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("B"), k)
	assert.Equal(t, []byte("2"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("C"), k)
	assert.Equal(t, []byte("3"), v)
}

func TestMemStore_DupSort(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals // known dupsort table

	// Insert multiple values for same key
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val2")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val3")))
	require.NoError(t, s.Put(table, []byte("key2"), []byte("val4")))

	c := s.newCursor(table)
	defer c.Close()

	// First
	k, v, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val1"), v)

	// NextDup
	k, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val2"), v)

	k, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val3"), v)

	// NextDup at end of duplicates
	k, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)

	// NextNoDup from key1 to key2
	_, _, err = c.First()
	require.NoError(t, err)
	k, v, err = c.NextNoDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("key2"), k)
	assert.Equal(t, []byte("val4"), v)
}

func TestMemStore_DupSortSeekBothRange(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals

	require.NoError(t, s.Put(table, []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val3")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val5")))

	c := s.newCursor(table)
	defer c.Close()

	// SeekBothRange: exact key match, range on value
	v, err := c.SeekBothRange([]byte("key1"), []byte("val2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val3"), v) // next value >= "val2"

	// SeekBothRange: wrong key
	v, err = c.SeekBothRange([]byte("key2"), []byte("val1"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemStore_DupSortFirstLastDup(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals

	require.NoError(t, s.Put(table, []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val2")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val3")))
	require.NoError(t, s.Put(table, []byte("key2"), []byte("val4")))

	c := s.newCursor(table)
	defer c.Close()

	// Position at key1/val2
	_, err := c.SeekBothRange([]byte("key1"), []byte("val2"))
	require.NoError(t, err)

	// FirstDup should go back to val1
	v, err := c.FirstDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), v)

	// LastDup should go to val3
	v, err = c.LastDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("val3"), v)
}

func TestMemStore_DupSortDelete(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals

	require.NoError(t, s.Put(table, []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val2")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val3")))
	require.NoError(t, s.Put(table, []byte("key2"), []byte("val4")))

	// Delete all entries for key1
	require.NoError(t, s.Delete(table, []byte("key1")))

	c := s.newCursor(table)
	defer c.Close()

	k, v, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("key2"), k)
	assert.Equal(t, []byte("val4"), v)

	k, _, err = c.Next()
	require.NoError(t, err)
	assert.Nil(t, k)
}

func TestMemStore_Sequences(t *testing.T) {
	s := newMemStore()

	val, err := s.ReadSequence("seq1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), val)

	prev, err := s.IncrementSequence("seq1", 10)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), prev)

	val, err = s.ReadSequence("seq1")
	require.NoError(t, err)
	assert.Equal(t, uint64(10), val)

	prev, err = s.IncrementSequence("seq1", 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), prev)

	val, err = s.ReadSequence("seq1")
	require.NoError(t, err)
	assert.Equal(t, uint64(15), val)

	require.NoError(t, s.ResetSequence("seq1", 100))
	val, err = s.ReadSequence("seq1")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), val)
}

func TestMemStore_Range(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("B"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))
	require.NoError(t, s.Put("test", []byte("D"), []byte("4")))

	// Ascending range [B, D)
	iter, err := s.Range("test", []byte("B"), []byte("D"), true, -1)
	require.NoError(t, err)
	defer iter.Close()

	var keys []string
	for iter.HasNext() {
		k, _, err := iter.Next()
		require.NoError(t, err)
		keys = append(keys, string(k))
	}
	assert.Equal(t, []string{"B", "C"}, keys)

	// Descending range
	iter2, err := s.Range("test", []byte("B"), []byte("D"), false, -1)
	require.NoError(t, err)
	defer iter2.Close()

	keys = nil
	for iter2.HasNext() {
		k, _, err := iter2.Next()
		require.NoError(t, err)
		keys = append(keys, string(k))
	}
	assert.Equal(t, []string{"C", "B"}, keys)

	// With limit
	iter3, err := s.Range("test", nil, nil, true, 2)
	require.NoError(t, err)
	defer iter3.Close()

	keys = nil
	for iter3.HasNext() {
		k, _, err := iter3.Next()
		require.NoError(t, err)
		keys = append(keys, string(k))
	}
	assert.Equal(t, []string{"A", "B"}, keys)
}

func TestMemStore_Prefix(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("abc1"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("abc2"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("abd1"), []byte("3")))
	require.NoError(t, s.Put("test", []byte("xyz"), []byte("4")))

	iter, err := s.Prefix("test", []byte("abc"))
	require.NoError(t, err)
	defer iter.Close()

	var keys []string
	for iter.HasNext() {
		k, _, err := iter.Next()
		require.NoError(t, err)
		keys = append(keys, string(k))
	}
	assert.Equal(t, []string{"abc1", "abc2"}, keys)
}

func TestMemStore_TableOperations(t *testing.T) {
	s := newMemStore()

	require.NoError(t, s.Put("t1", []byte("k"), []byte("v")))
	require.NoError(t, s.Put("t2", []byte("k"), []byte("v")))

	tables, err := s.ListTables()
	require.NoError(t, err)
	assert.Equal(t, []string{"t1", "t2"}, tables)

	exists, err := s.ExistsTable("t1")
	require.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, s.ClearTable("t1"))
	v, err := s.GetOne("t1", []byte("k"))
	require.NoError(t, err)
	assert.Nil(t, v)

	require.NoError(t, s.DropTable("t2"))
	exists, err = s.ExistsTable("t2")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestMemStore_ForEach(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("B"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))

	var keys []string
	err := s.ForEach("test", []byte("B"), func(k, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"B", "C"}, keys)
}

func TestMemStore_ForAmount(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("B"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))

	var keys []string
	err := s.ForAmount("test", []byte("A"), 2, func(k, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B"}, keys)
}

func TestMemStore_DeleteCurrent(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("A"), []byte("1")))
	require.NoError(t, s.Put("test", []byte("B"), []byte("2")))
	require.NoError(t, s.Put("test", []byte("C"), []byte("3")))

	c := s.newCursor("test")
	defer c.Close()

	_, _, err := c.Seek([]byte("B"))
	require.NoError(t, err)

	require.NoError(t, c.DeleteCurrent())

	v, err := s.GetOne("test", []byte("B"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemStore_DeleteExact(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals // dupsort

	require.NoError(t, s.Put(table, []byte("key1"), []byte("val1")))
	require.NoError(t, s.Put(table, []byte("key1"), []byte("val2")))

	c := s.newCursor(table)
	defer c.Close()

	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("val1")))

	k, v, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("val2"), v)

	k, _, err = c.Next()
	require.NoError(t, err)
	assert.Nil(t, k)
}

func TestMemStore_EmptyTable(t *testing.T) {
	s := newMemStore()

	c := s.newCursor("empty")
	defer c.Close()

	k, v, err := c.First()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)

	k, v, err = c.Last()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)

	k, v, err = c.Seek([]byte("anything"))
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)
}

func TestMemStore_DataIsolation(t *testing.T) {
	// Verify returned slices are copies (not references to internal data)
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("key"), []byte("original")))

	v, err := s.GetOne("test", []byte("key"))
	require.NoError(t, err)

	// Mutate the returned slice
	v[0] = 'X'

	// Original should be unchanged
	v2, err := s.GetOne("test", []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), v2)
}

// --- Concurrent access tests ---

func TestMemStore_ConcurrentReadWrite(t *testing.T) {
	s := newMemStore()
	const numWriters = 4
	const numReaders = 4
	const numOps = 1000

	// Pre-populate
	for i := range 100 {
		require.NoError(t, s.Put("test", []byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i))))
	}

	var wg sync.WaitGroup

	// Writers: continuously put new keys
	for w := range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numOps {
				key := fmt.Sprintf("w%d-key%04d", w, i)
				val := fmt.Sprintf("w%d-val%04d", w, i)
				if err := s.Put("test", []byte(key), []byte(val)); err != nil {
					t.Errorf("Put error: %v", err)
					return
				}
			}
		}()
	}

	// Readers: continuously read and iterate
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numOps {
				// Random reads
				_, _ = s.GetOne("test", []byte("key0050"))
				_, _ = s.Has("test", []byte("key0050"))

				// ForEach (partial)
				count := 0
				_ = s.ForEach("test", []byte("key00"), func(k, v []byte) error {
					count++
					if count >= 10 {
						return fmt.Errorf("stop")
					}
					return nil
				})
			}
		}()
	}

	wg.Wait()

	// Verify all written data exists
	for w := range numWriters {
		for i := range numOps {
			key := fmt.Sprintf("w%d-key%04d", w, i)
			val := fmt.Sprintf("w%d-val%04d", w, i)
			v, err := s.GetOne("test", []byte(key))
			require.NoError(t, err)
			require.Equal(t, []byte(val), v, "key=%s", key)
		}
	}
}

func TestMemStore_ConcurrentCursors(t *testing.T) {
	s := newMemStore()
	const numGoroutines = 8
	const numOps = 500

	// Pre-populate with sorted data
	for i := range 200 {
		require.NoError(t, s.Put("test", []byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i))))
	}

	var wg sync.WaitGroup

	// Concurrent cursor operations
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numOps {
				c := s.newCursor("test")

				// Iterate a few entries
				k, _, err := c.First()
				if err != nil {
					t.Errorf("First error: %v", err)
					c.Close()
					return
				}
				if k == nil {
					c.Close()
					continue
				}

				for i := 0; i < 5; i++ {
					k, _, err = c.Next()
					if err != nil {
						t.Errorf("Next error: %v", err)
						break
					}
					if k == nil {
						break
					}
				}

				c.Close()
			}
		}()
	}

	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range numOps {
			key := fmt.Sprintf("ckey%04d", i)
			_ = s.Put("test", []byte(key), []byte("cval"))
		}
	}()

	wg.Wait()
}

func TestMemStore_ConcurrentSequences(t *testing.T) {
	s := newMemStore()
	const numGoroutines = 8
	const numIncrements = 100

	var wg sync.WaitGroup
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIncrements {
				_, err := s.IncrementSequence("counter", 1)
				if err != nil {
					t.Errorf("IncrementSequence error: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	val, err := s.ReadSequence("counter")
	require.NoError(t, err)
	assert.Equal(t, uint64(numGoroutines*numIncrements), val)
}

func TestMemStore_ConcurrentRangeAndWrite(t *testing.T) {
	s := newMemStore()

	// Pre-populate
	for i := range 100 {
		require.NoError(t, s.Put("test", []byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i))))
	}

	var wg sync.WaitGroup

	// Range iterators while writing
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				iter, err := s.Range("test", []byte("key0010"), []byte("key0090"), true, -1)
				if err != nil {
					t.Errorf("Range error: %v", err)
					return
				}
				count := 0
				for iter.HasNext() {
					k, _, err := iter.Next()
					if err != nil {
						t.Errorf("Range.Next error: %v", err)
						break
					}
					if k == nil {
						break
					}
					count++
				}
				iter.Close()
				// Should get roughly 80 entries (key0010..key0089)
				if count < 1 {
					t.Errorf("Range returned 0 entries")
				}
			}
		}()
	}

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			key := fmt.Sprintf("key%04d", 1000+i)
			_ = s.Put("test", []byte(key), []byte("new"))
		}
	}()

	wg.Wait()
}

func TestMemStore_ConcurrentDupSort(t *testing.T) {
	s := newMemStore()
	table := kv.TblAccountVals // dupsort table

	// Pre-populate
	for i := range 10 {
		for j := range 10 {
			require.NoError(t, s.Put(table,
				[]byte(fmt.Sprintf("key%02d", i)),
				[]byte(fmt.Sprintf("val%02d", j))))
		}
	}

	var wg sync.WaitGroup

	// Concurrent readers with SeekBothRange
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				c := s.newCursor(table)
				v, err := c.SeekBothRange([]byte("key05"), []byte("val03"))
				if err != nil {
					t.Errorf("SeekBothRange error: %v", err)
					c.Close()
					return
				}
				if !bytes.HasPrefix(v, []byte("val")) {
					t.Errorf("unexpected value: %s", v)
				}
				c.Close()
			}
		}()
	}

	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			_ = s.Put(table,
				[]byte(fmt.Sprintf("key%02d", i%10)),
				[]byte(fmt.Sprintf("new%04d", i)))
		}
	}()

	wg.Wait()
}
