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
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// Put must not retain the caller's buffers: they often alias MDBX mmap pages that
// change or unmap under us. Mutating the input after Put must not touch stored data.
func TestMemStore_PutCopiesInput(t *testing.T) {
	s := newMemStore()
	k := []byte("key")
	v := []byte("original")
	require.NoError(t, s.Put("test", k, v))

	for i := range k {
		k[i] = 'X'
	}
	for i := range v {
		v[i] = 'Y'
	}

	got, err := s.GetOne("test", []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), got)
}

func TestArena_PutCopiesAndDoesNotAlias(t *testing.T) {
	var a arena

	src := []byte("hello")
	got := a.put(src)
	assert.Equal(t, src, got)
	src[0] = 'J'
	assert.Equal(t, []byte("hello"), got, "arena slice must not alias the input")

	assert.Empty(t, a.put(nil))
	assert.Empty(t, a.put([]byte{}))
}

func TestArena_SlabRolloverPreservesEntries(t *testing.T) {
	var a arena
	// Many small copies spanning well past one slab, each distinct, so a rollover
	// bug (overwriting a live region or handing back an aliased slice) corrupts one.
	n := (arenaSlabSize / 8) * 3
	got := make([][]byte, n)
	want := make([][]byte, n)
	for i := range n {
		b := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0xAB, 0xCD}
		want[i] = bytes.Clone(b)
		got[i] = a.put(b)
	}
	for i := range n {
		require.Equal(t, want[i], got[i], "entry %d corrupted across slab rollover", i)
	}
}

func TestArena_OversizedGetsOwnAllocation(t *testing.T) {
	var a arena
	big := bytes.Repeat([]byte{0xEE}, arenaMaxEntrySize+1)
	got := a.put(big)
	assert.Equal(t, big, got)
	big[0] = 0x11
	assert.Equal(t, byte(0xEE), got[0], "oversized arena slice must not alias the input")
}

func TestArena_FirstSlabIsSmall(t *testing.T) {
	var a arena
	a.put([]byte("x"))
	assert.Equal(t, arenaMinSlabSize, cap(a.cur))
}

func TestMemStore_OverwriteDoesNotGrowArena(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("key"), []byte("v1")))
	used := len(s.tables["test"].arena.cur)

	require.NoError(t, s.Put("test", []byte("key"), []byte("v2")))
	assert.Equal(t, used, len(s.tables["test"].arena.cur))

	got, err := s.GetOne("test", []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)
}

func TestMemStore_PutNilValueStaysNil(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("key"), nil))
	got, err := s.GetOne("test", []byte("key"))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestArena_LargeValueDoesNotAbandonSlab(t *testing.T) {
	var a arena
	a.put([]byte("x"))
	slab := a.cur
	a.put(make([]byte, arenaSlabSize/2+1))
	assert.Equal(t, unsafe.SliceData(slab), unsafe.SliceData(a.cur))
	assert.Equal(t, 1, len(a.cur))
}

func TestMemStore_DupSortRePutDoesNotAllocate(t *testing.T) {
	s := newMemStore()
	k, v := []byte("key"), []byte("v1")
	require.NoError(t, s.Put(kv.PlainState, k, v))
	allocs := testing.AllocsPerRun(100, func() { _ = s.Put(kv.PlainState, k, v) })
	assert.Zero(t, allocs)
	assert.Equal(t, 1, s.tables[kv.PlainState].tree.Len())
}

func TestMemStore_DupSortRePutKeepsNilStatus(t *testing.T) {
	s := newMemStore()
	k := []byte("key")
	require.NoError(t, s.Put(kv.PlainState, k, nil))
	require.NoError(t, s.Put(kv.PlainState, k, []byte{}))
	got, err := s.GetOne(kv.PlainState, k)
	require.NoError(t, err)
	assert.NotNil(t, got)

	require.NoError(t, s.Put(kv.PlainState, k, nil))
	got, err = s.GetOne(kv.PlainState, k)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMemStore_OverwriteKeepsKeyNilStatus(t *testing.T) {
	for _, table := range []string{"test", kv.PlainState} {
		for _, tc := range []struct{ first, second []byte }{{nil, []byte{}}, {[]byte{}, nil}} {
			s := newMemStore()
			require.NoError(t, s.Put(table, tc.first, []byte("v")))
			require.NoError(t, s.Put(table, tc.second, []byte("v")))
			c, err := s.Cursor(table)
			require.NoError(t, err)
			t.Cleanup(c.Close)
			k, v, err := c.First()
			require.NoError(t, err)
			assert.Equal(t, tc.second == nil, k == nil, table)
			assert.Equal(t, []byte("v"), v, table)
		}
	}
}

func TestMemStore_ClearTableReleasesArena(t *testing.T) {
	s := newMemStore()
	require.NoError(t, s.Put("test", []byte("key"), []byte("value")))
	require.NoError(t, s.ClearTable("test"))
	assert.Nil(t, s.tables["test"].arena.cur)
}
