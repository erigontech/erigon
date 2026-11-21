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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	big := bytes.Repeat([]byte{0xEE}, arenaSlabSize+1)
	got := a.put(big)
	assert.Equal(t, big, got)
	big[0] = 0x11
	assert.Equal(t, byte(0xEE), got[0], "oversized arena slice must not alias the input")
}

func BenchmarkMemStore_Put(b *testing.B) {
	s := newMemStore()
	key := make([]byte, 32)
	val := make([]byte, 96)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		binaryPut(key, uint64(i))
		_ = s.Put("t", key, val)
		i++
	}
}

func binaryPut(dst []byte, v uint64) {
	for i := 0; i < 8 && i < len(dst); i++ {
		dst[i] = byte(v >> (8 * i))
	}
}
