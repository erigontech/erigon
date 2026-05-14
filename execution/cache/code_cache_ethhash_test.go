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

package cache

import (
	"bytes"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeEthHash builds a deterministic 32-byte ethHash for tests/benchmarks.
func makeEthHash(i int) []byte {
	h := make([]byte, 32)
	h[0] = byte(i)
	h[1] = byte(i >> 8)
	return h
}

func TestCodeCache_GetByEthHash_HitAfterPut(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	addr := makeAddr(1)
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52} // small contract preamble
	ethHash := makeEthHash(0xab)

	// Empty cache: miss.
	v, ok := c.GetByEthHash(ethHash)
	require.False(t, ok)
	require.Nil(t, v)

	// Populate via PutWithEthHash — both addr and ethHash paths fill.
	c.PutWithEthHash(addr, code, ethHash)

	// Hit by ethHash directly.
	v, ok = c.GetByEthHash(ethHash)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))

	// Hit by addr via existing two-level path.
	v, ok = c.Get(addr)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))
}

func TestCodeCache_GetByEthHash_DistinctAddrsSameCode(t *testing.T) {
	// The point of L2b: many addresses sharing one codeHash all hit a
	// single entry once any one of them has been populated.
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}
	ethHash := makeEthHash(0xcd)

	addr1 := makeAddr(1)
	c.PutWithEthHash(addr1, code, ethHash)

	// A different address never seen at L1 still hits the L2b layer
	// when looked up by the shared ethHash.
	v, ok := c.GetByEthHash(ethHash)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))

	// The addr2 lookup itself still misses (L1 unknown), as expected —
	// L2b is meant to be probed by callers that already hold the hash.
	addr2 := makeAddr(2)
	_, ok = c.Get(addr2)
	require.False(t, ok)
}

func TestCodeCache_PutWithEthHash_EmptyHashOrCodeIsNoOp(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	addr := makeAddr(1)
	code := []byte{0x60, 0x00}

	c.PutWithEthHash(addr, code, nil) // empty hash → skip L2b
	v, ok := c.GetByEthHash(makeEthHash(7))
	require.False(t, ok)
	require.Nil(t, v)

	c.PutWithEthHash(addr, nil, makeEthHash(7)) // empty code → skip both
	v, ok = c.GetByEthHash(makeEthHash(7))
	require.False(t, ok)
	require.Nil(t, v)
}

func TestCodeCache_PutWithEthHash_RespectsCodeCapacity(t *testing.T) {
	// 8-byte cap: 32-byte ethHash + 4-byte code > 32. New L2b puts must
	// no-op when the layer is full. Use tiny code to keep math obvious.
	c := NewCodeCache(8, 1*datasize.MB)
	c.PutWithEthHash(makeAddr(1), []byte{1, 2, 3, 4}, makeEthHash(1))
	// Second put exceeds the L2b budget — must no-op.
	c.PutWithEthHash(makeAddr(2), []byte{5, 6, 7, 8}, makeEthHash(2))

	_, ok := c.GetByEthHash(makeEthHash(2))
	assert.False(t, ok, "second L2b entry should not exist when capacity is exceeded")
}

// =============================================================================
// Microbenchmarks — measure the per-op cost of the L2b path.
// =============================================================================

func BenchmarkCodeCache_GetByEthHash_Hit(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048) // 2 KiB typical contract size
	ethHash := makeEthHash(0x11)
	c.PutWithEthHash(makeAddr(1), code, ethHash)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.GetByEthHash(ethHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

func BenchmarkCodeCache_GetByEthHash_Miss(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	missHash := makeEthHash(0x22)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetByEthHash(missHash)
	}
}

// BenchmarkCodeCache_Get_AddrLevel_Hit baseline: the existing addr-keyed
// path. Compare against GetByEthHash to verify the L2b lookup is at least
// as fast (one map probe vs two: addr→hash then hash→code).
func BenchmarkCodeCache_Get_AddrLevel_Hit(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048)
	addr := makeAddr(1)
	c.PutWithEthHash(addr, code, makeEthHash(0x33))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.Get(addr)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

// BenchmarkCodeCache_GetByEthHash_ManyAddrs_OneCode measures the workload
// shape this layer is designed for: many addresses sharing one codeHash.
// Without L2b every fresh addr would pay a file read. With L2b every
// caller that already knows the hash hits one shared entry.
func BenchmarkCodeCache_GetByEthHash_ManyAddrs_OneCode(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048)
	ethHash := makeEthHash(0x44)
	c.PutWithEthHash(makeAddr(1), code, ethHash) // populate once

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Caller knows the hash from a prior account read; probes L2b.
		v, ok := c.GetByEthHash(ethHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}
