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

// makeCodeHash builds a deterministic 32-byte codeHash for tests/benchmarks.
func makeCodeHash(i int) []byte {
	h := make([]byte, 32)
	h[0] = byte(i)
	h[1] = byte(i >> 8)
	return h
}

func TestCodeCache_GetByCodeHash_HitAfterPut(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	addr := makeAddr(1)
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52} // small contract preamble
	codeHash := makeCodeHash(0xab)

	// Empty cache: miss.
	v, ok := c.GetByCodeHash(codeHash)
	require.False(t, ok)
	require.Nil(t, v)

	// Populate via PutWithCodeHash — both addr and codeHash paths fill.
	c.PutWithCodeHash(addr, code, codeHash, 0)

	// Hit by codeHash directly.
	v, ok = c.GetByCodeHash(codeHash)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))

	// Hit by addr via existing two-level path.
	v, ok = c.Get(addr)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))
}

func TestCodeCache_GetByCodeHash_DistinctAddrsSameCode(t *testing.T) {
	// The point of codeHashToCode: many addresses sharing one codeHash all hit a
	// single entry once any one of them has been populated.
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}
	codeHash := makeCodeHash(0xcd)

	addr1 := makeAddr(1)
	c.PutWithCodeHash(addr1, code, codeHash, 0)

	// A different address never seen at L1 still hits the codeHashToCode layer
	// when looked up by the shared codeHash.
	v, ok := c.GetByCodeHash(codeHash)
	require.True(t, ok)
	require.True(t, bytes.Equal(v, code))

	// The addr2 lookup itself still misses (L1 unknown), as expected —
	// codeHashToCode is meant to be probed by callers that already hold the hash.
	addr2 := makeAddr(2)
	_, ok = c.Get(addr2)
	require.False(t, ok)
}

func TestCodeCache_PutWithCodeHash_EmptyHashOrCodeIsNoOp(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	addr := makeAddr(1)
	code := []byte{0x60, 0x00}

	c.PutWithCodeHash(addr, code, nil, 0) // empty hash → skip codeHashToCode
	v, ok := c.GetByCodeHash(makeCodeHash(7))
	require.False(t, ok)
	require.Nil(t, v)

	c.PutWithCodeHash(addr, nil, makeCodeHash(7), 0) // empty code → skip both
	v, ok = c.GetByCodeHash(makeCodeHash(7))
	require.False(t, ok)
	require.Nil(t, v)
}

func TestCodeCache_PutWithCodeHash_RespectsCodeCapacity(t *testing.T) {
	// 8-byte cap: 32-byte codeHash + 4-byte code > 32. New codeHashToCode puts must
	// no-op when the layer is full. Use tiny code to keep math obvious.
	c := NewCodeCache(8, 1*datasize.MB)
	c.PutWithCodeHash(makeAddr(1), []byte{1, 2, 3, 4}, makeCodeHash(1), 0)
	// Second put exceeds the codeHashToCode budget — must no-op.
	c.PutWithCodeHash(makeAddr(2), []byte{5, 6, 7, 8}, makeCodeHash(2), 0)

	_, ok := c.GetByCodeHash(makeCodeHash(2))
	assert.False(t, ok, "second codeHashToCode entry should not exist when capacity is exceeded")
}

func TestCodeCache_CodeSize_PopulatedAlongsideBytes(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52, 0x60, 0x10}
	codeHash := makeCodeHash(0xee)

	// Miss before any populate.
	_, ok := c.GetCodeSizeByCodeHash(codeHash)
	require.False(t, ok)

	// PutWithCodeHash should fill the size layer alongside the bytes.
	c.PutWithCodeHash(makeAddr(1), code, codeHash, 0)

	size, ok := c.GetCodeSizeByCodeHash(codeHash)
	require.True(t, ok)
	require.Equal(t, len(code), size)
}

func TestCodeCache_CodeSize_DirectPutAndGet(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	codeHash := makeCodeHash(0xff)

	// Direct Put without going through the bytes layer.
	c.PutCodeSizeByCodeHash(codeHash, 4096, 0)

	size, ok := c.GetCodeSizeByCodeHash(codeHash)
	require.True(t, ok)
	require.Equal(t, 4096, size)
}

func TestCodeCache_CodeSize_EmptyHashOrNegativeIsNoOp(t *testing.T) {
	c := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	c.PutCodeSizeByCodeHash(nil, 100, 0)
	c.PutCodeSizeByCodeHash(makeCodeHash(1), -1, 0)
	_, ok := c.GetCodeSizeByCodeHash(makeCodeHash(1))
	assert.False(t, ok)
}

// =============================================================================
// Microbenchmarks — measure the per-op cost of the codeHashToCode path.
// =============================================================================

func BenchmarkCodeCache_GetByCodeHash_Hit(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048) // 2 KiB typical contract size
	codeHash := makeCodeHash(0x11)
	c.PutWithCodeHash(makeAddr(1), code, codeHash, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.GetByCodeHash(codeHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

func BenchmarkCodeCache_GetByCodeHash_Miss(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	missHash := makeCodeHash(0x22)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetByCodeHash(missHash)
	}
}

// BenchmarkCodeCache_Get_AddrLevel_Hit baseline: the existing addr-keyed
// path. Compare against GetByCodeHash to verify the codeHashToCode lookup is at least
// as fast (one map probe vs two: addr→hash then hash→code).
func BenchmarkCodeCache_Get_AddrLevel_Hit(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048)
	addr := makeAddr(1)
	c.PutWithCodeHash(addr, code, makeCodeHash(0x33), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.Get(addr)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

// BenchmarkCodeCache_GetByCodeHash_ManyAddrs_OneCode measures the workload
// shape this layer is designed for: many addresses sharing one codeHash.
// Without codeHashToCode every fresh addr would pay a file read. With codeHashToCode every
// caller that already knows the hash hits one shared entry.
func BenchmarkCodeCache_GetByCodeHash_ManyAddrs_OneCode(b *testing.B) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	code := bytes.Repeat([]byte{0x5b}, 2048)
	codeHash := makeCodeHash(0x44)
	c.PutWithCodeHash(makeAddr(1), code, codeHash, 0) // populate once

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Caller knows the hash from a prior account read; probes codeHashToCode.
		v, ok := c.GetByCodeHash(codeHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

// TestCodeCache_Unwind_DropsUnwoundCodeEverywhere verifies the (txNum, epoch)
// model the user requires (#21752): code deployed on a fork that is later
// unwound must stop being discoverable on EVERY layer — addr→code, the
// content-addressed codeHash→code, and the size layer — not just the addr
// layer. The code's value is invariant for a hash, but its existence is not.
func TestCodeCache_Unwind_DropsUnwoundCodeEverywhere(t *testing.T) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)

	addr := makeAddr(1)
	code := bytes.Repeat([]byte{0x60}, 64)
	codeHash := makeCodeHash(0x11)

	// Deploy at txNum=100.
	c.PutWithCodeHash(addr, code, codeHash, 100)

	// All layers hit before unwind.
	got, ok := c.Get(addr)
	require.True(t, ok)
	require.Equal(t, code, got)
	_, ok = c.GetByCodeHash(codeHash)
	require.True(t, ok, "codeHash lookup must hit before unwind")
	sz, ok := c.GetCodeSizeByCodeHash(codeHash)
	require.True(t, ok)
	require.Equal(t, len(code), sz)

	// Unwind to txNum=50 — the deploy at 100 is rolled back.
	c.Unwind(50)

	_, ok = c.Get(addr)
	require.False(t, ok, "addr→code must drop")
	_, ok = c.GetByCodeHash(codeHash)
	require.False(t, ok, "unwound code must NOT be discoverable by codeHash")
	_, ok = c.GetCodeSizeByCodeHash(codeHash)
	require.False(t, ok, "size of unwound code must drop too")
}

// TestCodeCache_Unwind_BelowFloorSurvives verifies code deployed below the
// unwind floor (still live after the unwind) stays warm on all layers.
func TestCodeCache_Unwind_BelowFloorSurvives(t *testing.T) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)

	addr := makeAddr(2)
	code := bytes.Repeat([]byte{0x61}, 32)
	codeHash := makeCodeHash(0x22)
	c.PutWithCodeHash(addr, code, codeHash, 40)

	c.Unwind(50) // floor=50; the deploy at 40 predates it

	got, ok := c.Get(addr)
	require.True(t, ok, "below-floor addr→code must survive")
	require.Equal(t, code, got)
	_, ok = c.GetByCodeHash(codeHash)
	require.True(t, ok, "below-floor codeHash→code must survive")
}

// TestCodeCache_Unwind_RedeployRevives verifies that re-deploying the same code
// on the live fork (current epoch) after an unwind makes it discoverable again,
// even though a stale entry at the same txNum was left behind.
func TestCodeCache_Unwind_RedeployRevives(t *testing.T) {
	c := NewCodeCache(64*datasize.MB, 16*datasize.MB)

	addr := makeAddr(3)
	code := bytes.Repeat([]byte{0x62}, 48)
	codeHash := makeCodeHash(0x33)

	c.PutWithCodeHash(addr, code, codeHash, 100) // old fork
	c.Unwind(50)
	c.PutWithCodeHash(addr, code, codeHash, 100) // re-executed on live fork, new epoch

	got, ok := c.Get(addr)
	require.True(t, ok, "re-deployed addr→code must be live")
	require.Equal(t, code, got)
	gotH, ok := c.GetByCodeHash(codeHash)
	require.True(t, ok, "re-deployed codeHash→code must be live")
	require.Equal(t, code, gotH)
}
