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

package vm

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
)

func TestMemoryCopy(t *testing.T) {
	t.Parallel()
	// Test cases from https://eips.ethereum.org/EIPS/eip-5656#test-cases
	for i, tc := range []struct {
		dst, src, len uint64
		pre           string
		want          string
	}{
		{ // MCOPY 0 32 32 - copy 32 bytes from offset 32 to offset 0.
			0, 32, 32,
			"0000000000000000000000000000000000000000000000000000000000000000 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		},

		{ // MCOPY 0 0 32 - copy 32 bytes from offset 0 to offset 0.
			0, 0, 32,
			"0101010101010101010101010101010101010101010101010101010101010101",
			"0101010101010101010101010101010101010101010101010101010101010101",
		},
		{ // MCOPY 0 1 8 - copy 8 bytes from offset 1 to offset 0 (overlapping).
			0, 1, 8,
			"000102030405060708 000000000000000000000000000000000000000000000000",
			"010203040506070808 000000000000000000000000000000000000000000000000",
		},
		{ // MCOPY 1 0 8 - copy 8 bytes from offset 0 to offset 1 (overlapping).
			1, 0, 8,
			"000102030405060708 000000000000000000000000000000000000000000000000",
			"000001020304050607 000000000000000000000000000000000000000000000000",
		},
		// Tests below are not in the EIP, but maybe should be added
		{ // MCOPY 0xFFFFFFFFFFFF 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds index(overlapping).
			0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF, 0,
			"11",
			"11",
		},
		{ // MCOPY 0xFFFFFFFFFFFF 0 0 - copy zero bytes from start of mem to out-of-bounds.
			0xFFFFFFFFFFFF, 0, 0,
			"11",
			"11",
		},
		{ // MCOPY 0 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds to start of mem
			0, 0xFFFFFFFFFFFF, 0,
			"11",
			"11",
		},
	} {
		m := NewMemory()
		// Clean spaces
		data := common.FromHex(strings.ReplaceAll(tc.pre, " ", ""))
		// Set pre
		m.Resize(uint64(len(data)))
		m.Set(0, uint64(len(data)), data)
		// Do the copy
		m.Copy(tc.dst, tc.src, tc.len)
		want := common.FromHex(strings.ReplaceAll(tc.want, " ", ""))
		if have := m.store; !bytes.Equal(want, have) {
			t.Errorf("case %d: want: %#x\nhave: %#x\n", i, want, have)
		}
	}
}

// TestMemoryResizeZeroesReusedBuffer pins the fast path Resize takes once the
// buffer is warm: a frame must not see the previous frame's bytes.
func TestMemoryResizeZeroesReusedBuffer(t *testing.T) {
	t.Parallel()
	var m Memory
	m.Resize(64)
	for i := range m.store {
		m.store[i] = 0xFF
	}
	m.reset()

	m.Resize(64)
	if want := make([]byte, 64); !bytes.Equal(m.store, want) {
		t.Fatalf("reused buffer not zeroed: %#x", m.store)
	}
}

func TestMemoryResizeCapacity(t *testing.T) {
	t.Parallel()
	var m Memory
	m.Resize(32)
	if got := cap(m.store); got != memoryPageSize {
		t.Fatalf("cold resize to 32: cap %d, want %d", got, memoryPageSize)
	}

	m.Resize(memoryPageSize + 32)
	if got := cap(m.store); got != 2*memoryPageSize {
		t.Fatalf("grow past one page: cap %d, want %d", got, 2*memoryPageSize)
	}

	// Doubling wins over page alignment here: align-only would give 3 pages.
	m.Resize(2*memoryPageSize + 32)
	if got := cap(m.store); got != 4*memoryPageSize {
		t.Fatalf("grow past two pages: cap %d, want %d", got, 4*memoryPageSize)
	}
}

func BenchmarkResize(b *testing.B) {
	memory := NewMemory()
	var i uint64
	for b.Loop() {
		memory.Resize(i)
		i++
	}
}

// BenchmarkResizeCold grows a fresh memory word-by-word, the pattern a call
// frame sees when it gets a CallContext whose buffer the pool has not warmed.
func BenchmarkResizeCold(b *testing.B) {
	for _, target := range []uint64{4 * 1024, 64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("%dKiB", target/1024), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var m Memory
				for size := uint64(32); size <= target; size += 32 {
					m.Resize(size)
				}
			}
		})
	}
}
