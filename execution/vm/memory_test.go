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

func TestSetFromData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		data       []byte
		dataOffset uint64
		size       uint64
		want       []byte
	}{
		{"exact copy", []byte{1, 2, 3, 4}, 0, 4, []byte{1, 2, 3, 4}},
		{"partial with zero-pad", []byte{1, 2}, 0, 4, []byte{1, 2, 0, 0}},
		{"offset beyond data", []byte{1, 2, 3}, 10, 4, []byte{0, 0, 0, 0}},
		{"offset into data", []byte{1, 2, 3, 4, 5}, 2, 3, []byte{3, 4, 5}},
		{"offset with partial pad", []byte{1, 2, 3, 4, 5}, 3, 4, []byte{4, 5, 0, 0}},
		{"size zero", []byte{1, 2, 3}, 0, 0, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMemory()
			if tc.size > 0 {
				m.Resize(tc.size)
				m.SetFromData(0, tc.size, tc.dataOffset, tc.data)
			}
			if !bytes.Equal(m.store[:tc.size], tc.want) {
				t.Errorf("want %x, got %x", tc.want, m.store[:tc.size])
			}
		})
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
