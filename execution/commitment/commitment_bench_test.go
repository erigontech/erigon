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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func BenchmarkBranchMerger_Merge(b *testing.B) {

	row, bm := generateCellRow(b, 16)

	be := NewBranchEncoder(1024)
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*cell, error) {
		return row[i], nil
	})
	require.NoError(b, err)

	var copies [16][]byte
	var tm uint16
	am := bm

	for i := 15; i >= 0; i-- {
		row[i] = nil
		tm, bm, am = uint16(1<<i), bm>>1, am>>1
		enc1, _, err := be.EncodeBranch(bm, tm, am, func(i int, skip bool) (*cell, error) {
			return row[i], nil
		})
		require.NoError(b, err)

		copies[i] = common.Copy(enc1)
	}

	bmg := NewHexBranchMerger(4096)
	var ci int
	for b.Loop() {
		_, err := bmg.Merge(enc, copies[ci])
		if err != nil {
			b.Fatal(err)
		}
		ci++
		if ci == len(copies) {
			ci = 0
		}
	}
}

// BenchmarkReplacePlainKeys_BufferReuse compares the old pattern (fresh make each call)
// against the new pattern (reused scratch buffer + bytes.Clone), matching what
// replaceShortenedKeysInBranch now does on AggregatorRoTx.
func BenchmarkReplacePlainKeys_BufferReuse(b *testing.B) {
	row, bm := generateCellRow(b, 16)
	be := NewBranchEncoder(1024)
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(nibble int, skip bool) (*cell, error) {
		return row[nibble], nil
	})
	if err != nil {
		b.Fatal(err)
	}
	replacer := func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			return key[:8], nil
		}
		return key[:4], nil
	}

	b.Run("fresh-make", func(b *testing.B) {
		for b.Loop() {
			aux := make([]byte, 0, 256)
			_, _, _ = enc.ReplacePlainKeys(aux, replacer)
		}
	})

	b.Run("reuse-clone", func(b *testing.B) {
		var buf []byte
		for b.Loop() {
			_, buf, _ = enc.ReplacePlainKeys(buf[:0], replacer)
		}
	})
}

func BenchmarkGetDeferredUpdate(b *testing.B) {
	// Create a cell grid similar to what fold() would produce
	var cells [16]cell
	var bitmap uint16

	// Fill cells with realistic data
	for i := 0; i < 16; i++ {
		c := &cells[i]
		c.hashLen = 32
		for j := 0; j < 32; j++ {
			c.hash[j] = byte(i*32 + j)
		}

		// Vary the cell types like real trie data
		switch i % 4 {
		case 0: // account cell
			c.accountAddrLen = 20
			for j := 0; j < 20; j++ {
				c.accountAddr[j] = byte(i + j)
			}
		case 1: // storage cell
			c.storageAddrLen = 52
			for j := 0; j < 52; j++ {
				c.storageAddr[j] = byte(i + j)
			}
		case 2: // extension cell
			c.extLen = 10
			for j := 0; j < 10; j++ {
				c.extension[j] = byte(i + j)
			}
		case 3: // hash-only cell
			// just hash, already set
		}

		bitmap |= uint16(1 << i)
	}

	touchMap := bitmap
	afterMap := bitmap
	prefix := []byte{0x01, 0x02, 0x03}
	prev := []byte{0x04, 0x05, 0x06}
	// prevStep removed

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		upd := getDeferredUpdate(prefix, bitmap, touchMap, afterMap, &cells, 5, prev)
		putDeferredUpdate(upd)
	}
}

func BenchmarkGetDeferredUpdate_FewCells(b *testing.B) {
	// Benchmark with only 2 cells set (more realistic for sparse updates)
	var cells [16]cell
	var bitmap uint16

	// Only set cells 0 and 5
	for _, i := range []int{0, 5} {
		c := &cells[i]
		c.hashLen = 32
		for j := 0; j < 32; j++ {
			c.hash[j] = byte(i*32 + j)
		}
		c.accountAddrLen = 20
		for j := 0; j < 20; j++ {
			c.accountAddr[j] = byte(i + j)
		}
		bitmap |= uint16(1 << i)
	}

	touchMap := bitmap
	afterMap := bitmap
	prefix := []byte{0x01, 0x02, 0x03}
	prev := []byte{0x04, 0x05, 0x06}
	// prevStep removed

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		upd := getDeferredUpdate(prefix, bitmap, touchMap, afterMap, &cells, 5, prev)
		putDeferredUpdate(upd)
	}
}
