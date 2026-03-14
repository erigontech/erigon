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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
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

// benchReplacePlainKeys runs ReplacePlainKeys in a tight loop with b.ReportAllocs().
// buf is a pre-allocated output buffer (pass nil to let ReplacePlainKeys allocate).
// No assertions inside the hot loop.
func benchReplacePlainKeys(b *testing.B, data BranchData, buf []byte, fn func(key []byte, isStorage bool) ([]byte, error)) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		_, err := data.ReplacePlainKeys(buf[:0], fn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// encodeSyntheticBranch creates encoded BranchData from generateCellRow output.
func encodeSyntheticBranch(b *testing.B, nCells int) (BranchData, uint16) {
	b.Helper()
	row, bm := generateCellRow(b, nCells)
	be := NewBranchEncoder(1024)
	enc, _, err := be.EncodeBranch(bm, bm, bm, func(i int, skip bool) (*cell, error) {
		return row[i], nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return BranchData(common.Copy(enc)), bm
}

// preshortenBranchData shortens keys in enc and returns the shortened data plus
// a map from shortened key to original key (for the expand benchmark).
func preshortenBranchData(b *testing.B, enc BranchData) (BranchData, map[string][]byte) {
	b.Helper()
	keyMap := make(map[string][]byte)
	shortened, err := enc.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		var short []byte
		if isStorage {
			short = key[:8]
		} else {
			short = key[:4]
		}
		keyMap[string(short)] = common.Copy(key)
		return short, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return BranchData(common.Copy(shortened)), keyMap
}

func BenchmarkBranchData_ReplacePlainKeys(b *testing.B) {
	// Shorten callback: truncate account keys to 4 bytes, storage keys to 8 bytes
	shortenFn := func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			return key[:8], nil
		}
		return key[:4], nil
	}

	// --- Synthetic dense (16-cell) data ---
	denseEnc, _ := encodeSyntheticBranch(b, 16)
	denseShortened, denseKeyMap := preshortenBranchData(b, denseEnc)

	// --- Synthetic sparse (2-cell) data ---
	sparseEnc, _ := encodeSyntheticBranch(b, 2)
	sparseShortened, sparseKeyMap := preshortenBranchData(b, sparseEnc)

	// --- Real data from production hex string ---
	// This data has already-shortened 4-byte storage keys and no account keys,
	// so it exercises the no-change fast path (all keys already short).
	// Its value is measuring parsing overhead on production-format data with
	// mixed field types (hashes, extensions, storage addresses).
	realDataHex := "86e586e5082035e72a782b51d9c98548467e3f868294d923cdbbdf4ce326c867bd972c4a2395090109203b51781a76dc87640aea038e3fdd8adca94049aaa436735b162881ec159f6fb408201aa2fa41b5fb019e8abf8fc32800805a2743cfa15373cf64ba16f4f70e683d8e0404a192d9050404f993d9050404e594d90508208642542ff3ce7d63b9703e85eb924ab3071aa39c25b1651c6dda4216387478f10404bd96d905"
	realDataBytes, err := hex.DecodeString(realDataHex)
	if err != nil {
		b.Fatal(err)
	}
	realData := BranchData(realDataBytes)

	// --- Sub-benchmarks ---

	b.Run("Shorten/Dense", func(b *testing.B) {
		buf := make([]byte, 0, len(denseEnc))
		benchReplacePlainKeys(b, denseEnc, buf, shortenFn)
	})

	b.Run("Shorten/Sparse", func(b *testing.B) {
		buf := make([]byte, 0, len(sparseEnc))
		benchReplacePlainKeys(b, sparseEnc, buf, shortenFn)
	})

	b.Run("Expand/Dense", func(b *testing.B) {
		buf := make([]byte, 0, len(denseEnc))
		benchReplacePlainKeys(b, denseShortened, buf, func(key []byte, isStorage bool) ([]byte, error) {
			return denseKeyMap[string(key)], nil
		})
	})

	b.Run("Expand/Sparse", func(b *testing.B) {
		buf := make([]byte, 0, len(sparseEnc))
		benchReplacePlainKeys(b, sparseShortened, buf, func(key []byte, isStorage bool) ([]byte, error) {
			return sparseKeyMap[string(key)], nil
		})
	})

	b.Run("NoChange/Dense", func(b *testing.B) {
		buf := make([]byte, 0, len(denseEnc))
		benchReplacePlainKeys(b, denseEnc, buf, func(key []byte, isStorage bool) ([]byte, error) {
			return nil, nil
		})
	})

	b.Run("BufferReuse/FreshAlloc", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := denseEnc.ReplacePlainKeys(make([]byte, 0, len(denseEnc)), shortenFn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("RealData/NoChange", func(b *testing.B) {
		buf := make([]byte, 0, len(realData))
		benchReplacePlainKeys(b, realData, buf, func(key []byte, isStorage bool) ([]byte, error) {
			return nil, nil
		})
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

// populateUpdates inserts n unique keys into the given Updates instance.
// Each key is 20 bytes (account-sized) with a unique 8-byte suffix.
func populateUpdates(b *testing.B, upd *Updates, n int) {
	b.Helper()
	key := make([]byte, 20)
	val := make([]byte, 8)
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(key[12:], uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i+1))
		upd.TouchPlainKey(string(key), val, upd.TouchStorage)
	}
}

func BenchmarkHashSort_ModeDirect(b *testing.B) {
	for _, n := range []int{50, 5000, 50000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			upd := NewUpdates(ModeDirect, b.TempDir(), keyHasherNoop)

			ctx := context.Background()
			noop := func(hk, pk []byte, update *Update) error { return nil }

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Re-populate for each iteration since ETL is consumed by Load
				b.StopTimer()
				upd.Reset()
				populateUpdates(b, upd, n)
				b.StartTimer()

				if err := upd.HashSort(ctx, nil, noop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHashSort_ModeUpdate(b *testing.B) {
	for _, n := range []int{50, 5000, 50000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			upd := NewUpdates(ModeUpdate, b.TempDir(), keyHasherNoop)

			ctx := context.Background()
			noop := func(hk, pk []byte, update *Update) error { return nil }

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Re-populate for each iteration since HashSort clears the tree
				b.StopTimer()
				populateUpdates(b, upd, n)
				b.StartTimer()

				if err := upd.HashSort(ctx, nil, noop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
