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

package btindex

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func Benchmark_BtreeIndex_GetVsGetValSize(b *testing.B) {
	tmp := b.TempDir()
	logger := log.New()
	compressFlags := seg.CompressVals
	dataPath := generateKV(b, tmp, 20, 64*1024, 512, logger, compressFlags)
	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(b, dataPath, indexPath, compressFlags, 1, logger, true)
	kvFile, index, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, compressFlags, false)
	require.NoError(b, err)
	defer index.Close()
	defer kvFile.Close()
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(b, err)
	getter := seg.NewReader(kvFile.MakeGetter(), compressFlags)

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _, _, err := index.Get(keys[i%len(keys)], nil, getter)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("GetValSize", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, err := index.GetValSize(keys[i%len(keys)], getter)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBtIndex_Get(b *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	for _, M := range []uint64{256, 128, 64, 32} {
		tmp := b.TempDir()
		kvPath := generateKV(b, tmp, 20, 10, keyCount, log.New(), compress)
		keys, err := pivotKeysFromKV(kvPath)
		require.NoError(b, err)

		indexPath := filepath.Join(tmp, fmt.Sprintf("m%d.bt", M))
		buildBtreeIndexWithM(b, kvPath, indexPath, compress, M, log.New())

		b.Run(fmt.Sprintf("M%d", M), func(b *testing.B) {
			decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
			require.NoError(b, err)
			defer bt.Close()
			defer decomp.Close()

			getter := seg.NewReader(decomp.MakeGetter(), compress)
			rnd := newRnd(uint64(b.N))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				p := rnd.IntN(len(keys))
				k, _, _, found, err := bt.Get(keys[p], nil, getter)
				if err != nil {
					b.Fatal(err)
				}
				if !found || !bytes.Equal(keys[p], k) {
					b.Fatal("key not found or mismatch")
				}
			}
		})
	}
}
