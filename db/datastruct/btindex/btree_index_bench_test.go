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
			_, _, _, _, err := index.Get(keys[i%len(keys)], getter)
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
