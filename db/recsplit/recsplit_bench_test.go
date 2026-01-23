// Copyright 2021 The Erigon Authors
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

package recsplit

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

func BenchmarkRecSplitBuild(b *testing.B) {
	logger := log.New()
	benchSizes := []int{1000, 10000, 100000}

	for _, size := range benchSizes {
		keys := make([][]byte, size)
		// Add keys
		for j := 0; j < size; j++ {
			keys[j] = fmt.Appendf(nil, "key_%d", j)
		}

		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tmpDir := b.TempDir()
				indexFile := filepath.Join(tmpDir, "index")
				salt := uint32(1)

				rs, err := NewRecSplit(RecSplitArgs{
					KeyCount:   size,
					BucketSize: 100,
					Salt:       &salt,
					TmpDir:     tmpDir,
					IndexFile:  indexFile,
					LeafSize:   8,
					NoFsync:    true,
				}, logger)
				if err != nil {
					b.Fatal(err)
				}

				// Add keys
				for j := uint64(0); j < uint64(size); j++ {
					if err := rs.AddKey(keys[j], j); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()
				// Benchmark only the Build phase
				if err := rs.Build(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()

				rs.Close()
			}
		})
	}
}

func BenchmarkRecSplitBuildLarge(b *testing.B) {
	logger := log.New()
	size := 100000

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		indexFile := filepath.Join(tmpDir, "index")
		salt := uint32(1)

		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   size,
			BucketSize: 100,
			Salt:       &salt,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
			NoFsync:    true,
			Enums:      true,
		}, logger)
		if err != nil {
			b.Fatal(err)
		}

		// Add keys
		for j := 0; j < size; j++ {
			if err := rs.AddKey(fmt.Appendf(nil, "key_%d", j), uint64(j*17)); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		// Benchmark only the Build phase
		if err := rs.Build(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		rs.Close()
	}
}
