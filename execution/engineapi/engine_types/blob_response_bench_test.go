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

package engine_types

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// BenchmarkBlobsBundleV2Marshal compares the worst-case getBlobsV3 response (128 blobs, each with
// its full set of cell proofs) encoded by stdlib reflection vs MarshalFastJSONTo.
func BenchmarkBlobsBundleV2Marshal(b *testing.B) {
	bundle := worstCaseBundleV2()
	enc, _ := jsonstream.Marshal(bundle)
	size := int64(len(enc))

	b.Run("stdlib_reflect", func(b *testing.B) {
		slice := []*BlobAndProofV2(bundle)
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(slice); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fast", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := jsonstream.Marshal(bundle); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBlobCellsAndProofsV1Marshal(b *testing.B) {
	for _, blobs := range []int{6, 128} {
		for _, cells := range []int{8, 64, int(params.CellsPerExtBlob)} {
			b.Run(fmt.Sprintf("blobs=%d/cells=%d", blobs, cells), func(b *testing.B) {
				slice := blobCellsAndProofsBundle(blobs, cells)
				bundle := BlobsBundleV3(slice)
				enc, err := json.Marshal(slice)
				if err != nil {
					b.Fatal(err)
				}
				size := int64(len(enc))
				b.Run("stdlib_reflect", func(b *testing.B) {
					b.SetBytes(size)
					b.ReportAllocs()
					for b.Loop() {
						if _, err := json.Marshal(slice); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("fast", func(b *testing.B) {
					b.SetBytes(size)
					b.ReportAllocs()
					for b.Loop() {
						if _, err := jsonstream.Marshal(bundle); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
