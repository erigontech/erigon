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

package handler

import (
	"fmt"
	"slices"
	"testing"

	"golang.org/x/sync/errgroup"
)

// BenchmarkVerifyBlobBundles times concurrent verifications of the same blob count; ns/op is the
// time until the slowest caller is done.
func BenchmarkVerifyBlobBundles(b *testing.B) {
	const maxBlobs = 21
	all := newCellProofBundles(b, maxBlobs)
	for _, blobs := range []int{6, maxBlobs} {
		for _, callers := range []int{1, 2} {
			b.Run(fmt.Sprintf("blobs=%d/callers=%d", blobs, callers), func(b *testing.B) {
				sets := make([][]BlobBundle, callers)
				for i := range sets {
					sets[i] = slices.Clone(all[:blobs])
				}
				for b.Loop() {
					var g errgroup.Group
					for _, set := range sets {
						g.Go(func() error { return verifyBlobBundles(set, true) })
					}
					if err := g.Wait(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
