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

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"golang.org/x/sync/errgroup"

	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

// verifyBlobBundlesSerial verifies the cells of all blobs in a single batch, one blob after the other.
func verifyBlobBundlesSerial(bundles []BlobBundle) error {
	var (
		commitments []goethkzg.KZGCommitment
		cellIndices []uint64
		cells       []*goethkzg.Cell
		proofs      []goethkzg.KZGProof
	)
	for i := range bundles {
		blobCells, err := peerdasutils.ComputeCells(bundles[i].Blob)
		if err != nil {
			return err
		}
		bundles[i].Cells = blobCells
		for j := range blobCells {
			commitments = append(commitments, goethkzg.KZGCommitment(bundles[i].Commitment))
			cellIndices = append(cellIndices, uint64(j))
			cells = append(cells, (*goethkzg.Cell)(&blobCells[j]))
			proofs = append(proofs, goethkzg.KZGProof(bundles[i].KzgProofs[j]))
		}
	}
	return kzg.Ctx().VerifyCellKZGProofBatch(commitments, cellIndices, cells, proofs)
}

// BenchmarkVerifyBlobBundles times concurrent verifications of the same blob count; ns/op is the
// time until the slowest caller is done.
func BenchmarkVerifyBlobBundles(b *testing.B) {
	const maxBlobs = 21
	all := newCellProofBundles(b, maxBlobs)
	verifiers := []struct {
		name   string
		verify func([]BlobBundle) error
	}{
		{"serial", verifyBlobBundlesSerial},
		{"parallel", func(bundles []BlobBundle) error { return verifyBlobBundles(bundles, true) }},
	}
	for _, blobs := range []int{6, maxBlobs} {
		for _, callers := range []int{1, 2} {
			for _, verifier := range verifiers {
				b.Run(fmt.Sprintf("blobs=%d/callers=%d/%s", blobs, callers, verifier.name), func(b *testing.B) {
					sets := make([][]BlobBundle, callers)
					for i := range sets {
						sets[i] = slices.Clone(all[:blobs])
					}
					for b.Loop() {
						var g errgroup.Group
						for _, set := range sets {
							g.Go(func() error { return verifier.verify(set) })
						}
						if err := g.Wait(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
