// Copyright 2025 The Erigon Authors
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

package kzg

import (
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
)

func BenchmarkComputeCells(b *testing.B) {
	blob := randBlob()
	blobs := []goethkzg.Blob{blob}
	// Warm up the KZG context
	_, _ = ComputeCells(blobs)
	b.ResetTimer()

	for b.Loop() {
		_, err := ComputeCells(blobs)
		if err != nil {
			b.Fatalf("failed to compute cells: %v", err)
		}
	}
}

func BenchmarkVerifyCells(b *testing.B) {
	blob := randBlob()
	kzgCtx := Ctx()

	commitment, err := kzgCtx.BlobToKZGCommitment(&blob, 0)
	if err != nil {
		b.Fatalf("failed to compute commitment: %v", err)
	}
	_, cellProofs, err := kzgCtx.ComputeCellsAndKZGProofs(&blob, 2)
	if err != nil {
		b.Fatalf("failed to compute cell proofs: %v", err)
	}
	cells, err := ComputeCells([]goethkzg.Blob{blob})
	if err != nil {
		b.Fatalf("failed to compute cells: %v", err)
	}

	indices := make([]uint64, 8)
	for i := range indices {
		indices[i] = uint64(i * 16) // spread across the blob
	}
	partialCells := make([]goethkzg.Cell, len(indices))
	partialProofs := make([]goethkzg.KZGProof, len(indices))
	for i, idx := range indices {
		partialCells[i] = cells[idx]
		partialProofs[i] = cellProofs[idx]
	}
	b.ResetTimer()

	for b.Loop() {
		if err := VerifyCells(partialCells, []goethkzg.KZGCommitment{commitment}, partialProofs, indices); err != nil {
			b.Fatalf("verification failed: %v", err)
		}
	}
}

func BenchmarkRecoverBlobs(b *testing.B) {
	blob := randBlob()
	blobs := []goethkzg.Blob{blob}
	cells, err := ComputeCells(blobs)
	if err != nil {
		b.Fatalf("failed to compute cells: %v", err)
	}

	// Use exactly DataPerBlob cells (minimum for recovery)
	indices := make([]uint64, DataPerBlob)
	for i := range indices {
		indices[i] = uint64(i)
	}
	partialCells := cells[:DataPerBlob]
	b.ResetTimer()

	for b.Loop() {
		_, err := RecoverBlobs(partialCells, indices)
		if err != nil {
			b.Fatalf("failed to recover blob: %v", err)
		}
	}
}
