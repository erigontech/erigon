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
	"crypto/rand"
	mrand "math/rand"
	"slices"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	goethkzg "github.com/crate-crypto/go-eth-kzg"
)

// randFieldElement returns a random valid BLS12-381 scalar in serialized form.
func randFieldElement() [32]byte {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		panic("failed to get random field element")
	}
	var r fr.Element
	r.SetBytes(bytes)

	return goethkzg.SerializeScalar(r)
}

// randBlob returns a blob filled with random, properly-reduced field elements.
func randBlob() goethkzg.Blob {
	var blob goethkzg.Blob
	for i := 0; i < len(blob); i += goethkzg.SerializedScalarSize {
		fieldElementBytes := randFieldElement()
		copy(blob[i:i+goethkzg.SerializedScalarSize], fieldElementBytes[:])
	}
	return blob
}

// randCellIndices picks n random unique indices from [0, CellsPerBlob) in sorted order.
func randCellIndices(rng *mrand.Rand, n int) []uint64 {
	perm := rng.Perm(CellsPerBlob)
	indices := make([]uint64, n)
	for i := 0; i < n; i++ {
		indices[i] = uint64(perm[i])
	}
	slices.Sort(indices)
	return indices
}

// testBlobData holds precomputed blob data for reuse across test iterations.
type testBlobData struct {
	blobs       []goethkzg.Blob
	commitments []goethkzg.KZGCommitment
	cells       []goethkzg.Cell     // flat: blobs[i] cells at [i*CellsPerBlob : (i+1)*CellsPerBlob]
	proofs      []goethkzg.KZGProof // flat: blobs[i] proofs at [i*CellsPerBlob : (i+1)*CellsPerBlob]
}

func newTestBlobData(t *testing.T, blobCount int) *testBlobData {
	t.Helper()
	kzgCtx := Ctx()

	d := &testBlobData{
		blobs:       make([]goethkzg.Blob, blobCount),
		commitments: make([]goethkzg.KZGCommitment, blobCount),
	}
	for i := range blobCount {
		d.blobs[i] = randBlob()
		commitment, err := kzgCtx.BlobToKZGCommitment(&d.blobs[i], 0)
		if err != nil {
			t.Fatalf("failed to compute commitment for blob %d: %v", i, err)
		}
		d.commitments[i] = commitment

		_, cellProofs, err := kzgCtx.ComputeCellsAndKZGProofs(&d.blobs[i], 2)
		if err != nil {
			t.Fatalf("failed to compute cell proofs for blob %d: %v", i, err)
		}
		for _, p := range &cellProofs {
			d.proofs = append(d.proofs, p)
		}
	}
	cells, err := ComputeCells(d.blobs)
	if err != nil {
		t.Fatalf("failed to compute cells: %v", err)
	}
	d.cells = cells
	return d
}

// ---------------------------------------------------------------------------
// ComputeCells + VerifyCellProofBatch (full-blob verification)
// ---------------------------------------------------------------------------

func TestComputeCellsAndVerifyBatch(t *testing.T) {
	blob1 := randBlob()
	blob2 := randBlob()

	kzgCtx := Ctx()
	commitment1, err := kzgCtx.BlobToKZGCommitment(&blob1, 0)
	if err != nil {
		t.Fatalf("failed to create KZG commitment from blob: %v", err)
	}
	commitment2, err := kzgCtx.BlobToKZGCommitment(&blob2, 0)
	if err != nil {
		t.Fatalf("failed to create KZG commitment from blob: %v", err)
	}

	_, proofs1, err := kzgCtx.ComputeCellsAndKZGProofs(&blob1, 2)
	if err != nil {
		t.Fatalf("failed to compute cell proofs for blob 1: %v", err)
	}
	_, proofs2, err := kzgCtx.ComputeCellsAndKZGProofs(&blob2, 2)
	if err != nil {
		t.Fatalf("failed to compute cell proofs for blob 2: %v", err)
	}

	var cellProofs []goethkzg.KZGProof
	for _, p := range &proofs1 {
		cellProofs = append(cellProofs, p)
	}
	for _, p := range &proofs2 {
		cellProofs = append(cellProofs, p)
	}

	blobsBytes := [][]byte{blob1[:], blob2[:]}
	if err := VerifyCellProofBatch(blobsBytes, []goethkzg.KZGCommitment{commitment1, commitment2}, cellProofs); err != nil {
		t.Fatalf("failed to verify cell proof batch: %v", err)
	}
}

// ---------------------------------------------------------------------------
// VerifyCells (partial cell verification)
// ---------------------------------------------------------------------------

func TestVerifyPartialCells(t *testing.T) {
	const (
		iterations = 50
		blobCount  = 3
		cellsCount = 8
	)
	d := newTestBlobData(t, blobCount)

	for iter := range iterations {
		rng := mrand.New(mrand.NewSource(int64(iter)))
		indices := randCellIndices(rng, cellsCount)

		var partialCells []goethkzg.Cell
		var partialProofs []goethkzg.KZGProof
		for i := range blobCount {
			for _, idx := range indices {
				partialCells = append(partialCells, d.cells[i*CellsPerBlob+int(idx)])
				partialProofs = append(partialProofs, d.proofs[i*CellProofsPerBlob+int(idx)])
			}
		}
		if err := VerifyCells(partialCells, d.commitments, partialProofs, indices); err != nil {
			t.Fatalf("iter %d: failed to verify partial cells: %v", iter, err)
		}
	}
}

func TestVerifyCellsWithCorruptedCells(t *testing.T) {
	const blobCount = 3
	d := newTestBlobData(t, blobCount)
	indices := []uint64{0, 15, 63, 64, 95, 100, 120, 127}

	var partialCells []goethkzg.Cell
	var partialProofs []goethkzg.KZGProof
	for i := range blobCount {
		for _, idx := range indices {
			partialCells = append(partialCells, d.cells[i*CellsPerBlob+int(idx)])
			partialProofs = append(partialProofs, d.proofs[i*CellProofsPerBlob+int(idx)])
		}
	}
	// Corrupt the first cell
	corruptedCells := make([]goethkzg.Cell, len(partialCells))
	copy(corruptedCells, partialCells)
	corruptedCells[0][0] ^= 0xff

	if err := VerifyCells(corruptedCells, d.commitments, partialProofs, indices); err == nil {
		t.Fatal("expected verification failure with corrupted cell")
	}
}

func TestVerifyCellsWithCorruptedProofs(t *testing.T) {
	const blobCount = 3
	d := newTestBlobData(t, blobCount)
	indices := []uint64{0, 15, 63, 64, 95, 100, 120, 127}

	var partialCells []goethkzg.Cell
	var partialProofs []goethkzg.KZGProof
	for i := range blobCount {
		for _, idx := range indices {
			partialCells = append(partialCells, d.cells[i*CellsPerBlob+int(idx)])
			partialProofs = append(partialProofs, d.proofs[i*CellProofsPerBlob+int(idx)])
		}
	}
	// Swap first and last proof
	wrongProofs := make([]goethkzg.KZGProof, len(partialProofs))
	copy(wrongProofs, partialProofs)
	wrongProofs[0], wrongProofs[len(wrongProofs)-1] = wrongProofs[len(wrongProofs)-1], wrongProofs[0]

	if err := VerifyCells(partialCells, d.commitments, wrongProofs, indices); err == nil {
		t.Fatal("expected verification failure with swapped proofs")
	}
}

// ---------------------------------------------------------------------------
// VerifyCells input validation
// ---------------------------------------------------------------------------

func TestVerifyCellsValidation(t *testing.T) {
	d := newTestBlobData(t, 1)
	indices := []uint64{0, 1}
	cells := d.cells[:2]
	proofs := d.proofs[:2]

	t.Run("no commitments", func(t *testing.T) {
		if err := VerifyCells(cells, nil, proofs, indices); err == nil {
			t.Fatal("expected error for empty commitments")
		}
	})
	t.Run("proofs not multiple of commitments", func(t *testing.T) {
		// 2 proofs, 0 commitments would fail on "no commitments" first,
		// so use 3 proofs with 2 commitments to hit the modular check.
		threeProofs := append(proofs, proofs[0])
		threeCells := append(cells, cells[0])
		if err := VerifyCells(threeCells, d.commitments[:1], threeProofs, indices); err == nil {
			t.Fatal("expected error for mismatched proofs/commitments count")
		}
	})
	t.Run("mismatched cells and proofs", func(t *testing.T) {
		if err := VerifyCells(cells[:1], d.commitments[:1], proofs, indices); err == nil {
			t.Fatal("expected error for mismatched cells/proofs length")
		}
	})
	t.Run("no cellIndices", func(t *testing.T) {
		if err := VerifyCells(cells, d.commitments[:1], proofs, nil); err == nil {
			t.Fatal("expected error for nil cellIndices")
		}
	})
}

// ---------------------------------------------------------------------------
// RecoverBlobs
// ---------------------------------------------------------------------------

func TestRecoverBlob(t *testing.T) {
	d := newTestBlobData(t, 3)

	for iter := range 50 {
		rng := mrand.New(mrand.NewSource(int64(iter)))
		numCells := DataPerBlob + rng.Intn(CellsPerBlob-DataPerBlob+1)
		indices := randCellIndices(rng, numCells)

		var partialCells []goethkzg.Cell
		for bi := range 3 {
			for _, idx := range indices {
				partialCells = append(partialCells, d.cells[bi*CellsPerBlob+int(idx)])
			}
		}
		recovered, err := RecoverBlobs(partialCells, indices)
		if err != nil {
			t.Fatalf("iter %d: failed to recover blob with %d cells: %v", iter, numCells, err)
		}
		// Verify recovered blobs produce valid cell proofs
		var allProofs []goethkzg.KZGProof
		kzgCtx := Ctx()
		for i := range recovered {
			_, cellProofs, err := kzgCtx.ComputeCellsAndKZGProofs(&recovered[i], 2)
			if err != nil {
				t.Fatalf("iter %d: failed to compute cell proofs for recovered blob %d: %v", iter, i, err)
			}
			for _, p := range &cellProofs {
				allProofs = append(allProofs, p)
			}
		}
		recoveredBytes := make([][]byte, len(recovered))
		for i := range recovered {
			b := recovered[i]
			recoveredBytes[i] = b[:]
		}
		if err := VerifyCellProofBatch(recoveredBytes, d.commitments, allProofs); err != nil {
			t.Fatalf("iter %d: recovered blobs failed verification: %v", iter, err)
		}
		for i := range d.blobs {
			if recovered[i] != d.blobs[i] {
				t.Fatalf("iter %d: recovered blob %d does not match original", iter, i)
			}
		}
	}
}

func TestRecoverBlobWithInsufficientCells(t *testing.T) {
	const blobCount = 3
	d := newTestBlobData(t, blobCount)

	// Use DataPerBlob-1 cells (one short of minimum required)
	indices := make([]uint64, DataPerBlob-1)
	for i := range indices {
		indices[i] = uint64(i)
	}
	var partialCells []goethkzg.Cell
	for bi := range blobCount {
		for _, idx := range indices {
			partialCells = append(partialCells, d.cells[bi*CellsPerBlob+int(idx)])
		}
	}
	if _, err := RecoverBlobs(partialCells, indices); err == nil {
		t.Fatalf("expected error with only %d cells, got none", len(indices))
	}
}

// ---------------------------------------------------------------------------
// KZGToVersionedHash
// ---------------------------------------------------------------------------

func TestKZGToVersionedHash(t *testing.T) {
	kzgCtx := Ctx()
	blob := randBlob()
	commitment, err := kzgCtx.BlobToKZGCommitment(&blob, 0)
	if err != nil {
		t.Fatalf("failed to create commitment: %v", err)
	}
	vh := KZGToVersionedHash(commitment)

	// Version byte must be BlobCommitmentVersionKZG
	if vh[0] != BlobCommitmentVersionKZG {
		t.Fatalf("wrong version byte: got %#x, want %#x", vh[0], BlobCommitmentVersionKZG)
	}
	// Same input must produce the same hash
	vh2 := KZGToVersionedHash(commitment)
	if vh != vh2 {
		t.Fatal("KZGToVersionedHash is not deterministic")
	}
	// Different commitment must produce a different hash
	blob2 := randBlob()
	commitment2, err := kzgCtx.BlobToKZGCommitment(&blob2, 0)
	if err != nil {
		t.Fatalf("failed to create commitment: %v", err)
	}
	vh3 := KZGToVersionedHash(commitment2)
	if vh == vh3 {
		t.Fatal("different commitments produced the same versioned hash")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

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

	commitment, _ := kzgCtx.BlobToKZGCommitment(&blob, 0)
	_, cellProofs, _ := kzgCtx.ComputeCellsAndKZGProofs(&blob, 2)
	cells, _ := ComputeCells([]goethkzg.Blob{blob})

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
	cells, _ := ComputeCells(blobs)

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
