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
	"errors"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
)

// VerifyCellProofBatch verifies the cellproofs in batch, corresponding to a set of blobs
func VerifyCellProofBatch(blobsBytes [][]byte, commitments []goethkzg.KZGCommitment, cellProofs []goethkzg.KZGProof) error {
	InitKZGCtx()

	totalCells := len(blobsBytes) * goethkzg.CellsPerExtBlob
	commitsExt := make([]goethkzg.KZGCommitment, totalCells)
	cellIndices := make([]uint64, totalCells)
	cells := make([]*goethkzg.Cell, totalCells)

	// Fill commitments: each commitment is repeated CellsPerExtBlob times.
	off := 0
	for _, commitment := range commitments {
		for range goethkzg.CellsPerExtBlob {
			commitsExt[off] = commitment
			off++
		}
	}

	// Build the per-blob index template [0, 1, …, CellsPerExtBlob-1] once.
	var idxTemplate [goethkzg.CellsPerExtBlob]uint64
	for i := range idxTemplate {
		idxTemplate[i] = uint64(i)
	}

	// Compute cells and fill cellIndices per blob.
	for i, blob := range blobsBytes {
		cellsI, err := gokzgCtx.ComputeCells((*goethkzg.Blob)(blob), 2)
		if err != nil {
			return err
		}
		base := i * goethkzg.CellsPerExtBlob
		copy(cellIndices[base:], idxTemplate[:])
		copy(cells[base:], cellsI[:])
	}
	return gokzgCtx.VerifyCellKZGProofBatch(commitsExt, cellIndices, cells, cellProofs)
}

// VerifyCells verifies a batch of proofs corresponding to the cells and blob commitments.
//
// For this function, it is sufficient to only provide some of the cells.
//
// The `cellIndices` specify which of the 128 cells of each blob are given.
// Indices must be given in ascending order.
//
// Note the list of indices is shared among all blobs, i.e. for a given list of indices
// [1, 2, 13], the cells slice must contain cells [1, 2, 13] of each blob.
// Thus, `len(cells)` must be a multiple of `len(cellIndices)`.
//
// One proof must be given for each cell. As such, `len(proofs)` must equal `len(cells)`.
func VerifyCells(cells []goethkzg.Cell, commitments []goethkzg.KZGCommitment, proofs []goethkzg.KZGProof, cellIndices []uint64) error {
	// commitments/proofs/cells validation
	switch {
	case len(commitments) == 0:
		return errors.New("no commitments")
	case len(proofs)%len(commitments) != 0:
		return errors.New("len(proofs) must be a multiple of len(commitments)")
	case len(cells) != len(proofs):
		return errors.New("mismatched len(cellProofs) and len(cells)")
	}
	if err := validateCellIndices(cells, cellIndices); err != nil {
		return err
	}
	if len(cells)/len(cellIndices) != len(commitments) {
		return errors.New("invalid number of cells for blob count")
	}

	return gokzgVerifyCells(cells, commitments, proofs, cellIndices)
}

// ComputeCells computes the cells from the given blobs.
func ComputeCells(blobs []goethkzg.Blob) ([]goethkzg.Cell, error) {
	InitKZGCtx()
	cells := make([]goethkzg.Cell, goethkzg.CellsPerExtBlob*len(blobs))

	for i := range blobs {
		cellsI, err := gokzgCtx.ComputeCells((*goethkzg.Blob)(&blobs[i]), 2)
		if err != nil {
			return []goethkzg.Cell{}, err
		}
		base := i * goethkzg.CellsPerExtBlob
		for j, c := range &cellsI {
			cells[base+j] = *c
		}
	}
	return cells, nil
}

// RecoverBlobs recovers blobs from the given cells and cell indices.
// In order to successfully recover, at least DataPerBlob (64) cells must be provided.
//
// For the layout of cells and cellIndices, please see [VerifyCells].
func RecoverBlobs(cells []goethkzg.Cell, cellIndices []uint64) ([]goethkzg.Blob, error) {
	if err := validateCellIndices(cells, cellIndices); err != nil {
		return nil, err
	}
	return gokzgRecoverBlobs(cells, cellIndices)
}

func validateCellIndices(cells []goethkzg.Cell, cellIndices []uint64) error {
	switch {
	case len(cellIndices) == 0:
		return errors.New("no cellIndices given")
	case len(cellIndices) > len(cells):
		return errors.New("less cells than cellIndices")
	case len(cellIndices) > CellsPerBlob:
		return errors.New("too many cellIndices")
	case len(cells)%len(cellIndices) != 0:
		return errors.New("len(cells) must be a multiple of len(cellIndices)")
	}
	// The library checks the canonical ordering of indices, so we don't have to do it here.
	return nil
}

// gokzgVerifyCells verifies that the cell data corresponds to the provided commitment.
func gokzgVerifyCells(cells []goethkzg.Cell, commitments []goethkzg.KZGCommitment, cellProofs []goethkzg.KZGProof, cellIndices []uint64) error {
	InitKZGCtx()

	n := len(cellProofs)
	cellsPerCommit := n / len(commitments)

	commits := make([]goethkzg.KZGCommitment, n)
	indices := make([]uint64, n)
	kzgcells := make([]*goethkzg.Cell, n)

	for i := range n {
		kzgcells[i] = &cells[i]
	}

	// Expand commitments: each repeated cellsPerCommit times.
	off := 0
	for i := range commitments {
		for range cellsPerCommit {
			commits[off] = commitments[i]
			off++
		}
	}

	for i := range len(commitments) {
		copy(indices[i*len(cellIndices):], cellIndices)
	}
	return gokzgCtx.VerifyCellKZGProofBatch(commits, indices, kzgcells, cellProofs)
}

// gokzgRecoverBlobs recovers blobs from cells and cell indices.
func gokzgRecoverBlobs(cells []goethkzg.Cell, cellIndices []uint64) ([]goethkzg.Blob, error) {
	InitKZGCtx()

	blobCount := len(cells) / len(cellIndices)
	blobs := make([]goethkzg.Blob, blobCount)
	kzgcells := make([]*goethkzg.Cell, len(cellIndices))

	for i := range blobCount {
		offset := i * len(cellIndices)
		for j := range len(cellIndices) {
			kzgcells[j] = &cells[offset+j]
		}

		extCells, err := gokzgCtx.RecoverCells(cellIndices, kzgcells, 2)
		if err != nil {
			return nil, err
		}

		for j, cell := range extCells[:DataPerBlob] {
			copy(blobs[i][j*len(cell):], cell[:])
		}
	}
	return blobs, nil
}
