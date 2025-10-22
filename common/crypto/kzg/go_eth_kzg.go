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
	goethkzg "github.com/crate-crypto/go-eth-kzg"
)

// VerifyCellProofBatch verifies the cellproofs in batch, corresponding to a set of blobs
func VerifyCellProofBatch(blobsBytes [][]byte, commitments []goethkzg.KZGCommitment, cellProofs []goethkzg.KZGProof) error {
	InitKZGCtx()
	var (
		commitsExt  = make([]goethkzg.KZGCommitment, 0, len(cellProofs))
		cellIndices = make([]uint64, 0, len(cellProofs))
		cells       = make([]*goethkzg.Cell, 0, len(cellProofs))
	)

	// Extend Commitments to be of the same size as CellProofs
	for _, commitment := range commitments {
		for range goethkzg.CellsPerExtBlob {
			commitsExt = append(commitsExt, commitment)
		}
	}
	// Compute cells and cellIndices
	for _, blob := range blobsBytes {
		cellsI, err := gokzgCtx.ComputeCells((*goethkzg.Blob)(blob), 2)
		if err != nil {
			return err
		}
		cells = append(cells, cellsI[:]...)
		for idx := range len(cellsI) {
			cellIndices = append(cellIndices, uint64(idx))
		}
	}
	return gokzgCtx.VerifyCellKZGProofBatch(commitsExt, cellIndices, cells, cellProofs)
}
