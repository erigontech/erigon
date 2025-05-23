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
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/crate-crypto/go-eth-kzg"
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
)

var goethkzgContext *goethkzg.Context
var gokzgIniter sync.Once

// InitKZGCtx initializes the global context object returned via CryptoCtx
func InitGoEthKZGCtx() {
	gokzgIniter.Do(func() {
		if trustedSetupFile != "" {
			file, err := os.ReadFile(trustedSetupFile)
			if err != nil {
				panic(fmt.Sprintf("could not read file, err: %v", err))
			}

			setup := new(goethkzg.JSONTrustedSetup)
			if err = json.Unmarshal(file, setup); err != nil {
				panic(fmt.Sprintf("could not unmarshal, err: %v", err))
			}

			goethkzgContext, err = goethkzg.NewContext4096(setup)
			if err != nil {
				panic(fmt.Sprintf("could not create KZG context, err: %v", err))
			}
		} else {
			var err error
			// Initialize context to match the configurations that the
			// specs are using.
			goethkzgContext, err = goethkzg.NewContext4096Secure()
			if err != nil {
				panic(fmt.Sprintf("could not create context, err : %v", err))
			}
		}
	})
}

func GoEthKzgCtx() *goethkzg.Context {
	InitGoEthKZGCtx()
	return goethkzgContext
}

// VerifyCellProofBatch verifies the cellproofs in batch, corresponding to a set of blobs
func VerifyCellProofBatch(blobsBytes [][]byte, commitments []gokzg4844.KZGCommitment, cellProofs []gokzg4844.KZGProof) error {
	InitGoEthKZGCtx()
	var (
		commitsExt  = make([]goethkzg.KZGCommitment, 0, len(cellProofs))
		cellIndices = make([]uint64, 0, len(cellProofs))
		cells       = make([]*goethkzg.Cell, 0, len(cellProofs))
		proofs      = make([]goethkzg.KZGProof, len(cellProofs))
	)

	// Cast the cell proofs
	for i, proof := range cellProofs {
		proofs[i] = goethkzg.KZGProof(proof)
	}
	// Extend Commitments to be of the same size as CellProofs
	for _, commitment := range commitments {
		for range goethkzg.CellsPerExtBlob {
			commitsExt = append(commitsExt, goethkzg.KZGCommitment(commitment))
		}
	}
	// Compute cells and cellIndices
	for _, blob := range blobsBytes {
		cellsI, err := goethkzgContext.ComputeCells((*goethkzg.Blob)(blob), 2)
		if err != nil {
			return err
		}
		cells = append(cells, cellsI[:]...)
		for idx := range len(cellsI) {
			cellIndices = append(cellIndices, uint64(idx))
		}
	}
	return goethkzgContext.VerifyCellKZGProofBatch(commitsExt, cellIndices, cells[:], proofs)
}
