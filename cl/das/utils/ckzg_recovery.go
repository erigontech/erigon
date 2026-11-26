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

package peerdasutils

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
)

// ckzgPrecompute is the window size of the c-kzg multiplication tables: 8 recovers a blob in about 105 ms
// (143 ms at 0) for about 96 MiB of tables outside the Go heap.
const ckzgPrecompute = 8

var (
	ckzgOnce sync.Once
	ckzgErr  error
)

// ckzgReady loads the c-kzg settings on the first call and reports whether they can be used.
// The loaded settings are read-only, so concurrent recoveries may share them.
func ckzgReady() error {
	return loadCkzgOnce(kzg.TrustedSetup())
}

func loadCkzgOnce(setup *goethkzg.JSONTrustedSetup) error {
	ckzgOnce.Do(func() {
		if ckzgErr = loadCkzgSettings(setup); ckzgErr != nil {
			log.Error("[peerdas] failed to load the c-kzg settings, recovering cells with go-eth-kzg", "err", ckzgErr)
		}
	})
	return ckzgErr
}

// WarmUpRecovery loads the c-kzg settings in the background, so that the first recovery does not wait for them.
func WarmUpRecovery() {
	go func() { _ = ckzgReady() }()
}

// loadCkzgSettings loads the parsed trusted setup into c-kzg.
func loadCkzgSettings(setup *goethkzg.JSONTrustedSetup) error {
	g1Monomial, err := decodeHexPoints(setup.SetupG1Monomial[:])
	if err != nil {
		return err
	}
	g1Lagrange, err := decodeHexPoints(setup.SetupG1Lagrange[:])
	if err != nil {
		return err
	}
	g2Monomial, err := decodeHexPoints(setup.SetupG2)
	if err != nil {
		return err
	}
	return ckzg.LoadTrustedSetup(g1Monomial, g1Lagrange, g2Monomial, ckzgPrecompute)
}

func decodeHexPoints(points []string) ([]byte, error) {
	return hex.DecodeString(strings.ReplaceAll(strings.Join(points, ""), "0x", ""))
}

func recoverWithCkzg(cellIndices []ColumnIndex, cells []cltypes.Cell) ([]cltypes.Cell, []cltypes.KZGProof, error) {
	ckzgCells := make([]ckzg.Cell, len(cells))
	for i := range cells {
		ckzgCells[i] = ckzg.Cell(cells[i])
	}

	recoveredCells, recoveredProofs, err := ckzg.RecoverCellsAndKZGProofs(cellIndices, ckzgCells)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to recover cells and proofs: %w", err)
	}

	convertCells := make([]cltypes.Cell, len(recoveredCells))
	for i := range recoveredCells {
		convertCells[i] = cltypes.Cell(recoveredCells[i])
	}

	convertProofs := make([]cltypes.KZGProof, len(recoveredProofs))
	for i := range recoveredProofs {
		convertProofs[i] = cltypes.KZGProof(recoveredProofs[i])
	}
	return convertCells, convertProofs, nil
}
