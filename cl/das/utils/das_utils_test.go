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
	"slices"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

// testMatrix builds the full extended matrix of blobCount distinct blobs and the half of it a node
// needs for recovery: the even columns, listed last row first.
func testMatrix(tb testing.TB, blobCount int) (full [][]cltypes.MatrixEntry, half []cltypes.MatrixEntry) {
	tb.Helper()
	kzg.InitKZGCtx()
	for row := range blobCount {
		var blob goethkzg.Blob
		for i := 0; i < len(blob); i += goethkzg.SerializedScalarSize {
			var e fr.Element
			e.SetUint64(uint64(row*len(blob) + i + 1))
			b := e.Bytes()
			copy(blob[i:], b[:])
		}
		cells, proofs, err := kzg.Ctx().ComputeCellsAndKZGProofs(&blob, 0)
		require.NoError(tb, err)
		entries := make([]cltypes.MatrixEntry, 0, len(cells))
		for column := range cells {
			entries = append(entries, cltypes.MatrixEntry{
				Cell:        cltypes.Cell(*cells[column]),
				KzgProof:    cltypes.KZGProof(proofs[column]),
				ColumnIndex: ColumnIndex(column),
				RowIndex:    RowIndex(row),
			})
		}
		full = append(full, entries)
	}
	for _, row := range slices.Backward(full) {
		for column := 0; column < len(row); column += 2 {
			half = append(half, row[column])
		}
	}
	return full, half
}

func TestRecoverMatrixRestoresEveryRowInOrder(t *testing.T) {
	full, half := testMatrix(t, 3)

	recovered, err := RecoverMatrix(half, uint64(len(full)))

	require.NoError(t, err)
	require.Equal(t, full, recovered)
}

func TestRecoverMatrixReportsTheRowThatCannotBeRecovered(t *testing.T) {
	full, half := testMatrix(t, 3)
	half = slices.DeleteFunc(half, func(e cltypes.MatrixEntry) bool { return e.RowIndex == 1 && e.ColumnIndex > 10 })

	_, err := RecoverMatrix(half, uint64(len(full)))

	require.ErrorContains(t, err, "blob 1")
}
