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
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
)

// testMatrix builds the full extended matrix of blobCount distinct dense blobs (sparse ones recover
// faster and would flatter the benchmark) and the half of it a node needs for recovery: the even
// columns, listed last row first.
func testMatrix(tb testing.TB, blobCount int) (full [][]cltypes.MatrixEntry, half []cltypes.MatrixEntry) {
	tb.Helper()
	for row := range blobCount {
		var blob goethkzg.Blob
		for i := range blob {
			if i%goethkzg.SerializedScalarSize != 0 { // a zero top byte keeps each field element canonical
				blob[i] = byte(row + i)
			}
		}
		cells, proofs, err := ComputeCellsAndKZGProofs(blob[:])
		require.NoError(tb, err)
		entries := make([]cltypes.MatrixEntry, 0, len(cells))
		for column := range cells {
			entries = append(entries, cltypes.MatrixEntry{
				Cell:        cells[column],
				KzgProof:    proofs[column],
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

// With several broken rows the error names the lowest one, whichever row fails first.
func TestRecoverMatrixReportsTheLowestBrokenRow(t *testing.T) {
	full, half := testMatrix(t, 3)
	half = slices.DeleteFunc(half, func(e cltypes.MatrixEntry) bool { return e.RowIndex > 0 && e.ColumnIndex > 10 })

	for range 20 {
		_, err := RecoverMatrix(half, uint64(len(full)))
		require.ErrorContains(t, err, "blob 1")
	}
}

// The row slots are shared by all calls: with every slot taken, no row starts, and each row gives
// its slot back when it finishes.
func TestRecoverMatrixWaitsForAFreeRowSlot(t *testing.T) {
	full, half := testMatrix(t, 1)
	for range cap(rowRecoverySlots) {
		rowRecoverySlots <- struct{}{}
	}
	done := make(chan error, 1)
	go func() {
		_, err := RecoverMatrix(half, uint64(len(full)))
		done <- err
	}()
	time.Sleep(time.Second)
	returnedWithoutSlot := len(done) > 0
	for range cap(rowRecoverySlots) {
		<-rowRecoverySlots
	}
	require.False(t, returnedWithoutSlot, "a row ran while every slot was taken")
	require.NoError(t, <-done)
	require.Zero(t, len(rowRecoverySlots))
}

func TestComputeBlobsCellsMatchesPerBlobCells(t *testing.T) {
	blobs := make([][]byte, 5)
	for i := range blobs {
		blob := goethkzg.Blob{}
		blob[31], blob[63] = byte(i+1), byte(i)
		blobs[i] = blob[:]
	}

	cells, err := ComputeBlobsCells(blobs)

	require.NoError(t, err)
	require.Len(t, cells, len(blobs))
	for i, blob := range blobs {
		want, _, err := ComputeCellsAndKZGProofs(blob)
		require.NoError(t, err)
		require.Equal(t, want, cells[i], "blob %d", i)
	}
}

func TestComputeBlobsCellsFailsOnInvalidBlob(t *testing.T) {
	invalid := goethkzg.Blob{}
	for i := range invalid {
		invalid[i] = 0xff // not a canonical field element
	}
	valid := goethkzg.Blob{}
	blobs := [][]byte{valid[:], valid[:], invalid[:], valid[:]}

	_, err := ComputeBlobsCells(blobs)

	require.ErrorContains(t, err, "blob 2")
}
