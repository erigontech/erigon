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
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

func randomBlobCells(t testing.TB, rng *rand.Rand) (blob goethkzg.Blob, cells []cltypes.Cell, proofs []cltypes.KZGProof) {
	t.Helper()
	for i := range blob {
		blob[i] = byte(rng.Uint32())
	}
	for i := 0; i < len(blob); i += goethkzg.SerializedScalarSize {
		blob[i] &= 0x3f // keeps each big-endian field element below the modulus
	}
	cells, proofs, err := ComputeCellsAndKZGProofs(blob[:])
	require.NoError(t, err)
	return blob, cells, proofs
}

// randomCells returns the given columns, ascending, of the cells of a random blob.
func randomCells(t testing.TB, rng *rand.Rand, given int) (indices []ColumnIndex, givenCells []cltypes.Cell) {
	t.Helper()
	_, cells, _ := randomBlobCells(t, rng)
	for _, column := range slices.Sorted(slices.Values(rng.Perm(len(cells))[:given])) {
		indices = append(indices, ColumnIndex(column))
		givenCells = append(givenCells, cells[column])
	}
	return indices, givenCells
}

// c-kzg reports every invalid input as ErrBadArgs, which tells it apart from go-eth-kzg.
func TestRecoverCellsAndKZGProofsRejectsInvalidInput(t *testing.T) {
	require.NoError(t, ckzgReady())
	indices, given := randomCells(t, rand.New(rand.NewPCG(1, 2)), 64)
	for _, tc := range []struct {
		name    string
		indices []ColumnIndex
		cells   []cltypes.Cell
	}{
		{"unsorted", slices.Concat(indices[1:2], indices[:1], indices[2:]), given},
		{"repeated", slices.Concat(indices[:1], indices[:1], indices[2:]), given},
		{"too few cells", indices[:63], given[:63]},
		{"index out of range", slices.Concat(indices[:63], []ColumnIndex{128}), given},
		{"fewer cells than indices", indices, given[:63]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := RecoverCellsAndKZGProofs(tc.indices, tc.cells)

			require.ErrorIs(t, err, ckzg.ErrBadArgs)
		})
	}
}

func TestRecoverCellsAndKZGProofsMatchesGoEthKZG(t *testing.T) {
	require.NoError(t, ckzgReady(), "the test would compare go-eth-kzg with itself")
	rng := rand.New(rand.NewPCG(3, 4))
	for _, given := range []int{64, 100, 127} {
		t.Run(strconv.Itoa(given), func(t *testing.T) {
			indices, givenCells := randomCells(t, rng, given)
			wantCells, wantProofs, err := recoverWithGoEthKZG(indices, givenCells)
			require.NoError(t, err)

			gotCells, gotProofs, err := RecoverCellsAndKZGProofs(indices, givenCells)

			require.NoError(t, err)
			require.True(t, slices.Equal(wantCells, gotCells), "recovered cells differ from go-eth-kzg")
			require.True(t, slices.Equal(wantProofs, gotProofs), "recovered proofs differ from go-eth-kzg")
		})
	}
}

// The embedded setup is a second copy of the one go-eth-kzg embeds: the commitment checks the Lagrange points, the
// recovered proofs check the monomial points and the batch verification checks the G2 points.
func TestCkzgDefaultSettingsMatchGoEthKZG(t *testing.T) {
	require.NoError(t, ckzgReady())
	blob, cells, proofs := randomBlobCells(t, rand.New(rand.NewPCG(5, 6)))
	wantCommitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0 /* numGoRoutines */)
	require.NoError(t, err)

	commitment, err := ckzg.BlobToKZGCommitment((*ckzg.Blob)(&blob))

	require.NoError(t, err)
	require.Equal(t, wantCommitment, goethkzg.KZGCommitment(commitment))
	commitments := make([]ckzg.Bytes48, len(cells))
	indices := make([]uint64, len(cells))
	ckzgCells := make([]ckzg.Cell, len(cells))
	ckzgProofs := make([]ckzg.Bytes48, len(cells))
	for i := range cells {
		commitments[i] = ckzg.Bytes48(commitment)
		indices[i] = uint64(i)
		ckzgCells[i] = ckzg.Cell(cells[i])
		ckzgProofs[i] = ckzg.Bytes48(proofs[i])
	}
	ok, err := ckzg.VerifyCellKZGProofBatch(commitments, indices, ckzgCells, ckzgProofs)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCkzgSettingsLoadFromTrustedSetupFile(t *testing.T) {
	require.NoError(t, ckzgReady())
	dir := t.TempDir()
	valid := filepath.Join(dir, "trusted_setup.json")
	require.NoError(t, os.WriteFile(valid, defaultTrustedSetup, 0o600))
	empty := filepath.Join(dir, "empty.json")
	require.NoError(t, os.WriteFile(empty, []byte("{}"), 0o600))
	indices, givenCells := randomCells(t, rand.New(rand.NewPCG(7, 8)), 64)
	wantCells, wantProofs, err := recoverWithGoEthKZG(indices, givenCells)
	require.NoError(t, err)
	ckzg.FreeTrustedSetup() // the binding holds one global setup; the loaded default is replaced below
	restored := false
	t.Cleanup(func() {
		if !restored {
			require.NoError(t, loadCkzgSettings(""))
		}
	})

	require.Error(t, loadCkzgSettings(empty))
	require.Error(t, loadCkzgSettings(filepath.Join(dir, "missing.json")))
	require.NoError(t, loadCkzgSettings(valid))
	restored = true

	gotCells, gotProofs, err := recoverWithCkzg(indices, givenCells)
	require.NoError(t, err)
	require.True(t, slices.Equal(wantCells, gotCells), "recovered cells differ from go-eth-kzg")
	require.True(t, slices.Equal(wantProofs, gotProofs), "recovered proofs differ from go-eth-kzg")
}

func TestRecoverCellsAndKZGProofsFallsBackToGoEthKZGWhenSettingsFailToLoad(t *testing.T) {
	kzg.InitKZGCtx() // go-eth-kzg reads the setup path once, before the broken one is set
	require.NoError(t, ckzgReady())
	indices, givenCells := randomCells(t, rand.New(rand.NewPCG(9, 10)), 64)
	wantCells, wantProofs, err := recoverWithGoEthKZG(indices, givenCells)
	require.NoError(t, err)
	kzg.SetTrustedSetupFilePath(filepath.Join(t.TempDir(), "missing.json"))
	ckzgOnce = sync.Once{}
	t.Cleanup(func() {
		kzg.SetTrustedSetupFilePath("")
		ckzgOnce = sync.Once{}
		ckzgOnce.Do(func() {})
		ckzgErr = nil
	})

	gotCells, gotProofs, err := RecoverCellsAndKZGProofs(indices, givenCells)

	require.NoError(t, err)
	require.Error(t, ckzgErr)
	require.True(t, slices.Equal(wantCells, gotCells), "recovered cells differ from go-eth-kzg")
	require.True(t, slices.Equal(wantProofs, gotProofs), "recovered proofs differ from go-eth-kzg")
	_, _, err = RecoverCellsAndKZGProofs(slices.Concat(indices[1:2], indices[:1], indices[2:]), givenCells)
	require.ErrorContains(t, err, "cell IDs are not ordered")
}
