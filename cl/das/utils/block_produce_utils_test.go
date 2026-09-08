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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func TestGetDataColumnSidecarsGloasUsesProgressiveLists(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	cfg := *clparams.GetBeaconConfig()
	cfg.NumberOfColumns = 1
	cells := make([]cltypes.Cell, cfg.NumberOfColumns)
	proofs := make([]cltypes.KZGProof, cfg.NumberOfColumns)
	cells[0][0] = 1
	proofs[0][0] = 2

	sidecars, err := GetDataColumnSidecarsGloas(&cfg, 3, common.HexToHash("0x1234"), []CellsAndKZGProofs{{Blobs: cells, Proofs: proofs}})
	require.NoError(t, err)
	require.Len(t, sidecars, 1)
	expected := solid.NewStaticProgressiveListSSZ[*cltypes.Cell](int(cfg.MaxBlobCommittmentsPerBlock), cltypes.BytesPerCell)
	expected.Append(&cells[0])
	wantRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := sidecars[0].Column.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	expectedProofs := solid.NewStaticProgressiveListSSZ[*cltypes.KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), cltypes.BYTES_KZG_PROOF)
	expectedProofs.Append(&proofs[0])
	wantProofRoot, err := expectedProofs.HashSSZ()
	require.NoError(t, err)
	gotProofRoot, err := sidecars[0].KzgProofs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantProofRoot, gotProofRoot)
}

func TestGetDataColumnSidecarsGloasRejectsIncompleteCellVectors(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}

	cfg := *clparams.GetBeaconConfig()
	cfg.NumberOfColumns = 1
	require.NotPanics(t, func() {
		_, err := GetDataColumnSidecarsGloas(&cfg, 3, common.Hash{}, []CellsAndKZGProofs{{Blobs: nil, Proofs: nil}})
		require.ErrorContains(t, err, "incomplete cell data")
	})
}
