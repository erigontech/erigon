// Copyright 2024 The Erigon Authors
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

package cltypes_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize global config for tests
	cfg := &clparams.BeaconChainConfig{
		MaxBlobCommittmentsPerBlock: 6,
		NumberOfColumns:             128,
		GloasForkEpoch:              18446744073709551615, // Max uint64 - Gloas not activated by default
	}
	clparams.InitGlobalStaticConfig(cfg, &clparams.CaplinConfig{})
}

func TestNewDataColumnSidecar(t *testing.T) {
	sidecar := cltypes.NewDataColumnSidecar()

	assert.NotNil(t, sidecar.Column, "Column should be initialized")
	assert.NotNil(t, sidecar.KzgProofs, "KzgProofs should be initialized")
	// Pre-Gloas fields should be initialized by default (version 0)
	assert.NotNil(t, sidecar.KzgCommitments, "KzgCommitments should be initialized for pre-Gloas")
	assert.NotNil(t, sidecar.SignedBlockHeader, "SignedBlockHeader should be initialized for pre-Gloas")
	assert.NotNil(t, sidecar.KzgCommitmentsInclusionProof, "KzgCommitmentsInclusionProof should be initialized for pre-Gloas")
}

func TestNewDataColumnSidecarWithVersionFulu(t *testing.T) {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)

	assert.Equal(t, clparams.FuluVersion, sidecar.Version())
	assert.NotNil(t, sidecar.Column, "Column should be initialized")
	assert.NotNil(t, sidecar.KzgProofs, "KzgProofs should be initialized")
	// Pre-Gloas fields should be initialized for Fulu
	assert.NotNil(t, sidecar.KzgCommitments, "KzgCommitments should be initialized for Fulu")
	assert.NotNil(t, sidecar.SignedBlockHeader, "SignedBlockHeader should be initialized for Fulu")
	assert.NotNil(t, sidecar.KzgCommitmentsInclusionProof, "KzgCommitmentsInclusionProof should be initialized for Fulu")
}

func TestNewDataColumnSidecarWithVersionGloas(t *testing.T) {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)

	assert.Equal(t, clparams.GloasVersion, sidecar.Version())
	assert.NotNil(t, sidecar.Column, "Column should be initialized")
	assert.NotNil(t, sidecar.KzgProofs, "KzgProofs should be initialized")
	// Pre-Gloas fields should NOT be initialized for Gloas
	assert.Nil(t, sidecar.KzgCommitments, "KzgCommitments should not be initialized for Gloas")
	assert.Nil(t, sidecar.SignedBlockHeader, "SignedBlockHeader should not be initialized for Gloas")
	assert.Nil(t, sidecar.KzgCommitmentsInclusionProof, "KzgCommitmentsInclusionProof should not be initialized for Gloas")
}

func TestDataColumnSidecarEncodeDecodeFulu(t *testing.T) {
	require := require.New(t)

	// Create a Fulu sidecar
	original := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	original.Index = 42
	original.SignedBlockHeader.Header.Slot = 1000
	original.SignedBlockHeader.Header.ProposerIndex = 123

	// Encode
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(err)

	// Decode
	decoded := cltypes.NewDataColumnSidecar()
	err = decoded.DecodeSSZ(encoded, int(clparams.FuluVersion))
	require.NoError(err)

	// Verify
	assert.Equal(t, original.Index, decoded.Index)
	assert.Equal(t, clparams.FuluVersion, decoded.Version())
	assert.Equal(t, original.SignedBlockHeader.Header.Slot, decoded.SignedBlockHeader.Header.Slot)
	assert.Equal(t, original.SignedBlockHeader.Header.ProposerIndex, decoded.SignedBlockHeader.Header.ProposerIndex)
}

func TestDataColumnSidecarEncodeDecodeGloas(t *testing.T) {
	require := require.New(t)

	// Create a Gloas sidecar
	original := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	original.Index = 42
	original.Slot = 2000
	original.BeaconBlockRoot = [32]byte{1, 2, 3, 4, 5}

	// Encode
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(err)

	// Decode
	decoded := cltypes.NewDataColumnSidecar()
	err = decoded.DecodeSSZ(encoded, int(clparams.GloasVersion))
	require.NoError(err)

	// Verify
	assert.Equal(t, original.Index, decoded.Index)
	assert.Equal(t, clparams.GloasVersion, decoded.Version())
	assert.Equal(t, original.Slot, decoded.Slot)
	assert.Equal(t, original.BeaconBlockRoot, decoded.BeaconBlockRoot)
}

func TestDataColumnSidecarEncodingSizeSSZ(t *testing.T) {
	// Fulu version should include pre-Gloas fields
	fuluSidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	fuluSize := fuluSidecar.EncodingSizeSSZ()

	// Gloas version should be smaller (no pre-Gloas fields)
	gloasSidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	gloasSize := gloasSidecar.EncodingSizeSSZ()

	// Fulu should include SignedBlockHeader, KzgCommitments, etc.
	// Gloas only has Index(8) + Column + KzgProofs + Slot(8) + BeaconBlockRoot(32)
	assert.Greater(t, fuluSize, gloasSize, "Fulu encoding size should be larger than Gloas")
}

func TestDataColumnSidecarClone(t *testing.T) {
	original := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	original.Slot = 1000
	original.BeaconBlockRoot = [32]byte{1, 2, 3}
	original.BlockRoot = [32]byte{4, 5, 6}

	cloned := original.Clone().(*cltypes.DataColumnSidecar)

	// Clone preserves metadata fields
	assert.Equal(t, original.Slot, cloned.Slot)
	assert.Equal(t, original.BeaconBlockRoot, cloned.BeaconBlockRoot)
	assert.Equal(t, original.BlockRoot, cloned.BlockRoot)
	assert.Equal(t, original.Version(), cloned.Version())

	// Clone initializes fields based on version
	assert.NotNil(t, cloned.Column)
	assert.NotNil(t, cloned.KzgProofs)
}

func TestDataColumnSidecarStatic(t *testing.T) {
	sidecar := cltypes.NewDataColumnSidecar()
	assert.False(t, sidecar.Static(), "DataColumnSidecar should not be static")
}

func TestDataColumnSidecarHashSSZ(t *testing.T) {
	require := require.New(t)

	// Fulu version
	fuluSidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	fuluSidecar.Index = 1
	fuluHash, err := fuluSidecar.HashSSZ()
	require.NoError(err)
	assert.NotEqual(t, [32]byte{}, fuluHash)

	// Gloas version
	gloasSidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	gloasSidecar.Index = 1
	gloasHash, err := gloasSidecar.HashSSZ()
	require.NoError(err)
	assert.NotEqual(t, [32]byte{}, gloasHash)

	// Different versions should produce different hashes
	assert.NotEqual(t, fuluHash, gloasHash)
}
