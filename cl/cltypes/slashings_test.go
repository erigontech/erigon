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

package cltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestProposerSlashing(t *testing.T) {
	// Create sample data
	header1 := &SignedBeaconBlockHeader{
		Header: &BeaconBlockHeader{
			Slot: 69,
		}} // Create a SignedBeaconBlockHeader object
	header2 := &SignedBeaconBlockHeader{
		Header: &BeaconBlockHeader{
			Slot: 99,
		}} // Create another SignedBeaconBlockHeader object

	// Create ProposerSlashing
	proposerSlashing := &ProposerSlashing{
		Header1: header1,
		Header2: header2,
	}

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := proposerSlashing.EncodeSSZ(nil)
	require.NoError(t, err)

	decodedProposerSlashing := &ProposerSlashing{}
	err = decodedProposerSlashing.DecodeSSZ(encodedData, 0)
	require.NoError(t, err)

	// Test EncodingSizeSSZ
	expectedEncodingSize := proposerSlashing.EncodingSizeSSZ()
	encodingSize := proposerSlashing.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	expectedRoot := common.HexToHash("0x5b69db0d6559ec57c3869eabc50cadb0a956071716b9174ed8647f23a37b6cd8") // Expected root value
	root, err := proposerSlashing.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))
}

func TestAttesterSlashing(t *testing.T) {
	// Create sample data
	attestation1 := &IndexedAttestation{
		AttestingIndices: solid.NewRawUint64List(9192, nil),
		Data:             &solid.AttestationData{},
	}
	// Create an IndexedAttestation object
	attestation2 := &IndexedAttestation{
		AttestingIndices: solid.NewRawUint64List(9192, nil),
		Data:             &solid.AttestationData{},
	}
	// Create AttesterSlashing
	attesterSlashing := &AttesterSlashing{
		Attestation_1: attestation1,
		Attestation_2: attestation2,
	}

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := attesterSlashing.EncodeSSZ(nil)
	require.NoError(t, err)

	decodedAttesterSlashing := &AttesterSlashing{}
	err = decodedAttesterSlashing.DecodeSSZ(encodedData, 0)
	require.NoError(t, err)

	// Test EncodingSizeSSZ
	expectedEncodingSize := attesterSlashing.EncodingSizeSSZ()
	encodingSize := attesterSlashing.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	expectedRoot := common.HexToHash("54b2c5a7b42c22af13ee41982858a6977af16358b5ced64f985385944c305e99") // Expected root value
	root, err := attesterSlashing.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))
}
