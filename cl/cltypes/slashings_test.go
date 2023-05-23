package cltypes

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)

	decodedProposerSlashing := &ProposerSlashing{}
	err = decodedProposerSlashing.DecodeSSZ(encodedData, 0)
	assert.NoError(t, err)

	// Test EncodingSizeSSZ
	expectedEncodingSize := proposerSlashing.EncodingSizeSSZ()
	encodingSize := proposerSlashing.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	expectedRoot := common.HexToHash("0x5b69db0d6559ec57c3869eabc50cadb0a956071716b9174ed8647f23a37b6cd8") // Expected root value
	root, err := proposerSlashing.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))
}

func TestAttesterSlashing(t *testing.T) {
	// Create sample data
	attestation1 := &IndexedAttestation{
		AttestingIndices: solid.NewUint64ListSSZ(9192),
		Data:             solid.NewAttestationData(),
	}
	// Create an IndexedAttestation object
	attestation2 := &IndexedAttestation{
		AttestingIndices: solid.NewUint64ListSSZ(9192),
		Data:             solid.NewAttestationData(),
	}
	// Create AttesterSlashing
	attesterSlashing := &AttesterSlashing{
		Attestation_1: attestation1,
		Attestation_2: attestation2,
	}

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := attesterSlashing.EncodeSSZ(nil)
	assert.NoError(t, err)

	decodedAttesterSlashing := &AttesterSlashing{}
	err = decodedAttesterSlashing.DecodeSSZ(encodedData, 0)
	assert.NoError(t, err)

	// Test EncodingSizeSSZ
	expectedEncodingSize := attesterSlashing.EncodingSizeSSZ()
	encodingSize := attesterSlashing.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	expectedRoot := common.HexToHash("54b2c5a7b42c22af13ee41982858a6977af16358b5ced64f985385944c305e99") // Expected root value
	root, err := attesterSlashing.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))
}
