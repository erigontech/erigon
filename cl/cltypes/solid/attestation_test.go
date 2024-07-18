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

package solid

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestAttestationData(t *testing.T) {
	slot := uint64(123)
	validatorIndex := uint64(456)
	beaconBlockRoot := common.HexToHash("0x63426b1ac6f47473ce3386469f2408f992a0a18c52e343d63b6872be45f4e6f2")
	source := NewCheckpointFromParameters(common.HexToHash("0x63426b1ac6f47473ce3386469f2408f992a0a18c52e343d63b6872be45f4e6f1"), 123)
	target := NewCheckpointFromParameters(common.HexToHash("0x63426b1ac6f47473ce3386469f2408f992a0a18c52e343d63b6872be45f4e6f3"), 456)

	attData := NewAttestionDataFromParameters(slot, validatorIndex, beaconBlockRoot, source, target)

	// Ensure that the data was set correctly
	assert.Equal(t, slot, attData.Slot())
	assert.Equal(t, validatorIndex, attData.CommitteeIndex())
	assert.Equal(t, beaconBlockRoot, attData.BeaconBlockRoot())
	assert.Equal(t, source, attData.Source())
	assert.Equal(t, target, attData.Target())

	// Test clone functionality
	clone := attData.Clone().(AttestationData)
	assert.Equal(t, NewAttestationData(), clone)

	// Test SSZ encoding and decoding
	encoded, err := attData.EncodeSSZ(nil)
	assert.NoError(t, err)

	clone = NewAttestationData()
	err = clone.DecodeSSZ(encoded, 0)
	assert.NoError(t, err)

	assert.Equal(t, attData, clone)

	// Test SSZ Hash
	_, err = attData.HashSSZ()
	assert.NoError(t, err)

	// Test equality
	assert.True(t, attData.Equal(clone))
	assert.False(t, attData.Equal(NewAttestationData()))
}

func TestAttestation(t *testing.T) {
	aggregationBits := []byte{1, 0, 1, 0, 1, 0, 1, 0}
	data := NewAttestationData()
	signature := [96]byte{}
	for i := range signature {
		signature[i] = byte(i)
	}

	// Test NewAttestionFromParameters
	attestation := NewAttestionFromParameters(aggregationBits, data, signature)
	assert.NotNil(t, attestation)

	// Test getters
	assert.Equal(t, aggregationBits, attestation.AggregationBits())
	assert.Equal(t, data, attestation.AttestantionData())
	assert.Equal(t, signature, attestation.Signature())

	// Test setters
	newData := NewAttestationData()
	newSignature := [96]byte{}
	for i := range newSignature {
		newSignature[i] = byte(95 - i)
	}
	attestation.SetAttestationData(newData)
	attestation.SetSignature(newSignature)
	assert.Equal(t, newData, attestation.AttestantionData())
	assert.Equal(t, newSignature, attestation.Signature())

	// Test Encoding and Decoding
	buf, err := attestation.EncodeSSZ(nil)
	assert.NoError(t, err)
	newAttestation := &Attestation{}
	err = newAttestation.DecodeSSZ(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, attestation, newAttestation)

	// Test HashSSZ
	hash, err := attestation.HashSSZ()
	assert.NoError(t, err)
	assert.NotNil(t, hash)

	// Test Clone
	cloned := attestation.Clone()
	assert.NotEqual(t, nil, cloned.(*Attestation))
}
