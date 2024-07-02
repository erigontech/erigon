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

	"github.com/stretchr/testify/assert"
)

func TestPendingAttestation(t *testing.T) {
	// Create sample data
	aggregationBits := []byte{1, 0, 1, 0}
	attestationData := AttestationData{1, 2, 3}
	inclusionDelay := uint64(10)
	proposerIndex := uint64(20)

	// Test NewPendingAttestionFromParameters
	pendingAttestation := NewPendingAttestionFromParameters(aggregationBits, attestationData, inclusionDelay, proposerIndex)
	assert.NotNil(t, pendingAttestation)
	assert.Equal(t, aggregationBits, pendingAttestation.AggregationBits())
	assert.Equal(t, inclusionDelay, pendingAttestation.InclusionDelay())
	assert.Equal(t, proposerIndex, pendingAttestation.ProposerIndex())

	// Test EncodingSizeSSZ
	expectedEncodingSize := pendingAttestationStaticBufferSize + len(aggregationBits)
	encodingSize := pendingAttestation.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := pendingAttestation.EncodeSSZ(nil)
	assert.NoError(t, err)
	decodedPendingAttestation := &PendingAttestation{}
	err = decodedPendingAttestation.DecodeSSZ(encodedData, encodingSize)
	assert.NoError(t, err)
	assert.Equal(t, pendingAttestation, decodedPendingAttestation)
}
