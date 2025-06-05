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
	"github.com/stretchr/testify/require"
)

func TestPendingAttestation(t *testing.T) {
	// Create sample data
	aggregationBits := NewBitList(0, 2048)
	aggregationBits.Append(0xa)
	inclusionDelay := uint64(10)
	proposerIndex := uint64(20)

	// Test NewPendingAttestionFromParameters
	pendingAttestation := &PendingAttestation{
		AggregationBits: aggregationBits,
		InclusionDelay:  inclusionDelay,
		ProposerIndex:   proposerIndex,
		Data: &AttestationData{
			Source: Checkpoint{
				Epoch: 1,
				Root:  [32]byte{0, 4, 2, 6},
			},
			Target: Checkpoint{
				Epoch: 1,
				Root:  [32]byte{0, 4, 2, 6},
			},
		},
	}

	// Test EncodingSizeSSZ
	expectedEncodingSize := 149
	encodingSize := pendingAttestation.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := pendingAttestation.EncodeSSZ(nil)
	require.NoError(t, err)
	decodedPendingAttestation := &PendingAttestation{}
	err = decodedPendingAttestation.DecodeSSZ(encodedData, encodingSize)
	require.NoError(t, err)
	h1, _ := pendingAttestation.HashSSZ()
	h2, _ := decodedPendingAttestation.HashSSZ()
	assert.Equal(t, h1, h2)
}
