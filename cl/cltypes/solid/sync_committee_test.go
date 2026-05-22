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

	"github.com/erigontech/erigon/common"
)

func TestSyncCommittee(t *testing.T) {
	// Test NewSyncCommitteeFromParameters
	committee := make([]common.Bytes48, 512)
	aggregatePublicKey := [48]byte{1, 2, 3} // Example aggregate public key
	syncCommittee := NewSyncCommitteeFromParameters(committee, aggregatePublicKey)
	assert.NotNil(t, syncCommittee)

	// Test GetCommittee
	gotCommittee := syncCommittee.GetCommittee()
	assert.Equal(t, committee, gotCommittee)

	// Test SetCommittee
	newCommittee := make([]common.Bytes48, 512)
	for i := 0; i < 512; i++ {
		copy(newCommittee[i][:], []byte{byte(i)})
	}
	syncCommittee.SetCommittee(newCommittee)
	updatedCommittee := syncCommittee.GetCommittee()
	assert.Equal(t, newCommittee, updatedCommittee)

	// Test AggregatePublicKey
	gotAggregatePublicKey := syncCommittee.AggregatePublicKey()
	assert.Equal(t, common.Bytes48(aggregatePublicKey), gotAggregatePublicKey)

	// Test SetAggregatePublicKey
	newAggregatePublicKey := [48]byte{4, 5, 6} // Example new aggregate public key
	syncCommittee.SetAggregatePublicKey(newAggregatePublicKey)
	updatedAggregatePublicKey := syncCommittee.AggregatePublicKey()
	assert.Equal(t, common.Bytes48(newAggregatePublicKey), updatedAggregatePublicKey)

	// Test EncodingSizeSSZ
	expectedEncodingSize := (defaultSyncCommitteeSize + 1) * 48
	encodingSize := syncCommittee.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := syncCommittee.EncodeSSZ(nil)
	require.NoError(t, err)
	decodedSyncCommittee := &SyncCommittee{}
	err = decodedSyncCommittee.DecodeSSZ(encodedData, encodingSize)
	require.NoError(t, err)
	assert.Equal(t, syncCommittee, decodedSyncCommittee)

	// Test Clone
	clone := syncCommittee.Clone().(*SyncCommittee)
	assert.NotNil(t, clone)

	// Test Copy
	copy := syncCommittee.Copy()
	assert.Equal(t, syncCommittee, copy)

	// Test Equal
	otherSyncCommittee := &SyncCommittee{}
	assert.False(t, syncCommittee.Equal(otherSyncCommittee))
	assert.True(t, syncCommittee.Equal(syncCommittee)) //nolint:gocritic

	// Test HashSSZ
	expectedRoot := common.HexToHash("28628f3f10fa1070f2a42aeeeae792cd6ded1ef81030104e765e1498a1cfcfbd") // Example expected root
	root, err := syncCommittee.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))

	// Test Static
	assert.True(t, syncCommittee.Static())
}

func TestSyncCommitteeMinimalPreset(t *testing.T) {
	const minimalSize = 32

	// Build a minimal-preset committee with distinct keys.
	committee := make([]common.Bytes48, minimalSize)
	for i := range committee {
		committee[i][0] = byte(i + 1)
	}
	aggregatePublicKey := common.Bytes48{0xAB}

	sc := NewSyncCommitteeFromParameters(committee, aggregatePublicKey)
	require.Equal(t, minimalSize, sc.CommitteeSize())

	// GetCommittee round-trip.
	assert.Equal(t, committee, sc.GetCommittee())

	// AggregatePublicKey round-trip.
	assert.Equal(t, aggregatePublicKey, sc.AggregatePublicKey())

	// EncodingSizeSSZ must reflect minimal size.
	assert.Equal(t, (minimalSize+1)*48, sc.EncodingSizeSSZ())

	// SSZ encode → decode round-trip using correctly-sized target.
	encoded, err := sc.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, (minimalSize+1)*48, len(encoded))

	decoded := NewSyncCommitteeWithSize(minimalSize)
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	assert.Equal(t, sc, decoded)

	// Clone preserves size.
	cloned := sc.Clone().(*SyncCommittee)
	assert.Equal(t, minimalSize, cloned.CommitteeSize())

	// Copy preserves data.
	copied := sc.Copy()
	assert.True(t, sc.Equal(copied))

	// JSON round-trip.
	jsonData, err := sc.MarshalJSON()
	require.NoError(t, err)
	fromJSON := &SyncCommittee{}
	require.NoError(t, fromJSON.UnmarshalJSON(jsonData))
	assert.True(t, sc.Equal(fromJSON))
}

func TestSyncCommitteeJson(t *testing.T) {
	// Test MarshalJSON and UnmarshalJSON
	committee := make([]common.Bytes48, 512)
	for i := 0; i < 512; i++ {
		copy(committee[i][:], []byte{byte(i)})
	}
	aggregatePublicKey := [48]byte{1, 2, 3} // Example aggregate public key
	syncCommittee := NewSyncCommitteeFromParameters(committee, aggregatePublicKey)
	encodedData, err := syncCommittee.MarshalJSON()
	require.NoError(t, err)
	decodedSyncCommittee := &SyncCommittee{}
	err = decodedSyncCommittee.UnmarshalJSON(encodedData)
	require.NoError(t, err)
	assert.Equal(t, syncCommittee, decodedSyncCommittee)
}
