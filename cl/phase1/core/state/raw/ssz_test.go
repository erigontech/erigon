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

package raw

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/stretchr/testify/require"
)

// newTestState creates a properly initialized BeaconState for the given version
// that can be safely encoded/decoded without nil pointer panics.
func newTestState(version clparams.StateVersion) *BeaconState {
	state := New(&clparams.MainnetBeaconConfig)
	state.SetVersion(version)

	// Eth1Header needs ExtraData initialized for versions >= Bellatrix
	if version >= clparams.BellatrixVersion {
		state.latestExecutionPayloadHeader = cltypes.NewEth1Header(version)
	}

	// ExecutionPayloadBid needs BlobKzgCommitments initialized for GLOAS
	if version >= clparams.GloasVersion {
		state.latestExecutionPayloadBid = &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		}
	}

	return state
}

// computeFixedPortionSize computes the actual fixed portion size of the SSZ schema.
// For each element: fixed-size objects contribute their full EncodingSizeSSZ,
// variable-size objects contribute 4 bytes (offset pointer), uint64 contributes 8,
// []byte contributes len(slice), bool contributes 1.
func computeFixedPortionSize(schema []any) uint32 {
	var size uint32
	for _, element := range schema {
		switch obj := element.(type) {
		case uint64, *uint64:
			size += 8
		case []byte:
			size += uint32(len(obj))
		case bool:
			size += 1
		case ssz2.SizedObjectSSZ:
			if obj.Static() {
				size += uint32(obj.EncodingSizeSSZ())
			} else {
				size += 4 // offset pointer
			}
		}
	}
	return size
}

var allVersions = []struct {
	version clparams.StateVersion
	name    string
}{
	{clparams.Phase0Version, "Phase0"},
	{clparams.AltairVersion, "Altair"},
	{clparams.BellatrixVersion, "Bellatrix"},
	{clparams.CapellaVersion, "Capella"},
	{clparams.DenebVersion, "Deneb"},
	{clparams.ElectraVersion, "Electra"},
	{clparams.FuluVersion, "Fulu"},
	{clparams.GloasVersion, "Gloas"},
}

// TestBeaconStateEncodingSizeSSZ verifies that EncodingSizeSSZ() matches the actual
// encoded byte length for every beacon state version. This catches bugs in baseOffsetSSZ
// or the size adjustment logic in EncodingSizeSSZ.
func TestBeaconStateEncodingSizeSSZ(t *testing.T) {
	for _, tc := range allVersions {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(tc.version)

			encoded, err := state.EncodeSSZ(nil)
			require.NoError(t, err, "EncodeSSZ failed")

			expectedSize := state.EncodingSizeSSZ()
			require.Equal(t, expectedSize, len(encoded),
				"EncodingSizeSSZ() = %d but actual encoded length = %d (diff = %d)",
				expectedSize, len(encoded), expectedSize-len(encoded))
		})
	}
}

// TestBaseOffsetSSZ verifies that baseOffsetSSZ matches the actual fixed portion size
// computed from the schema for every beacon state version.
func TestBaseOffsetSSZ(t *testing.T) {
	for _, tc := range allVersions {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(tc.version)

			schema := state.getSchema()
			actualFixed := computeFixedPortionSize(schema)
			baseOffset := state.baseOffsetSSZ()

			require.Equal(t, actualFixed, baseOffset,
				"baseOffsetSSZ() = %d but actual fixed portion from schema = %d (diff = %d)",
				baseOffset, actualFixed, int64(baseOffset)-int64(actualFixed))
		})
	}
}

// TestBeaconStateSSZRoundtrip verifies that encoding then decoding a beacon state
// produces the same SSZ hash and identical bytes for every version.
func TestBeaconStateSSZRoundtrip(t *testing.T) {
	for _, tc := range allVersions {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(tc.version)

			// Encode
			encoded1, err := state.EncodeSSZ(nil)
			require.NoError(t, err, "first EncodeSSZ failed")

			hash1, err := state.HashSSZ()
			require.NoError(t, err, "first HashSSZ failed")

			// Decode into a fresh state
			state2 := New(&clparams.MainnetBeaconConfig)
			err = state2.DecodeSSZ(encoded1, int(tc.version))
			require.NoError(t, err, "DecodeSSZ failed")

			hash2, err := state2.HashSSZ()
			require.NoError(t, err, "second HashSSZ failed")

			require.Equal(t, hash1, hash2,
				"HashSSZ mismatch after roundtrip: before=%x after=%x", hash1, hash2)

			// Re-encode and verify bytes match
			encoded2, err := state2.EncodeSSZ(nil)
			require.NoError(t, err, "second EncodeSSZ failed")

			require.Equal(t, len(encoded1), len(encoded2),
				"encoded length mismatch: first=%d second=%d", len(encoded1), len(encoded2))
			require.Equal(t, encoded1, encoded2,
				fmt.Sprintf("encoded bytes differ after roundtrip (len1=%d, len2=%d)", len(encoded1), len(encoded2)))
		})
	}
}
