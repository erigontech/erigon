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

package cltypes

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestGloasBlockContentsSSZRoundTrip(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	contents := NewGloasBlockContents(&cfg, 64)
	contents.Block.Slot = 64
	contents.ExecutionPayloadEnvelope.BuilderIndex = 7

	encoded, err := contents.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := NewGloasBlockContents(&cfg, 64)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, uint64(64), decoded.Block.Slot)
	require.Equal(t, uint64(7), decoded.ExecutionPayloadEnvelope.BuilderIndex)
	require.Equal(t, encoded, requireEncodedSSZ(t, decoded))
}

func TestSignedExecutionPayloadEnvelopeContentsSSZRoundTrip(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	contents := NewSignedExecutionPayloadEnvelopeContents(&cfg, 64)
	contents.SignedExecutionPayloadEnvelope.Message.BuilderIndex = 9

	encoded, err := contents.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := NewSignedExecutionPayloadEnvelopeContents(&cfg, 64)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, uint64(9), decoded.SignedExecutionPayloadEnvelope.Message.BuilderIndex)
	require.Equal(t, encoded, requireEncodedSSZ(t, decoded))
}

func TestSignedExecutionPayloadEnvelopeContentsRejectsNonCanonicalSSZOffsets(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	contents := NewSignedExecutionPayloadEnvelopeContents(&cfg, 64)
	encoded, err := contents.EncodeSSZ(nil)
	require.NoError(t, err)
	nonCanonical := append([]byte(nil), encoded[:12]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[12:]...)
	for offset := 0; offset < 12; offset += 4 {
		binary.LittleEndian.PutUint32(nonCanonical[offset:], binary.LittleEndian.Uint32(encoded[offset:])+4)
	}
	require.NoError(t, NewSignedExecutionPayloadEnvelopeContents(&cfg, 64).DecodeSSZ(nonCanonical, int(clparams.GloasVersion)))
	require.Error(t, NewSignedExecutionPayloadEnvelopeContents(&cfg, 64).DecodeSSZStrict(nonCanonical, int(clparams.GloasVersion)))
}

func TestSignedExecutionPayloadEnvelopeRejectsNonCanonicalSSZOffsets(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	envelope := &SignedExecutionPayloadEnvelope{Message: NewExecutionPayloadEnvelope(&cfg), beaconCfg: &cfg}
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	const fixedSize = 4 + 96
	nonCanonical := append([]byte(nil), encoded[:fixedSize]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[fixedSize:]...)
	binary.LittleEndian.PutUint32(nonCanonical, binary.LittleEndian.Uint32(encoded)+4)
	decoded := &SignedExecutionPayloadEnvelope{Message: NewExecutionPayloadEnvelope(&cfg), beaconCfg: &cfg}
	require.NoError(t, decoded.DecodeSSZ(nonCanonical, int(clparams.GloasVersion)))
	strict := &SignedExecutionPayloadEnvelope{Message: NewExecutionPayloadEnvelope(&cfg), beaconCfg: &cfg}
	require.Error(t, strict.DecodeSSZStrict(nonCanonical, int(clparams.GloasVersion)))
}

func TestExecutionRequestsRejectsNonCanonicalSSZOffsets(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	encoded, err := requests.EncodeSSZ(nil)
	require.NoError(t, err)
	const fixedSize = 5 * 4
	nonCanonical := append([]byte(nil), encoded[:fixedSize]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[fixedSize:]...)
	for offset := 0; offset < fixedSize; offset += 4 {
		binary.LittleEndian.PutUint32(nonCanonical[offset:], binary.LittleEndian.Uint32(encoded[offset:])+4)
	}
	require.NoError(t, NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion).DecodeSSZ(nonCanonical, int(clparams.GloasVersion)))
	require.Error(t, NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion).DecodeSSZStrict(nonCanonical, int(clparams.GloasVersion)))
}

func TestExecutionPayloadBidRejectsNonCanonicalSSZOffsets(t *testing.T) {
	bid := &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48),
	}
	encoded, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)
	const fixedSize = 224
	nonCanonical := append([]byte(nil), encoded[:fixedSize]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[fixedSize:]...)
	const commitmentsOffsetPosition = 188
	binary.LittleEndian.PutUint32(nonCanonical[commitmentsOffsetPosition:], binary.LittleEndian.Uint32(encoded[commitmentsOffsetPosition:])+4)
	loose := &ExecutionPayloadBid{BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)}
	require.NoError(t, loose.DecodeSSZ(nonCanonical, int(clparams.GloasVersion)))
	strict := &ExecutionPayloadBid{BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)}
	require.Error(t, strict.DecodeSSZStrict(nonCanonical, int(clparams.GloasVersion)))
}

func requireEncodedSSZ(t *testing.T, value interface {
	EncodeSSZ([]byte) ([]byte, error)
}) []byte {
	t.Helper()
	encoded, err := value.EncodeSSZ(nil)
	require.NoError(t, err)
	return encoded
}
