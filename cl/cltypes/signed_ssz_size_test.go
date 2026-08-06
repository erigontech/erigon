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

package cltypes_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/ssz"
)

func TestSignedContainerEncodingSizeSSZ(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)

	tests := []struct {
		name      string
		container ssz.Marshaler
	}{
		{
			name:      "voluntary exit",
			container: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{}},
		},
		{
			name:      "beacon block header",
			container: &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}},
		},
		{
			name:      "BLS to execution change",
			container: &cltypes.SignedBLSToExecutionChange{Message: &cltypes.BLSToExecutionChange{}},
		},
		{
			name: "contribution and proof",
			container: &cltypes.SignedContributionAndProof{Message: &cltypes.ContributionAndProof{
				Contribution: &cltypes.Contribution{},
			}},
		},
		{
			name: "aggregate and proof",
			container: &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxValidatorsPerCommittee)),
					Data:            &solid.AttestationData{},
				},
			}},
		},
		{
			name:      "proposer preferences",
			container: &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{}},
		},
		{
			name: "execution payload bid",
			container: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
				BlobKzgCommitments: *commitments,
			}},
		},
		{
			name: "execution payload envelope",
			container: &cltypes.SignedExecutionPayloadEnvelope{
				Message: cltypes.NewExecutionPayloadEnvelope(cfg),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.container.EncodingSizeSSZ()
			encoded, err := tt.container.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Len(t, encoded, size)
		})
	}
}
