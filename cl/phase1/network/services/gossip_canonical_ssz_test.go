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

package services

import (
	"encoding/binary"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func appendTrailingByte(encoded []byte) []byte {
	return append(append([]byte(nil), encoded...), 0)
}

func insertOffsetGap(encoded []byte, offsetPositions ...int) []byte {
	fixedSize := int(binary.LittleEndian.Uint32(encoded[offsetPositions[0]:]))
	out := append([]byte(nil), encoded[:fixedSize]...)
	out = append(out, make([]byte, 4)...)
	out = append(out, encoded[fixedSize:]...)
	for _, pos := range offsetPositions {
		binary.LittleEndian.PutUint32(out[pos:], binary.LittleEndian.Uint32(encoded[pos:])+4)
	}
	return out
}

func TestGossipDecodeRejectsNonCanonicalSSZ(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	pid := peer.ID("peer")
	electra := clparams.ElectraVersion
	fulu := clparams.FuluVersion

	encode := func(obj interface{ EncodeSSZ([]byte) ([]byte, error) }) []byte {
		enc, err := obj.EncodeSSZ(nil)
		require.NoError(t, err)
		return enc
	}

	aggregationBits := solid.NewBitList(0, 2048*64)
	aggregationBits.Append(1)
	aggregate := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{
			Aggregate: &solid.Attestation{
				AggregationBits: aggregationBits,
				Data:            &solid.AttestationData{},
				CommitteeBits:   solid.NewBitVector(int(cfg.MaxCommitteesPerSlot)),
			},
		},
	}
	aggregateEnc := encode(aggregate)

	slashing := cltypes.NewAttesterSlashing(electra)
	slashing.Attestation_1.AttestingIndices.Append(1)
	slashingEnc := encode(slashing)

	columnEnc := encode(cltypes.NewDataColumnSidecarWithVersion(fulu))

	contribution := &cltypes.Contribution{}
	contribution.SetAggregationBitsSize(int(cfg.SyncCommitteeSize / cfg.SyncCommitteeSubnetCount / 8))
	contributionEnc := encode(&cltypes.SignedContributionAndProof{Message: &cltypes.ContributionAndProof{Contribution: contribution}})

	cases := []struct {
		name      string
		canonical []byte
		malformed []byte
		decode    func([]byte) error
	}{
		{
			name:      "beacon_attestation",
			canonical: make([]byte, (&solid.SingleAttestation{}).EncodingSizeSSZ()),
			decode: func(b []byte) error {
				_, err := (&attestationService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "beacon_aggregate_and_proof",
			canonical: aggregateEnc,
			malformed: insertOffsetGap(aggregateEnc, 0),
			decode: func(b []byte) error {
				_, err := (&aggregateAndProofServiceImpl{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "voluntary_exit",
			canonical: make([]byte, 112),
			decode: func(b []byte) error {
				_, err := (&voluntaryExitService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "sync_committee",
			canonical: make([]byte, (&cltypes.SyncCommitteeMessage{}).EncodingSizeSSZ()),
			decode: func(b []byte) error {
				_, err := (&syncCommitteeMessagesService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "sync_committee_contribution_and_proof",
			canonical: contributionEnc,
			decode: func(b []byte) error {
				_, err := (&syncContributionService{beaconCfg: &cfg}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "bls_to_execution_change",
			canonical: make([]byte, (&cltypes.SignedBLSToExecutionChange{Message: &cltypes.BLSToExecutionChange{}}).EncodingSizeSSZ()),
			decode: func(b []byte) error {
				_, err := (&blsToExecutionChangeService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "proposer_slashing",
			canonical: make([]byte, 2*(&cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}}).EncodingSizeSSZ()),
			decode: func(b []byte) error {
				_, err := (&proposerSlashingService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "attester_slashing",
			canonical: slashingEnc,
			malformed: insertOffsetGap(slashingEnc, 0, 4),
			decode: func(b []byte) error {
				_, err := (&attesterSlashingService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "blob_sidecar",
			canonical: make([]byte, (&cltypes.BlobSidecar{}).EncodingSizeSSZ()),
			decode: func(b []byte) error {
				_, err := (&blobSidecarService{}).DecodeGossipMessage(pid, b, electra)
				return err
			},
		},
		{
			name:      "data_column_sidecar",
			canonical: columnEnc,
			malformed: insertOffsetGap(columnEnc, 8, 12, 16),
			decode: func(b []byte) error {
				_, err := (&dataColumnSidecarService{}).DecodeGossipMessage(pid, b, fulu)
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.decode(tc.canonical))
			malformed := tc.malformed
			if malformed == nil {
				malformed = appendTrailingByte(tc.canonical)
			}
			require.Error(t, tc.decode(malformed))
		})
	}
}
