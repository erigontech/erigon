package solid

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// TestSingleAttestationToAttestationRejectsOutOfRangeCommitteeIndex pins that
// a CommitteeIndex at or beyond maxCommittees is rejected instead of silently
// producing an Attestation with an empty CommitteeBits.
func TestSingleAttestationToAttestationRejectsOutOfRangeCommitteeIndex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxCommitteesPerSlot = 4
	single := &SingleAttestation{
		CommitteeIndex: 4, // == maxCommittees, out of range for a 4-bit vector
		AttesterIndex:  0,
		Data:           &AttestationData{},
	}

	_, err := single.ToAttestation(0, 8, int(cfg.MaxCommitteesPerSlot), &cfg)
	require.Error(t, err)
}

func TestSingleAttestationToAttestationSetsCommitteeBit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxCommitteesPerSlot = 4
	single := &SingleAttestation{
		CommitteeIndex: 2,
		AttesterIndex:  0,
		Data:           &AttestationData{},
	}

	att, err := single.ToAttestation(0, 8, int(cfg.MaxCommitteesPerSlot), &cfg)
	require.NoError(t, err)
	require.True(t, att.CommitteeBits.GetBitAt(2))
}
