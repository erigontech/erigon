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

// TestSingleAttestationToAttestationRejectsOutOfRangeMemberIndex pins that a
// member index beyond the committee is rejected instead of panicking on the
// aggregation-bits write. The dev validator feeds both values straight from a
// beacon-API duties response.
func TestSingleAttestationToAttestationRejectsOutOfRangeMemberIndex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxCommitteesPerSlot = 4
	single := &SingleAttestation{
		CommitteeIndex: 2,
		AttesterIndex:  0,
		Data:           &AttestationData{},
	}

	for _, memberIndex := range []int{-1, 8, 64} {
		_, err := single.ToAttestation(memberIndex, 8, int(cfg.MaxCommitteesPerSlot), &cfg)
		require.Error(t, err, "member index %d", memberIndex)
	}
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
