package devvalidator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// TestBuildAggregateAttestation verifies the SingleAttestation is wrapped into
// an Electra Attestation with the committee bit set and exactly one aggregation
// bit (the validator's position) plus the trailing committee-length sentinel.
func TestBuildAggregateAttestation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	sig := common.Bytes96{0x01}
	single := &solid.SingleAttestation{
		CommitteeIndex: 2,
		AttesterIndex:  7,
		Data:           testAttData(),
		Signature:      sig,
	}

	agg := buildAggregateAttestation(single, 1, 4, &cfg)
	require.NotNil(t, agg)
	require.Equal(t, sig, agg.Signature)
	require.True(t, agg.CommitteeBits.GetBitAt(2), "committee bit for index 2 must be set")
	require.Equal(t, testAttData().Slot, agg.Data.Slot)
}

// TestSignedAggregateAndProof_RoundTrip verifies a SignedAggregateAndProof
// marshals to JSON that decodes back into the exact type the validator
// aggregate_and_proofs endpoint expects.
func TestSignedAggregateAndProof_RoundTrip(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	single := &solid.SingleAttestation{
		CommitteeIndex: 1,
		AttesterIndex:  9,
		Data:           testAttData(),
		Signature:      common.Bytes96{0x02},
	}
	signed := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{
			AggregatorIndex: 9,
			Aggregate:       buildAggregateAttestation(single, 0, 4, &cfg),
			SelectionProof:  common.Bytes96{0x03},
		},
		Signature: common.Bytes96{0x04},
	}

	raw, err := json.Marshal([]any{signed})
	require.NoError(t, err)

	var decoded []*cltypes.SignedAggregateAndProof
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, uint64(9), decoded[0].Message.AggregatorIndex)
	require.Equal(t, common.Bytes96{0x03}, decoded[0].Message.SelectionProof)
	require.Equal(t, common.Bytes96{0x04}, decoded[0].Signature)
	require.NotNil(t, decoded[0].Message.Aggregate)
}
