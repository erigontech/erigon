package devvalidator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// TestBuildContribution verifies the contribution has the validator's single
// bit set at the right position and round-trips through the type the
// contribution_and_proofs endpoint decodes.
func TestBuildContribution(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig // SyncCommitteeSize=512, subnets=4 → 16-byte agg bits
	root := common.Hash{0xab}
	sig := common.Bytes96{0x05}

	contrib := buildContribution(7, root, 2, 9, sig, &cfg)
	require.Equal(t, cfg.SyncCommitteeAggregationBitsSize(), len(contrib.AggregationBits))
	require.Equal(t, byte(1<<(9%8)), contrib.AggregationBits[9/8])
	require.Equal(t, uint64(2), contrib.SubcommitteeIndex)
	require.Equal(t, root, contrib.BeaconBlockRoot)

	signed := &cltypes.SignedContributionAndProof{
		Message: &cltypes.ContributionAndProof{
			AggregatorIndex: 7,
			Contribution:    contrib,
			SelectionProof:  common.Bytes96{0x06},
		},
		Signature: common.Bytes96{0x07},
	}
	raw, err := json.Marshal([]interface{}{signed})
	require.NoError(t, err)

	var decoded []*cltypes.SignedContributionAndProof
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, uint64(7), decoded[0].Message.AggregatorIndex)
	require.Equal(t, uint64(2), decoded[0].Message.Contribution.SubcommitteeIndex)
	require.Equal(t, common.Bytes96{0x07}, decoded[0].Signature)
}

// TestIsSyncCommitteeAggregator_MinimalAlwaysAggregator confirms that on the
// minimal preset (SyncCommitteeSize=32) the modulo is 1, so any selection proof
// makes the validator an aggregator.
func TestIsSyncCommitteeAggregator_MinimalAlwaysAggregator(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SyncCommitteeSize = 32 // minimal preset → modulo = max(1, 32/4/16) = 1
	require.True(t, isSyncCommitteeAggregator(&cfg, common.Bytes96{0x01}))
	require.True(t, isSyncCommitteeAggregator(&cfg, common.Bytes96{0xff, 0xff}))
}
