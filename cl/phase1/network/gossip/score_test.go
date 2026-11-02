package gossip

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/stretchr/testify/require"
)

func TestTopicScoreParamsExactTopicMatching(t *testing.T) {
	g := &GossipManager{
		beaconConfig: &clparams.BeaconChainConfig{
			SlotsPerEpoch:  32,
			SecondsPerSlot: 12,
		},
	}

	require.Equal(t, executionPayloadWeight, g.topicScoreParams(gossipnames.TopicNameExecutionPayload).TopicWeight)
	require.Equal(t, executionPayloadBidWeight, g.topicScoreParams(gossipnames.TopicNameExecutionPayloadBid).TopicWeight)
	require.Nil(t, g.topicScoreParams(gossipnames.TopicNameExecutionPayload+"_extra"))
	require.Nil(t, g.topicScoreParams(gossipnames.TopicNameBeaconBlock+"_extra"))
}

func TestAttestationSubnetScoringWithFewValidatorsPerMeshPeer(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	for _, activeValidators := range []uint64{4096, 6144, 8191, 8192} {
		g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig, activeIndicies: activeValidators}
		require.NotNil(t, g.defaultAggregateSubnetTopicParams(), "active validators %d", activeValidators)
	}
}

func TestSyncSubnetScoringWithFewValidatorsPerMeshPeer(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig}
	for _, activeValidators := range []uint64{8, 15, 16} {
		require.NotNil(t, g.defaultSyncSubnetTopicParams(activeValidators), "active validators %d", activeValidators)
	}
}
