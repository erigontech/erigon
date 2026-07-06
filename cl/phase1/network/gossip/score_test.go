package gossip

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
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
