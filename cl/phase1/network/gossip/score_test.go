package gossip

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
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

func TestTopicScoreParamsCoverEverySubscribedTopic(t *testing.T) {
	mainnet := clparams.NetworkConfigs[chainspec.MainnetChainID]
	g := &GossipManager{beaconConfig: &clparams.MainnetBeaconConfig, networkConfig: &mainnet, activeIndicies: 1_000_000}
	for _, name := range []string{
		gossipnames.TopicNameBeaconAggregateAndProof,
		gossipnames.TopicNameSyncCommitteeContributionAndProof,
		gossipnames.TopicNameAttesterSlashing,
		gossipnames.TopicNameProposerSlashing,
		gossipnames.TopicNameBlsToExecutionChange,
		gossipnames.TopicNameDataColumnSidecar(7),
	} {
		params := g.topicScoreParams(name)
		require.NotNil(t, params, name)
		require.Negative(t, params.InvalidMessageDeliveriesWeight, name)
		require.Positive(t, params.TopicWeight, name)
	}
}

// Registers every parameter set on a scoring-enabled router, which validates them the
// same way registerGossipService does at startup.
func TestTopicScoreParamsAreAcceptedByGossipsub(t *testing.T) {
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer host.Close()
	scoreParams := &pubsub.PeerScoreParams{
		Topics:           map[string]*pubsub.TopicScoreParams{},
		AppSpecificScore: func(peer.ID) float64 { return 0 },
		DecayInterval:    time.Second,
		DecayToZero:      0.01,
		RetainScore:      time.Minute,
	}
	thresholds := &pubsub.PeerScoreThresholds{GossipThreshold: -1, PublishThreshold: -2, GraylistThreshold: -3}
	ps, err := pubsub.NewGossipSub(context.Background(), host, pubsub.WithPeerScore(scoreParams, thresholds))
	require.NoError(t, err)

	mainnet := clparams.NetworkConfigs[chainspec.MainnetChainID]
	g := &GossipManager{beaconConfig: &clparams.MainnetBeaconConfig, networkConfig: &mainnet, activeIndicies: 1_000_000}
	for _, name := range []string{
		gossipnames.TopicNameBeaconBlock,
		gossipnames.TopicNameBeaconAggregateAndProof,
		gossipnames.TopicNameSyncCommitteeContributionAndProof,
		gossipnames.TopicNameAttesterSlashing,
		gossipnames.TopicNameProposerSlashing,
		gossipnames.TopicNameVoluntaryExit,
		gossipnames.TopicNameBlsToExecutionChange,
		gossipnames.TopicNameBeaconAttestation(0),
		gossipnames.TopicNameSyncCommittee(0),
		gossipnames.TopicNameDataColumnSidecar(0),
		gossipnames.TopicNameExecutionPayload,
		gossipnames.TopicNameExecutionPayloadBid,
		gossipnames.TopicNamePayloadAttestation,
		gossipnames.TopicNameProposerPreferences,
	} {
		params := g.topicScoreParams(name)
		require.NotNil(t, params, name)
		topic, err := ps.Join(composeTopic(common.Bytes4{}, name))
		require.NoError(t, err, name)
		require.NoError(t, topic.SetScoreParams(params), name)
	}
}
