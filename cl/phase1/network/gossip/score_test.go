package gossip

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
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
	newManager := func(activeValidators uint64) *GossipManager {
		return &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig, activeIndicies: activeValidators}
	}
	require.Nil(t, newManager(2047).defaultAggregateSubnetTopicParams())
	topic := newScoredTopicForTest(t)
	for _, tc := range []struct {
		activeValidators uint64
		rate             float64
	}{{2048, 0.25}, {4096, 0.5}, {6144, 0.75}, {8191, 0.75}, {8192, 1}} {
		requireAcceptedTopicScoreParams(t, topic, newManager(tc.activeValidators).defaultAggregateSubnetTopicParams(), tc.activeValidators, tc.rate)
	}
}

func TestSyncSubnetScoringWithFewValidatorsPerMeshPeer(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig}
	require.Nil(t, g.defaultSyncSubnetTopicParams(3))
	topic := newScoredTopicForTest(t)
	for _, tc := range []struct {
		activeValidators uint64
		rate             float64
	}{{4, 0.25}, {8, 0.5}, {15, 0.75}, {16, 1}} {
		requireAcceptedTopicScoreParams(t, topic, g.defaultSyncSubnetTopicParams(tc.activeValidators), tc.activeValidators, tc.rate)
	}
}

func newScoredTopicForTest(t *testing.T) *pubsub.Topic {
	t.Helper()
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	ps, err := pubsub.NewGossipSub(t.Context(), h, pubsub.WithPeerScore(
		&pubsub.PeerScoreParams{
			Topics:            make(map[string]*pubsub.TopicScoreParams),
			AppSpecificScore:  func(peer.ID) float64 { return 0 },
			AppSpecificWeight: 1,
			DecayInterval:     time.Second,
			DecayToZero:       0.01,
			RetainScore:       time.Minute,
		},
		&pubsub.PeerScoreThresholds{},
	))
	require.NoError(t, err)
	topic, err := ps.Join("topic")
	require.NoError(t, err)
	return topic
}

func requireAcceptedTopicScoreParams(t *testing.T, topic *pubsub.Topic, params *pubsub.TopicScoreParams, activeValidators uint64, rate float64) {
	t.Helper()
	require.NotNil(t, params, "active validators %d", activeValidators)
	require.NoError(t, topic.SetScoreParams(params), "active validators %d", activeValidators)
	require.InDelta(t, rate, params.FirstMessageDeliveriesCap*(1-params.FirstMessageDeliveriesDecay), 1e-9, "active validators %d", activeValidators)
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

func TestTopicScoreParamsAreAcceptedByGossipsub(t *testing.T) {
	topic := newScoredTopicForTest(t)

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
		require.NoError(t, topic.SetScoreParams(params), name)
	}
}
