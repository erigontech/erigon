package gossip

import (
	"strconv"
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
	topic := newScoredTopicForTest(t)
	for _, tc := range []struct {
		activeValidators uint64
		rate             float64
	}{{2047, 0}, {2048, 0.25}, {4096, 0.5}, {6144, 0.75}, {8191, 0.75}, {8192, 1}} {
		requireAcceptedTopicScoreParams(t, topic, newManager(tc.activeValidators).defaultAttestationSubnetTopicParams(), tc.activeValidators, tc.rate)
	}
}

func TestSyncSubnetScoringWithFewValidatorsPerMeshPeer(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig}
	topic := newScoredTopicForTest(t)
	for _, tc := range []struct {
		activeValidators uint64
		rate             float64
	}{{3, 0}, {4, 0.25}, {8, 0.5}, {15, 0.75}, {16, 1}} {
		requireAcceptedTopicScoreParams(t, topic, g.defaultSyncSubnetTopicParams(tc.activeValidators), tc.activeValidators, tc.rate)
	}
}

func TestAggregateScoringWithFewAggregatorsPerMeshPeer(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	beaconConfig.TargetAggregatorsPerCommittee = 1
	topic := newScoredTopicForTest(t)
	for _, tc := range []struct {
		activeValidators uint64
		rate             float64
	}{{4096, 0.25}, {8192, 0.5}, {12288, 0.75}, {16384, 1}, {20480, 1.25}} {
		g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig, activeIndicies: tc.activeValidators}
		requireAcceptedTopicScoreParams(t, topic, g.topicScoreParams(gossipnames.TopicNameBeaconAggregateAndProof), tc.activeValidators, tc.rate)
	}
}

func TestRateBasedTopicParamsWithoutTraffic(t *testing.T) {
	_, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	g := &GossipManager{beaconConfig: beaconConfig}
	params := g.rateBasedTopicParams(aggregateWeight, 0)
	require.NotNil(t, params)
	require.Positive(t, params.TopicWeight)
	require.Negative(t, params.InvalidMessageDeliveriesWeight)
	require.Zero(t, params.TimeInMeshWeight)
	require.Zero(t, params.FirstMessageDeliveriesWeight)
	require.Zero(t, params.MeshMessageDeliveriesWeight)
	require.NoError(t, newScoredTopicForTest(t).SetScoreParams(params))
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

func TestTopicScoreParamsAreAcceptedByGossipsub(t *testing.T) {
	topic := newScoredTopicForTest(t)
	for _, preset := range []string{"mainnet", "minimal"} {
		t.Run(preset, func(t *testing.T) {
			networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
			if preset == "minimal" {
				clparams.ApplyMinimalPreset(beaconConfig)
			}
			for _, activeValidators := range []uint64{0, 1, 3, 64, 2047, 2048, 1_000_000} {
				t.Run(strconv.FormatUint(activeValidators, 10), func(t *testing.T) {
					g := &GossipManager{beaconConfig: beaconConfig, networkConfig: networkConfig, activeIndicies: activeValidators}
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
						gossipnames.TopicNameDataColumnSidecar(7),
						gossipnames.TopicNameExecutionPayload,
						gossipnames.TopicNameExecutionPayloadBid,
						gossipnames.TopicNamePayloadAttestation,
						gossipnames.TopicNameProposerPreferences,
					} {
						params := g.topicScoreParams(name)
						require.NotNil(t, params, name)
						require.Negative(t, params.InvalidMessageDeliveriesWeight, name)
						require.Positive(t, params.TopicWeight, name)
						require.NoError(t, topic.SetScoreParams(params), name)
					}
				})
			}
		})
	}
}
