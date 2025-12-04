package p2p

import (
	"math"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// overlay parameters
	gossipSubD    = 4 // topic stable mesh target count
	gossipSubDlo  = 2 // topic stable mesh low watermark
	gossipSubDhi  = 6 // topic stable mesh high watermark
	gossipSubDout = 1 // topic stable mesh target out degree. // Dout must be set below Dlo, and must not exceed D / 2.

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs
	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// decayToZero specifies the terminal value that we will use when decaying
	// a value.
	decayToZero = 0.01
)

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func scoreDecay(totalDurationDecay time.Duration, beaconConfig *clparams.BeaconChainConfig) float64 {
	oneSlotDuration := time.Duration(beaconConfig.SecondsPerSlot) * time.Second
	numOfTimes := totalDurationDecay / oneSlotDuration
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}

func (s *p2pManager) pubsubOptions(beaconConfig *clparams.BeaconChainConfig) []pubsub.Option {
	oneSlotDuration := time.Duration(beaconConfig.SecondsPerSlot) * time.Second
	oneEpochDuration := time.Duration(beaconConfig.SlotsPerEpoch) * oneSlotDuration

	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}
	scoreParams := &pubsub.PeerScoreParams{
		Topics:        make(map[string]*pubsub.TopicScoreParams),
		TopicScoreCap: 32.72,
		AppSpecificScore: func(p peer.ID) float64 {
			return 0
		},
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       scoreDecay(10*oneEpochDuration, beaconConfig), // 10 epochs
		DecayInterval:               oneSlotDuration,
		DecayToZero:                 decayToZero,
		RetainScore:                 100 * oneEpochDuration, // Retain for 100 epochs
	}
	pubsubQueueSize := 600
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithMessageIdFn(s.msgId),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMaxMessageSize(int(s.cfg.NetworkConfig.GossipMaxSizeBellatrix)),
		pubsub.WithValidateQueueSize(pubsubQueueSize),
		pubsub.WithPeerScore(scoreParams, thresholds),
		pubsub.WithGossipSubParams(pubsubGossipParam()),
	}
	return psOpts
}

// creates a custom gossipsub parameter set.
func pubsubGossipParam() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.Dlo = gossipSubDlo
	gParams.Dhi = gossipSubDhi
	gParams.D = gossipSubD
	gParams.Dout = gossipSubDout

	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	gParams.HistoryLength = gossipSubMcacheLen
	gParams.HistoryGossip = gossipSubMcacheGossip
	return gParams
}
