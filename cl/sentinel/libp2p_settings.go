// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sentinel

import (
	"math"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func (s *Sentinel) scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / s.oneSlotDuration()
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}

func (s *Sentinel) pubsubOptions() []pubsub.Option {
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
		BehaviourPenaltyDecay:       s.scoreDecay(10 * s.oneEpochDuration()), // 10 epochs
		DecayInterval:               s.oneSlotDuration(),
		DecayToZero:                 decayToZero,
		RetainScore:                 100 * s.oneEpochDuration(), // Retain for 100 epochs
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
