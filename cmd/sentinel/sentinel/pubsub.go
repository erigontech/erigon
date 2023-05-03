/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var (
	// maxInMeshScore describes the max score a peer can attain from being in the mesh.
	maxInMeshScore = 10.
	// beaconBlockWeight specifies the scoring weight that we apply to
	// our beacon block topic.
	beaconBlockWeight = 0.8
)

const SSZSnappyCodec = "ssz_snappy"

type TopicName string

const (
	BeaconBlockTopic                 TopicName = "beacon_block"
	BeaconAggregateAndProofTopic     TopicName = "beacon_aggregate_and_proof"
	VoluntaryExitTopic               TopicName = "voluntary_exit"
	ProposerSlashingTopic            TopicName = "proposer_slashing"
	AttesterSlashingTopic            TopicName = "attester_slashing"
	LightClientFinalityUpdateTopic   TopicName = "light_client_finality_update"
	LightClientOptimisticUpdateTopic TopicName = "light_client_optimistic_update"
)

type GossipTopic struct {
	Name     TopicName
	CodecStr string
}

var BeaconBlockSsz = GossipTopic{
	Name:     BeaconBlockTopic,
	CodecStr: SSZSnappyCodec,
}
var BeaconAggregateAndProofSsz = GossipTopic{
	Name:     BeaconAggregateAndProofTopic,
	CodecStr: SSZSnappyCodec,
}
var VoluntaryExitSsz = GossipTopic{
	Name:     VoluntaryExitTopic,
	CodecStr: SSZSnappyCodec,
}
var ProposerSlashingSsz = GossipTopic{
	Name:     ProposerSlashingTopic,
	CodecStr: SSZSnappyCodec,
}
var AttesterSlashingSsz = GossipTopic{
	Name:     AttesterSlashingTopic,
	CodecStr: SSZSnappyCodec,
}
var LightClientFinalityUpdateSsz = GossipTopic{
	Name:     LightClientFinalityUpdateTopic,
	CodecStr: SSZSnappyCodec,
}
var LightClientOptimisticUpdateSsz = GossipTopic{
	Name:     LightClientOptimisticUpdateTopic,
	CodecStr: SSZSnappyCodec,
}

type GossipManager struct {
	ch            chan *pubsub.Message
	subscriptions map[string]*GossipSubscription
	mu            sync.RWMutex
}

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *pubsub.Message, 1),
		subscriptions: map[string]*GossipSubscription{},
	}
	return g
}

func (s *GossipManager) Recv() <-chan *pubsub.Message {
	return s.ch
}

func (s *GossipManager) GetMatchingSubscription(match string) *GossipSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sub *GossipSubscription
	for topic, currSub := range s.subscriptions {
		if strings.Contains(topic, match) {
			sub = currSub
		}
	}
	return sub
}

func (s *GossipManager) AddSubscription(topic string, sub *GossipSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[topic] = sub
}

func (s *GossipManager) unsubscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[topic]; !ok {
		return
	}
	s.subscriptions[topic].Close()
	delete(s.subscriptions, topic)
}

func (s *Sentinel) SubscribeGossip(topic GossipTopic, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	digest, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	sub = &GossipSubscription{
		gossip_topic: topic,
		ch:           s.subManager.ch,
		host:         s.host.ID(),
		ctx:          s.ctx,
	}
	path := fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr)
	sub.topic, err = s.pubsub.Join(path, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s, err=%w", path, err)
	}
	topicScoreParams := s.topicScoreParams(string(topic.Name))
	if topicScoreParams != nil {
		sub.topic.SetScoreParams(topicScoreParams)
	}
	s.subManager.AddSubscription(path, sub)

	return sub, nil
}

func (s *Sentinel) Unsubscribe(topic GossipTopic, opts ...pubsub.TopicOpt) (err error) {
	digest, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	s.subManager.unsubscribe(fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr))

	return nil
}

func (s *Sentinel) topicScoreParams(topic string) *pubsub.TopicScoreParams {
	switch {
	case strings.Contains(topic, string(BeaconBlockTopic)):
		return s.defaultBlockTopicParams()
	/*case strings.Contains(topic, GossipAggregateAndProofMessage):
		return defaultAggregateTopicParams(activeValidators), nil
	case strings.Contains(topic, GossipAttestationMessage):
		return defaultAggregateSubnetTopicParams(activeValidators), nil
	case strings.Contains(topic, GossipSyncCommitteeMessage):
		return defaultSyncSubnetTopicParams(activeValidators), nil
	case strings.Contains(topic, GossipContributionAndProofMessage):
		return defaultSyncContributionTopicParams(), nil
	case strings.Contains(topic, GossipExitMessage):
		return defaultVoluntaryExitTopicParams(), nil
	case strings.Contains(topic, GossipProposerSlashingMessage):
		return defaultProposerSlashingTopicParams(), nil
	case strings.Contains(topic, GossipAttesterSlashingMessage):
		return defaultAttesterSlashingTopicParams(), nil
	case strings.Contains(topic, GossipBlsToExecutionChangeMessage):
		return defaultBlsToExecutionChangeTopicParams(), nil*/
	default:
		return nil
	}
}

// Based on the lighthouse parameters.
// https://gist.github.com/blacktemplar/5c1862cb3f0e32a1a7fb0b25e79e6e2c
func (s *Sentinel) defaultBlockTopicParams() *pubsub.TopicScoreParams {
	blocksPerEpoch := s.cfg.BeaconConfig.SlotsPerEpoch
	meshWeight := float64(0)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     beaconBlockWeight,
		TimeInMeshWeight:                maxInMeshScore / s.inMeshCap(),
		TimeInMeshQuantum:               s.oneSlotDuration(),
		TimeInMeshCap:                   s.inMeshCap(),
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     s.scoreDecay(20 * s.oneEpochDuration()),
		FirstMessageDeliveriesCap:       23,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      s.scoreDecay(5 * s.oneEpochDuration()),
		MeshMessageDeliveriesCap:        float64(blocksPerEpoch * 5),
		MeshMessageDeliveriesThreshold:  float64(blocksPerEpoch*5) / 10,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 4 * s.oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         s.scoreDecay(5 * s.oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -140.4475,
		InvalidMessageDeliveriesDecay:   s.scoreDecay(50 * s.oneEpochDuration()),
	}
}

func (g *GossipManager) Close() {
	for _, topic := range g.subscriptions {
		if topic != nil {
			topic.Close()
		}
	}
}
