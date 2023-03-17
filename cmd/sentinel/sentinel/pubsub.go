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

	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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

func (s *Sentinel) SubscribeGossip(topic GossipTopic, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	paths := s.getTopics(topic)
	for _, path := range paths {
		sub = &GossipSubscription{
			gossip_topic: topic,
			ch:           s.subManager.ch,
			host:         s.host.ID(),
			ctx:          s.ctx,
		}
		sub.topic, err = s.pubsub.Join(path, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to join topic %s, err=%w", path, err)
		}
		s.subManager.AddSubscription(path, sub)
	}

	return sub, nil
}

func (s *Sentinel) getTopics(topic GossipTopic) []string {
	digest, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	nextDigest, err := fork.ComputeNextForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	return []string{
		fmt.Sprintf("/eth2/%x/%s/%s", nextDigest, topic.Name, topic.CodecStr),
		fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr),
	}
}
