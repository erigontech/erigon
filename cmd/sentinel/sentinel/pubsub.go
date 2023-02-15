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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs

	// fanout ttl
	gossipSubFanoutTTL = 60000000000 // TTL for fanout maps for topics we are not subscribed to but have published to, in nano seconds

	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// misc
	rSubD = 8 // random gossip target
)

// Specifies the prefix for any pubsub topic.
const gossipTopicPrefix = "/eth2/"
const blockSubnetTopicFormat = "/eth2/%x/beacon_block"
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

// closes a specific topic
func (s *GossipManager) CloseTopic(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.subscriptions[topic]; ok {
		val.Close()
		delete(s.subscriptions, topic)
	}
}

// reset'em
func (s *GossipManager) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, val := range s.subscriptions {
		val.Close() // Close all.
	}
	s.subscriptions = map[string]*GossipSubscription{}
}

// get a specific topic
func (s *GossipManager) GetSubscription(topic string) (*GossipSubscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.subscriptions[topic]; ok {
		return val, true
	}
	return nil, false
}

func (s *GossipManager) GetMatchingSubscription(match string) *GossipSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sub *GossipSubscription
	for topic, currSub := range s.subscriptions {
		if strings.Contains(topic, string(BeaconBlockTopic)) {
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

// starts listening to a specific topic (forwarding its messages to the gossip manager channel)
func (s *GossipManager) ListenTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.subscriptions[topic]; ok {
		return val.Listen()
	}
	return nil
}

// closes the gossip manager
func (s *GossipManager) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, val := range s.subscriptions {
		val.Close()
	}
	close(s.ch)
}

func (s *GossipManager) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb := strings.Builder{}
	sb.Grow(len(s.subscriptions) * 4)

	for _, v := range s.subscriptions {
		sb.Write([]byte(v.topic.String()))
		sb.WriteString("=")
		sb.WriteString(strconv.Itoa(len(v.topic.ListPeers())))
		sb.WriteString(" ")
	}
	return sb.String()
}

func (s *Sentinel) RestartTopics() {
	// Reset all topics
	s.subManager.Reset()
	for _, topic := range s.gossipTopics {
		s.SubscribeGossip(topic)
	}
}

func (s *Sentinel) SubscribeGossip(topic GossipTopic, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	sub = &GossipSubscription{
		gossip_topic: topic,
		ch:           s.subManager.ch,
		host:         s.host.ID(),
		ctx:          s.ctx,
	}
	path := s.getTopic(topic)
	sub.topic, err = s.pubsub.Join(path, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s, err=%w", path, err)
	}
	s.subManager.AddSubscription(path, sub)
	for _, t := range s.gossipTopics {
		if t.CodecStr == topic.CodecStr {
			return sub, nil
		}
	}
	s.gossipTopics = append(s.gossipTopics, topic)
	return sub, nil
}

func (s *Sentinel) LogTopicPeers() {
	log.Info("[Gossip] Network Update", "topic peers", s.subManager.String())
}

func (s *Sentinel) getTopic(topic GossipTopic) string {
	o, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	return fmt.Sprintf("/eth2/%x/%s/%s", o, topic.Name, topic.CodecStr)
}
