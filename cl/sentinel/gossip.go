//  Copyright 2022 Erigon-Caplin contributors
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package sentinel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	// maxInMeshScore describes the max score a peer can attain from being in the mesh.
	maxInMeshScore = 10.
	// beaconBlockWeight specifies the scoring weight that we apply to
	// our beacon block topic.
	beaconBlockWeight = 0.8
)

const SSZSnappyCodec = "ssz_snappy"

type GossipTopic struct {
	Name     string
	CodecStr string
}

var BeaconBlockSsz = GossipTopic{
	Name:     gossip.TopicNameBeaconBlock,
	CodecStr: SSZSnappyCodec,
}

var BeaconAggregateAndProofSsz = GossipTopic{
	Name:     gossip.TopicNameBeaconAggregateAndProof,
	CodecStr: SSZSnappyCodec,
}

var VoluntaryExitSsz = GossipTopic{
	Name:     gossip.TopicNameVoluntaryExit,
	CodecStr: SSZSnappyCodec,
}

var ProposerSlashingSsz = GossipTopic{
	Name:     gossip.TopicNameProposerSlashing,
	CodecStr: SSZSnappyCodec,
}

var AttesterSlashingSsz = GossipTopic{
	Name:     gossip.TopicNameAttesterSlashing,
	CodecStr: SSZSnappyCodec,
}

var BlsToExecutionChangeSsz = GossipTopic{
	Name:     gossip.TopicNameBlsToExecutionChange,
	CodecStr: SSZSnappyCodec,
}

var SyncCommitteeContributionAndProofSsz = GossipTopic{
	Name:     gossip.TopicNameSyncCommitteeContributionAndProof,
	CodecStr: SSZSnappyCodec,
}

var LightClientFinalityUpdateSsz = GossipTopic{
	Name:     gossip.TopicNameLightClientFinalityUpdate,
	CodecStr: SSZSnappyCodec,
}

var LightClientOptimisticUpdateSsz = GossipTopic{
	Name:     gossip.TopicNameLightClientOptimisticUpdate,
	CodecStr: SSZSnappyCodec,
}

type GossipManager struct {
	ch            chan *GossipMessage
	subscriptions map[string]*GossipSubscription
	mu            sync.RWMutex
}

const maxIncomingGossipMessages = 1 << 16

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *GossipMessage, maxIncomingGossipMessages),
		subscriptions: map[string]*GossipSubscription{},
	}
	return g
}

func GossipSidecarTopics(maxBlobs uint64) (ret []GossipTopic) {
	for i := uint64(0); i < maxBlobs; i++ {
		ret = append(ret, GossipTopic{
			Name:     gossip.TopicNameBlobSidecar(int(i)),
			CodecStr: SSZSnappyCodec,
		})
	}
	return
}

func (s *GossipManager) Recv() <-chan *GossipMessage {
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
	sub := s.subscriptions[topic]
	go func() {
		timer := time.NewTimer(time.Hour)
		ctx := sub.ctx
		select {
		case <-ctx.Done():
			sub.Close()
		case <-timer.C:
			sub.Close()
		}
	}()
	delete(s.subscriptions, topic)
}

func (s *Sentinel) forkWatcher() {
	prevDigest, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
		return
	}
	iterationInterval := time.NewTicker(30 * time.Millisecond)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-iterationInterval.C:
			digest, err := fork.ComputeForkDigest(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
			if err != nil {
				log.Error("[Gossip] Failed to calculate fork choice", "err", err)
				return
			}
			if prevDigest != digest {
				subs := s.subManager.subscriptions
				for path, sub := range subs {
					s.subManager.unsubscribe(path)
					newSub, err := s.SubscribeGossip(sub.gossip_topic)
					if err != nil {
						log.Error("[Gossip] Failed to resubscribe to topic", "err", err)
						return
					}
					newSub.Listen()
				}
				prevDigest = digest
			}
		}
	}
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
	topicScoreParams := s.topicScoreParams(topic.Name)
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
	case strings.Contains(topic, gossip.TopicNameBeaconBlock):
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

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	gossip_topic GossipTopic
	host         peer.ID
	ch           chan *GossipMessage
	ctx          context.Context

	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc

	setup  sync.Once
	stopCh chan struct{}
}

func (sub *GossipSubscription) Listen() (err error) {
	sub.setup.Do(func() {
		sub.stopCh = make(chan struct{}, 3)
		sub.sub, err = sub.topic.Subscribe()
		if err != nil {
			err = fmt.Errorf("failed to begin topic %s subscription, err=%w", sub.topic.String(), err)
			return
		}
		var sctx context.Context
		sctx, sub.cf = context.WithCancel(sub.ctx)
		go sub.run(sctx, sub.sub, sub.sub.Topic())
	})
	return nil
}

// calls the cancel func for the subscriber and closes the topic and sub
func (s *GossipSubscription) Close() {
	s.stopCh <- struct{}{}
	if s.cf != nil {
		s.cf()
	}
	if s.rf != nil {
		s.rf()
	}
	if s.sub != nil {
		s.sub.Cancel()
		s.sub = nil
	}
	if s.topic != nil {
		s.topic.Close()
		s.topic = nil
	}
}

type GossipMessage struct {
	From      peer.ID
	TopicName string
	Data      []byte
}

// this is a helper to begin running the gossip subscription.
// function should not be used outside of the constructor for gossip subscription
func (s *GossipSubscription) run(ctx context.Context, sub *pubsub.Subscription, topicName string) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("[Sentinel Gossip] Message Handler Crashed", "err", r)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		default:
			msg, err := sub.Next(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("[Sentinel] fail to decode gossip packet", "err", err, "topicName", topicName)
				return
			}
			if msg.GetFrom() == s.host {
				continue
			}
			s.ch <- &GossipMessage{
				From:      msg.GetFrom(),
				TopicName: topicName,
				Data:      common.Copy(msg.Data),
			}
		}
	}
}

func (g *GossipSubscription) Publish(data []byte) error {
	return g.topic.Publish(g.ctx, data)
}
