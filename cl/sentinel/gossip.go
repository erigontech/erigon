// Copyright 2022 The Erigon Authors
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
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	// beaconBlockWeight specifies the scoring weight that we apply to
	// our beacon block topic.
	beaconBlockWeight = 0.8
	// aggregateWeight specifies the scoring weight that we apply to
	// our aggregate topic.
	aggregateWeight = 0.5
	// syncContributionWeight specifies the scoring weight that we apply to
	// our sync contribution topic.
	syncContributionWeight = 0.2
	// attestationTotalWeight specifies the scoring weight that we apply to
	// our attestation subnet topic.
	attestationTotalWeight = 1
	// syncCommitteesTotalWeight specifies the scoring weight that we apply to
	// our sync subnet topic.
	syncCommitteesTotalWeight = 0.4
	// attesterSlashingWeight specifies the scoring weight that we apply to
	// our attester slashing topic.
	attesterSlashingWeight = 0.05
	// proposerSlashingWeight specifies the scoring weight that we apply to
	// our proposer slashing topic.
	proposerSlashingWeight = 0.05
	// voluntaryExitWeight specifies the scoring weight that we apply to
	// our voluntary exit topic.
	voluntaryExitWeight = 0.05
	// blsToExecutionChangeWeight specifies the scoring weight that we apply to
	// our bls to execution topic.
	blsToExecutionChangeWeight = 0.05

	// maxInMeshScore describes the max score a peer can attain from being in the mesh.
	maxInMeshScore = 10
	// maxFirstDeliveryScore describes the max score a peer can obtain from first deliveries.
	maxFirstDeliveryScore = 40

	// dampeningFactor reduces the amount by which the various thresholds and caps are created.
	dampeningFactor = 90
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
	subscriptions sync.Map // map from topic string to *GossipSubscription
}

const maxIncomingGossipMessages = 1 << 16

// construct a new gossip manager that will handle packets with the given handlerfunc
func NewGossipManager(
	ctx context.Context,
) *GossipManager {
	g := &GossipManager{
		ch:            make(chan *GossipMessage, maxIncomingGossipMessages),
		subscriptions: sync.Map{},
	}
	return g
}

func GossipSidecarTopics(maxBlobs uint64) (ret []GossipTopic) {
	for i := uint64(0); i < maxBlobs; i++ {
		ret = append(ret, GossipTopic{
			Name:     gossip.TopicNameBlobSidecar(i),
			CodecStr: SSZSnappyCodec,
		})
	}
	return
}

func (s *GossipManager) Recv() <-chan *GossipMessage {
	return s.ch
}

func (s *GossipManager) GetMatchingSubscription(match string) *GossipSubscription {
	var sub *GossipSubscription
	s.subscriptions.Range(func(topic, value interface{}) bool {
		topicStr := topic.(string)
		// take out third part of the topic by splitting on "/"
		// reference: /eth2/d31f6191/beacon_attestation_45/ssz_snappy
		parts := strings.Split(topicStr, "/")
		if len(parts) < 4 {
			return true
		}
		if parts[3] == match {
			sub = value.(*GossipSubscription)
			return false
		}
		return true
	})
	return sub
}

func (s *GossipManager) AddSubscription(topic string, sub *GossipSubscription) {
	s.subscriptions.Store(topic, sub)
}

func (s *GossipManager) unsubscribe(topic string) {
	sub, ok := s.subscriptions.LoadAndDelete(topic)
	if !ok || sub == nil {
		return
	}
	sub.(*GossipSubscription).Close()
}

func (s *Sentinel) forkWatcher() {
	prevDigest, err := s.ethClock.CurrentForkDigest()
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
			digest, err := s.ethClock.CurrentForkDigest()
			if err != nil {
				log.Error("[Gossip] Failed to calculate fork choice", "err", err)
				return
			}
			if prevDigest != digest {
				// unsubscribe and resubscribe to all topics
				s.subManager.subscriptions.Range(func(key, value interface{}) bool {
					sub := value.(*GossipSubscription)
					s.subManager.unsubscribe(key.(string))
					_, err := s.SubscribeGossip(sub.gossip_topic, sub.expiration.Load().(time.Time))
					if err != nil {
						log.Warn("[Gossip] Failed to resubscribe to topic", "err", err)
					}
					return true
				})
				prevDigest = digest
			}
		}
	}
}

func (s *Sentinel) SubscribeGossip(topic GossipTopic, expiration time.Time, opts ...pubsub.TopicOpt) (sub *GossipSubscription, err error) {
	/*	digest, err := s.ethClock.CurrentForkDigest()
		if err != nil {
			log.Error("[Gossip] Failed to calculate fork choice", "err", err)
		}
		var exp atomic.Value
		exp.Store(expiration)
		sub = &GossipSubscription{
			gossip_topic: topic,
			ch:           s.subManager.ch,
			host:         s.p2p.Host().ID(),
			ctx:          s.ctx,
			expiration:   exp,
			s:            s,
		}
		path := fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr)
		sub.topic, err = s.p2p.Pubsub().Join(path, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to join topic %s, err=%w", path, err)
		}
		topicScoreParams := s.topicScoreParams(topic.Name)
		if topicScoreParams != nil {
			sub.topic.SetScoreParams(topicScoreParams)
		}
		s.subManager.AddSubscription(path, sub)

		return sub, nil*/
	panic("do not call this")
}

func (s *Sentinel) Unsubscribe(topic GossipTopic, opts ...pubsub.TopicOpt) (err error) {
	/*	digest, err := s.ethClock.CurrentForkDigest()
		if err != nil {
			log.Error("[Gossip] Failed to calculate fork choice", "err", err)
		}
		s.subManager.unsubscribe(fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr))

		return nil*/
	panic("do not call this")
}

func (g *GossipManager) Close() {
	g.subscriptions.Range(func(key, value interface{}) bool {
		if value != nil {
			value.(*GossipSubscription).Close()
		}
		return true
	})
}

func (g *GossipManager) Start(ctx context.Context) {
	go func() {
		checkingInterval := time.NewTicker(time.Second)
		dbgLogInterval := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-checkingInterval.C:
				g.subscriptions.Range(func(key, value any) bool {
					sub := value.(*GossipSubscription)
					sub.checkIfTopicNeedsToEnabledOrDisabled()
					return true
				})
			case <-dbgLogInterval.C:
				logArgs := []interface{}{}
				g.subscriptions.Range(func(key, value any) bool {
					sub := value.(*GossipSubscription)
					sub.lock.Lock()
					if sub.topic != nil {
						logArgs = append(logArgs, sub.topic.String(), sub.subscribed.Load())
					}
					sub.lock.Unlock()
					return true
				})
				log.Trace("[Gossip] Subscriptions", "subscriptions", logArgs)
			}
		}
	}()
}

// GossipSubscription abstracts a gossip subscription to write decoded structs.
type GossipSubscription struct {
	gossip_topic GossipTopic
	host         peer.ID
	ch           chan *GossipMessage
	ctx          context.Context
	expiration   atomic.Value // Unix nano for how much we should listen to this topic
	subscribed   atomic.Bool

	topic *pubsub.Topic
	sub   *pubsub.Subscription

	cf context.CancelFunc
	rf pubsub.RelayCancelFunc

	s *Sentinel

	stopCh    chan struct{}
	closeOnce sync.Once
	lock      sync.Mutex
}

func (sub *GossipSubscription) checkIfTopicNeedsToEnabledOrDisabled() {
	sub.lock.Lock()
	defer sub.lock.Unlock()
	var err error
	expirationTime := sub.expiration.Load().(time.Time)
	if sub.subscribed.Load() && time.Now().After(expirationTime) {
		sub.stopCh <- struct{}{}
		sub.subscribed.Store(false)
		log.Info("[Gossip] Unsubscribed from topic", "topic", sub.sub.Topic())
		sub.s.updateENROnSubscription(sub.sub.Topic(), false)
		return
	}
	if !sub.subscribed.Load() && time.Now().Before(expirationTime) {
		sub.stopCh = make(chan struct{}, 3)

		sub.sub, err = sub.topic.Subscribe()
		if err != nil {
			log.Warn("[Gossip] failed to begin topic subscription", "err", err)
			return
		}
		var sctx context.Context
		sctx, sub.cf = context.WithCancel(sub.ctx)
		go sub.run(sctx, sub.sub, sub.sub.Topic())
		sub.subscribed.Store(true)
		sub.s.updateENROnSubscription(sub.sub.Topic(), true)
		log.Debug("[Gossip] Subscribed to topic", "topic", sub.sub.Topic())
	}

}

func (sub *GossipSubscription) OverwriteSubscriptionExpiry(expiry time.Time) {
	if expiry.After(sub.expiration.Load().(time.Time)) {
		sub.expiration.Store(expiry)
	}
}

// calls the cancel func for the subscriber and closes the topic and sub
func (s *GossipSubscription) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closeOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
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
	})
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
			sub.Cancel()
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

			if msg.ReceivedFrom == s.host {
				continue
			}

			s.ch <- &GossipMessage{
				From:      msg.ReceivedFrom,
				TopicName: topicName,
				Data:      common.Copy(msg.Data),
			}
		}
	}
}

func (g *GossipSubscription) Publish(data []byte) error {
	if len(g.topic.ListPeers()) < 2 {
		log.Trace("[Gossip] No peers to publish to for topic", "topic", g.topic.String())
		go func() {
			if err := g.topic.Publish(g.ctx, data, pubsub.WithReadiness(pubsub.MinTopicSize(1))); err != nil {
				g.s.logger.Debug("[Gossip] Published to topic", "topic", g.topic.String(), "err", err)
			}
		}()
		if len(g.topic.ListPeers()) == 0 {
			return errors.New("not enough peers to publish the message")
		}
		return nil
	}

	err := g.topic.Publish(g.ctx, data)
	if err != nil {
		return errors.New("failed to publish to topic due to lack of routing capacity (topic too small)")
	}
	return nil
}
