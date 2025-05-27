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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/gossip"
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
	digest, err := s.ethClock.CurrentForkDigest()
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	var exp atomic.Value
	exp.Store(expiration)
	sub = &GossipSubscription{
		gossip_topic: topic,
		ch:           s.subManager.ch,
		host:         s.host.ID(),
		ctx:          s.ctx,
		expiration:   exp,
		s:            s,
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
	digest, err := s.ethClock.CurrentForkDigest()
	if err != nil {
		log.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}
	s.subManager.unsubscribe(fmt.Sprintf("/eth2/%x/%s/%s", digest, topic.Name, topic.CodecStr))

	return nil
}

func (s *Sentinel) topicScoreParams(topic string) *pubsub.TopicScoreParams {
	switch {
	case strings.Contains(topic, gossip.TopicNameBeaconBlock) || gossip.IsTopicBlobSidecar(topic):
		return s.defaultBlockTopicParams()
	case strings.Contains(topic, gossip.TopicNameVoluntaryExit):
		return s.defaultVoluntaryExitTopicParams()
	case gossip.IsTopicBeaconAttestation(topic):
		return s.defaultAggregateSubnetTopicParams()
	case gossip.IsTopicSyncCommittee(topic):
		return s.defaultSyncSubnetTopicParams(s.cfg.ActiveIndicies)

	default:
		return nil
	}
}

// Based on the prysm parameters.
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

func (s *Sentinel) defaultVoluntaryExitTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     voluntaryExitWeight,
		TimeInMeshWeight:                maxInMeshScore / s.inMeshCap(),
		TimeInMeshQuantum:               s.oneSlotDuration(),
		TimeInMeshCap:                   s.inMeshCap(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     s.scoreDecay(100 * s.oneEpochDuration()),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   s.scoreDecay(50 * s.oneEpochDuration()),
	}
}

func (s *Sentinel) defaultSyncSubnetTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	subnetCount := s.cfg.BeaconConfig.SyncCommitteeSubnetCount
	// Get weight for each specific subnet.
	topicWeight := syncCommitteesTotalWeight / float64(subnetCount)
	syncComSize := s.cfg.BeaconConfig.SyncCommitteeSize
	// Set the max as the sync committee size
	if activeValidators > syncComSize {
		activeValidators = syncComSize
	}
	subnetWeight := activeValidators / subnetCount
	if subnetWeight == 0 {
		log.Warn("Subnet weight is 0, skipping initializing topic scoring")
		return nil
	}
	firstDecayDuration := 1 * s.oneEpochDuration()
	meshDecayDuration := 4 * s.oneEpochDuration()

	rate := subnetWeight * 2 / gossipSubD
	if rate == 0 {
		log.Warn("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(s.scoreDecay(firstDecayDuration), float64(rate))
	if err != nil {
		log.Warn("Skipping initializing topic scoring")
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(s.scoreDecay(meshDecayDuration), float64(subnetWeight)/dampeningFactor)
	if err != nil {
		log.Warn("Skipping initializing topic scoring")
		return nil
	}
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / s.inMeshCap(),
		TimeInMeshQuantum:               s.oneSlotDuration(),
		TimeInMeshCap:                   s.inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     s.scoreDecay(firstDecayDuration),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      s.scoreDecay(meshDecayDuration),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: s.oneEpochDuration(),
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         s.scoreDecay(meshDecayDuration),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   s.scoreDecay(50 * s.oneEpochDuration()),
	}
}

// decayLimit provides the value till which a decay process will
// limit till provided with an expected growth rate.
func decayLimit(decayRate, rate float64) (float64, error) {
	if 1 <= decayRate {
		return 0, fmt.Errorf("got an invalid decayLimit rate: %f", decayRate)
	}
	return rate / (1 - decayRate), nil
}

func (s *Sentinel) committeeCountPerSlot() uint64 {
	activeValidatorCount := s.cfg.ActiveIndicies
	cfg := s.cfg.BeaconConfig
	var committeesPerSlot = activeValidatorCount / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize

	if committeesPerSlot > cfg.MaxCommitteesPerSlot {
		return cfg.MaxCommitteesPerSlot
	}
	if committeesPerSlot == 0 {
		return 1
	}

	return committeesPerSlot
}

// maxScore attainable by a peer.
func maxScore() float64 {
	totalWeight := beaconBlockWeight + aggregateWeight + syncContributionWeight +
		attestationTotalWeight + syncCommitteesTotalWeight + attesterSlashingWeight +
		proposerSlashingWeight + voluntaryExitWeight + blsToExecutionChangeWeight
	return (maxInMeshScore + maxFirstDeliveryScore) * totalWeight
}

// is used to determine the threshold from the decay limit with
// a provided growth rate. This applies the decay rate to a
// computed limit.
func decayThreshold(decayRate, rate float64) (float64, error) {
	d, err := decayLimit(decayRate, rate)
	if err != nil {
		return 0, err
	}
	return d * decayRate, nil
}

func (s *Sentinel) defaultAggregateSubnetTopicParams() *pubsub.TopicScoreParams {
	subnetCount := s.cfg.NetworkConfig.AttestationSubnetCount
	// Get weight for each specific subnet.
	topicWeight := float64(attestationTotalWeight) / float64(subnetCount)
	subnetWeight := s.cfg.ActiveIndicies / subnetCount
	if subnetWeight == 0 {
		log.Warn("Subnet weight is 0, skipping initializing topic scoring", "activeValidatorCount", s.cfg.ActiveIndicies)
		return nil
	}
	// Determine the amount of validators expected in a subnet in a single slot.
	numPerSlot := time.Duration(subnetWeight / s.cfg.BeaconConfig.SlotsPerEpoch)
	if numPerSlot == 0 {
		log.Trace("numPerSlot is 0, skipping initializing topic scoring")
		return nil
	}
	comsPerSlot := s.committeeCountPerSlot()
	exceedsThreshold := comsPerSlot >= 2*subnetCount/s.cfg.BeaconConfig.SlotsPerEpoch
	firstDecayDuration := 1 * s.oneEpochDuration()
	meshDecayDuration := 4 * s.oneEpochDuration()
	if exceedsThreshold {
		firstDecayDuration = 4 * s.oneEpochDuration()
		meshDecayDuration = 16 * s.oneEpochDuration()
	}
	rate := numPerSlot * 2 / gossipSubD
	if rate == 0 {
		log.Trace("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(s.scoreDecay(firstDecayDuration), float64(rate))
	if err != nil {
		log.Trace("skipping initializing topic scoring", "err", err)
		return nil
	}
	firstMessageWeight := float64(maxFirstDeliveryScore) / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(s.scoreDecay(meshDecayDuration), float64(numPerSlot)/float64(dampeningFactor))
	if err != nil {
		log.Trace("skipping initializing topic scoring", "err", err)
		return nil
	}
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / s.inMeshCap(),
		TimeInMeshQuantum:               s.oneSlotDuration(),
		TimeInMeshCap:                   s.inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     s.scoreDecay(firstDecayDuration),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesDecay:      s.scoreDecay(meshDecayDuration),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * s.oneEpochDuration(),
		MeshFailurePenaltyDecay:         s.scoreDecay(meshDecayDuration),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   s.scoreDecay(50 * s.oneEpochDuration()),
	}
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
