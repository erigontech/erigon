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

package gossip

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	sentinel sentinelproto.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	networkConfig *clparams.NetworkConfig
	ethClock      eth_clock.EthereumClock

	registeredServices []GossipService
	stats              *gossipMessageStats
	p2p                *p2p.P2Pmanager

	subscriptions *TopicSubscriptions
	subscribeAll  bool
}

func NewGossipManager(
	p2p *p2p.P2Pmanager,
	s sentinelproto.SentinelClient,
	beaconConfig *clparams.BeaconChainConfig,
	networkConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	subscribeAll bool,
) *GossipManager {
	gm := &GossipManager{
		p2p:                p2p,
		sentinel:           s,
		beaconConfig:       beaconConfig,
		networkConfig:      networkConfig,
		ethClock:           ethClock,
		registeredServices: []GossipService{},
		stats:              &gossipMessageStats{},
		subscriptions:      NewTopicSubscriptions(),
		subscribeAll:       subscribeAll,
	}

	gm.goCheckResubscribe()
	gm.stats.goPrintStats()
	return gm
}

/*
func (g *GossipManager) onRecv(ctx context.Context, data *sentinelproto.GossipData, l log.Ctx) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	// Make a copy of the gossip data so that we the received data is not modified.
	// 1) When we publish and corrupt the data, the peers bans us.
	// 2) We decode the block wrong
	data = &sentinelproto.GossipData{
		Name:     data.Name,
		Peer:     data.Peer,
		SubnetId: data.SubnetId,
		Data:     common.CopyBytes(data.Data),
	}
	monitor.ObserveGossipTopicSeen(data.Name, len(data.Data))

	if err := g.routeAndProcess(ctx, data); errors.Is(err, services.ErrIgnore) || errors.Is(err, synced_data.ErrNotSynced) {
		return nil
	} else if err != nil { //nolint:govet
		g.sentinel.BanPeer(ctx, data.Peer)
		return err
	}
	return nil
}

func (g *GossipManager) routeAndProcess(ctx context.Context, data *sentinelproto.GossipData) error {
	currentEpoch := g.ethClock.GetCurrentEpoch()
	version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)
	for _, s := range g.registeredServices {
		if s.service.IsMyGossipMessage(data.Name) {
			// check if the message satisfies the conditions
			if !s.SatisfiesConditions(data, version) {
				g.stats.addReject(data.Name)
				return services.ErrIgnore
			}
			// decode the message
			msg, err := s.service.DecodeGossipMessage(data, version)
			if err != nil {
				log.Debug("Failed to decode gossip message", "name", data.Name, "error", err)
				g.stats.addReject(data.Name)
				g.sentinel.BanPeer(ctx, data.Peer)
				return err
			}
			// process the message
			g.stats.addAccept(data.Name)
			return s.service.ProcessMessage(ctx, data.SubnetId, msg)
		}
	}
	return fmt.Errorf("unknown message topic: %s", data.Name)
}

func (g *GossipManager) isReadyToProcessOperations() bool {
	return g.forkChoice.HighestSeen()+8 >= g.ethClock.GetCurrentSlot()
}

func (g *GossipManager) Start(ctx context.Context) {
	attestationCh := make(chan *sentinelproto.GossipData, 1<<20) // large quantity of attestation messages from gossip
	operationsCh := make(chan *sentinelproto.GossipData, 1<<16)
	blobsCh := make(chan *sentinelproto.GossipData, 1<<16)
	blocksCh := make(chan *sentinelproto.GossipData, 1<<10)
	syncCommitteesCh := make(chan *sentinelproto.GossipData, 1<<16)
	dataColumnSidecarCh := make(chan *sentinelproto.GossipData, 1<<16)
	defer close(operationsCh)
	defer close(blobsCh)
	defer close(blocksCh)
	defer close(syncCommitteesCh)
	defer close(attestationCh)
	defer close(dataColumnSidecarCh)

	// Start couple of goroutines that listen for new gossip messages and sends them to the operations processor.
	goWorker := func(ch <-chan *sentinelproto.GossipData, workerCount int) {
		worker := func() {
			for {
				select {
				case <-ctx.Done():
					return
				case data := <-ch:
					l := log.Ctx{}
					if err := g.onRecv(ctx, data, l); err != nil && !errors.Is(err, services.ErrIgnore) && !errors.Is(err, synced_data.ErrNotSynced) {
						log.Debug("[Beacon Gossip] Recoverable Error", "err", err)
					}
				}
			}
		}
		for i := 0; i < workerCount; i++ {
			go worker()
		}
	}
	goWorker(attestationCh, int(g.networkConfig.AttestationSubnetCount))
	goWorker(syncCommitteesCh, 4)
	goWorker(operationsCh, 1)
	goWorker(blocksCh, 1)
	goWorker(blobsCh, 6)
	goWorker(dataColumnSidecarCh, 6)

	sendOrDrop := func(ch chan<- *sentinelproto.GossipData, data *sentinelproto.GossipData) {
		// Skip processing the received data if the node is not ready to process operations.
		if !g.isReadyToProcessOperations() &&
			data.Name != gossip.TopicNameBeaconBlock &&
			!gossip.IsTopicBlobSidecar(data.Name) &&
			!gossip.IsTopicDataColumnSidecar(data.Name) {
			return
		}
		select {
		case ch <- data:
		default:
			log.Trace("[Beacon Gossip] Dropping message due to full channel", "topic", data.Name)
		}
	}

Reconnect:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		subscription, err := g.sentinel.SubscribeGossip(ctx, &sentinelproto.SubscriptionData{}, grpc.WaitForReady(true))
		if err != nil {
			return
		}

		for {
			data, err := subscription.Recv()
			if err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue Reconnect
				}
				log.Warn("[Beacon Gossip] Fatal error receiving gossip", "err", err)
				continue Reconnect
			}

			switch {
			case data.Name == gossip.TopicNameBeaconBlock:
				sendOrDrop(blocksCh, data)
			case gossip.IsTopicBlobSidecar(data.Name):
				sendOrDrop(blobsCh, data)
			case gossip.IsTopicDataColumnSidecar(data.Name):
				sendOrDrop(dataColumnSidecarCh, data)
			case gossip.IsTopicSyncCommittee(data.Name) || data.Name == gossip.TopicNameSyncCommitteeContributionAndProof:
				sendOrDrop(syncCommitteesCh, data)
			case gossip.IsTopicBeaconAttestation(data.Name):
				sendOrDrop(attestationCh, data)
			default:
				sendOrDrop(operationsCh, data)
			}
		}
	}
}
*/

func (g *GossipManager) registerGossipService(service GossipService) error {
	// wrap service.ProcessMessage to ValidatorEx
	validator := pubsub.ValidatorEx(func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		curVersion := g.beaconConfig.GetCurrentStateVersion(g.ethClock.GetCurrentEpoch())
		passes := service.SatisfiesConditions(pid, msg, curVersion)
		if !passes {
			g.stats.addReject(msg.GetTopic())
			return pubsub.ValidationIgnore
		}

		// parse the topic and subnet
		topic := msg.GetTopic()
		if topic == "" {
			return pubsub.ValidationReject
		}
		name := extractTopicName(topic)
		if name == "" {
			return pubsub.ValidationReject
		}
		// decode the message
		msgData := msg.GetData()
		if msgData == nil {
			return pubsub.ValidationReject
		}
		version := g.beaconConfig.GetCurrentStateVersion(g.ethClock.GetCurrentEpoch())
		msgObj, err := service.Service.DecodeGossipMessage(pid, msgData, version)
		if err != nil {
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}

		// process msg
		var subnetId *uint64
		if gossip.IsTopicNameWithSubnet(name) {
			subnet := extractSubnetIndexByGossipTopic(name)
			if subnet < 0 {
				g.stats.addReject(name)
				return pubsub.ValidationReject
			}
			subnetIdVal := uint64(subnet)
			subnetId = &subnetIdVal
		}
		err = service.Service.ProcessMessage(ctx, subnetId, msgObj)
		//if errors.Is(err, services.ErrIgnore) || errors.Is(err, synced_data.ErrNotSynced) {
		if true {
			return pubsub.ValidationIgnore
		} else if err != nil {
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}

		// accept
		monitor.ObserveGossipTopicSeen(name, len(msgData))
		g.stats.addAccept(name)
		return pubsub.ValidationAccept
	})

	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}

	// register all topics and subscribe
	for _, name := range service.Service.Names() {
		topic := fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, name, gossip.SSZSnappyCodec)
		if err := g.p2p.Pubsub().RegisterTopicValidator(topic, validator); err != nil {
			return err
		}
		topicHandle, err := g.p2p.Pubsub().Join(topic)
		if err != nil {
			return err
		}
		if err := topicHandle.SetScoreParams(g.topicScoreParams(name)); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.Add(topic, topicHandle, validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.SubscribeWithExpiry(topic, g.defaultExpiryForTopic(name)); err != nil {
			return err
		}
		log.Debug("[GossipManager] registered topic", "topic", topic)
	}
	return nil
}

func (g *GossipManager) defaultExpiryForTopic(name string) time.Time {
	if g.subscribeAll {
		return time.Unix(0, math.MaxInt64)
	}

	if gossip.IsTopicBeaconAttestation(name) ||
		gossip.IsTopicSyncCommittee(name) ||
		gossip.IsTopicDataColumnSidecar(name) {
		// control by other modules
		return time.Unix(0, 0)
	}

	return time.Unix(0, math.MaxInt64)
}

func (g *GossipManager) SubscribeWithExpiry(name string, expiry time.Time) error {
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	topic := fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, name, gossip.SSZSnappyCodec)
	return g.subscriptions.SubscribeWithExpiry(topic, expiry)
}

func (g *GossipManager) Publish(ctx context.Context, name string, data []byte) error {
	compressedData := utils.CompressSnappy(data)
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	topic := fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, name, gossip.SSZSnappyCodec)
	topicHandle := g.subscriptions.Get(topic)
	if topicHandle == nil {
		return fmt.Errorf("topic not found: %s", topic)
	}
	return topicHandle.topic.Publish(ctx, compressedData, pubsub.WithReadiness(pubsub.MinTopicSize(1)))
}

func (g *GossipManager) goCheckResubscribe() {
	// check upcoming fork digest every slot
	ticker := time.NewTicker(time.Duration(g.beaconConfig.SecondsPerSlot))
	defer ticker.Stop()
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		log.Error("[GossipManager] failed to get current fork digest", "err", err)
		panic(err)
	}
	go func() {
		for {
			// compute upcoming ForkDigest immediately
			epoch := g.ethClock.GetEpochAtSlot(g.ethClock.GetCurrentSlot() + 2)
			upcomingForkDigest, err := g.ethClock.ComputeForkDigest(epoch)
			if err != nil {
				log.Warn("[GossipManager] failed to compute upcoming fork digest", "err", err)
				continue
			}
			if upcomingForkDigest != forkDigest {
				oldForkDigest := fmt.Sprintf("%x", forkDigest)
				go func(oldForkDigest string) {
					// unsubscribe old topics after 2 slots
					time.Sleep(2 * time.Duration(g.beaconConfig.SecondsPerSlot) * time.Second)
					allTopics := g.subscriptions.AllTopics()
					for _, topic := range allTopics {
						if strings.Contains(topic, oldForkDigest) {
							if err := g.subscriptions.Remove(topic); err != nil {
								log.Warn("[GossipManager] failed to remove old topic", "topic", topic, "err", err)
							}
						}
					}
				}(oldForkDigest)

				// subscribe new topics immediately
				g.subscribeUpcomingTopics(upcomingForkDigest)
				forkDigest = upcomingForkDigest
			}

			<-ticker.C
		}
	}()
}

func (g *GossipManager) subscribeUpcomingTopics(digest common.Bytes4) error {
	allTopics := g.subscriptions.AllTopics()
	for _, oldTopic := range allTopics {
		// replace fork digest with new one
		name := extractTopicName(oldTopic)
		if name == "" {
			continue
		}
		prevTopicHandle := g.subscriptions.Get(oldTopic)
		// register and subscribe new newTopic
		newTopic := fmt.Sprintf("/eth2/%x/%s/%s", digest, name, gossip.SSZSnappyCodec)
		if newTopic == oldTopic {
			continue
		}
		topicHandle, err := g.p2p.Pubsub().Join(newTopic)
		if err != nil {
			return err
		}
		if err := topicHandle.SetScoreParams(g.topicScoreParams(name)); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.Add(newTopic, topicHandle, prevTopicHandle.validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.SubscribeWithExpiry(newTopic, prevTopicHandle.expiration); err != nil {
			return err
		}
	}
	return nil
}

func (g *GossipManager) topicScoreParams(topic string) *pubsub.TopicScoreParams {
	// todo
	return &pubsub.TopicScoreParams{}
}

func extractTopicName(topic string) string {
	// /eth2/[fork_digest]/[topic]/ssz_snappy
	tokens := strings.Split(topic, "/")
	if len(tokens) != 5 {
		return ""
	}
	return tokens[3]
}

func extractSubnetIndexByGossipTopic(name string) int {
	// e.g blob_sidecar_3, we want to extract 3
	// reject if last character is not a number
	if !unicode.IsNumber(rune(name[len(name)-1])) {
		return -1
	}
	// get the last part of the topic
	parts := strings.Split(name, "_")
	// convert it to int
	index, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		log.Warn("[Sentinel] failed to parse subnet index", "topic", name, "err", err)
		return -1
	}
	return index
}
