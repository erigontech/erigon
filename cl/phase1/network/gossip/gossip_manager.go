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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/p2p"
	serviceintf "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// GossipManager is responsible for managing the gossip subscriptions and publications
// making sure that this module is simple and don't depend on network services pkg
type GossipManager struct {
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	networkConfig *clparams.NetworkConfig
	ethClock      eth_clock.EthereumClock

	registeredServices []GossipService
	stats              *gossipMessageStats
	p2p                p2p.P2PManager

	activeIndicies uint64
	subscriptions  *TopicSubscriptions
	subscribeAll   bool

	// For graceful shutdown
	cancel context.CancelFunc
}

func NewGossipManager(
	ctx context.Context,
	p2p p2p.P2PManager,
	beaconConfig *clparams.BeaconChainConfig,
	networkConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	subscribeAll bool,
	activeIndicies uint64,
	maxInboundTrafficPerPeer datasize.ByteSize,
	maxOutboundTrafficPerPeer datasize.ByteSize,
	adaptableTrafficRequirements bool,
) *GossipManager {
	cctx, cancel := context.WithCancel(ctx)

	gm := &GossipManager{
		p2p:                p2p,
		beaconConfig:       beaconConfig,
		networkConfig:      networkConfig,
		ethClock:           ethClock,
		registeredServices: []GossipService{},
		stats:              newGossipMessageStats(),
		subscriptions:      NewTopicSubscriptions(cctx, p2p),
		subscribeAll:       subscribeAll,
		activeIndicies:     activeIndicies,
		cancel:             cancel,
	}

	go gm.observeBandwidth(cctx, maxInboundTrafficPerPeer, maxOutboundTrafficPerPeer, adaptableTrafficRequirements)
	go gm.goCheckForkAndResubscribe(cctx)
	gm.stats.goPrintStats(cctx)
	return gm
}

// Close gracefully shuts down the GossipManager and all its goroutines
func (g *GossipManager) Close() error {
	g.cancel()
	return nil
}

func (g *GossipManager) newPubsubValidator(service serviceintf.Service[any], conditions ...ConditionFunc) pubsub.ValidatorEx {
	var selfID peer.ID
	if h := g.p2p.Host(); h != nil {
		selfID = h.ID()
	}
	return func(ctx context.Context, pid peer.ID, msg *pubsub.Message) (result pubsub.ValidationResult) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[GossipManager] panic in validator, rejecting message", "err", r, "topic", msg.GetTopic())
				result = pubsub.ValidationReject
			}
		}()
		// Skip validation for self-published messages: they were already validated
		// by ProcessMessage before Publish was called.
		if selfID != "" && pid == selfID {
			return pubsub.ValidationAccept
		}
		curVersion := g.beaconConfig.GetCurrentStateVersion(g.ethClock.GetCurrentEpoch())
		// parse the topic and subnet
		topic := msg.GetTopic()
		if topic == "" {
			return pubsub.ValidationReject
		}
		name := extractTopicName(topic)
		if name == "" {
			return pubsub.ValidationReject
		}

		// check if the message satisfies the extra conditions
		for _, condition := range conditions {
			if !condition(pid, msg, curVersion) {
				g.stats.addIgnore(name)
				return pubsub.ValidationIgnore
			}
		}

		// decode the message
		msgData := msg.GetData()
		if msgData == nil {
			log.Debug("[GossipManager] reject nil message", "topic", name)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}
		msgData, err := utils.DecompressSnappy(msgData, true)
		if err != nil {
			log.Debug("[GossipManager] reject decompress message", "topic", name, "err", err)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}
		version := g.beaconConfig.GetCurrentStateVersion(g.ethClock.GetCurrentEpoch())
		msgObj, err := service.DecodeGossipMessage(pid, msgData, version)
		if err != nil {
			log.Debug("[GossipManager] reject decode message", "topic", name, "err", err)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}

		// process msg
		var subnetId *uint64
		if gossip.IsTopicNameWithSubnet(name) {
			subnet := extractSubnetIndexByGossipTopic(name)
			if subnet < 0 {
				log.Debug("[GossipManager] reject invalid subnet", "topic", name, "subnet", subnet)
				g.stats.addReject(name)
				return pubsub.ValidationReject
			}
			subnetIdVal := uint64(subnet)
			subnetId = &subnetIdVal
		}
		err = service.ProcessMessage(ctx, subnetId, msgObj)
		if errors.Is(err, synced_data.ErrNotSynced) || (err != nil && strings.Contains(err.Error(), "ignore")) {
			// services.ErrIgnore is a big package. To avoid circular dependency, we use a simple string check.
			log.Trace("[GossipManager] ignore message", "topic", name, "err", err)
			g.stats.addIgnore(name)
			return pubsub.ValidationIgnore
		} else if err != nil {
			log.Warn("[GossipManager] reject message", "topic", name, "err", err)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}

		// accept
		monitor.ObserveGossipTopicSeen(name, len(msgData))
		g.stats.addAccept(name)
		return pubsub.ValidationAccept
	}
}

func (g *GossipManager) registerGossipService(service serviceintf.Service[any], conditions ...ConditionFunc) error {
	validator := g.newPubsubValidator(service)
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	// register all topics and subscribe
	for _, name := range service.Names() {
		topic := composeTopic(forkDigest, name)
		if err := g.p2p.Pubsub().RegisterTopicValidator(topic, validator); err != nil {
			return err
		}
		topicHandle, err := g.p2p.Pubsub().Join(topic)
		if err != nil {
			return err
		}
		if params := g.topicScoreParams(name); params != nil {
			if err := topicHandle.SetScoreParams(params); err != nil {
				topicHandle.Close()
				return err
			}
		}
		if err := g.subscriptions.Add(topic, topicHandle, validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.SubscribeWithExpiry(topic, g.defaultExpiryForTopic(name)); err != nil && !errors.Is(err, ErrExpiryInThePast) {
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
	topic := composeTopic(forkDigest, name)
	if err := g.subscriptions.SubscribeWithExpiry(topic, expiry); err != nil {
		return err
	}

	return nil
}

func (g *GossipManager) Publish(ctx context.Context, name string, data []byte) error {
	compressedData := utils.CompressSnappy(data)
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	topic := composeTopic(forkDigest, name)
	topicHandle := g.subscriptions.Get(topic)
	if topicHandle == nil {
		return fmt.Errorf("topic not found: %s", topic)
	}
	// Note: before publishing the message to the network, Publish() internally runs the validator function.
	return topicHandle.topic.Publish(ctx, compressedData, pubsub.WithReadiness(pubsub.MinTopicSize(1)))
}

func (g *GossipManager) goCheckForkAndResubscribe(ctx context.Context) {
	// check upcoming fork digest every slot
	ticker := time.NewTicker(time.Duration(g.beaconConfig.SecondsPerSlot) * time.Second)
	defer ticker.Stop()

	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		log.Error("[GossipManager] failed to get current fork digest", "err", err)
		panic(err)
	}

	slotLookahead := uint64(8)
	for {
		// compute upcoming ForkDigest
		epoch := g.ethClock.GetEpochAtSlot(g.ethClock.GetCurrentSlot() + slotLookahead)
		upcomingForkDigest, err := g.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			log.Warn("[GossipManager] failed to compute upcoming fork digest", "err", err)
			continue
		}
		if upcomingForkDigest != forkDigest {
			log.Debug("[GossipManager] upcoming fork digest", "old", fmt.Sprintf("%x", forkDigest), "new", fmt.Sprintf("%x", upcomingForkDigest))
			oldForkDigest := fmt.Sprintf("%x", forkDigest)

			// Start goroutine to unsubscribe old topics after delay
			go func(oldForkDigest string) {
				timer := time.NewTimer(2 * time.Duration(slotLookahead) * time.Duration(g.beaconConfig.SecondsPerSlot) * time.Second)
				defer timer.Stop()

				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					allTopics := g.subscriptions.AllTopics()
					for _, topic := range allTopics {
						if strings.Contains(topic, oldForkDigest) {
							g.subscriptions.Unsubscribe(topic)
							if err := g.subscriptions.Remove(topic); err != nil {
								log.Warn("[GossipManager] failed to remove old topic", "topic", topic, "err", err)
							}
						}
					}
				}
			}(oldForkDigest)

			// subscribe new topics immediately
			g.subscribeUpcomingTopics(upcomingForkDigest)
			forkDigest = upcomingForkDigest
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
		newTopic := composeTopic(digest, name)
		if newTopic == oldTopic {
			continue
		}
		topicHandle, err := g.p2p.Pubsub().Join(newTopic)
		if err != nil {
			return err
		}
		if params := g.topicScoreParams(name); params != nil {
			if err := topicHandle.SetScoreParams(params); err != nil {
				topicHandle.Close()
				return err
			}
		}
		if err := g.subscriptions.Add(newTopic, topicHandle, prevTopicHandle.validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.SubscribeWithExpiry(newTopic, prevTopicHandle.expiry); err != nil {
			return err
		}
	}
	return nil
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

func (g *GossipManager) observeBandwidth(ctx context.Context, maxInboundTrafficPerPeer datasize.ByteSize, maxOutboundTrafficPerPeer datasize.ByteSize, adaptableTrafficRequirements bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		topics := g.subscriptions.AllTopics()
		countAttSubnetsSubscribed := 0
		countColumnSidecarSubscribed := 0
		for _, topic := range topics {
			if strings.Contains(topic, "beacon_attestation") {
				countAttSubnetsSubscribed++
			}
			if strings.Contains(topic, "data_column_sidecar") {
				countColumnSidecarSubscribed++
			}
		}

		multiplierForAdaptableTraffic := 1.0
		if adaptableTrafficRequirements {
			multiplierForAdaptableTraffic = ((float64(countAttSubnetsSubscribed) / float64(g.networkConfig.AttestationSubnetCount)) * 8) + 1
			multiplierForAdaptableTraffic += ((float64(countColumnSidecarSubscribed) / float64(g.beaconConfig.NumberOfColumns)) * 16)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bandwidthCounter := g.p2p.BandwidthCounter()
			if bandwidthCounter == nil {
				continue
			}
			totals := bandwidthCounter.GetBandwidthTotals()
			monitor.ObserveTotalInBytes(totals.TotalIn)
			monitor.ObserveTotalOutBytes(totals.TotalOut)
			minBound := datasize.KB
			// define rate cap
			maxRateIn := float64(max(maxInboundTrafficPerPeer, minBound)) * multiplierForAdaptableTraffic
			maxRateOut := float64(max(maxOutboundTrafficPerPeer, minBound)) * multiplierForAdaptableTraffic
			host := g.p2p.Host()
			if host == nil {
				continue
			}
			peers := host.Network().Peers()
			maxPeersToBan := 16
			// do not ban peers if we have less than 1/8 of max peer count
			if len(peers) <= maxPeersToBan {
				continue
			}
			maxPeersToBan = min(maxPeersToBan, len(peers)-maxPeersToBan)

			peersToBan := make([]peer.ID, 0, len(peers))
			// Check which peers should be banned
			for _, p := range peers {
				// get peer bandwidth
				peerBandwidth := g.p2p.BandwidthCounter().GetBandwidthForPeer(p)
				// check if peer is over limit
				if peerBandwidth.RateIn > maxRateIn || peerBandwidth.RateOut > maxRateOut {
					peersToBan = append(peersToBan, p)
				}
			}
			// if we have more than 1/8 of max peer count to ban, limit to maxPeersToBan
			if len(peersToBan) > maxPeersToBan {
				peersToBan = peersToBan[:maxPeersToBan]
			}
			// ban hammer
			for _, p := range peersToBan {
				//g.p2p.Peers().SetBanStatus(p, true)
				host.Peerstore().RemovePeer(p)
				host.Network().ClosePeer(p)
			}
		}
	}
}

func composeTopic(forkDigest common.Bytes4, name string) string {
	return fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, name, gossip.SSZSnappyCodec)
}
