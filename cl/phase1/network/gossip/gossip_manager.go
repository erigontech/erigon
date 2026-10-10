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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/c2h5oh/datasize"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

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
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// PeerBanner is an interface for banning misbehaving peers.
type PeerBanner interface {
	BanPeer(pid string)
}

// minPublishQueueSize is the floor for the background-publish queue
// capacity, used when a chain config's sync committee is smaller than this
// (e.g. the minimal preset).
const (
	minPublishQueueSize = 64
	peerBanQueueSize    = 256
)

// publishQueueSizeFor sizes the background-publish queue to hold at least
// one full sync-committee-sized burst without dropping, when the queue
// starts empty: a validator client batches all of a slot's sync-committee
// duties into a single request. If jobs from a previous burst are still
// queued when the next one arrives, the combined total can exceed this
// capacity and part of the new burst can be rejected.
func publishQueueSizeFor(cfg *clparams.BeaconChainConfig) int {
	return max(int(cfg.SyncCommitteeSize), minPublishQueueSize)
}

// Sentinel admission errors PublishBackground returns. A non-nil return
// means the message was never queued - the caller learns this before it
// responds, instead of the failure being invisible behind an HTTP 200.
var (
	ErrPublishQueueFull      = errors.New("gossip: publish queue full")
	ErrGossipManagerShutdown = errors.New("gossip: manager shut down")
	ErrPublishJobExpired     = errors.New("gossip: message expired before publish")
	ErrPublishForkDigest     = errors.New("gossip: failed to resolve fork digest")
)

// publishQueueDroppedCounter counts messages that were never admitted to
// the queue. publishAcceptedCounter and publishOutcomeCounter below track
// admitted work separately: a message counted here was never accepted, so
// it must not also appear in publishOutcomeCounter, and vice versa.
var publishQueueDroppedCounter = metrics.GetOrCreateCounterVec(
	"caplin_gossip_publish_queue_rejected_total",
	[]string{"topic", "reason"},
	"Total background gossip publishes rejected at admission, never queued",
)

var publishAcceptedCounter = metrics.GetOrCreateCounterVec(
	"caplin_gossip_publish_accepted_total",
	[]string{"topic"},
	"Total background gossip publishes admitted to the queue",
)

// publishOutcomeCounter records exactly one terminal outcome per accepted
// job that the worker actually processes: handoff_ok (the library accepted
// the local publish - not remote delivery, which this package cannot
// observe), publish_error, panic, or expired (still queued past its
// deadline). During normal operation every accepted job reaches exactly
// one of these, so the sums are equal at quiescence; a job still queued
// when the manager shuts down reaches none of them, since nothing is left
// to drain it - that's the one case where the sums are allowed to diverge.
var publishOutcomeCounter = metrics.GetOrCreateCounterVec(
	"caplin_gossip_publish_outcome_total",
	[]string{"topic", "outcome"},
	"Terminal outcome of each background gossip publish admitted to the queue",
)

type publishJob struct {
	name       string
	data       []byte
	forkDigest common.Bytes4
	// expiry, when non-zero, is the latest wall-clock time this job is
	// still worth publishing. Checked both at admission and again just
	// before the actual publish, so work already stale by the time the
	// worker gets to it is dropped instead of spending a compress/peer
	// lookup/publish cycle on it.
	expiry time.Time
	logCtx []any
}

type peerBan struct {
	banner PeerBanner
	pid    string
}

type topicForkDigestSetter interface {
	SetTopicForkDigest(common.Bytes4)
}

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
	peerBanner         PeerBanner

	activeIndicies uint64
	subscriptions  *TopicSubscriptions
	subscribeAll   bool

	publishQueue chan publishJob
	peerBanQueue chan peerBan
	// nowFunc returns the current time for expiry checks; time.Now unless
	// overridden in tests.
	nowFunc func() time.Time
	// workerDone is closed once publishWorker has returned. Tests wait on
	// this instead of polling.
	workerDone chan struct{}
	// publishHookForTest, when non-nil, runs inside the publish worker
	// immediately before the real Publish call. Tests use it to observe or
	// pause a queued job at a known point.
	publishHookForTest func(name string, data []byte)

	// lifetimeCtx is the context the worker watches, cancelled by Close or
	// by NewGossipManager's parent context ending. PublishBackground checks
	// it directly, so a job admitted right before shutdown may end up
	// queued with nothing left to drain it - that has no functional effect,
	// since it was never going to publish either way.
	lifetimeCtx context.Context
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
		publishQueue:       make(chan publishJob, publishQueueSizeFor(beaconConfig)),
		peerBanQueue:       make(chan peerBan, peerBanQueueSize),
		nowFunc:            time.Now,
		workerDone:         make(chan struct{}),
		lifetimeCtx:        cctx,
		cancel:             cancel,
	}

	go gm.observeBandwidth(cctx, maxInboundTrafficPerPeer, maxOutboundTrafficPerPeer, adaptableTrafficRequirements)
	go gm.goCheckForkAndResubscribe(cctx)
	go gm.publishWorker(cctx)
	go gm.runPeerBans(cctx)
	//gm.stats.goPrintStats(cctx)
	return gm
}

// SetPeerBanner sets the peer banner used to ban peers that fail message verification.
func (g *GossipManager) SetPeerBanner(pb PeerBanner) {
	g.peerBanner = pb
}

// Close gracefully shuts down the GossipManager and all its goroutines.
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
		digestBytes, err := hex.DecodeString(strings.Split(topic, "/")[2])
		if err != nil || len(digestBytes) != len(common.Bytes4{}) {
			g.stats.addIgnore(name)
			return pubsub.ValidationIgnore
		}
		var forkDigest common.Bytes4
		copy(forkDigest[:], digestBytes)
		version, err := g.ethClock.StateVersionByForkDigest(forkDigest)
		if err != nil {
			g.stats.addIgnore(name)
			return pubsub.ValidationIgnore
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
		msgData, err = utils.DecompressSnappy(msgData, true)
		if err != nil {
			log.Debug("[GossipManager] reject decompress message", "topic", name, "err", err)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}
		msgObj, err := service.DecodeGossipMessage(pid, msgData, version)
		if err != nil {
			log.Debug("[GossipManager] reject decode message", "topic", name, "err", err)
			g.stats.addReject(name)
			return pubsub.ValidationReject
		}
		if message, ok := msgObj.(topicForkDigestSetter); ok {
			message.SetTopicForkDigest(forkDigest)
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
			log.Warn("[GossipManager] reject message", "topic", name, "err", err, "peer", pid)
			g.stats.addReject(name)
			if banner := g.peerBanner; banner != nil {
				select {
				case g.peerBanQueue <- peerBan{banner: banner, pid: pid.String()}:
				default:
					log.Debug("[GossipManager] peer ban queue full, dropping ban", "peer", pid)
				}
			}
			return pubsub.ValidationReject
		}

		// accept
		monitor.ObserveGossipTopicSeen(name, len(msgData))
		g.stats.addAccept(name)
		return pubsub.ValidationAccept
	}
}

func (g *GossipManager) runPeerBans(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ban := <-g.peerBanQueue:
			g.runPeerBan(ban)
		}
	}
}

func (g *GossipManager) runPeerBan(ban peerBan) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("[GossipManager] panic banning peer", "peer", ban.pid, "err", r)
		}
	}()
	ban.banner.BanPeer(ban.pid)
}

func (g *GossipManager) registerGossipService(service serviceintf.Service[any], conditions ...ConditionFunc) (subscribed, expired int, err error) {
	validator := g.newPubsubValidator(service, conditions...)
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return
	}
	// register all topics and subscribe
	for _, name := range service.Names() {
		topic := composeTopic(forkDigest, name)
		if err = g.p2p.Pubsub().RegisterTopicValidator(topic, validator); err != nil {
			return
		}
		topicHandle, joinErr := g.p2p.Pubsub().Join(topic)
		if joinErr != nil {
			err = joinErr
			return
		}
		if params := g.topicScoreParams(name); params != nil {
			if err = topicHandle.SetScoreParams(params); err != nil {
				topicHandle.Close()
				return
			}
		}
		if err = g.subscriptions.Add(topic, topicHandle, validator); err != nil {
			topicHandle.Close()
			return
		}
		err = g.subscriptions.SubscribeWithExpiry(topic, g.defaultExpiryForTopic(name))
		switch {
		case err == nil:
			subscribed++
		case errors.Is(err, ErrExpiryInThePast):
			expired++
		default:
			return
		}
		log.Debug("[GossipManager] registered topic", "topic", topic)
	}
	err = nil
	return
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
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	return g.PublishToForkDigest(ctx, forkDigest, name, data)
}

func (g *GossipManager) PublishToForkDigest(ctx context.Context, forkDigest common.Bytes4, name string, data []byte) error {
	compressedData := utils.CompressSnappy(data)
	topic := composeTopic(forkDigest, name)
	topicHandle := g.subscriptions.GetTopic(topic)
	if topicHandle == nil {
		return fmt.Errorf("topic not found: %s", topic)
	}
	// Log peer count for attestation topics to help diagnose propagation issues
	if gossip.IsTopicBeaconAttestation(name) {
		peerCount := len(g.p2p.Pubsub().ListPeers(topic))
		if peerCount == 0 {
			log.Warn("[Gossip] Publishing attestation with NO peers on subnet", "topic", name, "peerCount", peerCount)
		} else if peerCount < 3 {
			log.Debug("[Gossip] Publishing attestation with low peer count", "topic", name, "peerCount", peerCount)
		}
	}
	// Note: before publishing the message to the network, Publish() internally runs the validator function.
	// Removed MinTopicSize(1) - don't fail if no peers on subnet, message will propagate when peers join
	return topicHandle.Publish(ctx, compressedData)
}

// PublishBackground queues data for asynchronous publish to the given
// gossip topic without waiting for the network call: the actual publish
// runs on this GossipManager's own background worker. It never blocks the
// caller - not on queue capacity, and not on shutdown - though it does
// resolve the fork digest, log synchronously, and clone data (the caller
// keeps ownership of its own slice; the worker reads its own copy later).
// The fork digest is captured now, at enqueue time, rather than
// re-resolved when the worker drains the job, so a message accepted just
// before a fork activates still publishes to the topic it was validated
// against.
//
// A non-nil return means the message was never admitted: the caller learns
// this before it responds, rather than it being invisible behind an HTTP
// 200. expiry, if non-zero, is the latest time this message is still worth
// publishing - checked here and again just before the actual publish.
func (g *GossipManager) PublishBackground(name string, data []byte, expiry time.Time, logCtx ...any) error {
	if g.lifetimeCtx.Err() != nil {
		publishQueueDroppedCounter.WithLabelValues(name, "shutdown").Inc()
		fields := append([]any{"topic", name}, logCtx...)
		log.Debug("[GossipManager] gossip manager shut down, dropping message", fields...)
		return ErrGossipManagerShutdown
	}
	if !expiry.IsZero() && g.nowFunc().After(expiry) {
		publishQueueDroppedCounter.WithLabelValues(name, "expired").Inc()
		fields := append([]any{"topic", name, "expiry", expiry}, logCtx...)
		log.Debug("[GossipManager] message already expired, dropping", fields...)
		return ErrPublishJobExpired
	}
	forkDigest, err := g.ethClock.CurrentForkDigest()
	if err != nil {
		publishQueueDroppedCounter.WithLabelValues(name, "fork_digest_error").Inc()
		fields := append([]any{"topic", name, "err", err}, logCtx...)
		log.Warn("[GossipManager] failed to resolve fork digest, dropping message", fields...)
		return fmt.Errorf("%w: %w", ErrPublishForkDigest, err)
	}
	// Cloned so a caller reusing or mutating data after this call returns
	// can't change what the worker publishes later - the synchronous
	// Publish read data before returning, so it never had this gap.
	select {
	case g.publishQueue <- publishJob{name: name, data: bytes.Clone(data), forkDigest: forkDigest, expiry: expiry, logCtx: logCtx}:
		publishAcceptedCounter.WithLabelValues(name).Inc()
		return nil
	default:
		publishQueueDroppedCounter.WithLabelValues(name, "queue_full").Inc()
		// Debug, not Warn: under congestion this can fire hundreds of times
		// per slot: publishQueueDroppedCounter is the aggregate signal, and
		// callers with a request-level view can log a summary themselves.
		fields := append([]any{"topic", name}, logCtx...)
		log.Debug("[GossipManager] publish queue full, dropping message", fields...)
		return ErrPublishQueueFull
	}
}

func (g *GossipManager) publishWorker(ctx context.Context) {
	defer close(g.workerDone)
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-g.publishQueue:
			g.runPublishJob(ctx, job)
		}
	}
}

func (g *GossipManager) runPublishJob(ctx context.Context, job publishJob) {
	defer func() {
		if r := recover(); r != nil {
			fields := append([]any{"err", r, "topic", job.name}, job.logCtx...)
			log.Error("[GossipManager] panic in background publish, dropping message", fields...)
			publishOutcomeCounter.WithLabelValues(job.name, "panic").Inc()
		}
	}()
	if g.publishHookForTest != nil {
		g.publishHookForTest(job.name, job.data)
	}
	if !job.expiry.IsZero() && g.nowFunc().After(job.expiry) {
		fields := append([]any{"topic", job.name, "expiry", job.expiry}, job.logCtx...)
		log.Debug("[GossipManager] queued message expired before publish, dropping", fields...)
		publishOutcomeCounter.WithLabelValues(job.name, "expired").Inc()
		return
	}
	if err := g.PublishToForkDigest(ctx, job.forkDigest, job.name, job.data); err != nil {
		fields := append([]any{"topic", job.name, "err", err}, job.logCtx...)
		log.Warn("[GossipManager] failed to publish message to gossip", fields...)
		publishOutcomeCounter.WithLabelValues(job.name, "publish_error").Inc()
		return
	}
	publishOutcomeCounter.WithLabelValues(job.name, "handoff_ok").Inc()
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
		// Wait for the next slot tick before checking. This ensures that
		// RegisterGossipServices has completed before we attempt to
		// re-subscribe topics for an upcoming fork.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// compute upcoming ForkDigest
		epoch := g.ethClock.GetEpochAtSlot(g.ethClock.GetCurrentSlot() + slotLookahead)
		upcomingForkDigest, err := g.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			log.Warn("[GossipManager] failed to compute upcoming fork digest", "err", err)
			continue
		}
		if upcomingForkDigest != forkDigest {
			log.Info("[GossipManager] upcoming fork digest change detected", "old", fmt.Sprintf("%x", forkDigest), "new", fmt.Sprintf("%x", upcomingForkDigest))
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
							if err := g.subscriptions.Unsubscribe(topic); err != nil {
								log.Warn("[GossipManager] failed to unsubscribe from old topic", "topic", topic, "err", err)
							}
							if err := g.subscriptions.Remove(topic); err != nil {
								log.Warn("[GossipManager] failed to remove old topic", "topic", topic, "err", err)
							}
						}
					}
				}
			}(oldForkDigest)

			// subscribe new topics immediately
			if err := g.subscribeUpcomingTopics(upcomingForkDigest); err != nil {
				log.Warn("[GossipManager] failed to subscribe upcoming topics", "err", err)
			}
			forkDigest = upcomingForkDigest
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
		if err := g.p2p.Pubsub().RegisterTopicValidator(newTopic, prevTopicHandle.validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.Add(newTopic, topicHandle, prevTopicHandle.validator); err != nil {
			topicHandle.Close()
			return err
		}
		if err := g.subscriptions.SubscribeWithExpiry(newTopic, prevTopicHandle.expiry); err != nil && !errors.Is(err, ErrExpiryInThePast) {
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
	if name == "" {
		return -1
	}
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
				if err := host.Network().ClosePeer(p); err != nil {
					log.Debug("[GossipManager] failed to close bandwidth-banned peer", "peer", p, "err", err)
				}
			}
		}
	}
}

func composeTopic(forkDigest common.Bytes4, name string) string {
	return fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, name, gossip.SSZSnappyCodec)
}
