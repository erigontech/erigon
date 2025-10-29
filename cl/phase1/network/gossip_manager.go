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

package network

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinelproto.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	networkConfig *clparams.NetworkConfig
	ethClock      eth_clock.EthereumClock

	emitters     *beaconevents.EventEmitter
	committeeSub *committee_subscription.CommitteeSubscribeMgmt

	registeredServices []gossipService
	stats              *gossipMessageStats
}

func NewGossipReceiver(
	s sentinelproto.SentinelClient,
	forkChoice *forkchoice.ForkChoiceStore,
	beaconConfig *clparams.BeaconChainConfig,
	networkConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.EventEmitter,
	comitteeSub *committee_subscription.CommitteeSubscribeMgmt,
	blockService services.BlockService,
	blobService services.BlobSidecarsService,
	dataColumnSidecarService services.DataColumnSidecarService,
	syncCommitteeMessagesService services.SyncCommitteeMessagesService,
	syncContributionService services.SyncContributionService,
	aggregateAndProofService services.AggregateAndProofService,
	attestationService services.AttestationService,
	voluntaryExitService services.VoluntaryExitService,
	blsToExecutionChangeService services.BLSToExecutionChangeService,
	proposerSlashingService services.ProposerSlashingService,
) *GossipManager {
	gm := &GossipManager{
		sentinel:           s,
		forkChoice:         forkChoice,
		emitters:           emitters,
		beaconConfig:       beaconConfig,
		networkConfig:      networkConfig,
		ethClock:           ethClock,
		committeeSub:       comitteeSub,
		registeredServices: []gossipService{},
		stats:              &gossipMessageStats{},
	}
	attesterSlashingService := services.NewAttesterSlashingService(forkChoice)
	// register services
	RegisterGossipService(gm, blockService, withRateLimiterByPeer(1, 2))
	RegisterGossipService(gm, syncContributionService, withRateLimiterByPeer(8, 16))
	RegisterGossipService(gm, aggregateAndProofService, withRateLimiterByPeer(8, 16))
	RegisterGossipService(gm, syncCommitteeMessagesService, withRateLimiterByPeer(8, 16))
	RegisterGossipService(gm, attesterSlashingService, withRateLimiterByPeer(2, 8))
	RegisterGossipService(gm, voluntaryExitService, withRateLimiterByPeer(2, 8))
	RegisterGossipService(gm, blsToExecutionChangeService, withRateLimiterByPeer(2, 8))
	RegisterGossipService(gm, proposerSlashingService, withRateLimiterByPeer(2, 8))
	RegisterGossipService(gm, attestationService, withGlobalTimeBasedRateLimiter(6*time.Second, 250))
	RegisterGossipService(gm, blobService, withBeginVersion(clparams.DenebVersion), withEndVersion(clparams.FuluVersion), withGlobalTimeBasedRateLimiter(6*time.Second, 32))
	// fulu
	RegisterGossipService(gm, dataColumnSidecarService, withBeginVersion(clparams.FuluVersion), withRateLimiterByPeer(32, 64))

	gm.stats.goPrintStats()
	return gm
}

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
