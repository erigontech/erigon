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

	"github.com/c2h5oh/datasize"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"

	"github.com/erigontech/erigon-lib/log/v3"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinel.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	networkConfig *clparams.NetworkConfig
	ethClock      eth_clock.EthereumClock

	emitters     *beaconevents.EventEmitter
	committeeSub *committee_subscription.CommitteeSubscribeMgmt

	// Services for processing messages from the network
	blockService                 services.BlockService
	blobService                  services.BlobSidecarsService
	dataColumnSidecarService     services.DataColumnSidecarService
	syncCommitteeMessagesService services.SyncCommitteeMessagesService
	syncContributionService      services.SyncContributionService
	aggregateAndProofService     services.AggregateAndProofService
	attestationService           services.AttestationService
	voluntaryExitService         services.VoluntaryExitService
	blsToExecutionChangeService  services.BLSToExecutionChangeService
	proposerSlashingService      services.ProposerSlashingService
	attestationsLimiter          *timeBasedRateLimiter
}

func NewGossipReceiver(
	s sentinel.SentinelClient,
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
	return &GossipManager{
		sentinel:                     s,
		forkChoice:                   forkChoice,
		emitters:                     emitters,
		beaconConfig:                 beaconConfig,
		networkConfig:                networkConfig,
		ethClock:                     ethClock,
		committeeSub:                 comitteeSub,
		blockService:                 blockService,
		blobService:                  blobService,
		dataColumnSidecarService:     dataColumnSidecarService,
		syncCommitteeMessagesService: syncCommitteeMessagesService,
		syncContributionService:      syncContributionService,
		aggregateAndProofService:     aggregateAndProofService,
		attestationService:           attestationService,
		voluntaryExitService:         voluntaryExitService,
		blsToExecutionChangeService:  blsToExecutionChangeService,
		proposerSlashingService:      proposerSlashingService,
		attestationsLimiter:          newTimeBasedRateLimiter(6*time.Second, 250),
	}
}

func (g *GossipManager) onRecv(ctx context.Context, data *sentinel.GossipData, l log.Ctx) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	// Make a copy of the gossip data so that we the received data is not modified.
	// 1) When we publish and corrupt the data, the peers bans us.
	// 2) We decode the block wrong
	data = &sentinel.GossipData{
		Name:     data.Name,
		Peer:     data.Peer,
		SubnetId: data.SubnetId,
		Data:     common.CopyBytes(data.Data),
	}
	monitor.ObserveGossipTopicSeen(data.Name, len(data.Data))

	if err := g.routeAndProcess(ctx, data); err != nil {
		return err
	}
	if errors.Is(err, services.ErrIgnore) || errors.Is(err, synced_data.ErrNotSynced) {
		return nil
	}
	if err != nil { //nolint:govet
		g.sentinel.BanPeer(ctx, data.Peer)
		return err
	}
	return nil
}

func (g *GossipManager) isReadyToProcessOperations() bool {
	return g.forkChoice.HighestSeen()+8 >= g.ethClock.GetCurrentSlot()
}

func copyOfPeerData(in *sentinel.GossipData) *sentinel.Peer {
	if in == nil || in.Peer == nil {
		return nil
	}
	ret := new(sentinel.Peer)
	ret.State = in.Peer.State
	ret.Pid = in.Peer.Pid
	ret.Enr = in.Peer.Enr
	ret.Direction = in.Peer.Direction
	ret.AgentVersion = in.Peer.AgentVersion
	ret.Address = in.Peer.Address

	return ret
}

func (g *GossipManager) routeAndProcess(ctx context.Context, data *sentinel.GossipData) error {
	currentEpoch := g.ethClock.GetCurrentEpoch()
	version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)

	// Depending on the type of the received data, we create an instance of a specific type that implements the ObjectSSZ interface,
	// then attempts to deserialize the received data into it.
	// If the deserialization fails, an error is logged and the loop returns to the next iteration.
	// If the deserialization is successful, the object is set to the deserialized value and the loop returns to the next iteration.
	switch data.Name {
	case gossip.TopicNameBeaconBlock:
		obj := cltypes.NewSignedBeaconBlock(g.beaconConfig, version)
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		log.Debug("Received block via gossip", "slot", obj.Block.Slot)
		return g.blockService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameSyncCommitteeContributionAndProof:
		obj := &services.SignedContributionAndProofForGossip{
			Receiver:                   copyOfPeerData(data),
			SignedContributionAndProof: &cltypes.SignedContributionAndProof{},
		}
		if err := obj.SignedContributionAndProof.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.syncContributionService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameVoluntaryExit:
		obj := &services.SignedVoluntaryExitForGossip{
			Receiver:            copyOfPeerData(data),
			SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{},
		}
		if err := obj.SignedVoluntaryExit.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.voluntaryExitService.ProcessMessage(ctx, data.SubnetId, obj)

	case gossip.TopicNameProposerSlashing:
		obj := &cltypes.ProposerSlashing{}
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.proposerSlashingService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameAttesterSlashing:
		attesterSlashing := cltypes.NewAttesterSlashing(version)
		if err := attesterSlashing.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		g.sentinel.BanPeer(ctx, data.Peer)
		if err := g.forkChoice.OnAttesterSlashing(attesterSlashing, false); err != nil {
			return err
		}

		return nil
	case gossip.TopicNameBlsToExecutionChange:
		obj := &services.SignedBLSToExecutionChangeForGossip{
			Receiver:                   copyOfPeerData(data),
			SignedBLSToExecutionChange: &cltypes.SignedBLSToExecutionChange{},
		}
		if err := obj.SignedBLSToExecutionChange.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.blsToExecutionChangeService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameBeaconAggregateAndProof:
		obj := &services.SignedAggregateAndProofForGossip{
			Receiver:                copyOfPeerData(data),
			SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{},
		}
		if err := obj.SignedAggregateAndProof.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
			return err
		}
		return g.aggregateAndProofService.ProcessMessage(ctx, data.SubnetId, obj)
	default:
		switch {
		case gossip.IsTopicBlobSidecar(data.Name):
			// decode sidecar
			blobSideCar := &cltypes.BlobSidecar{}
			if err := blobSideCar.DecodeSSZ(data.Data, int(version)); err != nil {
				return err
			}
			defer log.Debug("Received blob sidecar via gossip", "index", *data.SubnetId, "size", datasize.ByteSize(len(blobSideCar.Blob)))
			// The background checks above are enough for now.
			return g.blobService.ProcessMessage(ctx, data.SubnetId, blobSideCar)
		case gossip.IsTopicDataColumnSidecar(data.Name):
			// decode sidecar
			dataColumnSidecar := &cltypes.DataColumnSidecar{}
			if err := dataColumnSidecar.DecodeSSZ(data.Data, int(version)); err != nil {
				return err
			}
			return g.dataColumnSidecarService.ProcessMessage(ctx, data.SubnetId, dataColumnSidecar)
		case gossip.IsTopicSyncCommittee(data.Name):
			obj := &services.SyncCommitteeMessageForGossip{
				Receiver:             copyOfPeerData(data),
				SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{},
			}
			if err := obj.SyncCommitteeMessage.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
				return err
			}
			return g.syncCommitteeMessagesService.ProcessMessage(ctx, data.SubnetId, obj)
		case gossip.IsTopicBeaconAttestation(data.Name):
			obj := &services.AttestationForGossip{
				Receiver: copyOfPeerData(data),
				//Attestation:       &solid.Attestation{},
				//SingleAttestation: &solid.SingleAttestation{},
				ImmediateProcess: false,
			}
			if version <= clparams.DenebVersion {
				obj.Attestation = &solid.Attestation{}
				if err := obj.Attestation.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
					return err
				}
				if g.committeeSub.NeedToAggregate(obj.Attestation) || g.attestationsLimiter.tryAcquire() {
					return g.attestationService.ProcessMessage(ctx, data.SubnetId, obj)
				}
			} else {
				// after electra
				obj.SingleAttestation = &solid.SingleAttestation{}
				if err := obj.SingleAttestation.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
					return err
				}
				if g.attestationsLimiter.tryAcquire() {
					return g.attestationService.ProcessMessage(ctx, data.SubnetId, obj)
				}
			}
			return services.ErrIgnore
		default:
			return fmt.Errorf("unknown topic %s", data.Name)
		}
	}
}

func (g *GossipManager) Start(ctx context.Context) {
	attestationCh := make(chan *sentinel.GossipData, 1<<20) // large quantity of attestation messages from gossip
	operationsCh := make(chan *sentinel.GossipData, 1<<16)
	blobsCh := make(chan *sentinel.GossipData, 1<<16)
	blocksCh := make(chan *sentinel.GossipData, 1<<10)
	syncCommitteesCh := make(chan *sentinel.GossipData, 1<<16)
	dataColumnSidecarCh := make(chan *sentinel.GossipData, 1<<16)
	defer close(operationsCh)
	defer close(blobsCh)
	defer close(blocksCh)
	defer close(syncCommitteesCh)
	defer close(attestationCh)
	defer close(dataColumnSidecarCh)

	// Start couple of goroutines that listen for new gossip messages and sends them to the operations processor.
	goWorker := func(ch <-chan *sentinel.GossipData, workerCount int) {
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

	sendOrDrop := func(ch chan<- *sentinel.GossipData, data *sentinel.GossipData) {
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

		subscription, err := g.sentinel.SubscribeGossip(ctx, &sentinel.SubscriptionData{}, grpc.WaitForReady(true))
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
