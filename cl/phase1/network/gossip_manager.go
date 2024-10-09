package network

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/network/services"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/cl/validator/committee_subscription"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinel.SentinelClient
	// configs
	beaconConfig *clparams.BeaconChainConfig
	ethClock     eth_clock.EthereumClock

	emitters     *beaconevents.Emitters
	committeeSub *committee_subscription.CommitteeSubscribeMgmt

	// Services for processing messages from the network
	blockService                 services.BlockService
	blobService                  services.BlobSidecarsService
	syncCommitteeMessagesService services.SyncCommitteeMessagesService
	syncContributionService      services.SyncContributionService
	aggregateAndProofService     services.AggregateAndProofService
	attestationService           services.AttestationService
	voluntaryExitService         services.VoluntaryExitService
	blsToExecutionChangeService  services.BLSToExecutionChangeService
	proposerSlashingService      services.ProposerSlashingService
}

func NewGossipReceiver(
	s sentinel.SentinelClient,
	forkChoice *forkchoice.ForkChoiceStore,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.Emitters,
	comitteeSub *committee_subscription.CommitteeSubscribeMgmt,
	blockService services.BlockService,
	blobService services.BlobSidecarsService,
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
		ethClock:                     ethClock,
		committeeSub:                 comitteeSub,
		blockService:                 blockService,
		blobService:                  blobService,
		syncCommitteeMessagesService: syncCommitteeMessagesService,
		syncContributionService:      syncContributionService,
		aggregateAndProofService:     aggregateAndProofService,
		attestationService:           attestationService,
		voluntaryExitService:         voluntaryExitService,
		blsToExecutionChangeService:  blsToExecutionChangeService,
		proposerSlashingService:      proposerSlashingService,
	}
}

func operationsContract[T ssz.EncodableSSZ](ctx context.Context, g *GossipManager, data *sentinel.GossipData, version int, name string, fn func(T, bool) error) error {
	var t T
	object := t.Clone().(T)
	if err := object.DecodeSSZ(common.CopyBytes(data.Data), version); err != nil {
		g.sentinel.BanPeer(ctx, data.Peer)
		return err
	}
	if err := fn(object /*test=*/, false); err != nil {
		return err
	}
	if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
		log.Debug("failed publish gossip", "err", err)
	}
	return nil
}

func (g *GossipManager) onRecv(ctx context.Context, data *sentinel.GossipData, l log.Ctx) (err error) {
	// defer func() {
	// 	r := recover()
	// 	if r != nil {
	// 		err = fmt.Errorf("%v", r)
	// 	}
	// }()
	// Make a copy of the gossip data so that we the received data is not modified.
	// 1) When we publish and corrupt the data, the peers bans us.
	// 2) We decode the block wrong
	data = &sentinel.GossipData{
		Name:     data.Name,
		Peer:     data.Peer,
		SubnetId: data.SubnetId,
		Data:     common.CopyBytes(data.Data),
	}

	if err := g.routeAndProcess(ctx, data); err != nil {
		return err
	}
	if errors.Is(err, services.ErrIgnore) {
		return nil
	}
	if err != nil {
		g.sentinel.BanPeer(ctx, data.Peer)
		return err
	}
	if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
		log.Warn("failed publish gossip", "err", err)
	}
	return nil
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
		obj := cltypes.NewSignedBeaconBlock(g.beaconConfig)
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		log.Debug("Received block via gossip", "slot", obj.Block.Slot)
		return g.blockService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameSyncCommitteeContributionAndProof:
		obj := &cltypes.SignedContributionAndProof{}
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.syncContributionService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameVoluntaryExit:
		obj := &cltypes.SignedVoluntaryExit{}
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
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
		return operationsContract[*cltypes.AttesterSlashing](ctx, g, data, int(version), "attester slashing", g.forkChoice.OnAttesterSlashing)
	case gossip.TopicNameBlsToExecutionChange:
		obj := &cltypes.SignedBLSToExecutionChange{}
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
			return err
		}
		return g.blsToExecutionChangeService.ProcessMessage(ctx, data.SubnetId, obj)
	case gossip.TopicNameBeaconAggregateAndProof:
		obj := &cltypes.SignedAggregateAndProof{}
		if err := obj.DecodeSSZ(data.Data, int(version)); err != nil {
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
		case gossip.IsTopicSyncCommittee(data.Name):
			msg := &cltypes.SyncCommitteeMessage{}
			if err := msg.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
				return err
			}
			return g.syncCommitteeMessagesService.ProcessMessage(ctx, data.SubnetId, msg)
		case gossip.IsTopicBeaconAttestation(data.Name):
			att := &solid.Attestation{}
			if err := att.DecodeSSZ(data.Data, int(version)); err != nil {
				return err
			}
			return g.attestationService.ProcessMessage(ctx, data.SubnetId, att)
		default:
			return fmt.Errorf("unknown topic %s", data.Name)
		}
	}
}

func (g *GossipManager) Start(ctx context.Context) {
	operationsCh := make(chan *sentinel.GossipData, 1<<16)
	blobsCh := make(chan *sentinel.GossipData, 1<<16)
	blocksCh := make(chan *sentinel.GossipData, 1<<16)
	syncCommitteesCh := make(chan *sentinel.GossipData, 1<<16)
	defer close(operationsCh)
	defer close(blobsCh)
	defer close(blocksCh)

	// Start couple of goroutines that listen for new gossip messages and sends them to the operations processor.
	goWorker := func(ch <-chan *sentinel.GossipData, workerCount int) {
		worker := func() {
			for {
				select {
				case <-ctx.Done():
					return
				case data := <-ch:
					l := log.Ctx{}
					if err := g.onRecv(ctx, data, l); err != nil && !errors.Is(err, services.ErrIgnore) {
						log.Debug("[Beacon Gossip] Recoverable Error", "err", err)
					}
				}
			}
		}
		for i := 0; i < workerCount; i++ {
			go worker()
		}
	}
	goWorker(operationsCh, 1)
	goWorker(blocksCh, 1)
	goWorker(blobsCh, 1)
	goWorker(syncCommitteesCh, 1)

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

			if data.Name == gossip.TopicNameBeaconBlock || gossip.IsTopicBlobSidecar(data.Name) {
				blocksCh <- data
			} else if gossip.IsTopicSyncCommittee(data.Name) || data.Name == gossip.TopicNameSyncCommitteeContributionAndProof {
				syncCommitteesCh <- data
			} else {
				operationsCh <- data
			}
		}
	}
}
