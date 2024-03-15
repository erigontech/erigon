package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinel.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig

	emitters     *beaconevents.Emitters
	mu           sync.RWMutex
	gossipSource *persistence.GossipSource
}

func NewGossipReceiver(s sentinel.SentinelClient, forkChoice *forkchoice.ForkChoiceStore,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, emitters *beaconevents.Emitters, gossipSource *persistence.GossipSource) *GossipManager {
	return &GossipManager{
		sentinel:      s,
		forkChoice:    forkChoice,
		emitters:      emitters,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		gossipSource:  gossipSource,
	}
}

func operationsContract[T ssz.EncodableSSZ](ctx context.Context, g *GossipManager, l log.Ctx, data *sentinel.GossipData, version int, name string, fn func(T, bool) error) error {
	var t T
	object := t.Clone().(T)
	if err := object.DecodeSSZ(common.CopyBytes(data.Data), version); err != nil {
		g.sentinel.BanPeer(ctx, data.Peer)
		l["at"] = fmt.Sprintf("decoding %s", name)
		return err
	}
	if err := fn(object /*test=*/, false); err != nil {
		l["at"] = fmt.Sprintf("verify %s", name)
		return err
	}
	if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
		log.Debug("failed publish gossip", "err", err)
	}
	return nil
}

func (g *GossipManager) onRecv(ctx context.Context, data *sentinel.GossipData, l log.Ctx) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	currentEpoch := utils.GetCurrentEpoch(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot, g.beaconConfig.SlotsPerEpoch)
	version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)

	// Depending on the type of the received data, we create an instance of a specific type that implements the ObjectSSZ interface,
	// then attempts to deserialize the received data into it.
	// If the deserialization fails, an error is logged and the loop returns to the next iteration.
	// If the deserialization is successful, the object is set to the deserialized value and the loop returns to the next iteration.
	var object ssz.Unmarshaler
	switch data.Name {
	case gossip.TopicNameBeaconBlock:
		object = cltypes.NewSignedBeaconBlock(g.beaconConfig)
		if err := object.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
			g.sentinel.BanPeer(ctx, data.Peer)
			l["at"] = "decoding block"
			return err
		}
		block := object.(*cltypes.SignedBeaconBlock)
		l["slot"] = block.Block.Slot
		currentSlotByTime := utils.GetCurrentSlot(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot)
		maxGossipSlotThreshold := uint64(4)
		// Skip if slot is too far behind.
		if block.Block.Slot+maxGossipSlotThreshold < currentSlotByTime {
			return nil
		}
		if block.Block.Slot == currentSlotByTime {
			if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
				log.Debug("failed publish gossip", "err", err)
			}
		}

		count, err := g.sentinel.GetPeers(ctx, &sentinel.EmptyMessage{})
		if err != nil {
			l["at"] = "sentinel peer count"
			return err
		}

		log.Debug("Received block via gossip",
			"peers", count.Active,
			"slot", block.Block.Slot,
		)
		g.gossipSource.InsertBlock(ctx, &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: block, Peer: data.Peer.Pid})
	case gossip.TopicNameLightClientFinalityUpdate:
		obj := &cltypes.LightClientFinalityUpdate{}
		if err := obj.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
			g.sentinel.BanPeer(ctx, data.Peer)
			l["at"] = "decoding lc finality update"
			return err
		}
	case gossip.TopicNameLightClientOptimisticUpdate:
		obj := &cltypes.LightClientOptimisticUpdate{}
		if err := obj.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
			g.sentinel.BanPeer(ctx, data.Peer)
			l["at"] = "decoding lc optimistic update"
			return err
		}
	case gossip.TopicNameSyncCommitteeContributionAndProof:
		if err := operationsContract[*cltypes.SignedContributionAndProof](ctx, g, l, data, int(version), "contribution and proof", g.forkChoice.OnSignedContributionAndProof); err != nil {
			return err
		}
	case gossip.TopicNameVoluntaryExit:
		if err := operationsContract[*cltypes.SignedVoluntaryExit](ctx, g, l, data, int(version), "voluntary exit", g.forkChoice.OnVoluntaryExit); err != nil {
			return err
		}
	case gossip.TopicNameProposerSlashing:
		if err := operationsContract[*cltypes.ProposerSlashing](ctx, g, l, data, int(version), "proposer slashing", g.forkChoice.OnProposerSlashing); err != nil {
			return err
		}
	case gossip.TopicNameAttesterSlashing:
		if err := operationsContract[*cltypes.AttesterSlashing](ctx, g, l, data, int(version), "attester slashing", g.forkChoice.OnAttesterSlashing); err != nil {
			return err
		}
	case gossip.TopicNameBlsToExecutionChange:
		if err := operationsContract[*cltypes.SignedBLSToExecutionChange](ctx, g, l, data, int(version), "bls to execution change", g.forkChoice.OnBlsToExecutionChange); err != nil {
			return err
		}
	case gossip.TopicNameBeaconAggregateAndProof:
		return nil
	// if err := operationsContract[*cltypes.SignedAggregateAndProof](ctx, g, l, data, int(version), "aggregate and proof", g.forkChoice.OnAggregateAndProof); err != nil {
	// 	return err
	// } Uncomment when fixed.
	default:
		switch {
		case gossip.IsTopicBlobSidecar(data.Name):
			// decode sidecar
			blobSideCar := &cltypes.BlobSidecar{}
			if err := blobSideCar.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
				g.sentinel.BanPeer(ctx, data.Peer)
				l["at"] = "decoding blob sidecar"
				return err
			}
			// [REJECT] The sidecar's index is consistent with MAX_BLOBS_PER_BLOCK -- i.e. blob_sidecar.index < MAX_BLOBS_PER_BLOCK.
			if blobSideCar.Index >= g.beaconConfig.MaxBlobsPerBlock {
				g.sentinel.BanPeer(ctx, data.Peer)
				return fmt.Errorf("blob index out of range")
			}
			sidecarSubnetIndex := blobSideCar.Index % g.beaconConfig.MaxBlobsPerBlock
			if sidecarSubnetIndex != *data.SubnetId {
				g.sentinel.BanPeer(ctx, data.Peer)
				return fmt.Errorf("blob index mismatch")
			}
			currentSlot := utils.GetCurrentSlot(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot)
			// [REJECT] The sidecar's slot is consistent with the current slot -- i.e. blob_sidecar.slot == current_slot.
			if blobSideCar.SignedBlockHeader.Header.Slot > currentSlot+1 {
				g.sentinel.BanPeer(ctx, data.Peer)
				return fmt.Errorf("blob slot too far ahead")
			}
			// [IGNORE] The sidecar's block's parent (defined by block_header.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue sidecars for processing once the parent block is retrieved).
			if _, has := g.forkChoice.GetHeader(blobSideCar.SignedBlockHeader.Header.ParentRoot); !has {
				return nil
			}
			blockRoot, err := blobSideCar.SignedBlockHeader.Header.HashSSZ()
			if err != nil {
				return err
			}
			// Do not bother with blocks processed by fork choice already.
			if _, has := g.forkChoice.GetHeader(blockRoot); has {
				return nil
			}
			// The background checks above are enough for now.
			if err := g.forkChoice.OnBlobSidecar(blobSideCar, false); err != nil {
				g.sentinel.BanPeer(ctx, data.Peer)
				return err
			}

			if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
				log.Debug("failed publish gossip", "err", err)
			}

			log.Debug("Received blob sidecar via gossip", "index", *data.SubnetId, "size", datasize.ByteSize(len(blobSideCar.Blob)))
		default:
		}
	}
	return nil
}

func (g *GossipManager) Start(ctx context.Context) {
	operationsCh := make(chan *sentinel.GossipData, 1<<16)
	blobsCh := make(chan *sentinel.GossipData, 1<<16)
	blocksCh := make(chan *sentinel.GossipData, 1<<16)
	defer close(operationsCh)
	defer close(blobsCh)
	defer close(blocksCh)

	// Start a goroutine that listens for new gossip messages and sends them to the operations processor.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-operationsCh:
				l := log.Ctx{}
				err := g.onRecv(ctx, data, l)
				if err != nil {
					log.Debug("[Beacon Gossip] Recoverable Error", "err", err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-blocksCh:
				l := log.Ctx{}
				err := g.onRecv(ctx, data, l)
				if err != nil {
					log.Debug("[Beacon Gossip] Recoverable Error", "err", err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-blobsCh:
				l := log.Ctx{}
				err := g.onRecv(ctx, data, l)
				if err != nil {
					log.Warn("[Beacon Gossip] Recoverable Error", "err", err)
				}
			}
		}
	}()

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

			if data.Name == gossip.TopicNameBeaconBlock {
				blocksCh <- data
			} else if gossip.IsTopicBlobSidecar(data.Name) {
				blobsCh <- data
			} else {
				operationsCh <- data
			}
		}
	}
}
