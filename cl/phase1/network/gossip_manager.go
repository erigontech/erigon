package network

import (
	"context"
	"fmt"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	recorder   freezer.Freezer
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
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, recorder freezer.Freezer, emitters *beaconevents.Emitters, gossipSource *persistence.GossipSource) *GossipManager {
	return &GossipManager{
		sentinel:      s,
		forkChoice:    forkChoice,
		emitters:      emitters,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		recorder:      recorder,
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
			log.Debug("Received blob sidecar via gossip", "index", *data.SubnetId, "size", datasize.ByteSize(len(blobSideCar.Blob)))

			// [REJECT] The sidecar's index is consistent with MAX_BLOBS_PER_BLOCK -- i.e. blob_sidecar.index < MAX_BLOBS_PER_BLOCK.
			if blobSideCar.Index >= g.beaconConfig.MaxBlobsPerBlock {
				g.sentinel.BanPeer(ctx, data.Peer)
				l["at"] = "validating blob sidecar"
				return fmt.Errorf("sidecar index exceeds maximum allowed value")
			}

			// [REJECT] The sidecar is for the correct subnet -- i.e. compute_subnet_for_blob_sidecar(blob_sidecar.index) == subnet_id.
			subnetId := blobSideCar.Index % g.beaconConfig.BlobSidecarSubnetCount
			if subnetId != *data.SubnetId {
				g.sentinel.BanPeer(ctx, data.Peer)
				l["at"] = "validating blob sidecar"
				return fmt.Errorf("sidecar index does not match topic")
			}

			// [IGNORE] The sidecar is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. validate that block_header.slot <= current_slot (a client MAY queue future sidecars for processing at the appropriate slot).
			maximumGossipClockDisparity := uint64(clparams.NetworkConfigs[clparams.MainnetNetwork].MaximumGossipClockDisparity)
			currentSlot := utils.GetCurrentSlot(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot)
			if blobSideCar.SignedBlockHeader.Header.Slot > currentSlot+maximumGossipClockDisparity {
				return fmt.Errorf("sidecar is from a future slot")
			}

			// [IGNORE] The sidecar is from a slot greater than the latest finalized slot -- i.e. validate that block_header.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
			lastFinializedSlot := (currentEpoch - 1) * g.beaconConfig.SlotsPerEpoch
			if blobSideCar.SignedBlockHeader.Header.Slot <= lastFinializedSlot {
				return fmt.Errorf("sidecar is from a slot less than or equal to the latest finalized slot")
			}

			// [REJECT] The proposer signature of blob_sidecar.signed_block_header, is valid with respect to the block_header.proposer_index pubkey.

			// [IGNORE] The sidecar's block's parent (defined by block_header.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue sidecars for processing once the parent block is retrieved).

			// // [REJECT] The sidecar's block's parent (defined by block_header.parent_root) passes validation.

			// [REJECT] The sidecar is from a higher slot than the sidecar's block's parent (defined by block_header.parent_root).

			// // [REJECT] The current finalized_checkpoint is an ancestor of the sidecar's block -- i.e. get_checkpoint_block(store, block_header.parent_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root.

			// TODO [REJECT] The sidecar's inclusion proof is valid as verified by verify_blob_sidecar_inclusion_proof(blob_sidecar).

			// // [REJECT] The sidecar's blob is valid as verified by verify_blob_kzg_proof(blob_sidecar.blob, blob_sidecar.kzg_commitment, blob_sidecar.kzg_proof).

			// [IGNORE] The sidecar is the first sidecar for the tuple (block_header.slot, block_header.proposer_index, blob_sidecar.index) with valid header signature, sidecar inclusion proof, and kzg proof.

			// [REJECT] The sidecar is proposed by the expected proposer_index for the block's slot in the context of the current shuffling (defined by block_header.parent_root/block_header.slot). If the proposer_index cannot immediately be verified against the expected shuffling, the sidecar MAY be queued for later processing while proposers for the block's branch are calculated -- in such a case do not REJECT, instead IGNORE this message.

			// if sidecar is valid
			if _, err := g.sentinel.PublishGossip(ctx, data); err != nil {
				log.Debug("failed publish gossip", "err", err)
			}
		default:
		}
	}
	return nil
}

func (g *GossipManager) Start(ctx context.Context) {
	subscription, err := g.sentinel.SubscribeGossip(ctx, &sentinel.SubscriptionData{})
	if err != nil {
		return
	}
	operationsCh := make(chan *sentinel.GossipData, 1<<16)
	blocksCh := make(chan *sentinel.GossipData, 1<<16)

	// Start a goroutine that listens for new gossip messages and sends them to the operations processor.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-operationsCh:
				l := log.Ctx{}
				err = g.onRecv(ctx, data, l)
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
				err = g.onRecv(ctx, data, l)
				if err != nil {
					log.Debug("[Beacon Gossip] Recoverable Error", "err", err)
				}
			}
		}
	}()

	for {
		data, err := subscription.Recv()
		if err != nil {
			log.Warn("[Beacon Gossip] Fatal error receiving gossip", "err", err)
			break
		}

		if data.Name == gossip.TopicNameBeaconBlock {
			blocksCh <- data
		} else {
			operationsCh <- data
		}
	}
}
