package lightclient

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

// ChainTipSubscriber tells us what are the newly received gossip arguments
type ChainTipSubscriber struct {
	ctx context.Context

	currBlock *cltypes.BeaconBlock // Most recent gossipped block

	lastUpdate       *cltypes.LightClientUpdate
	started          bool
	sentinel         sentinel.SentinelClient
	beaconConfig     *clparams.BeaconChainConfig
	genesisConfig    *clparams.GenesisConfig
	lastReceivedSlot uint64

	mu sync.Mutex
}

func NewChainTipSubscriber(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, sentinel sentinel.SentinelClient) *ChainTipSubscriber {
	return &ChainTipSubscriber{
		ctx:           ctx,
		started:       false,
		sentinel:      sentinel,
		genesisConfig: genesisConfig,
		beaconConfig:  beaconConfig,
	}
}

func (c *ChainTipSubscriber) StartLoop() {
	if c.started {
		log.Error("Chain tip subscriber already started")
		return
	}
	log.Info("[LightClient Gossip] Started Gossip")
	c.started = true
	stream, err := c.sentinel.SubscribeGossip(c.ctx, &sentinel.EmptyMessage{})
	if err != nil {
		log.Warn("could not start lightclient", "reason", err)
		return
	}
	defer stream.CloseSend()

	for {
		data, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Debug("[Lightclient] could not read gossip :/", "reason", err)
			}
			continue
		}
		if err := c.handleGossipData(data); err != nil {
			log.Warn("could not process new gossip",
				"gossipType", data.Type, "reason", err)
		}
	}
}

func (c *ChainTipSubscriber) handleGossipData(data *sentinel.GossipData) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentEpoch := utils.GetCurrentEpoch(c.genesisConfig.GenesisTime, c.beaconConfig.SecondsPerSlot, c.beaconConfig.SlotsPerEpoch)
	version := c.beaconConfig.GetCurrentStateVersion(currentEpoch)

	switch data.Type {
	case sentinel.GossipType_BeaconBlockGossipType:
		block := &cltypes.SignedBeaconBlock{}
		if err := block.DecodeSSZWithVersion(data.Data, int(version)); err != nil {
			return fmt.Errorf("could not unmarshall block: %s", err)
		}

		c.currBlock = block.Block
		c.lastReceivedSlot = block.Block.Slot
	case sentinel.GossipType_LightClientFinalityUpdateGossipType:
		finalityUpdate := &cltypes.LightClientFinalityUpdate{}
		if err := finalityUpdate.DecodeSSZWithVersion(data.Data, int(version)); err != nil {
			return fmt.Errorf("could not unmarshall finality update: %s", err)
		}
		c.lastUpdate = &cltypes.LightClientUpdate{
			AttestedHeader:          finalityUpdate.AttestedHeader,
			NextSyncCommitee:        nil,
			NextSyncCommitteeBranch: nil,
			FinalizedHeader:         finalityUpdate.FinalizedHeader,
			FinalityBranch:          finalityUpdate.FinalityBranch,
			SyncAggregate:           finalityUpdate.SyncAggregate,
			SignatureSlot:           finalityUpdate.SignatureSlot,
		}
	case sentinel.GossipType_LightClientOptimisticUpdateGossipType:
		if c.lastUpdate != nil && c.lastUpdate.IsFinalityUpdate() {
			// We already have a finality update, we can skip this one
			return nil
		}

		optimisticUpdate := &cltypes.LightClientOptimisticUpdate{}
		if err := optimisticUpdate.DecodeSSZWithVersion(data.Data, int(version)); err != nil {
			return fmt.Errorf("could not unmarshall optimistic update: %s", err)
		}
		c.lastUpdate = &cltypes.LightClientUpdate{
			AttestedHeader:          optimisticUpdate.AttestedHeader,
			NextSyncCommitee:        nil,
			NextSyncCommitteeBranch: nil,
			FinalizedHeader:         nil,
			FinalityBranch:          nil,
			SyncAggregate:           optimisticUpdate.SyncAggregate,
			SignatureSlot:           optimisticUpdate.SignatureSlot,
		}
	default:
	}
	return nil
}

func (c *ChainTipSubscriber) PopLastUpdate() *cltypes.LightClientUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	update := c.lastUpdate
	c.lastUpdate = nil
	return update
}

func (c *ChainTipSubscriber) GetLastBlock() *cltypes.BeaconBlock {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Check if we are up to date
	currentSlot := utils.GetCurrentSlot(c.genesisConfig.GenesisTime, c.beaconConfig.SecondsPerSlot)
	// Gossip failed use the rpc and attempt to retrieve last block.
	if c.lastReceivedSlot != currentSlot {
		return nil
	}
	block := c.currBlock
	c.currBlock = nil
	return block
}
