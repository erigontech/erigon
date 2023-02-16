package lightclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
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
	rpc              *rpc.BeaconRpcP2P
	lastReceivedSlot uint64

	mu sync.Mutex
}

func NewChainTipSubscriber(ctx context.Context, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, sentinel sentinel.SentinelClient, rpc *rpc.BeaconRpcP2P) *ChainTipSubscriber {
	return &ChainTipSubscriber{
		ctx:           ctx,
		started:       false,
		sentinel:      sentinel,
		genesisConfig: genesisConfig,
		beaconConfig:  beaconConfig,
		rpc:           rpc,
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

	go func() {
		reqAfter := time.NewTicker(time.Duration(c.beaconConfig.SecondsPerSlot * 2 * uint64(time.Second)))
		for {
			select {
			case <-reqAfter.C:
				var update *cltypes.LightClientOptimisticUpdate
				update, err = c.rpc.SendLightClientOptimisticUpdateReqV1()
				for err != nil {
					update, err = c.rpc.SendLightClientOptimisticUpdateReqV1()
				}
				if update.SignatureSlot < c.lastReceivedSlot {
					continue
				}
				c.lastUpdate = &cltypes.LightClientUpdate{
					AttestedHeader: update.AttestedHeader,
					SyncAggregate:  update.SyncAggregate,
					SignatureSlot:  update.SignatureSlot,
				}
			case <-c.ctx.Done():
				return
			}

		}

	}()
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
	if data.Type != sentinel.GossipType_BeaconBlockGossipType {
		return nil
	}
	currentEpoch := utils.GetCurrentEpoch(c.genesisConfig.GenesisTime, c.beaconConfig.SecondsPerSlot, c.beaconConfig.SlotsPerEpoch)
	version := c.beaconConfig.GetCurrentStateVersion(currentEpoch)

	block := &cltypes.SignedBeaconBlock{}
	if err := block.DecodeSSZWithVersion(data.Data, int(version)); err != nil {
		return fmt.Errorf("could not unmarshall block: %s", err)
	}

	c.currBlock = block.Block
	c.lastReceivedSlot = block.Block.Slot

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
