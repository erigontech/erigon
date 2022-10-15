package lightclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/log/v3"
)

// ChainTipSubscriber tells us what are the newly received gossip arguments
type ChainTipSubscriber struct {
	ctx        context.Context
	lastBlock  *cltypes.BeaconBlockBellatrix
	lastUpdate *cltypes.LightClientUpdate
	started    bool
	sentinel   lightrpc.SentinelClient

	mu sync.Mutex
}

func NewChainTipSubscriber(ctx context.Context, sentinel lightrpc.SentinelClient) *ChainTipSubscriber {
	return &ChainTipSubscriber{
		ctx:      ctx,
		started:  false,
		sentinel: sentinel,
	}
}

func (c *ChainTipSubscriber) StartLoop() {
	if c.started {
		log.Error("Chain tip subscriber already started")
		return
	}
	log.Info("[LightClient Gossip] Started Gossip")
	c.started = true
	stream, err := c.sentinel.SubscribeGossip(c.ctx, &lightrpc.EmptyRequest{})
	if err != nil {
		log.Warn("could not start lightclient", "reason", err)
		return
	}
	defer stream.CloseSend()

	for {
		data, err := stream.Recv()
		if err == context.Canceled {
			return
		}
		if err != nil {
			log.Warn("[Lightclient] could not read gossip :/", "reason", err)
			continue
		}
		if err := c.handleGossipData(data); err != nil {
			log.Warn("could not process new gossip",
				"gossipType", data.Type, "reason", err)
		}
	}
}

func (c *ChainTipSubscriber) handleGossipData(data *lightrpc.GossipData) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch data.Type {
	case lightrpc.GossipType_BeaconBlockGossipType:
		block := &cltypes.SignedBeaconBlockBellatrix{}
		if err := block.UnmarshalSSZ(data.Data); err != nil {
			return fmt.Errorf("could not unmarshall block: %s", err)
		}
		c.lastBlock = block.Block
	case lightrpc.GossipType_LightClientFinalityUpdateGossipType:
		finalityUpdate := &cltypes.LightClientFinalityUpdate{}
		if err := finalityUpdate.UnmarshalSSZ(data.Data); err != nil {
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
	case lightrpc.GossipType_LightClientOptimisticUpdateGossipType:
		if c.lastUpdate != nil && c.lastUpdate.IsFinalityUpdate() {
			// We already have a finality update, we can skip this one
			return nil
		}

		optimisticUpdate := &cltypes.LightClientOptimisticUpdate{}
		if err := optimisticUpdate.UnmarshalSSZ(data.Data); err != nil {
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

func (c *ChainTipSubscriber) GetLastBlock() *cltypes.BeaconBlockBellatrix {
	c.mu.Lock()
	defer c.mu.Unlock()
	block := c.lastBlock
	c.lastBlock = nil
	return block
}
