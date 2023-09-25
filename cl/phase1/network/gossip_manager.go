package network

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
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

	mu        sync.RWMutex
	subs      map[int]chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]
	totalSubs int
}

func NewGossipReceiver(s sentinel.SentinelClient, forkChoice *forkchoice.ForkChoiceStore,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, recorder freezer.Freezer) *GossipManager {
	return &GossipManager{
		sentinel:      s,
		forkChoice:    forkChoice,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		recorder:      recorder,
		subs:          make(map[int]chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]),
	}
}

// this subscribes to signed beacon blocks..... i wish this was better
func (g *GossipManager) SubscribeSignedBeaconBlocks(ctx context.Context) <-chan *peers.PeeredObject[*cltypes.SignedBeaconBlock] {
	// a really big limit because why not....
	out := make(chan *peers.PeeredObject[*cltypes.SignedBeaconBlock], 512)
	g.mu.Lock()
	g.totalSubs++
	idx := g.totalSubs
	g.subs[idx] = out
	g.mu.Unlock()
	go func() {
		<-ctx.Done()
		g.mu.Lock()
		delete(g.subs, idx)
		g.mu.Unlock()
	}()
	return out

}

func (g *GossipManager) onRecv(ctx context.Context, data *sentinel.GossipData, l log.Ctx) error {

	currentEpoch := utils.GetCurrentEpoch(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot, g.beaconConfig.SlotsPerEpoch)
	version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)

	// Depending on the type of the received data, we create an instance of a specific type that implements the ObjectSSZ interface,
	// then attempts to deserialize the received data into it.
	// If the deserialization fails, an error is logged and the loop returns to the next iteration.
	// If the deserialization is successful, the object is set to the deserialized value and the loop returns to the next iteration.
	var object ssz.Unmarshaler
	switch data.Type {
	case sentinel.GossipType_BeaconBlockGossipType:
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
			"peers", count.Amount,
			"slot", block.Block.Slot,
		)

		if err := freezer.PutObjectSSZIntoFreezer("signedBeaconBlock", "caplin_core", block.Block.Slot, block, g.recorder); err != nil {
			return err
		}

		g.mu.RLock()
		for _, v := range g.subs {
			select {
			case v <- &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: block, Peer: data.Peer.Pid}:
			default:
			}
		}
		g.mu.RUnlock()

	case sentinel.GossipType_VoluntaryExitGossipType:
		object = &cltypes.SignedVoluntaryExit{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			g.sentinel.BanPeer(ctx, data.Peer)
			l["at"] = "decode exit"
			return err
		}
	case sentinel.GossipType_ProposerSlashingGossipType:
		object = &cltypes.ProposerSlashing{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			l["at"] = "decode proposer slash"
			g.sentinel.BanPeer(ctx, data.Peer)
			return err
		}
	case sentinel.GossipType_AttesterSlashingGossipType:
		object = &cltypes.AttesterSlashing{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			l["at"] = "decode attester slash"
			g.sentinel.BanPeer(ctx, data.Peer)
			return err
		}
		if err := g.forkChoice.OnAttesterSlashing(object.(*cltypes.AttesterSlashing)); err != nil {
			l["at"] = "on attester slash"
			return err
		}
	case sentinel.GossipType_AggregateAndProofGossipType:
		object = &cltypes.SignedAggregateAndProof{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			l["at"] = "decoding proof"
			g.sentinel.BanPeer(ctx, data.Peer)
			return err
		}
		if err := g.forkChoice.OnAttestation(object.(*cltypes.SignedAggregateAndProof).Message.Aggregate, false); err != nil {
			l["at"] = "on attestation"
			log.Trace("Could not process attestation", "reason", err)
			return err
		}

	}
	return nil
}

func (g *GossipManager) Start(ctx context.Context) {
	subscription, err := g.sentinel.SubscribeGossip(ctx, &sentinel.EmptyMessage{})
	if err != nil {
		return
	}

	l := log.Ctx{}
	for {
		data, err := subscription.Recv()
		if err != nil {
			log.Warn("[Beacon Gossip] Fatal error receiving gossip", "err", err)
			break
		}
		for k := range l {
			delete(l, k)
		}
		err = g.onRecv(ctx, data, l)
		if err != nil {
			l["err"] = err
			log.Debug("[Beacon Gossip] Recoverable Error", l)
		}
	}
}
