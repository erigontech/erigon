package network

import (
	"context"
	"runtime"

	"github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
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
	ctx context.Context

	recorder   freezer.Freezer
	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinel.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
}

func NewGossipReceiver(ctx context.Context, s sentinel.SentinelClient, forkChoice *forkchoice.ForkChoiceStore,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, recorder freezer.Freezer) *GossipManager {
	return &GossipManager{
		sentinel:      s,
		forkChoice:    forkChoice,
		ctx:           ctx,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		recorder:      recorder,
	}
}

func (g *GossipManager) onRecv(data *sentinel.GossipData, l log.Ctx) error {

	currentEpoch := utils.GetCurrentEpoch(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot, g.beaconConfig.SlotsPerEpoch)
	version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)

	// Depending on the type of the received data, we create an instance of a specific type that implements the ObjectSSZ interface,
	// then attempts to deserialize the received data into it.
	// If the deserialization fails, an error is logged and the loop returns to the next iteration.
	// If the deserialization is successful, the object is set to the deserialized value and the loop returns to the next iteration.
	var object ssz.Unmarshaler
	switch data.Type {
	case sentinel.GossipType_BeaconBlockGossipType:
		object = &cltypes.SignedBeaconBlock{}
		if err := object.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
			g.sentinel.BanPeer(g.ctx, data.Peer)
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
			if _, err := g.sentinel.PublishGossip(g.ctx, data); err != nil {
				log.Debug("failed publish gossip", "err", err)
			}
		}

		count, err := g.sentinel.GetPeers(g.ctx, &sentinel.EmptyMessage{})
		if err != nil {
			l["at"] = "sentinel peer count"
			return err
		}
		var m runtime.MemStats
		dbg.ReadMemStats(&m)

		log.Debug("Received block via gossip",
			"peers", count.Amount,
			"slot", block.Block.Slot,
			"alloc/sys", libcommon.ByteCount(m.Alloc)+"/"+libcommon.ByteCount(m.Sys),
			"numGC", m.NumGC,
		)

		if err := freezer.PutObjectSSZIntoFreezer("signedBeaconBlock", "caplin_core", block.Block.Slot, block, g.recorder); err != nil {
			return err
		}

		peers := metrics.GetOrCreateGauge("caplin_peer_count", func() float64 {
			return float64(count.Amount)
		})

		peers.Get()

		if err := g.forkChoice.OnBlock(block, true, true); err != nil {
			// if we are within a quarter of an epoch within chain tip we ban it
			if currentSlotByTime < g.forkChoice.HighestSeen()+(g.beaconConfig.SlotsPerEpoch/4) {
				g.sentinel.BanPeer(g.ctx, data.Peer)
			}
			l["at"] = "block process"
			return err
		}
		block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
			if err = g.forkChoice.OnAttestation(a, true); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			l["at"] = "attestation process"
			return err
		}
		// Now check the head
		headRoot, headSlot, err := g.forkChoice.GetHead()
		if err != nil {
			l["slot"] = block.Block.Slot
			l["at"] = "fetch head data"
			return err
		}
		// Do forkchoice if possible
		if g.forkChoice.Engine() != nil {
			finalizedCheckpoint := g.forkChoice.FinalizedCheckpoint()
			// Run forkchoice
			if err := g.forkChoice.Engine().ForkChoiceUpdate(
				g.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
				g.forkChoice.GetEth1Hash(headRoot),
			); err != nil {
				l["at"] = "sending forkchoice"
				return err
			}
		}
		// Log final result
		log.Debug("New gossip block imported",
			"slot", block.Block.Slot,
			"head", headSlot,
			"headRoot", headRoot,
		)
	case sentinel.GossipType_VoluntaryExitGossipType:
		object = &cltypes.SignedVoluntaryExit{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			g.sentinel.BanPeer(g.ctx, data.Peer)
			l["at"] = "decode exit"
			return err
		}
	case sentinel.GossipType_ProposerSlashingGossipType:
		object = &cltypes.ProposerSlashing{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			l["at"] = "decode proposer slash"
			g.sentinel.BanPeer(g.ctx, data.Peer)
			return err
		}
	case sentinel.GossipType_AttesterSlashingGossipType:
		object = &cltypes.AttesterSlashing{}
		if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
			l["at"] = "decode attester slash"
			g.sentinel.BanPeer(g.ctx, data.Peer)
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
			g.sentinel.BanPeer(g.ctx, data.Peer)
			return err
		}
	}
	return nil
}

func (g *GossipManager) Start() {
	subscription, err := g.sentinel.SubscribeGossip(g.ctx, &sentinel.EmptyMessage{})
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
		err = g.onRecv(data, l)
		if err != nil {
			l["err"] = err
			log.Debug("[Beacon Gossip] Recoverable Error", l)
		}
	}
}
