package network

import (
	"context"
	"runtime"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

// Gossip manager is sending all messages to fork choice or others
type GossipManager struct {
	ctx context.Context

	forkChoice *forkchoice.ForkChoiceStore
	sentinel   sentinel.SentinelClient
	// configs
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
}

func NewGossipReceiver(ctx context.Context, s sentinel.SentinelClient, forkChoice *forkchoice.ForkChoiceStore, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) *GossipManager {
	return &GossipManager{
		sentinel:      s,
		forkChoice:    forkChoice,
		ctx:           ctx,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
	}
}

func (g *GossipManager) Start() {
	subscription, err := g.sentinel.SubscribeGossip(g.ctx, &sentinel.EmptyMessage{})
	if err != nil {
		return
	}

	for {
		data, err := subscription.Recv()
		if err != nil {
			log.Debug("[Beacon Gossip] Failure in receiving", "err", err)
			break
		}

		currentEpoch := utils.GetCurrentEpoch(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot, g.beaconConfig.SlotsPerEpoch)
		version := g.beaconConfig.GetCurrentStateVersion(currentEpoch)

		// Depending on the type of the received data, we create an instance of a specific type that implements the ObjectSSZ interface,
		// then attempts to deserialize the received data into it.
		// If the deserialization fails, an error is logged and the loop continues to the next iteration.
		// If the deserialization is successful, the object is set to the deserialized value and the loop continues to the next iteration.
		var object ssz.Unmarshaler
		switch data.Type {
		case sentinel.GossipType_BeaconBlockGossipType:
			object = &cltypes.SignedBeaconBlock{}
			if err := object.DecodeSSZ(common.CopyBytes(data.Data), int(version)); err != nil {
				log.Debug("[Beacon Gossip] Failure in decoding block", "err", err)
				g.sentinel.BanPeer(g.ctx, data.Peer)
				continue
			}
			block := object.(*cltypes.SignedBeaconBlock)

			currentSlotByTime := utils.GetCurrentSlot(g.genesisConfig.GenesisTime, g.beaconConfig.SecondsPerSlot)
			maxGossipSlotThreshold := uint64(4)
			// Skip if slot is too far behind.
			if block.Block.Slot+maxGossipSlotThreshold < currentSlotByTime {
				continue
			}
			if block.Block.Slot+maxGossipSlotThreshold == currentSlotByTime {
				if _, err := g.sentinel.PublishGossip(g.ctx, data); err != nil {
					log.Debug("cannot publish gossip", "err", err)
				}
			}

			log.Debug("Received block via gossip", "slot", block.Block.Slot)

			if err := g.forkChoice.OnBlock(block, true, true); err != nil {
				// if we are within a quarter of an epoch within chain tip we ban it
				if currentSlotByTime < g.forkChoice.HighestSeen()+(g.beaconConfig.SlotsPerEpoch/4) {
					g.sentinel.BanPeer(g.ctx, data.Peer)
				}
				log.Debug("[Beacon Gossip] Failure in processing block", "err", err)
				continue
			}
			for _, attestation := range block.Block.Body.Attestations {
				if err := g.forkChoice.OnAttestation(attestation, true); err != nil {
					log.Debug("[Beacon Gossip] Failure in processing attestation", "err", err)
					continue
				}
			}
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			// Now check the head
			headRoot, headSlot, err := g.forkChoice.GetHead()
			if err != nil {
				log.Debug("Could not fetch head data", "err", err)
				log.Debug("New block imported",
					"slot", block.Block.Slot,
					"alloc", libcommon.ByteCount(m.Alloc),
					"sys", libcommon.ByteCount(m.Sys),
					"numGC", m.NumGC,
				)
				continue
			}
			// Do forkchoice if possible
			if g.forkChoice.Engine() != nil {
				finalizedCheckpoint := g.forkChoice.FinalizedCheckpoint()
				// Run forkchoice
				if err := g.forkChoice.Engine().ForkChoiceUpdate(
					g.forkChoice.GetEth1Hash(finalizedCheckpoint.Root),
					g.forkChoice.GetEth1Hash(headRoot),
				); err != nil {
					log.Warn("Could send not forkchoice", "err", err)
					return
				}
			}

			// Log final result
			log.Debug("New block imported",
				"slot", block.Block.Slot, "head", headSlot, "headRoot", headRoot,
				"alloc", libcommon.ByteCount(m.Alloc),
				"sys", libcommon.ByteCount(m.Sys),
				"numGC", m.NumGC,
			)
		case sentinel.GossipType_VoluntaryExitGossipType:
			object = &cltypes.SignedVoluntaryExit{}
			if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
				log.Debug("[Beacon Gossip] Failure in decoding exit", "err", err)
				g.sentinel.BanPeer(g.ctx, data.Peer)
				continue
			}
		case sentinel.GossipType_ProposerSlashingGossipType:
			object = &cltypes.ProposerSlashing{}
			if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
				log.Debug("[Beacon Gossip] Failure in decoding proposer slashing", "err", err)
				g.sentinel.BanPeer(g.ctx, data.Peer)
				continue
			}
		case sentinel.GossipType_AttesterSlashingGossipType:
			object = &cltypes.AttesterSlashing{}
			if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
				log.Debug("[Beacon Gossip] Failure in decoding attester slashing", "err", err)
				g.sentinel.BanPeer(g.ctx, data.Peer)
				continue
			}
			if err := g.forkChoice.OnAttesterSlashing(object.(*cltypes.AttesterSlashing)); err != nil {
				log.Debug("[Beacon Gossip] Failure in processing block", "err", err)
				continue
			}
		case sentinel.GossipType_AggregateAndProofGossipType:
			object = &cltypes.SignedAggregateAndProof{}
			if err := object.DecodeSSZ(data.Data, int(version)); err != nil {
				log.Debug("[Beacon Gossip] Failure in decoding proof", "err", err)
				g.sentinel.BanPeer(g.ctx, data.Peer)
				continue
			}
		}
	}
}
