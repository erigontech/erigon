package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

type StageBeaconsBlockCfg struct {
	db         kv.RwDB
	downloader *network.ForwardBeaconDownloader
	genesisCfg *clparams.GenesisConfig
	beaconCfg  *clparams.BeaconChainConfig
	state      *cltypes.BeaconState
}

const maxOptimisticDistance = 8

func StageBeaconsBlock(db kv.RwDB, downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *cltypes.BeaconState) StageBeaconsBlockCfg {
	return StageBeaconsBlockCfg{
		db:         db,
		downloader: downloader,
		genesisCfg: genesisCfg,
		beaconCfg:  beaconCfg,
		state:      state,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageBeaconsBlocks(cfg StageBeaconsBlockCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// For now just collect the blocks downloaded in an array
	progress := cfg.state.LatestBlockHeader.Slot
	// We add one so that we wait for Gossiped blocks if we are on chain tip.
	targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) + 1

	log.Info("[Beacon Downloading] Started", "start", progress, "target", targetSlot)
	cfg.downloader.SetHighestProcessSlot(progress)
	cfg.downloader.SetTargetSlot(targetSlot)
	cfg.downloader.SetLimitSegmentsLength(1024)
	// On new blocks we just check slot sequencing for now :)
	cfg.downloader.SetProcessFunction(func(
		highestSlotProcessed uint64,
		highestRootProcessed common.Hash,
		newBlocks []*cltypes.SignedBeaconBlockBellatrix) (newHighestSlotProcessed uint64, newHighestBlockRootProcessed common.Hash, err error) {
		newHighestSlotProcessed = highestSlotProcessed
		newHighestBlockRootProcessed = highestRootProcessed
		for _, block := range newBlocks {
			slot := block.Block.Slot
			if slot <= highestSlotProcessed || slot > newHighestSlotProcessed+maxOptimisticDistance || slot > targetSlot {
				continue
			}

			newHighestSlotProcessed = slot
			if err = rawdb.WriteBeaconBlock(tx, block); err != nil {
				return
			}
			newHighestBlockRootProcessed, err = block.Block.HashTreeRoot()
			if err != nil {
				return
			}
		}
		return
	})
	cfg.downloader.SetIsDownloading(true)
	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()
	triggerInterval := time.NewTicker(50 * time.Millisecond)
	defer triggerInterval.Stop()
	// Process blocks until we reach our target
	for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); targetSlot > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
		currentSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
		// Send request every 50 Millisecond only if not on chain tip
		if currentSlot != highestProcessed {
			cfg.downloader.RequestMore()
		}

		if err := cfg.downloader.ProcessBlocks(); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("[Beacon Downloading] Progress", "slot", cfg.downloader.GetHighestProcessedSlot())
		case <-triggerInterval.C:
		}
	}
	log.Info("Processed and collected blocks", "count", targetSlot-progress)
	if err := s.Update(tx, cfg.downloader.GetHighestProcessedSlot()); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
