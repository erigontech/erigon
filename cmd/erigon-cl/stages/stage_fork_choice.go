package stages

import (
	"context"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type StageForkChoiceCfg struct {
	db              kv.RwDB
	downloader      *network.ForwardBeaconDownloader
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.BeaconState
	gossipManager   *network.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
}

func StageForkChoice(db kv.RwDB, downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, executionClient *execution_client.ExecutionClient, gossipManager *network.GossipManager, forkChoice *forkchoice.ForkChoiceStore) StageForkChoiceCfg {
	return StageForkChoiceCfg{
		db:              db,
		downloader:      downloader,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
	}
}

// StageForkChoice enables the fork choice state. it is never supposed to exit this stage once it gets in.
func SpawnStageForkChoice(cfg StageForkChoiceCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	/*useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}*/
	// Start download service
	log.Info("Listening to gossip now")
	// We start gossip management.
	go cfg.gossipManager.Start()
	go onTickService(ctx, cfg)
	startDownloadService(s, cfg)
	// We also need to start on tick service

	/*if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}*/
	return nil
}

func startDownloadService(s *stagedsync.StageState, cfg StageForkChoiceCfg) {
	cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
	cfg.downloader.SetHighestProcessedSlot(cfg.state.Slot())
	cfg.downloader.SetProcessFunction(func(highestSlotProcessed uint64, highestBlockRootProcessed libcommon.Hash, newBlocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, newHighestBlockRootProcessed libcommon.Hash, err error) {
		// Setup
		newHighestSlotProcessed = highestSlotProcessed
		newHighestBlockRootProcessed = highestBlockRootProcessed
		// Skip if segment is empty
		if len(newBlocks) == 0 {
			return
		}
		// Retrieve last blocks to do reverse soft checks
		var lastRootInSegment libcommon.Hash
		lastBlockInSegment := newBlocks[len(newBlocks)-1]
		lastSlotInSegment := lastBlockInSegment.Block.Slot
		lastRootInSegment, err = lastBlockInSegment.Block.HashSSZ()
		parentRoot := lastBlockInSegment.Block.ParentRoot

		if err != nil {
			return
		}

		for i := len(newBlocks) - 2; i >= 0; i-- {
			var blockRoot libcommon.Hash
			blockRoot, err = newBlocks[i].Block.HashSSZ()
			if err != nil {
				return
			}
			// Check if block root makes sense, if not segment is invalid
			if blockRoot != parentRoot {
				return
			}
			// Update the parent root.
			parentRoot = newBlocks[i].Block.ParentRoot
			if parentRoot == highestBlockRootProcessed {
				// We found a connection point? interrupt cycle and move on.
				newBlocks = newBlocks[i:]
				break
			}
		}
		// If segment is not recconecting then skip.
		if parentRoot != highestBlockRootProcessed && highestBlockRootProcessed != (libcommon.Hash{}) {
			return
		}
		for _, block := range newBlocks {
			if err := cfg.forkChoice.OnBlock(block, false); err != nil {
				log.Warn("Could not download block", "reason", err)
			}
		}
		// Checks done, update all internals accordingly
		return lastSlotInSegment, lastRootInSegment, nil
	})
	maxBlockBehindBeforeDownload := int64(5)
	firstTime := true

	for {
		targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
		if int64(targetSlot)-int64(cfg.forkChoice.HighestSeen()) < maxBlockBehindBeforeDownload {
			time.Sleep(time.Second)
			continue
		}
		// if not the first time then send back the downloader a little bit.
		if !firstTime {
			cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
			cfg.downloader.SetHighestProcessedSlot(cfg.forkChoice.HighestSeen() - uint64(maxBlockBehindBeforeDownload))
		}
		firstTime = false
		log.Info("Caplin is behind, started downloading chain")
		// If we are too behind we download
		logInterval := time.NewTicker(30 * time.Second)

		triggerInterval := time.NewTicker(150 * time.Millisecond)
		// Process blocks until we reach our target
		for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
			currentSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
			// Send request every 50 Millisecond only if not on chain tip
			if currentSlot != highestProcessed {
				cfg.downloader.RequestMore()
			}

			if err := cfg.downloader.ProcessBlocks(); err != nil {
				log.Warn("Could not download block in processing", "reason", err)
			}
			select {
			case <-logInterval.C:
				log.Info("Collected blocks", "slot", cfg.downloader.GetHighestProcessedSlot())
			case <-triggerInterval.C:
			}
		}
		log.Info("Finished catching up")
		logInterval.Stop()
		triggerInterval.Stop()
	}
}

func onTickService(ctx context.Context, cfg StageForkChoiceCfg) {
	tickInterval := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-tickInterval.C:
			//cfg.forkChoice.OnTick(uint64(time.Now().Unix()))
		case <-ctx.Done():
			return
		}
	}
}
