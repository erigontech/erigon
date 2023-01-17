package stages

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type StageBeaconsBlockCfg struct {
	db              kv.RwDB
	downloader      *network.ForwardBeaconDownloader
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.BeaconState
}

func StageBeaconsBlock(db kv.RwDB, downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, executionClient *execution_client.ExecutionClient) StageBeaconsBlockCfg {
	return StageBeaconsBlockCfg{
		db:              db,
		downloader:      downloader,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
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
	progress := s.BlockNumber
	var lastRoot libcommon.Hash
	if progress == 0 {
		progress = cfg.state.LatestBlockHeader().Slot
		lastRoot, err = cfg.state.BlockRoot()
	} else {
		_, _, _, lastRoot, err = rawdb.ReadBeaconBlockForStorage(tx, progress)
	}
	if err != nil {
		return err
	}
	// Initialize payload insertion batch
	executionPayloadInsertionBatch := execution_client.NewInsertBatch(cfg.executionClient)

	// We add one so that we wait for Gossiped blocks if we are on chain tip.
	targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) + 1

	log.Info(fmt.Sprintf("[%s] Started", s.LogPrefix()), "start", progress, "target", targetSlot)
	cfg.downloader.SetHighestProcessedSlot(progress)
	if cfg.downloader.HighestProcessedRoot() == (libcommon.Hash{}) {
		cfg.downloader.SetHighestProcessedRoot(lastRoot)
	}
	cfg.downloader.SetTargetSlot(targetSlot)
	cfg.downloader.SetLimitSegmentsLength(1024)
	// On new blocks we just check slot sequencing for now :)
	cfg.downloader.SetProcessFunction(func(
		highestSlotProcessed uint64,
		highestRootProcessed libcommon.Hash,
		newBlocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, newHighestBlockRootProcessed libcommon.Hash, err error) {
		// Setup
		newHighestSlotProcessed = highestSlotProcessed
		newHighestBlockRootProcessed = highestRootProcessed
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
			if parentRoot == highestRootProcessed {
				// We found a connection point? interrupt cycle and move on.
				newBlocks = newBlocks[i:]
				break
			}
		}
		// If segment is not recconecting then skip.
		if parentRoot != highestRootProcessed {
			return
		}
		for _, block := range newBlocks {
			if err = rawdb.WriteBeaconBlock(tx, block); err != nil {
				return
			}
			if cfg.executionClient != nil && block.Version() >= clparams.BellatrixVersion {
				if err = executionPayloadInsertionBatch.WriteExecutionPayload(block.Block.Body.ExecutionPayload); err != nil {
					log.Warn("Could not send Execution Payload", "err", err)
				}
			}
		}

		// Checks done, update all internals accordingly
		return lastSlotInSegment, lastRootInSegment, nil
	})
	cfg.downloader.SetIsDownloading(true)
	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()
	triggerInterval := time.NewTicker(150 * time.Millisecond)
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
			log.Info(fmt.Sprintf("[%s] Processed and collected blocks", s.LogPrefix()), "slot", cfg.downloader.GetHighestProcessedSlot())
		case <-triggerInterval.C:
		}
	}
	// Flush inserted payloads to execution client
	if err := executionPayloadInsertionBatch.Flush(); err != nil {
		return err
	}
	log.Info(fmt.Sprintf("[%s] Processed and collected blocks", s.LogPrefix()), "count", targetSlot-progress)
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
