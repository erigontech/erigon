package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/utils"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context,
	forkchoice StageForkChoiceCfg,
) []*stagedsync.Stage {
	return []*stagedsync.Stage{
		//{
		//	ID:          stages.BeaconHistoryReconstruction,
		//	Description: "Download beacon blocks backwards.",
		//	Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
		//		return SpawnStageHistoryReconstruction(historyReconstruction, s, tx, ctx, logger)
		//	},
		//	Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
		//		return nil
		//	},
		//},
		//{
		//	ID:          stages.BeaconState,
		//	Description: "Execute Consensus Layer transition",
		//	Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
		//		return SpawnStageBeaconState(beaconState, tx, ctx)
		//	},
		//	Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
		//		return nil
		//	},
		//},
		//{
		//	ID:          stages.BeaconBlocks,
		//	Description: "Download beacon blocks forward.",
		//	Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
		//		return SpawnStageForkChoice(forkchoice, s, tx, ctx)
		//	},
		//	Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
		//		return nil
		//	},
		//},
		{
			ID:          "wait_for_peers",
			Description: "wait for enough peers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageWaitForPeers(forkchoice, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "catch_up_blocks",
			Description: "catch up blocks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
				firstTime := false
				for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
					if !firstTime {
						firstTime = true
						log.Debug("Caplin may have missed some slots, started downloading chain")
					}
					ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
					cfg.downloader.RequestMore(ctx)
					cancel()
					if utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) == cfg.forkChoice.HighestSeen() {
						break
					}
				}
				if firstTime {
					log.Debug("Finished catching up", "slot", cfg.downloader.GetHighestProcessedSlot())
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "update_downloader",
			Description: "update downloader stage",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
				maxBlockBehindBeforeDownload := int64(32)
				overtimeMargin := uint64(6) // how much time has passed before trying download the next block in seconds

				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				overtime := utils.GetCurrentSlotOverTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				seenSlot := cfg.forkChoice.HighestSeen()
				if targetSlot == seenSlot || (targetSlot == seenSlot+1 && overtime < overtimeMargin) {
					return nil
				}
				highestSeen := cfg.forkChoice.HighestSeen()
				startDownloadSlot := highestSeen - uint64(maxBlockBehindBeforeDownload)
				// Detect underflow
				if startDownloadSlot > highestSeen {
					startDownloadSlot = 0
				}
				cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
				cfg.downloader.SetHighestProcessedSlot(
					utils.Max64(startDownloadSlot, cfg.forkChoice.FinalizedSlot()))
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "fork_choice",
			Description: "fork choice stage",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
				maxBlockBehindBeforeDownload := int64(32)
				overtimeMargin := uint64(6) // how much time has passed before trying download the next block in seconds

				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				overtime := utils.GetCurrentSlotOverTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				seenSlot := cfg.forkChoice.HighestSeen()

				if targetSlot == seenSlot || (targetSlot == seenSlot+1 && overtime < overtimeMargin) {
					time.Sleep(time.Second)
					return nil
				}
				highestSeen := cfg.forkChoice.HighestSeen()
				startDownloadSlot := highestSeen - uint64(maxBlockBehindBeforeDownload)
				// Detect underflow
				if startDownloadSlot > highestSeen {
					startDownloadSlot = 0
				}
				cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
				cfg.downloader.SetHighestProcessedSlot(
					utils.Max64(startDownloadSlot, cfg.forkChoice.FinalizedSlot()))
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
	}
}

var ConsensusUnwindOrder = stagedsync.UnwindOrder{}

var ConsensusPruneOrder = stagedsync.PruneOrder{}

func NewConsensusStagedSync(ctx context.Context,
	db kv.RwDB,
	forwardDownloader *network2.ForwardBeaconDownloader,
	backwardDownloader *network2.BackwardBeaconDownloader,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	tmpdir string,
	executionClient *execution_client.ExecutionClient,
	beaconDBCfg *rawdb.BeaconDataConfig,
	gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore,
	logger log.Logger,
) (*stagedsync.Sync, error) {
	return stagedsync.New(
		ConsensusStages(
			ctx,
			StageForkChoice(db, forwardDownloader, genesisCfg, beaconCfg, state, executionClient, gossipManager, forkChoice, nil, nil),
		),
		ConsensusUnwindOrder,
		ConsensusPruneOrder,
		logger,
	), nil
}
