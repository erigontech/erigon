package stages

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context, historyReconstruction StageHistoryReconstructionCfg, beaconState StageBeaconStateCfg, forkchoice StageForkChoiceCfg) []*stagedsync.Stage {
	return []*stagedsync.Stage{
		{
			ID:          stages.BeaconHistoryReconstruction,
			Description: "Download beacon blocks backwards.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageHistoryReconstruction(historyReconstruction, s, tx, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.BeaconState,
			Description: "Execute Consensus Layer transition",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageBeaconState(beaconState, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.BeaconBlocks,
			Description: "Download beacon blocks forward.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageForkChoice(forkchoice, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
	}
}

var ConsensusUnwindOrder = stagedsync.UnwindOrder{
	stages.BeaconState,
	stages.BeaconBlocks,
}

var ConsensusPruneOrder = stagedsync.PruneOrder{
	stages.BeaconState,
	stages.BeaconBlocks,
}

func NewConsensusStagedSync(ctx context.Context,
	db kv.RwDB,
	forwardDownloader *network2.ForwardBeaconDownloader,
	backwardDownloader *network2.BackwardBeaconDownloader,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.BeaconState,
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
			StageHistoryReconstruction(db, backwardDownloader, genesisCfg, beaconCfg, beaconDBCfg, state, tmpdir, executionClient),
			StageBeaconState(db, beaconCfg, state, executionClient),
			StageForkChoice(db, forwardDownloader, genesisCfg, beaconCfg, state, executionClient, gossipManager, forkChoice, nil),
		),
		ConsensusUnwindOrder,
		ConsensusPruneOrder,
		logger,
	), nil
}
