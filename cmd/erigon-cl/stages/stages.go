package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context, historyReconstruction StageHistoryReconstructionCfg, beaconsBlocks StageBeaconsBlockCfg, beaconState StageBeaconStateCfg) []*stagedsync.Stage {
	return []*stagedsync.Stage{
		{
			ID:          stages.BeaconHistoryReconstruction,
			Description: "Download beacon blocks backwards.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageHistoryReconstruction(historyReconstruction, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx) error {
				return nil
			},
		},
		{
			ID:          stages.BeaconBlocks,
			Description: "Download beacon blocks forward.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageBeaconsBlocks(beaconsBlocks, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx) error {
				return nil
			},
		},
		{
			ID:          stages.BeaconState,
			Description: "Execute Consensus Layer transition",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageBeaconState(beaconState, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx) error {
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
	forwardDownloader *network.ForwardBeaconDownloader,
	backwardDownloader *network.BackwardBeaconDownloader,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.BeaconState,
	triggerExecution triggerExecutionFunc,
	clearEth1Data bool,
	tmpdir string,
	executionClient *execution_client.ExecutionClient,
	beaconDBCfg *rawdb.BeaconDataConfig,
) (*stagedsync.Sync, error) {
	return stagedsync.New(
		ConsensusStages(
			ctx,
			StageHistoryReconstruction(db, backwardDownloader, genesisCfg, beaconCfg, beaconDBCfg, state, tmpdir, executionClient),
			StageBeaconsBlock(db, forwardDownloader, genesisCfg, beaconCfg, state, executionClient),
			StageBeaconState(db, beaconCfg, state, triggerExecution, clearEth1Data, executionClient),
		),
		ConsensusUnwindOrder,
		ConsensusPruneOrder,
	), nil
}
