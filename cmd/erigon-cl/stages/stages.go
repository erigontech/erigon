package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context, beaconsBlocks StageBeaconsBlockCfg, beaconState StageBeaconStateCfg) []*stagedsync.Stage {
	return []*stagedsync.Stage{
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
				return SpawnStageBeaconState(beaconState, s, tx, ctx)
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
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.BeaconState,
	triggerExecution triggerExecutionFunc,
	clearEth1Data bool,
) (*stagedsync.Sync, error) {
	return stagedsync.New(
		ConsensusStages(
			ctx,
			StageBeaconsBlock(db, forwardDownloader, genesisCfg, beaconCfg, state),
			StageBeaconState(db, genesisCfg, beaconCfg, state, triggerExecution, clearEth1Data),
		),
		ConsensusUnwindOrder,
		ConsensusPruneOrder,
	), nil
}
