package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/sync_stages"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context, historyReconstruction StageHistoryReconstructionCfg, beaconsBlocks StageBeaconsBlockCfg, beaconState StageBeaconStateCfg) []*sync_stages.Stage {
	return []*sync_stages.Stage{
		{
			ID:          sync_stages.BeaconHistoryReconstruction,
			Description: "Download beacon blocks backwards.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageHistoryReconstruction(historyReconstruction, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
		},
		{
			ID:          sync_stages.BeaconBlocks,
			Description: "Download beacon blocks forward.",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageBeaconsBlocks(beaconsBlocks, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
		},
		{
			ID:          sync_stages.BeaconState,
			Description: "Execute Consensus Layer transition",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageBeaconState(beaconState, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
		},
	}
}

var ConsensusUnwindOrder = sync_stages.UnwindOrder{
	sync_stages.BeaconState,
	sync_stages.BeaconBlocks,
}

var ConsensusPruneOrder = sync_stages.PruneOrder{
	sync_stages.BeaconState,
	sync_stages.BeaconBlocks,
}

func NewConsensusStagedSync(ctx context.Context,
	db kv.RwDB,
	forwardDownloader *network.ForwardBeaconDownloader,
	backwardDownloader *network.BackwardBeaconDownloader,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.BeaconState,
	tmpdir string,
	executionClient *execution_client.ExecutionClient,
	beaconDBCfg *rawdb.BeaconDataConfig,
) (*sync_stages.Sync, error) {
	return sync_stages.New(
		ConsensusStages(
			ctx,
			StageHistoryReconstruction(db, backwardDownloader, genesisCfg, beaconCfg, beaconDBCfg, state, tmpdir, executionClient),
			StageBeaconsBlock(db, forwardDownloader, genesisCfg, beaconCfg, state, executionClient),
			StageBeaconState(db, beaconCfg, state, executionClient),
		),
		ConsensusUnwindOrder,
		ConsensusPruneOrder,
	), nil
}
