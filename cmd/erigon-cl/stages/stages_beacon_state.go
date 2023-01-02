package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

// This function will trigger block execution, hence: insert + validate + fcu.
type triggerExecutionFunc func(*cltypes.SignedBeaconBlock) error

type StageBeaconStateCfg struct {
	db               kv.RwDB
	genesisCfg       *clparams.GenesisConfig
	beaconCfg        *clparams.BeaconChainConfig
	state            *state.BeaconState
	clearEth1Data    bool // Whether we want to discard eth1 data.
	triggerExecution triggerExecutionFunc
	executionClient  *execution_client.ExecutionClient
}

func StageBeaconState(db kv.RwDB, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, triggerExecution triggerExecutionFunc, clearEth1Data bool, executionClient *execution_client.ExecutionClient) StageBeaconStateCfg {
	return StageBeaconStateCfg{
		db:               db,
		genesisCfg:       genesisCfg,
		beaconCfg:        beaconCfg,
		state:            state,
		clearEth1Data:    clearEth1Data,
		triggerExecution: triggerExecution,
		executionClient:  executionClient,
	}
}

// SpawnStageBeaconForward spawn the beacon forward stage
func SpawnStageBeaconState(cfg StageBeaconStateCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	endSlot, err := stages.GetStageProgress(tx, stages.BeaconBlocks)
	if err != nil {
		return err
	}
	latestBlockHeader := cfg.state.LatestBlockHeader()

	fromSlot := latestBlockHeader.Slot
	for slot := fromSlot + 1; slot <= endSlot; slot++ {
		block, err := rawdb.ReadBeaconBlock(tx, slot)
		if err != nil {
			return err
		}
		// Missed proposal are absent slot
		if block == nil {
			continue
		}
		// TODO: Pass this to state transition with the state
		_ = block
	}
	// If successful update fork choice
	if cfg.executionClient != nil {
		_, _, eth1Hash, _, err := rawdb.ReadBeaconBlockForStorage(tx, endSlot)
		if err != nil {
			return err
		}
		receipt, err := cfg.executionClient.ForkChoiceUpdate(eth1Hash)
		if err != nil {
			return err
		}
		log.Info("Forkchoice Status", "outcome", receipt.Success)
	}

	// Clear all ETH1 data from CL db
	if cfg.clearEth1Data {
		if err := tx.ClearBucket(kv.Headers); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.BlockBody); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.EthTx); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.Sequence); err != nil {
			return err
		}
	}
	latestBlockHeader.Slot = endSlot
	cfg.state.SetLatestBlockHeader(latestBlockHeader)

	log.Info(fmt.Sprintf("[%s] Finished transitioning state", s.LogPrefix()), "from", fromSlot, "to", endSlot)
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
