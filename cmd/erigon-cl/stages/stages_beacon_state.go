package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

// This function will trigger block execution, hence: insert + validate + fcu.
type triggerExecutionFunc func(*cltypes.SignedBeaconBlockBellatrix) error

type StageBeaconStateCfg struct {
	db               kv.RwDB
	genesisCfg       *clparams.GenesisConfig
	beaconCfg        *clparams.BeaconChainConfig
	state            *cltypes.BeaconStateBellatrix
	clearEth1Data    bool // Whether we want to discard eth1 data.
	triggerExecution triggerExecutionFunc
}

func StageBeaconState(db kv.RwDB, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *cltypes.BeaconStateBellatrix, triggerExecution triggerExecutionFunc, clearEth1Data bool) StageBeaconStateCfg {
	return StageBeaconStateCfg{
		db:               db,
		genesisCfg:       genesisCfg,
		beaconCfg:        beaconCfg,
		state:            state,
		clearEth1Data:    clearEth1Data,
		triggerExecution: triggerExecution,
	}
}

// SpawnStageBeaconForward spawn the beacon forward stage
func SpawnStageBeaconState(cfg StageBeaconStateCfg, _ *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
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
	endSlot, err := stages.GetStageProgress(tx, stages.BeaconBlocks)
	if err != nil {
		return err
	}

	fromSlot := cfg.state.LatestBlockHeader.Slot
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
		// If successful call the insertion function
		if cfg.triggerExecution != nil {
			if err := cfg.triggerExecution(block); err != nil {
				return err
			}
		}
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
	cfg.state.LatestBlockHeader.Slot = endSlot

	log.Info("[BeaconState] Finished transitioning state", "from", fromSlot, "to", endSlot)
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
