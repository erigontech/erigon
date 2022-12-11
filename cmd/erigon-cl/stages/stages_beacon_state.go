package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

type StageBeaconStateCfg struct {
	db         kv.RwDB
	genesisCfg *clparams.GenesisConfig
	beaconCfg  *clparams.BeaconChainConfig
	state      *cltypes.BeaconState
}

func StageBeaconState(db kv.RwDB, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *cltypes.BeaconState) StageBeaconStateCfg {
	return StageBeaconStateCfg{
		db:         db,
		genesisCfg: genesisCfg,
		beaconCfg:  beaconCfg,
		state:      state,
	}
}

// SpawnStageBeaconForward spawn the beacon forward stage
func SpawnStageBeaconState(cfg StageBeaconStateCfg /*s *stagedsync.StageState,*/, tx kv.RwTx, ctx context.Context) error {
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
		// TODO: Pass this to state transition with the state
		_ = block
	}
	log.Info("[BeaconState] Finished transitioning state", "from", fromSlot, "to", endSlot)
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
