package stages

import (
	"context"
	"github.com/ledgerwatch/erigon/cl/transition"

	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

type StageBeaconStateCfg struct {
	db              kv.RwDB
	beaconCfg       *clparams.BeaconChainConfig
	state           *state2.CachingBeaconState
	executionClient *execution_client.ExecutionClient
	enabled         bool
}

func StageBeaconState(db kv.RwDB,
	beaconCfg *clparams.BeaconChainConfig, state *state2.CachingBeaconState, executionClient *execution_client.ExecutionClient) StageBeaconStateCfg {
	return StageBeaconStateCfg{
		db:              db,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		enabled:         false,
	}
}

// SpawnStageBeaconState is used to replay historical states
func SpawnStageBeaconState(cfg StageBeaconStateCfg, tx kv.RwTx, ctx context.Context) error {
	if !cfg.enabled {
		return nil
	}
	// This code need to be fixed.
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
		finalizedRoot, err := rawdb.ReadFinalizedBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		// Slot had a missing proposal in this case.
		if finalizedRoot == (libcommon.Hash{}) {
			continue
		}
		// TODO(Giulio2002): proper versioning
		block, eth1Number, eth1Hash, err := rawdb.ReadBeaconBlock(tx, finalizedRoot, slot, clparams.Phase0Version)
		if err != nil {
			return err
		}

		// Query execution engine only if the payload have an hash.
		if eth1Hash != (libcommon.Hash{}) {
			if block.Block.Body.ExecutionPayload, err = cfg.executionClient.ReadExecutionPayload(eth1Number, eth1Hash); err != nil {
				return err
			}
		}
		// validate fully only in current epoch.
		fullValidate := utils.GetCurrentEpoch(cfg.state.GenesisTime(), cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) == state2.Epoch(cfg.state.BeaconState)
		if err := transition.TransitionState(cfg.state, block, fullValidate); err != nil {
			log.Info("Found epoch, so stopping now...", "count", slot-(fromSlot+1), "slot", slot)
			return err
		}
		log.Info("Applied state transition", "from", slot, "to", slot+1)
	}

	log.Info("[CachingBeaconState] Finished transitioning state", "from", fromSlot, "to", endSlot)
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
