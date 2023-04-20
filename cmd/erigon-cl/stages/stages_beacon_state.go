package stages

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

type StageBeaconStateCfg struct {
	db              kv.RwDB
	beaconCfg       *clparams.BeaconChainConfig
	state           *state.BeaconState
	executionClient *execution_client.ExecutionClient
	enabled         bool
}

func StageBeaconState(db kv.RwDB,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, executionClient *execution_client.ExecutionClient) StageBeaconStateCfg {
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
		block, eth1Number, eth1Hash, err := rawdb.ReadBeaconBlock(tx, finalizedRoot, slot)
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
		fullValidate := utils.GetCurrentEpoch(cfg.state.GenesisTime(), cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) == cfg.state.Epoch()
		if err := transition.TransitionState(cfg.state, block, fullValidate); err != nil {
			log.Info("Found epoch, so stopping now...", "count", slot-(fromSlot+1), "slot", slot)
			return err
		}
		log.Info("Applied state transition", "from", slot, "to", slot+1)
	}

	log.Info("[BeaconState] Finished transitioning state", "from", fromSlot, "to", endSlot)
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
