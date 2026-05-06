package caplin1

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// writeDevGenesisBeaconBlock constructs and writes the genesis beacon block to
// the index DB. This is the minimal equivalent of clstages.writeGenesisBeaconBlock
// but runs during startup before the fork choice is created.
func writeDevGenesisBeaconBlock(ctx context.Context, genesisState *state.CachingBeaconState, cfg *clparams.BeaconChainConfig, db kv.RwDB) error {
	version := genesisState.Version()
	block := cltypes.NewSignedBeaconBlock(cfg, version)
	blk := block.Block

	header := genesisState.LatestBlockHeader()
	blk.Slot = header.Slot
	blk.ProposerIndex = header.ProposerIndex
	blk.ParentRoot = header.ParentRoot

	// Fill in the state root (zeroed in LatestBlockHeader per spec).
	stateRoot, err := genesisState.HashSSZ()
	if err != nil {
		return fmt.Errorf("compute genesis state root: %w", err)
	}
	blk.StateRoot = stateRoot

	// Initialize the body with preset-aware defaults.
	body := blk.Body
	if version >= clparams.AltairVersion {
		body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	}
	if version >= clparams.BellatrixVersion {
		body.ExecutionPayload.Extra = solid.NewExtraData()
		body.ExecutionPayload.Transactions = &solid.TransactionsSSZ{}

		// Copy execution payload header from genesis state so the block hash
		// matches the EL genesis (needed for fork choice to find the EL block).
		execHeader := genesisState.LatestExecutionPayloadHeader()
		if execHeader != nil {
			body.ExecutionPayload.BlockHash = execHeader.BlockHash
			body.ExecutionPayload.StateRoot = execHeader.StateRoot
			body.ExecutionPayload.ParentHash = execHeader.ParentHash
			body.ExecutionPayload.BlockNumber = execHeader.BlockNumber
			body.ExecutionPayload.GasLimit = execHeader.GasLimit
			body.ExecutionPayload.Time = execHeader.Time
			body.ExecutionPayload.BaseFeePerGas = execHeader.BaseFeePerGas
		}

		if version >= clparams.CapellaVersion {
			body.ExecutionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
		}
	}

	blockRoot, err := blk.HashSSZ()
	if err != nil {
		return fmt.Errorf("compute genesis block root: %w", err)
	}

	log.Info("[dev-genesis] writing genesis beacon block",
		"blockRoot", common.Hash(blockRoot).Hex(),
		"stateRoot", common.Hash(stateRoot).Hex(),
		"execBlockHash", body.ExecutionPayload.BlockHash.Hex(),
	)

	return db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, true)
	})
}
