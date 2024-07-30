package stages

import (
	"context"
	"fmt"
	"runtime"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func computeAndNotiftyServicesOfNewForkChoice(ctx context.Context, logger log.Logger, cfg *Cfg) (headSlot uint64, headRoot common.Hash, err error) {
	// Now check the head
	headRoot, headSlot, err = cfg.forkChoice.GetHead()
	if err != nil {
		err = fmt.Errorf("failed to get head: %w", err)
		return
	}

	// Do forkchoice if possible
	if cfg.forkChoice.Engine() != nil {
		finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
		logger.Debug("Caplin is sending forkchoice")
		// Run forkchoice
		if _, err = cfg.forkChoice.Engine().ForkChoiceUpdate(
			ctx,
			cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
			cfg.forkChoice.GetEth1Hash(headRoot), nil,
		); err != nil {
			err = fmt.Errorf("failed to run forkchoice: %w", err)
			return
		}
	}
	if err2 := cfg.rpc.SetStatus(cfg.forkChoice.FinalizedCheckpoint().BlockRoot(),
		cfg.forkChoice.FinalizedCheckpoint().Epoch(),
		headRoot, headSlot); err != nil {
		logger.Warn("Could not set status", "err", err2)
	}

	return
}

func updateCanonicalChainInTheDatabase(ctx context.Context, tx kv.RwTx, headSlot uint64, headRoot common.Hash) error {
	type canonicalEntry struct {
		slot uint64
		root common.Hash
	}

	currentRoot := headRoot
	currentSlot := headSlot
	currentCanonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
	if err != nil {
		return fmt.Errorf("failed to read canonical block root: %w", err)
	}
	reconnectionRoots := []canonicalEntry{{currentSlot, currentRoot}}

	for currentRoot != currentCanonical {
		var newFoundSlot *uint64

		if currentRoot, err = beacon_indicies.ReadParentBlockRoot(ctx, tx, currentRoot); err != nil {
			return fmt.Errorf("failed to read parent block root: %w", err)
		}
		if newFoundSlot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, currentRoot); err != nil {
			return fmt.Errorf("failed to read block slot by block root: %w", err)
		}
		if newFoundSlot == nil {
			break
		}
		currentSlot = *newFoundSlot
		currentCanonical, err = beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
		if err != nil {
			return fmt.Errorf("failed to read canonical block root: %w", err)
		}
		reconnectionRoots = append(reconnectionRoots, canonicalEntry{currentSlot, currentRoot})
	}
	if err := beacon_indicies.TruncateCanonicalChain(ctx, tx, currentSlot); err != nil {
		return fmt.Errorf("failed to truncate canonical chain: %w", err)
	}
	for i := len(reconnectionRoots) - 1; i >= 0; i-- {
		if err := beacon_indicies.MarkRootCanonical(ctx, tx, reconnectionRoots[i].slot, reconnectionRoots[i].root); err != nil {
			return fmt.Errorf("failed to mark root canonical: %w", err)
		}
	}
	if err := beacon_indicies.MarkRootCanonical(ctx, tx, headSlot, headRoot); err != nil {
		return fmt.Errorf("failed to mark root canonical: %w", err)
	}
	return nil
}

func runIndexingRoutines(ctx context.Context, tx kv.RwTx, cfg *Cfg, headState *state.CachingBeaconState) error {
	preverifiedValidators := cfg.forkChoice.PreverifiedValidator(headState.FinalizedCheckpoint().BlockRoot())
	preverifiedHistoricalSummary := cfg.forkChoice.PreverifiedHistoricalSummaries(headState.FinalizedCheckpoint().BlockRoot())
	preverifiedHistoricalRoots := cfg.forkChoice.PreverifiedHistoricalRoots(headState.FinalizedCheckpoint().BlockRoot())

	if err := state_accessors.IncrementPublicKeyTable(tx, headState, preverifiedValidators); err != nil {
		return fmt.Errorf("failed to increment public key table: %w", err)
	}
	if err := state_accessors.IncrementHistoricalSummariesTable(tx, headState, preverifiedHistoricalSummary); err != nil {
		return fmt.Errorf("failed to increment historical summaries table: %w", err)
	}
	if err := state_accessors.IncrementHistoricalRootsTable(tx, headState, preverifiedHistoricalRoots); err != nil {
		return fmt.Errorf("failed to increment historical roots table: %w", err)
	}
	return nil
}

func emitHeadEvent(cfg *Cfg, headSlot uint64, headRoot common.Hash, headState *state.CachingBeaconState) error {
	headEpoch := headSlot / cfg.beaconCfg.SlotsPerEpoch
	previous_duty_dependent_root, err := headState.GetBlockRootAtSlot((headEpoch-1)*cfg.beaconCfg.SlotsPerEpoch - 1)
	if err != nil {
		return fmt.Errorf("failed to get block root at slot for previous_duty_dependent_root: %w", err)
	}
	current_duty_dependent_root, err := headState.GetBlockRootAtSlot(headEpoch*cfg.beaconCfg.SlotsPerEpoch - 1)
	if err != nil {
		return fmt.Errorf("failed to get block root at slot for current_duty_dependent_root: %w", err)
	}

	stateRoot, err := headState.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to hash ssz: %w", err)
	}
	// emit the head event
	cfg.emitter.Publish("head", map[string]any{
		"slot":                         strconv.Itoa(int(headSlot)),
		"block":                        headRoot,
		"state":                        common.Hash(stateRoot),
		"epoch_transition":             true,
		"previous_duty_dependent_root": previous_duty_dependent_root,
		"current_duty_dependent_root":  current_duty_dependent_root,
		"execution_optimistic":         false,
	})
	return nil
}

func postForkchoiceOperations(ctx context.Context, tx kv.RwTx, logger log.Logger, cfg *Cfg, headSlot uint64, headRoot common.Hash) error {
	// Retrieve the head state
	headState, err := cfg.forkChoice.GetStateAtBlockRoot(headRoot, false)
	if err != nil {
		return fmt.Errorf("failed to get state at block root: %w", err)
	}
	cfg.forkChoice.SetSynced(true) // Now we are synced
	// Update the head state with the new head state
	if err := cfg.syncedData.OnHeadState(headState); err != nil {
		return fmt.Errorf("failed to set head state: %w", err)
	}
	headState = cfg.syncedData.HeadState() // headState is a copy of the head state here.

	// Produce and cache attestation data for validator node (this is not an expensive operation so we can do it for all nodes)
	if _, err = cfg.attestationDataProducer.ProduceAndCacheAttestationData(headState, headState.Slot(), 0); err != nil {
		logger.Warn("failed to produce and cache attestation data", "err", err)
	}

	// Run indexing routines for the database
	if err := runIndexingRoutines(ctx, tx, cfg, headState); err != nil {
		return fmt.Errorf("failed to run indexing routines: %w", err)
	}

	// Dump the head state on disk for ease of chain reorgs
	if err := cfg.forkChoice.DumpBeaconStateOnDisk(headState); err != nil {
		return fmt.Errorf("failed to dump beacon state on disk: %w", err)
	}
	// Lastly, emit the head event
	return emitHeadEvent(cfg, headSlot, headRoot, headState)
}

func doForkchoiceRoutine(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		headSlot uint64
		headRoot common.Hash
		err      error
	)
	if headSlot, headRoot, err = computeAndNotiftyServicesOfNewForkChoice(ctx, logger, cfg); err != nil {
		return fmt.Errorf("failed to compute and notify services of new fork choice: %w", err)
	}

	tx, err := cfg.indiciesDB.BeginRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := updateCanonicalChainInTheDatabase(ctx, tx, headSlot, headRoot); err != nil {
		return fmt.Errorf("failed to update canonical chain in the database: %w", err)
	}

	if err := postForkchoiceOperations(ctx, tx, logger, cfg, headSlot, headRoot); err != nil {
		return fmt.Errorf("failed to post forkchoice operations: %w", err)
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Debug("Imported chain segment",
		"hash", headRoot, "slot", headSlot,
		"alloc", common.ByteCount(m.Alloc),
		"sys", common.ByteCount(m.Sys))

	return tx.Commit()
}
