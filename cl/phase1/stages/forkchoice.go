package stages

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
)

// computeAndNotifyServicesOfNewForkChoice calculates the new head of the fork choice and notifies relevant services.
// It updates the fork choice if possible and sets the status in the RPC. It returns the head slot, head root, and any error encountered.
func computeAndNotifyServicesOfNewForkChoice(ctx context.Context, logger log.Logger, cfg *Cfg) (headSlot uint64, headRoot common.Hash, err error) {
	// Get the current head of the fork choice
	headRoot, headSlot, err = cfg.forkChoice.GetHead()
	if err != nil {
		err = fmt.Errorf("failed to get head: %w", err)
		return
	}

	// Perform fork choice update if the engine is available
	if cfg.forkChoice.Engine() != nil {
		finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
		logger.Debug("Caplin is sending forkchoice")

		// Run fork choice update with finalized checkpoint and head
		if _, err = cfg.forkChoice.Engine().ForkChoiceUpdate(
			ctx,
			cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
			cfg.forkChoice.GetEth1Hash(headRoot), nil,
		); err != nil {
			err = fmt.Errorf("failed to run forkchoice: %w", err)
			return
		}
	}

	// Set the status in the RPC
	if err2 := cfg.rpc.SetStatus(
		cfg.forkChoice.FinalizedCheckpoint().BlockRoot(),
		cfg.forkChoice.FinalizedCheckpoint().Epoch(),
		headRoot, headSlot); err2 != nil {
		logger.Warn("Could not set status", "err", err2)
	}

	return
}

// updateCanonicalChainInTheDatabase updates the canonical chain in the database by marking the given head slot and root as canonical.
// It traces back through parent block roots to find the common ancestor with the existing canonical chain, truncates the chain,
// and then marks the new chain segments as canonical.
func updateCanonicalChainInTheDatabase(ctx context.Context, tx kv.RwTx, headSlot uint64, headRoot common.Hash) error {
	type canonicalEntry struct {
		slot uint64
		root common.Hash
	}

	currentRoot := headRoot
	currentSlot := headSlot
	// Read the current canonical block root for the given slot
	currentCanonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
	if err != nil {
		return fmt.Errorf("failed to read canonical block root: %w", err)
	}

	// List of new canonical chain entries
	reconnectionRoots := []canonicalEntry{{currentSlot, currentRoot}}

	// Trace back through the parent block roots until the current root matches the canonical root
	for currentRoot != currentCanonical {
		var newFoundSlot *uint64

		// Read the parent block root
		if currentRoot, err = beacon_indicies.ReadParentBlockRoot(ctx, tx, currentRoot); err != nil {
			return fmt.Errorf("failed to read parent block root: %w", err)
		}

		// Read the slot for the current block root
		if newFoundSlot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, currentRoot); err != nil {
			return fmt.Errorf("failed to read block slot by block root: %w", err)
		}
		if newFoundSlot == nil {
			break
		}

		currentSlot = *newFoundSlot

		// Read the canonical block root for the new slot
		currentCanonical, err = beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
		if err != nil {
			return fmt.Errorf("failed to read canonical block root: %w", err)
		}

		// Append the current slot and root to the list of reconnection roots
		reconnectionRoots = append(reconnectionRoots, canonicalEntry{currentSlot, currentRoot})
	}

	// Truncate the canonical chain at the current slot
	if err := beacon_indicies.TruncateCanonicalChain(ctx, tx, currentSlot); err != nil {
		return fmt.Errorf("failed to truncate canonical chain: %w", err)
	}

	// Mark the new canonical chain segments in reverse order
	for i := len(reconnectionRoots) - 1; i >= 0; i-- {
		if err := beacon_indicies.MarkRootCanonical(ctx, tx, reconnectionRoots[i].slot, reconnectionRoots[i].root); err != nil {
			return fmt.Errorf("failed to mark root canonical: %w", err)
		}
	}

	// Mark the head slot and root as canonical
	if err := beacon_indicies.MarkRootCanonical(ctx, tx, headSlot, headRoot); err != nil {
		return fmt.Errorf("failed to mark root canonical: %w", err)
	}

	return nil
}

// runIndexingRoutines runs the indexing routines for the database.
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

// emitHeadEvent emits the head event with the given head slot, head root, and head state.
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

// saveHeadStateOnDiskIfNeeded saves the head state on disk for eventual node restarts without checkpoint sync.
func saveHeadStateOnDiskIfNeeded(cfg *Cfg, headState *state.CachingBeaconState) error {
	epochFrequency := uint64(5)
	if headState.Slot()%(cfg.beaconCfg.SlotsPerEpoch*epochFrequency) == 0 {
		dat, err := utils.EncodeSSZSnappy(headState)
		if err != nil {
			return fmt.Errorf("failed to encode ssz snappy: %w", err)
		}
		// Write the head state to disk
		fileToWriteTo := fmt.Sprintf("%s/%s", cfg.dirs.CaplinLatest, clparams.LatestStateFileName)

		// Create the directory if it doesn't exist
		err = os.MkdirAll(cfg.dirs.CaplinLatest, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Write the data to the file
		err = os.WriteFile(fileToWriteTo, dat, 0644)
		if err != nil {
			return fmt.Errorf("failed to write head state to disk: %w", err)
		}
	}
	return nil
}

// postForkchoiceOperations performs the post fork choice operations such as updating the head state, producing and caching attestation data,
// these sets of operations can take as long as they need to run, as by-now we are already synced.
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

	// Save the head state on disk for eventual node restarts without checkpoint sync
	if err := saveHeadStateOnDiskIfNeeded(cfg, headState); err != nil {
		return fmt.Errorf("failed to save head state on disk: %w", err)
	}
	// Lastly, emit the head event
	return emitHeadEvent(cfg, headSlot, headRoot, headState)
}

// doForkchoiceRoutine performs the fork choice routine by computing the new fork choice, updating the canonical chain in the database,
func doForkchoiceRoutine(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		headSlot uint64
		headRoot common.Hash
		err      error
	)
	if headSlot, headRoot, err = computeAndNotifyServicesOfNewForkChoice(ctx, logger, cfg); err != nil {
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
