package stages

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/monitor/shuffling_metrics"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

// computeAndNotifyServicesOfNewForkChoice calculates the new head of the fork choice and notifies relevant services.
// It updates the fork choice if possible and sets the status in the RPC. It returns the head slot, head root, and any error encountered.
func computeAndNotifyServicesOfNewForkChoice(ctx context.Context, logger log.Logger, cfg *Cfg) (headSlot uint64, headRoot common.Hash, err error) {
	if err = cfg.syncedData.ViewHeadState(func(prevHeadState *state.CachingBeaconState) error {
		// Get the current head of the fork choice
		headRoot, headSlot, err = cfg.forkChoice.GetHead(prevHeadState)
		if err != nil {
			return fmt.Errorf("failed to get head: %w", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, synced_data.ErrNotSynced) {
			// Get the current head of the fork choice
			headRoot, headSlot, err = cfg.forkChoice.GetHead(nil)
			if err != nil {
				return 0, common.Hash{}, fmt.Errorf("failed to get head: %w", err)
			}
		} else {
			return 0, common.Hash{}, fmt.Errorf("failed to get head: %w", err)
		}

	}
	// Observe the current slot and epoch in the monitor
	monitor.ObserveCurrentSlot(headSlot)
	monitor.ObserveCurrentEpoch(headSlot / cfg.beaconCfg.SlotsPerEpoch)
	if cfg.sn != nil {
		monitor.ObserveFrozenBlocks(int(cfg.sn.BlocksAvailable()))
		monitor.ObserveFrozenBlobs(int(cfg.sn.FrozenBlobs()))
	}

	// Perform fork choice update if the engine is available
	if cfg.forkChoice.Engine() != nil {
		finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
		justifiedCheckpoint := cfg.forkChoice.JustifiedCheckpoint()
		logger.Debug("Caplin is sending forkchoice")

		// Run fork choice update with finalized checkpoint and head
		if _, err = cfg.forkChoice.Engine().ForkChoiceUpdate(
			ctx,
			cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.Root),
			cfg.forkChoice.GetEth1Hash(justifiedCheckpoint.Root),
			cfg.forkChoice.GetEth1Hash(headRoot), nil,
		); err != nil {
			err = fmt.Errorf("failed to run forkchoice: %w", err)
			return
		}
	}

	// Set the status in the RPC
	if err2 := cfg.rpc.SetStatus(
		cfg.forkChoice.FinalizedCheckpoint().Root,
		cfg.forkChoice.FinalizedCheckpoint().Epoch,
		headRoot, headSlot); err2 != nil {
		logger.Warn("Could not set status", "err", err2)
	}
	return
}

// updateCanonicalChainInTheDatabase updates the canonical chain in the database by marking the given head slot and root as canonical.
// It traces back through parent block roots to find the common ancestor with the existing canonical chain, truncates the chain,
// and then marks the new chain segments as canonical.
func updateCanonicalChainInTheDatabase(ctx context.Context, tx kv.RwTx, headSlot uint64, headRoot common.Hash, cfg *Cfg) error {
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

	oldCanonical := common.Hash{}
	for i := currentSlot - 1; i > 0; i-- {
		oldCanonical, err = beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return fmt.Errorf("failed to read canonical block root: %w", err)
		}
		if oldCanonical != (common.Hash{}) {
			break
		}
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

	// check reorg
	parentRoot, err := beacon_indicies.ReadParentBlockRoot(ctx, tx, headRoot)
	if err != nil {
		return fmt.Errorf("failed to read parent block root: %w", err)
	}
	if parentRoot != oldCanonical {
		log.Debug("cl reorg", "new_head_slot", headSlot, "fork_slot", currentSlot, "old_canonical", oldCanonical, "new_canonical", headRoot)
		oldStateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, oldCanonical)
		if err != nil {
			log.Warn("failed to read state root by block root", "err", err, "block_root", oldCanonical)
			return nil
		}
		newStateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, headRoot)
		if err != nil {
			log.Warn("failed to read state root by block root", "err", err, "block_root", headRoot)
			return nil
		}
		reorgEvent := &beaconevents.ChainReorgData{
			Slot:                headSlot,
			Depth:               currentSlot - headSlot,
			OldHeadBlock:        oldCanonical,
			NewHeadBlock:        headRoot,
			OldHeadState:        oldStateRoot,
			NewHeadState:        newStateRoot,
			Epoch:               headSlot / cfg.beaconCfg.SlotsPerEpoch,
			ExecutionOptimistic: cfg.forkChoice.IsRootOptimistic(headRoot),
		}
		cfg.emitter.State().SendChainReorg(reorgEvent)
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
	cfg.emitter.State().SendHead(&beaconevents.HeadData{
		Slot:                      headSlot,
		Block:                     headRoot,
		State:                     stateRoot,
		EpochTransition:           true,
		PreviousDutyDependentRoot: previous_duty_dependent_root,
		CurrentDutyDependentRoot:  current_duty_dependent_root,
		ExecutionOptimistic:       false,
	})
	return nil
}

func emitNextPaylodAttributesEvent(cfg *Cfg, headSlot uint64, headRoot common.Hash, s *state.CachingBeaconState) error {
	headPayloadHeader := s.LatestExecutionPayloadHeader().Copy()
	nextSlot := headSlot + 1

	epoch := cfg.ethClock.GetEpochAtSlot(nextSlot)
	randaoMix := s.GetRandaoMixes(epoch)

	proposerIndex, err := s.GetBeaconProposerIndexForSlot(nextSlot)
	if err != nil {
		log.Warn("failed to get proposer index", "err", err)
		return err
	}
	withdrawals := []*types.Withdrawal{}
	expWithdrawals, _ := state.ExpectedWithdrawals(s, epoch)
	for _, w := range expWithdrawals {
		withdrawals = append(withdrawals, &types.Withdrawal{
			Amount:    w.Amount,
			Index:     w.Index,
			Validator: w.Validator,
			Address:   w.Address,
		})
	}
	payloadAttributes := engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(headPayloadHeader.Time + cfg.beaconCfg.SecondsPerSlot),
		PrevRandao:            randaoMix,
		SuggestedFeeRecipient: (common.Address{}), // We can not know this ahead of time
		ParentBeaconBlockRoot: &headRoot,
		Withdrawals:           withdrawals,
	}
	e := &beaconevents.PayloadAttributesData{
		Version: cfg.beaconCfg.GetCurrentStateVersion(epoch).String(),
		Data: beaconevents.PayloadAttributesContent{
			ProposerIndex:     proposerIndex,
			ProposalSlot:      nextSlot,
			ParentBlockNumber: headPayloadHeader.BlockNumber,
			ParentBlockHash:   headPayloadHeader.StateRoot,
			ParentBlockRoot:   headRoot,
			PayloadAttributes: payloadAttributes,
		},
	}
	cfg.emitter.State().SendPayloadAttributes(e)
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
	// fail-safe check§
	if headState == nil {
		return nil
	}
	if _, err = cfg.attestationDataProducer.ProduceAndCacheAttestationData(tx, headState, headRoot, headState.Slot()); err != nil {
		logger.Warn("failed to produce and cache attestation data", "err", err)
	}
	if err := beacon_indicies.WriteHighestFinalized(tx, cfg.forkChoice.FinalizedSlot()); err != nil {
		return err
	}
	start := time.Now()
	cfg.forkChoice.SetSynced(true) // Now we are synced
	// Update the head state with the new head state
	if err := cfg.syncedData.OnHeadState(headState); err != nil {
		return fmt.Errorf("failed to set head state: %w", err)
	}
	defer func() {
		logger.Debug("Post forkchoice operations", "duration", time.Since(start))
	}()

	return cfg.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// Dump the head state on disk for ease of chain reorgs
		if err := cfg.forkChoice.DumpBeaconStateOnDisk(headState); err != nil {
			return fmt.Errorf("failed to dump beacon state on disk: %w", err)
		}

		// Save the head state on disk for eventual node restarts without checkpoint sync
		if err := saveHeadStateOnDiskIfNeeded(cfg, headState); err != nil {
			return fmt.Errorf("failed to save head state on disk: %w", err)
		}

		// Lastly, emit the head event
		emitHeadEvent(cfg, headSlot, headRoot, headState)
		emitNextPaylodAttributesEvent(cfg, headSlot, headRoot, headState)

		// Shuffle validator set for the next epoch
		preCacheNextShuffledValidatorSet(ctx, logger, cfg, headState)
		return nil
	})

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
	if err := updateCanonicalChainInTheDatabase(ctx, tx, headSlot, headRoot, cfg); err != nil {
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

// we need to generate only one goroutine for pre-caching shuffled set
var workingPreCacheNextShuffledValidatorSet atomic.Bool

func preCacheNextShuffledValidatorSet(ctx context.Context, logger log.Logger, cfg *Cfg, b *state.CachingBeaconState) {
	if workingPreCacheNextShuffledValidatorSet.Load() {
		return
	}
	workingPreCacheNextShuffledValidatorSet.Store(true)
	go func() {
		defer workingPreCacheNextShuffledValidatorSet.Store(false)
		nextEpoch := state.Epoch(b) + 1
		beaconConfig := cfg.beaconCfg

		// Check if any action is needed
		refSlot := ((nextEpoch - 1) * b.BeaconConfig().SlotsPerEpoch) - 1
		if refSlot >= b.Slot() {
			return
		}
		// Get the block root at the beginning of the previous epoch
		blockRootAtBegginingPrevEpoch, err := b.GetBlockRootAtSlot(refSlot)
		if err != nil {
			logger.Warn("failed to get block root at slot for pre-caching shuffled set", "err", err)
			return
		}
		// Skip if the shuffled set is already pre-cached
		if _, ok := caches.ShuffledIndiciesCacheGlobal.Get(nextEpoch, blockRootAtBegginingPrevEpoch); ok {
			return
		}

		indicies := b.GetActiveValidatorsIndices(nextEpoch)
		shuffledIndicies := make([]uint64, len(indicies))
		mixPosition := (nextEpoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
			beaconConfig.EpochsPerHistoricalVector
		// Input for the seed hash.
		mix := b.GetRandaoMix(int(mixPosition))
		start := time.Now()
		shuffledIndicies = shuffling.ComputeShuffledIndicies(b.BeaconConfig(), mix, shuffledIndicies, indicies, nextEpoch*beaconConfig.SlotsPerEpoch)
		shuffling_metrics.ObserveComputeShuffledIndiciesTime(start)

		caches.ShuffledIndiciesCacheGlobal.Put(nextEpoch, blockRootAtBegginingPrevEpoch, shuffledIndicies)
		log.Info("Pre-cached shuffled set", "epoch", nextEpoch, "len", len(shuffledIndicies), "mix", common.Hash(mix))
	}()
}
