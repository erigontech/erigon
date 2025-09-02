// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package antiquary

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

// pool for buffers
var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (s *Antiquary) loopStates(ctx context.Context) {
	// Execute this each second
	reqRetryTimer := time.NewTicker(100 * time.Millisecond)
	defer reqRetryTimer.Stop()

	if !initial_state.IsGenesisStateSupported(clparams.NetworkType(s.cfg.DepositNetworkID)) {
		s.logger.Warn("Genesis state is not supported for this network, no historical states data will be available")
		return
	}
	_, beforeFinalized, err := s.readHistoricalProcessingProgress(ctx)
	if err != nil {
		s.logger.Error("Failed to read historical processing progress", "err", err)
		return
	}

	for {
		select {
		// Check if we are behind finalized
		case <-reqRetryTimer.C:
			if !s.backfilled.Load() {
				continue
			}
			// Check if we are behind finalized
			_, finalized, err := s.readHistoricalProcessingProgress(ctx)
			if err != nil {
				s.logger.Error("Failed to read historical processing progress", "err", err)
				continue
			}
			if s.sn == nil || s.syncedData.Syncing() {
				continue
			}

			// We wait for updated finality.
			if finalized == beforeFinalized {
				continue
			}
			beforeFinalized = finalized

			if err := s.IncrementBeaconState(ctx, finalized); err != nil {
				if s.currentState != nil {
					s.logger.Warn("Could not to increment beacon state, trying again later", "err", err, "slot", s.currentState.Slot())
				} else {
					s.logger.Warn("Failed to increment beacon state", "err", err)
				}
				s.currentState = nil
				time.Sleep(5 * time.Second)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *Antiquary) readHistoricalProcessingProgress(ctx context.Context) (progress, finalized uint64, err error) {
	var tx kv.Tx
	tx, err = s.mainDB.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	progress, err = state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return
	}
	if s.stateSn != nil {
		progress = max(progress, s.stateSn.BlocksAvailable())
	}

	finalized, err = beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return
	}
	return
}

func FillStaticValidatorsTableIfNeeded(ctx context.Context, logger log.Logger, stateSn *snapshotsync.CaplinStateSnapshots, validatorsTable *state_accessors.StaticValidatorTable) (bool, error) {
	if stateSn == nil || validatorsTable.Slot() != 0 {
		return false, nil
	}
	if err := stateSn.OpenFolder(); err != nil {
		return false, err
	}
	if stateSn.BlocksAvailable() == 0 {
		return false, nil
	}
	blocksAvaiable := stateSn.BlocksAvailable()
	stateSnRoTx := stateSn.View()
	defer stateSnRoTx.Close()
	log.Info("[Caplin-Archive] filling validators table", "from", 0, "to", stateSn.BlocksAvailable())
	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()
	start := time.Now()
	var lastSlot uint64
	for slot := uint64(0); slot <= stateSn.BlocksAvailable(); slot++ {
		select {
		case <-logTicker.C:
			log.Info("[Caplin-Archive] Filled validators table", "progress", fmt.Sprintf("%d/%d", slot, stateSn.BlocksAvailable()))
		default:
		}
		seg, ok := stateSnRoTx.VisibleSegment(slot, kv.StateEvents)
		if !ok {
			return false, fmt.Errorf("segment not found for slot %d", slot)
		}
		buf, err := seg.Get(slot)
		if err != nil {
			return false, err
		}
		if len(buf) == 0 {
			continue
		}
		event := state_accessors.NewStateEventsFromBytes(buf)
		state_accessors.ReplayEvents(
			func(validatorIndex uint64, validator solid.Validator) error {
				return validatorsTable.AddValidator(validator, validatorIndex, slot)
			},
			func(validatorIndex uint64, exitEpoch uint64) error {
				return validatorsTable.AddExitEpoch(validatorIndex, slot, exitEpoch)
			},
			func(validatorIndex uint64, withdrawableEpoch uint64) error {
				return validatorsTable.AddWithdrawableEpoch(validatorIndex, slot, withdrawableEpoch)
			},
			func(validatorIndex uint64, withdrawalCredentials common.Hash) error {
				return validatorsTable.AddWithdrawalCredentials(validatorIndex, slot, withdrawalCredentials)
			},
			func(validatorIndex uint64, activationEpoch uint64) error {
				return validatorsTable.AddActivationEpoch(validatorIndex, slot, activationEpoch)
			},
			func(validatorIndex uint64, activationEligibilityEpoch uint64) error {
				return validatorsTable.AddActivationEligibility(validatorIndex, slot, activationEligibilityEpoch)
			},
			func(validatorIndex uint64, slashed bool) error {
				return validatorsTable.AddSlashed(validatorIndex, slot, slashed)
			},
			event,
		)
		lastSlot = slot
	}
	validatorsTable.SetSlot(lastSlot)
	logger.Info("[Antiquary] Filled static validators table", "slots", blocksAvaiable, "elapsed", time.Since(start))
	return true, nil
}

func (s *Antiquary) IncrementBeaconState(ctx context.Context, to uint64) error {

	// Check if you need to fill the static validators table
	refilledStaticValidators, err := FillStaticValidatorsTableIfNeeded(ctx, s.logger, s.stateSn, s.validatorsTable)
	if err != nil {
		return err
	}

	tx, err := s.mainDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// maps which validators changes
	var changedValidators sync.Map

	if refilledStaticValidators {
		s.validatorsTable.ForEach(func(validatorIndex uint64, validator *state_accessors.StaticValidator) bool {
			changedValidators.Store(validatorIndex, struct{}{})
			return true
		})
	}

	stateAntiquaryCollector := newBeaconStatesCollector(s.cfg, s.dirs.Tmp, s.logger)
	defer stateAntiquaryCollector.close()

	if err := s.initializeStateAntiquaryIfNeeded(ctx, tx); err != nil {
		return err
	}
	if s.currentState.Slot() == s.genesisState.Slot() {
		// Collect genesis state if we are at genesis
		if err := stateAntiquaryCollector.addGenesisState(ctx, s.currentState); err != nil {
			return err
		}
		// Mark all validators as touched because we just initizialized the whole state.
		s.currentState.ForEachValidator(func(v solid.Validator, index, total int) bool {
			changedValidators.Store(uint64(index), struct{}{})
			if err = s.validatorsTable.AddValidator(v, uint64(index), 0); err != nil {
				return false
			}
			return true
		})
	}
	s.validatorsTable.SetSlot(s.currentState.Slot())

	logLvl := log.LvlInfo
	if to-s.currentState.Slot() < 96 {
		logLvl = log.LvlDebug
	}
	start := time.Now()

	// Use this as the event slot (it will be incremented by 1 each time we process a block)
	slot := s.currentState.Slot() + 1

	var prevValSet []byte
	events := state_accessors.NewStateEvents()
	slashingOccurred := false
	// setup the events handler for historical states replay.
	s.currentState.SetEvents(raw.Events{
		OnNewSlashingSegment: func(index int, segment uint64) error {
			slashingOccurred = true
			return nil
		},
		OnRandaoMixChange: func(index int, mix [32]byte) error {
			return stateAntiquaryCollector.collectIntraEpochRandaoMix(slot, mix)
		},
		OnNewValidator: func(index int, v solid.Validator, balance uint64) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.AddValidator(uint64(index), v)
			return s.validatorsTable.AddValidator(v, uint64(index), slot)
		},
		OnNewValidatorActivationEpoch: func(index int, epoch uint64) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeActivationEpoch(uint64(index), epoch)
			return s.validatorsTable.AddActivationEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorExitEpoch: func(index int, epoch uint64) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeExitEpoch(uint64(index), epoch)
			return s.validatorsTable.AddExitEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorWithdrawableEpoch: func(index int, epoch uint64) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeWithdrawableEpoch(uint64(index), epoch)
			return s.validatorsTable.AddWithdrawableEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorSlashed: func(index int, newSlashed bool) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeSlashed(uint64(index), newSlashed)
			return s.validatorsTable.AddSlashed(uint64(index), slot, newSlashed)
		},
		OnNewValidatorActivationEligibilityEpoch: func(index int, epoch uint64) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeActivationEligibilityEpoch(uint64(index), epoch)
			return s.validatorsTable.AddActivationEligibility(uint64(index), slot, epoch)
		},
		OnNewValidatorWithdrawalCredentials: func(index int, wc []byte) error {
			changedValidators.Store(uint64(index), struct{}{})
			events.ChangeWithdrawalCredentials(uint64(index), common.BytesToHash(wc))
			return s.validatorsTable.AddWithdrawalCredentials(uint64(index), slot, common.BytesToHash(wc))
		},
		OnEpochBoundary: func(epoch uint64) error {
			if err := stateAntiquaryCollector.storeEpochData(s.currentState); err != nil {
				return err
			}
			var prevEpoch uint64
			if epoch > 0 {
				prevEpoch = epoch - 1
			}
			mix := s.currentState.GetRandaoMixes(prevEpoch)
			if err := stateAntiquaryCollector.collectEpochRandaoMix(prevEpoch, mix); err != nil {
				return err
			}
			// Write active validator indicies
			if err := stateAntiquaryCollector.collectActiveIndices(
				prevEpoch,
				s.currentState.GetActiveValidatorsIndices(prevEpoch),
			); err != nil {
				return err
			}
			if err := stateAntiquaryCollector.collectActiveIndices(
				epoch,
				s.currentState.GetActiveValidatorsIndices(epoch),
			); err != nil {
				return err
			}
			return nil
		},
		OnNewBlockRoot: func(index int, root common.Hash) error {
			return stateAntiquaryCollector.collectBlockRoot(s.currentState.Slot(), root)
		},
		OnNewStateRoot: func(index int, root common.Hash) error {
			return stateAntiquaryCollector.collectStateRoot(s.currentState.Slot(), root)
		},
		OnNewNextSyncCommittee: func(committee *solid.SyncCommittee) error {
			return stateAntiquaryCollector.collectNextSyncCommittee(slot, committee)
		},
		OnNewCurrentSyncCommittee: func(committee *solid.SyncCommittee) error {
			return stateAntiquaryCollector.collectCurrentSyncCommittee(slot, committee)
		},
		OnAppendEth1Data: func(data *cltypes.Eth1Data) error {
			return stateAntiquaryCollector.collectEth1DataVote(slot, data)
		},
	})
	log.Log(logLvl, "Starting state processing", "from", slot, "to", to)
	// Set up a timer to log progress
	progressTimer := time.NewTicker(1 * time.Minute)
	defer progressTimer.Stop()
	prevSlot := slot
	first := false
	timeBeforeCommit := 30 * time.Minute
	blocksProcessed := 0

	startLoop := time.Now()

	for ; slot < to && startLoop.Add(timeBeforeCommit).After(time.Now()); slot++ {
		slashingOccurred = false // Set this to false at the beginning of each slot.

		isDumpSlot := slot%clparams.SlotsPerDump == 0
		block, err := s.snReader.ReadBlockBySlot(ctx, tx, slot)
		if err != nil {
			return err
		}
		prevValidatorSetLength := s.currentState.ValidatorLength()
		prevEpoch := state.Epoch(s.currentState)

		// If we have a missed block, we just skip it.
		if block == nil {
			if isDumpSlot {
				if err := stateAntiquaryCollector.collectBalancesDump(slot, s.currentState.RawBalances()); err != nil {
					return err
				}
				if err := stateAntiquaryCollector.collectEffectiveBalancesDump(slot, s.currentState.RawValidatorSet()); err != nil {
					return err
				}
				if s.currentState.Version() >= clparams.ElectraVersion {
					fmt.Println("not-found dumping electra queues", "slot", slot, "pendingDeposits", s.currentState.PendingDeposits().Len(), "pendingConsolidations", s.currentState.PendingConsolidations().Len(), "pendingWithdrawals", s.currentState.PendingPartialWithdrawals().Len())
					if err := stateAntiquaryCollector.collectPendingDepositsDump(slot, s.currentState.PendingDeposits()); err != nil {
						return err
					}
					if err := stateAntiquaryCollector.collectPendingConsolidationsDump(slot, s.currentState.PendingConsolidations()); err != nil {
						return err
					}
					if err := stateAntiquaryCollector.collectPendingWithdrawalsDump(slot, s.currentState.PendingPartialWithdrawals()); err != nil {
						return err
					}
				}
			}
			if slot%s.cfg.SlotsPerEpoch == 0 {
				if err := stateAntiquaryCollector.collectBalancesDiffs(ctx, slot, s.balances32, s.currentState.RawBalances()); err != nil {
					return err
				}

				s.balances32 = s.balances32[:0]
				s.balances32 = append(s.balances32, s.currentState.RawBalances()...)
			}
			continue
		}
		// We now compute the difference between the two balances.
		prevValSet = prevValSet[:0]
		prevValSet = append(prevValSet, s.currentState.RawValidatorSet()...)

		fullValidation := slot%1000 == 0 || first
		blockRewardsCollector := &eth2.BlockRewardsCollector{}

		stateAntiquaryCollector.preStateTransitionHook(s.currentState)
		// We sanity check the state every 1k slots or when we start.
		if err := transition.TransitionState(s.currentState, block, blockRewardsCollector, fullValidation); err != nil {
			return err
		}
		// if s.currentState.Slot() == 3751254 {
		// 	s.dumpFullBeaconState()
		// }
		blocksProcessed++

		first = false

		// dump the whole slashings vector, if the slashing actually occurred.
		if slashingOccurred {
			if err := stateAntiquaryCollector.collectSlashings(slot, s.currentState.RawSlashings()); err != nil {
				return err
			}
		}

		if err := stateAntiquaryCollector.storeSlotData(s.currentState, blockRewardsCollector); err != nil {
			return err
		}

		if err := stateAntiquaryCollector.collectStateEvents(slot, events); err != nil {
			return err
		}
		events.Reset()

		if isDumpSlot {
			if err := stateAntiquaryCollector.collectBalancesDump(slot, s.currentState.RawBalances()); err != nil {
				return err
			}
			if err := stateAntiquaryCollector.collectEffectiveBalancesDump(slot, s.currentState.RawValidatorSet()); err != nil {
				return err
			}
			if s.currentState.Version() >= clparams.ElectraVersion {
				fmt.Println("not-found dumping electra queues", "slot", slot, "pendingDeposits", s.currentState.PendingDeposits().Len(), "pendingConsolidations", s.currentState.PendingConsolidations().Len(), "pendingWithdrawals", s.currentState.PendingPartialWithdrawals().Len())
				if err := stateAntiquaryCollector.collectPendingDepositsDump(slot, s.currentState.PendingDeposits()); err != nil {
					return err
				}
				if err := stateAntiquaryCollector.collectPendingConsolidationsDump(slot, s.currentState.PendingConsolidations()); err != nil {
					return err
				}
				if err := stateAntiquaryCollector.collectPendingWithdrawalsDump(slot, s.currentState.PendingPartialWithdrawals()); err != nil {
					return err
				}
			}
		}
		// collect current diffs.
		if err := stateAntiquaryCollector.collectBalancesDiffs(ctx, slot, s.balances32, s.currentState.RawBalances()); err != nil {
			return err
		}

		if s.currentState.Version() >= clparams.ElectraVersion {
			if err := stateAntiquaryCollector.collectElectraQueuesDiffs(slot, s.currentState.PendingDeposits(), s.currentState.PendingConsolidations(), s.currentState.PendingPartialWithdrawals()); err != nil {
				return err
			}
		}
		// If we find an epoch, we need to reset the diffs.
		if slot%s.cfg.SlotsPerEpoch == 0 {
			s.balances32 = s.balances32[:0]
			s.balances32 = append(s.balances32, s.currentState.RawBalances()...)
		}

		// antiquate diffs
		isEpochCrossed := prevEpoch != state.Epoch(s.currentState)

		if prevValidatorSetLength != s.currentState.ValidatorLength() || isEpochCrossed {
			if err := stateAntiquaryCollector.collectEffectiveBalancesDiffs(ctx, slot, prevValSet, s.currentState.RawValidatorSet()); err != nil {
				return err
			}
			if s.currentState.Version() >= clparams.AltairVersion {
				if err := stateAntiquaryCollector.collectInactivityScores(slot, s.currentState.RawInactivityScores()); err != nil {
					return err
				}
			}
		}
		// We now do some post-processing on the state.
		select {
		case <-progressTimer.C:
			log.Log(logLvl, "[Caplin-Archive] Historical States reconstruction", "slot", slot, "blk/sec", fmt.Sprintf("%.2f", float64(slot-prevSlot)/60))
			prevSlot = slot
		default:
		}
	}
	tx.Rollback()

	log.Debug("Finished beacon state iteration", "elapsed", time.Since(start))

	log.Log(logLvl, "Stopped Caplin to load states")
	rwTx, err := s.mainDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()

	start = time.Now()
	// We now need to store the state
	if err := stateAntiquaryCollector.flush(ctx, rwTx); err != nil {
		return err
	}

	if err := state_accessors.SetStateProcessingProgress(rwTx, s.currentState.Slot()); err != nil {
		return err
	}

	s.validatorsTable.SetSlot(s.currentState.Slot())

	buf := &bytes.Buffer{}
	s.validatorsTable.ForEach(func(validatorIndex uint64, validator *state_accessors.StaticValidator) bool {
		if _, ok := changedValidators.Load(validatorIndex); !ok {
			return true
		}
		buf.Reset()
		if err = validator.WriteTo(buf); err != nil {
			return false
		}
		if err = rwTx.Put(kv.StaticValidators, base_encoding.Encode64ToBytes4(validatorIndex), common.Copy(buf.Bytes())); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	if err := rwTx.Commit(); err != nil {
		return err
	}
	endTime := time.Since(start)
	stateRoot, err := s.currentState.HashSSZ()
	if err != nil {
		return err
	}

	log.Info("[Caplin-Archive] Historical states antiquated", "slot", s.currentState.Slot(), "root", common.Hash(stateRoot), "latency", endTime)
	if s.stateSn != nil {
		if err := s.stateSn.OpenFolder(); err != nil {
			return err
		}
	}

	if s.snapgen {
		blocksPerStatefulFile := uint64(snaptype.CaplinMergeLimit * 5)
		from := s.stateSn.BlocksAvailable() + 1
		if from+blocksPerStatefulFile+safetyMargin > s.currentState.Slot() {
			return nil
		}
		to := s.currentState.Slot()
		if to < (safetyMargin + blocksPerStatefulFile) {
			return nil
		}
		to = to - (safetyMargin + blocksPerStatefulFile)
		if from >= to {
			return nil
		}
		if err := s.stateSn.DumpCaplinState(
			ctx,
			s.stateSn.BlocksAvailable()+1,
			to,
			blocksPerStatefulFile,
			s.sn.Salt,
			s.dirs,
			1,
			log.LvlInfo,
			s.logger,
		); err != nil {
			return err
		}
		paths := s.stateSn.SegFileNames(from, to)
		downloadItems := make([]*proto_downloader.AddItem, len(paths))
		for i, path := range paths {
			downloadItems[i] = &proto_downloader.AddItem{
				Path: path,
			}
		}
		if s.downloader != nil {
			// Notify bittorent to seed the new snapshots
			if _, err := s.downloader.Add(s.ctx, &proto_downloader.AddRequest{Items: downloadItems}); err != nil {
				s.logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
			}
		}
		if err := s.stateSn.OpenFolder(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Antiquary) initializeStateAntiquaryIfNeeded(ctx context.Context, tx kv.Tx) error {
	if s.currentState != nil {
		return nil
	}
	// Start by reading the state processing progress
	targetSlot, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return err
	}
	if s.stateSn != nil {
		targetSlot = max(targetSlot, s.stateSn.BlocksAvailable())
	}
	// We want to backoff by some slots until we get a correct state from DB.
	// we start from 10 * clparams.SlotsPerDump.
	backoffStrides := uint64(10)
	backoffStep := backoffStrides

	historicalReader := historical_states_reader.NewHistoricalStatesReader(s.cfg, s.snReader, s.validatorsTable, s.genesisState, s.stateSn, s.syncedData)

	for {
		attempt, err := computeSlotToBeRequested(tx, s.cfg, s.genesisState.Slot(), targetSlot, backoffStep)
		if err != nil {
			return err
		}
		// If we are attempting slot=0 then we are at genesis.
		if attempt == s.genesisState.Slot() {
			s.currentState, err = s.genesisState.Copy()
			if err != nil {
				return err
			}
			break
		}

		// progress not 0 ? we need to load the state from the DB
		s.currentState, err = historicalReader.ReadHistoricalState(ctx, tx, attempt)
		if err != nil {
			return fmt.Errorf("failed to read historical state at slot %d: %w", attempt, err)
		}

		if s.currentState == nil {
			log.Warn("historical state not found, backoff more and try again", "slot", attempt)
			backoffStep += backoffStrides
			continue
		}

		computedBlockRoot, err := s.currentState.BlockRoot()
		if err != nil {
			return err
		}
		expectedBlockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, attempt)
		if err != nil {
			return err
		}
		if computedBlockRoot != expectedBlockRoot {
			log.Debug("Block root mismatch, trying again", "slot", attempt, "expected", expectedBlockRoot)
			// backoff more
			backoffStep += backoffStrides
			continue
		}
		break
	}

	s.balances32 = s.balances32[:0]
	s.balances32 = append(s.balances32, s.currentState.RawBalances()...)
	return s.currentState.InitBeaconState()
}

func computeSlotToBeRequested(tx kv.Tx, cfg *clparams.BeaconChainConfig, genesisSlot uint64, targetSlot uint64, backoffStep uint64) (uint64, error) {
	// We want to backoff by some slots until we get a correct state from DB.
	// we start from 2 * clparams.SlotsPerDump.
	if targetSlot > clparams.SlotsPerDump*backoffStep {
		return findNearestSlotBackwards(tx, cfg, targetSlot-clparams.SlotsPerDump*backoffStep)
	}
	return genesisSlot, nil
}
