package antiquary

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/log/v3"
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

	finalized, err = beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return
	}
	return
}

func (s *Antiquary) IncrementBeaconState(ctx context.Context, to uint64) error {
	var tx kv.Tx

	tx, err := s.mainDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// maps which validators changes
	changedValidators := make(map[uint64]struct{})

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
			changedValidators[uint64(index)] = struct{}{}
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
	slashingOccured := false
	// setup the events handler for historical states replay.
	s.currentState.SetEvents(raw.Events{
		OnNewSlashingSegment: func(index int, segment uint64) error {
			slashingOccured = true
			return nil
		},
		OnRandaoMixChange: func(index int, mix [32]byte) error {
			return stateAntiquaryCollector.collectIntraEpochRandaoMix(slot, mix)
		},
		OnNewValidator: func(index int, v solid.Validator, balance uint64) error {
			changedValidators[uint64(index)] = struct{}{}
			events.AddValidator(uint64(index), v)
			return s.validatorsTable.AddValidator(v, uint64(index), slot)
		},
		OnNewValidatorActivationEpoch: func(index int, epoch uint64) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeActivationEpoch(uint64(index), epoch)
			return s.validatorsTable.AddActivationEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorExitEpoch: func(index int, epoch uint64) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeExitEpoch(uint64(index), epoch)
			return s.validatorsTable.AddExitEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorWithdrawableEpoch: func(index int, epoch uint64) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeWithdrawableEpoch(uint64(index), epoch)
			return s.validatorsTable.AddWithdrawableEpoch(uint64(index), slot, epoch)
		},
		OnNewValidatorSlashed: func(index int, newSlashed bool) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeSlashed(uint64(index), newSlashed)
			return s.validatorsTable.AddSlashed(uint64(index), slot, newSlashed)
		},
		OnNewValidatorActivationEligibilityEpoch: func(index int, epoch uint64) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeActivationEligibilityEpoch(uint64(index), epoch)
			return s.validatorsTable.AddActivationEligibility(uint64(index), slot, epoch)
		},
		OnNewValidatorWithdrawalCredentials: func(index int, wc []byte) error {
			changedValidators[uint64(index)] = struct{}{}
			events.ChangeWithdrawalCredentials(uint64(index), libcommon.BytesToHash(wc))
			return s.validatorsTable.AddWithdrawalCredentials(uint64(index), slot, libcommon.BytesToHash(wc))
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
			return stateAntiquaryCollector.collectFlattenedProposers(epoch, getProposerDutiesValue(s.currentState))
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
	blocksBeforeCommit := 350_000
	blocksProcessed := 0

	for ; slot < to && blocksProcessed < blocksBeforeCommit; slot++ {
		slashingOccured = false // Set this to false at the beginning of each slot.

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
		// We sanity check the state every 1k slots or when we start.
		if err := transition.TransitionState(s.currentState, block, blockRewardsCollector, fullValidation); err != nil {
			return err
		}
		// if s.currentState.Slot() == 3000010 {
		// 	s.dumpFullBeaconState()
		// }
		blocksProcessed++

		first = false

		// dump the whole slashings vector, if the slashing actually occured.
		if slashingOccured {
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
		}
		// collect current diffs.
		if err := stateAntiquaryCollector.collectBalancesDiffs(ctx, slot, s.balances32, s.currentState.RawBalances()); err != nil {
			return err
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
			log.Log(logLvl, "State processing progress", "slot", slot, "blk/sec", fmt.Sprintf("%.2f", float64(slot-prevSlot)/60))
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
		if _, ok := changedValidators[validatorIndex]; !ok {
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
	log.Info("Historical states antiquated", "slot", s.currentState.Slot(), "root", libcommon.Hash(stateRoot), "latency", endTime)
	return nil
}

func (s *Antiquary) antiquateField(ctx context.Context, slot uint64, uncompressed []byte, buffer *bytes.Buffer, compressor *zstd.Encoder, collector *etl.Collector) error {
	buffer.Reset()
	compressor.Reset(buffer)

	if _, err := compressor.Write(uncompressed); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	return collector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), common.Copy(buffer.Bytes()))
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
	// We want to backoff by some slots until we get a correct state from DB.
	// we start from 1 * clparams.SlotsPerDump.
	backoffStep := uint64(10)

	historicalReader := historical_states_reader.NewHistoricalStatesReader(s.cfg, s.snReader, s.validatorsTable, s.genesisState)

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
			backoffStep += 10
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
