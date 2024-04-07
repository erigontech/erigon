package antiquary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
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

func excludeDuplicatesIdentity() etl.LoadFunc {
	var prevKey, prevValue []byte
	prevValue = []byte{}
	return func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(prevKey) == 0 {
			prevKey = common.Copy(k)
			prevValue = common.Copy(v)
			return nil
		}
		if bytes.Equal(k, prevKey) {
			prevValue = common.Copy(v)
			return nil
		}
		if err := next(prevKey, prevKey, prevValue); err != nil {
			return err
		}
		prevKey = common.Copy(k)
		prevValue = common.Copy(v)
		return nil
	}
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

	stateAntiquaryCollector := newInternalBeaconStatesCollector(s.cfg, s.dirs.Tmp, s.logger)
	defer stateAntiquaryCollector.close()

	stageProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return err
	}
	progress := stageProgress
	// Go back a little bit
	if progress > (s.cfg.SlotsPerEpoch*2 + clparams.SlotsPerDump) {
		progress -= s.cfg.SlotsPerEpoch*2 + clparams.SlotsPerDump
	} else {
		progress = 0
	}
	progress, err = findNearestSlotBackwards(tx, s.cfg, progress) // Maybe the guess was a missed slot.
	if err != nil {
		return err
	}
	// buffers
	commonBuffer := &bytes.Buffer{}
	compressedWriter, err := zstd.NewWriter(commonBuffer, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}
	defer compressedWriter.Close()

	if s.currentState == nil {
		// progress is 0 when we are at genesis
		if progress == 0 {
			s.currentState, err = s.genesisState.Copy()
			if err != nil {
				return err
			}
			// Collect genesis state if we are at genesis
			if err := stateAntiquaryCollector.addGenesisState(ctx, compressedWriter, s.currentState); err != nil {
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
		} else {
			start := time.Now()
			// progress not 0? we need to load the state from the DB
			historicalReader := historical_states_reader.NewHistoricalStatesReader(s.cfg, s.snReader, s.validatorsTable, s.genesisState)
			s.currentState, err = historicalReader.ReadHistoricalState(ctx, tx, progress)
			if err != nil {
				s.currentState = nil
				return fmt.Errorf("failed to read historical state at slot %d: %w", progress, err)
			}
			end := time.Since(start)
			hashRoot, err := s.currentState.HashSSZ()
			if err != nil {
				return err
			}
			log.Info("Recovered Beacon State", "slot", s.currentState.Slot(), "elapsed", end, "root", libcommon.Hash(hashRoot).String())
			if err := s.currentState.InitBeaconState(); err != nil {
				return err
			}
		}
		s.balances32 = s.balances32[:0]
		s.balances32 = append(s.balances32, s.currentState.RawBalances()...)
	}

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
			if err := stateAntiquaryCollector.storeEpochData(commonBuffer, s.currentState); err != nil {
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
			if err := stateAntiquaryCollector.collectActiveIndices(commonBuffer,
				prevEpoch,
				s.currentState.GetActiveValidatorsIndices(prevEpoch),
			); err != nil {
				return err
			}
			if err := stateAntiquaryCollector.collectActiveIndices(commonBuffer,
				epoch,
				s.currentState.GetActiveValidatorsIndices(epoch),
			); err != nil {
				return err
			}
			return stateAntiquaryCollector.collectFlattenedProposers(epoch, getProposerDutiesValue(s.currentState))
		},
		OnNewBlockRoot: func(index int, root common.Hash) error {
			return stateAntiquaryCollector.collectBlockRoot(slot, root)
		},
		OnNewStateRoot: func(index int, root common.Hash) error {
			return stateAntiquaryCollector.collectStateRoot(slot, root)
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
	log.Log(logLvl, "Starting state processing", "from", slot, "to", to, "progress", stageProgress)
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
				if err := stateAntiquaryCollector.collectBalancesDump(slot, s.currentState.RawBalances(), commonBuffer, compressedWriter); err != nil {
					return err
				}
				if err := stateAntiquaryCollector.collectEffectiveBalancesDump(slot, s.currentState.RawValidatorSet(), commonBuffer, compressedWriter); err != nil {
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
			if err := stateAntiquaryCollector.collectSlashings(commonBuffer, compressedWriter, slot, s.currentState.RawSlashings()); err != nil {
				return err
			}
		}

		if err := stateAntiquaryCollector.storeSlotData(commonBuffer, s.currentState, blockRewardsCollector); err != nil {
			return err
		}

		if err := stateAntiquaryCollector.collectStateEvents(slot, events); err != nil {
			return err
		}
		events.Reset()

		if isDumpSlot {
			if err := stateAntiquaryCollector.collectBalancesDump(slot, s.currentState.RawBalances(), commonBuffer, compressedWriter); err != nil {
				return err
			}
			if err := stateAntiquaryCollector.collectEffectiveBalancesDump(slot, s.currentState.RawValidatorSet(), commonBuffer, compressedWriter); err != nil {
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
				if err := stateAntiquaryCollector.collectInactivityScores(commonBuffer, compressedWriter, slot, s.currentState.RawInactivityScores()); err != nil {
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

	s.validatorsTable.ForEach(func(validatorIndex uint64, validator *state_accessors.StaticValidator) bool {
		if _, ok := changedValidators[validatorIndex]; !ok {
			return true
		}
		commonBuffer.Reset()
		if err = validator.WriteTo(commonBuffer); err != nil {
			return false
		}
		if err = rwTx.Put(kv.StaticValidators, base_encoding.Encode64ToBytes4(validatorIndex), common.Copy(commonBuffer.Bytes())); err != nil {
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

func getProposerDutiesValue(s *state.CachingBeaconState) []byte {
	epoch := state.Epoch(s)
	var wg sync.WaitGroup
	list := make([]byte, s.BeaconConfig().SlotsPerEpoch*4)
	for slot := s.Slot(); slot < s.Slot()+s.BeaconConfig().SlotsPerEpoch; slot++ {
		var proposerIndex uint64
		// Lets do proposer index computation
		mixPosition := (epoch + s.BeaconConfig().EpochsPerHistoricalVector - s.BeaconConfig().MinSeedLookahead - 1) %
			s.BeaconConfig().EpochsPerHistoricalVector
		// Input for the seed hash.
		mix := s.GetRandaoMix(int(mixPosition))
		input := shuffling.GetSeed(s.BeaconConfig(), mix, epoch, s.BeaconConfig().DomainBeaconProposer)
		slotByteArray := make([]byte, 8)
		binary.LittleEndian.PutUint64(slotByteArray, slot)

		// Add slot to the end of the input.
		inputWithSlot := append(input[:], slotByteArray...)
		hash := sha256.New()

		// Calculate the hash.
		hash.Write(inputWithSlot)
		seed := hash.Sum(nil)

		indices := s.GetActiveValidatorsIndices(epoch)

		// Write the seed to an array.
		seedArray := [32]byte{}
		copy(seedArray[:], seed)
		wg.Add(1)

		// Do it in parallel
		go func(i, slot uint64, indicies []uint64, seedArray [32]byte) {
			defer wg.Done()
			var err error
			proposerIndex, err = shuffling.ComputeProposerIndex(s.BeaconState, indices, seedArray)
			if err != nil {
				panic(err)
			}
			binary.BigEndian.PutUint32(list[i*4:(i+1)*4], uint32(proposerIndex))
		}(slot-s.Slot(), slot, indices, seedArray)
	}
	wg.Wait()
	return list
}

// func (s *Antiquary) dumpFullBeaconState() {
// 	b, err := s.currentState.EncodeSSZ(nil)
// 	if err != nil {
// 		s.logger.Error("Failed to encode full beacon state", "err", err)
// 		return
// 	}
// 	// just dump it in a.txt like an idiot without afero
// 	if err := os.WriteFile("bab.txt", b, 0644); err != nil {
// 		s.logger.Error("Failed to write full beacon state", "err", err)
// 	}
// }

func findNearestSlotBackwards(tx kv.Tx, cfg *clparams.BeaconChainConfig, slot uint64) (uint64, error) {
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return 0, err
	}
	for (canonicalRoot == (common.Hash{}) && slot > 0) || slot%cfg.SlotsPerEpoch != 0 {
		slot--
		canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return 0, err
		}
	}
	return slot, nil
}
