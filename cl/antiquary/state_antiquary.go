package antiquary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
	"github.com/ledgerwatch/erigon/cl/transition"
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

	for {
		select {
		// Check if we are behind finalized
		case <-reqRetryTimer.C:
			if !s.backfilled.Load() {
				continue
			}
			// Check if we are behind finalized
			progress, finalized, err := s.readHistoricalProcessingProgress(ctx)
			if err != nil {
				s.logger.Error("Failed to read historical processing progress", "err", err)
				continue
			}
			// Stay behind a little bit we rely on forkchoice up until (finalized - 2*slotsPerEpoch)
			if progress+s.cfg.SlotsPerEpoch/2 >= finalized {
				continue
			}
			if err := s.IncrementBeaconState(ctx, finalized); err != nil {
				slot := uint64(0)
				if s.currentState != nil {
					slot = s.currentState.Slot()
				}
				s.logger.Error("Failed to increment beacon state", "err", err, "slot", slot)
				return
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

func uint64BalancesList(s *state.CachingBeaconState, out []uint64) []uint64 {
	if len(out) < s.ValidatorLength() {
		out = make([]uint64, s.ValidatorLength())
	}
	out = out[:s.ValidatorLength()]

	s.ForEachBalance(func(v uint64, index int, total int) bool {
		out[index] = v
		return true
	})
	return out
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

	loadfunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return next(k, k, v)
	}

	effectiveBalance := etl.NewCollector(kv.ValidatorEffectiveBalance, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer effectiveBalance.Close()
	balances := etl.NewCollector(kv.ValidatorBalance, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer balances.Close()
	randaoMixes := etl.NewCollector(kv.RandaoMixes, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer randaoMixes.Close()
	intraRandaoMixes := etl.NewCollector(kv.IntraRandaoMixes, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer intraRandaoMixes.Close()
	proposers := etl.NewCollector(kv.Proposers, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer proposers.Close()
	slashings := etl.NewCollector(kv.ValidatorSlashings, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer slashings.Close()
	blockRoots := etl.NewCollector(kv.BlockRoot, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer blockRoots.Close()
	stateRoots := etl.NewCollector(kv.StateRoot, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer stateRoots.Close()
	minimalBeaconStates := etl.NewCollector(kv.MinimalBeaconState, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer minimalBeaconStates.Close()
	inactivityScoresC := etl.NewCollector(kv.InactivityScores, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer inactivityScoresC.Close()
	checkpoints := etl.NewCollector(kv.Checkpoints, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer checkpoints.Close()
	nextSyncCommittee := etl.NewCollector(kv.NextSyncCommittee, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer nextSyncCommittee.Close()
	currentSyncCommittee := etl.NewCollector(kv.CurrentSyncCommittee, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer currentSyncCommittee.Close()
	currentEpochAttestations := etl.NewCollector(kv.CurrentEpochAttestations, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer currentEpochAttestations.Close()
	previousEpochAttestations := etl.NewCollector(kv.PreviousEpochAttestations, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer previousEpochAttestations.Close()
	eth1DataVotes := etl.NewCollector(kv.Eth1DataVotes, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer eth1DataVotes.Close()
	stateEvents := etl.NewCollector(kv.StateEvents, s.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), s.logger)
	defer stateEvents.Close()

	// buffers
	commonBuffer := &bytes.Buffer{}
	compressedWriter, err := zstd.NewWriter(commonBuffer, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}
	defer compressedWriter.Close()

	// TODO(Giulio2002): also store genesis information and resume from state.
	if s.currentState == nil {
		s.currentState, err = s.genesisState.Copy()
		if err != nil {
			return err
		}
		// Collect genesis state if we are at genesis
		if err := s.collectGenesisState(ctx, compressedWriter, s.currentState, slashings, proposers, minimalBeaconStates, stateEvents, changedValidators); err != nil {
			return err
		}
	}
	logLvl := log.LvlInfo
	if to-s.currentState.Slot() < 96 {
		logLvl = log.LvlDebug
	}
	start := time.Now()

	// Use this as the event slot (it will be incremented by 1 each time we process a block)
	slot := s.currentState.Slot() + 1

	var prevBalances, inactivityScores, prevValSet, slashingsBytes []byte
	events := state_accessors.NewStateEvents()

	// var validatorStaticState
	// var validatorStaticState map[uint64]*state.ValidatorStatic
	// Setup state events handlers
	s.currentState.SetEvents(raw.Events{
		OnRandaoMixChange: func(index int, mix [32]byte) error {
			return intraRandaoMixes.Collect(base_encoding.Encode64ToBytes4(slot), mix[:])
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
			v := append(s.currentState.CurrentJustifiedCheckpoint(), append(s.currentState.PreviousJustifiedCheckpoint(), s.currentState.FinalizedCheckpoint()...)...)
			k := base_encoding.Encode64ToBytes4(s.cfg.RoundSlotToEpoch(slot))
			if err := checkpoints.Collect(k, v); err != nil {
				return err
			}
			prevEpoch := epoch - 1
			mix := s.currentState.GetRandaoMixes(prevEpoch)
			if err := randaoMixes.Collect(base_encoding.Encode64ToBytes4(prevEpoch*s.cfg.SlotsPerEpoch), mix[:]); err != nil {
				return err
			}
			// truncate the file
			return proposers.Collect(base_encoding.Encode64ToBytes4(epoch), getProposerDutiesValue(s.currentState))
		},
		OnNewBlockRoot: func(index int, root common.Hash) error {
			return blockRoots.Collect(base_encoding.Encode64ToBytes4(s.currentState.Slot()), root[:])
		},
		OnNewStateRoot: func(index int, root common.Hash) error {
			return stateRoots.Collect(base_encoding.Encode64ToBytes4(s.currentState.Slot()), root[:])
		},
		OnNewNextSyncCommittee: func(committee *solid.SyncCommittee) error {
			roundedSlot := s.cfg.RoundSlotToSyncCommitteePeriod(slot)
			return nextSyncCommittee.Collect(base_encoding.Encode64ToBytes4(roundedSlot), committee[:])
		},
		OnNewCurrentSyncCommittee: func(committee *solid.SyncCommittee) error {
			roundedSlot := s.cfg.RoundSlotToSyncCommitteePeriod(slot)
			return currentSyncCommittee.Collect(base_encoding.Encode64ToBytes4(roundedSlot), committee[:])
		},
		OnAppendEth1Data: func(data *cltypes.Eth1Data) error {
			vote, err := data.EncodeSSZ(nil)
			if err != nil {
				return err
			}
			return eth1DataVotes.Collect(base_encoding.Encode64ToBytes4(slot), vote)
		},
		OnResetParticipation: func(previousParticipation *solid.BitList) error {
			if err := s.antiquateField(ctx, s.cfg.RoundSlotToEpoch(slot), previousParticipation.Bytes(), compressedWriter, "participation"); err != nil {
				s.logger.Error("Failed to antiquate participation", "err", err)
			}
			return err
		},
	})
	log.Log(logLvl, "Starting state processing", "from", slot, "to", to)
	// Set up a timer to log progress
	progressTimer := time.NewTicker(1 * time.Minute)
	defer progressTimer.Stop()
	prevSlot := slot
	// This tells us that transition and operations do not happen concurrently and access is safe, so we can optimize for GC.
	// there is optimized custom cache to recycle big GC overhead.
	for ; slot < to; slot++ {
		block, err := s.snReader.ReadBlockBySlot(ctx, tx, slot)
		if err != nil {
			return err
		}
		prevValidatorSetLength := s.currentState.ValidatorLength()
		prevEpoch := state.Epoch(s.currentState)

		if slot%s.cfg.SlotsPerEpoch == 0 && s.currentState.Version() == clparams.Phase0Version {
			encoded, err := s.currentState.CurrentEpochAttestations().EncodeSSZ(nil)
			if err != nil {
				return err
			}
			if err := s.dumpPayload(base_encoding.Encode64ToBytes4(s.cfg.RoundSlotToEpoch(slot-1)), encoded, currentEpochAttestations, commonBuffer, compressedWriter); err != nil {
				return err
			}
			encoded, err = s.currentState.PreviousEpochAttestations().EncodeSSZ(nil)
			if err != nil {
				return err
			}
			if err := s.dumpPayload(base_encoding.Encode64ToBytes4(s.cfg.RoundSlotToEpoch(slot-1)), encoded, previousEpochAttestations, commonBuffer, compressedWriter); err != nil {
				return err
			}
		}

		if slot%clparams.SlotsPerDump == 0 {
			if err := s.antiquateField(ctx, slot, s.currentState.RawBalances(), compressedWriter, "balances"); err != nil {
				return err
			}
			if err := s.antiquateEffectiveBalances(ctx, slot, s.currentState.RawBalances(), compressedWriter); err != nil {
				return err
			}
			if s.currentState.Version() >= clparams.AltairVersion {
				if err := s.antiquateField(ctx, slot, s.currentState.RawInactivityScores(), compressedWriter, "inactivity_scores"); err != nil {
					return err
				}
			}
			if err := s.antiquateFullSlashings(slashings, slot, s.currentState.RawSlashings(), commonBuffer, compressedWriter); err != nil {
				return err
			}
		}

		// If we have a missed block, we just skip it.
		if block == nil {
			continue
		}
		// We now compute the difference between the two balances.
		prevBalances = prevBalances[:0]
		prevBalances = append(prevBalances, s.currentState.RawBalances()...)
		inactivityScores = inactivityScores[:0]
		inactivityScores = append(inactivityScores, s.currentState.RawInactivityScores()...)
		prevValSet = prevValSet[:0]
		prevValSet = append(prevValSet, s.currentState.RawValidatorSet()...)
		slashingsBytes = slashingsBytes[:0]
		slashingsBytes = append(slashingsBytes, s.currentState.RawSlashings()...)

		// We sanity check the state every 100k slots.
		if err := transition.TransitionState(s.currentState, block, slot%100_000 == 0); err != nil {
			return err
		}

		if err := s.storeMinimalState(commonBuffer, s.currentState, minimalBeaconStates); err != nil {
			return err
		}
		if err := stateEvents.Collect(base_encoding.Encode64ToBytes4(slot), events.CopyBytes()); err != nil {
			return err
		}
		events.Reset()
		if slot%clparams.SlotsPerDump == 0 {
			continue
		}

		// antiquate fields
		key := base_encoding.Encode64ToBytes4(slot)
		if err := s.antiquateBytesListDiff(ctx, key, prevBalances, s.currentState.RawBalances(), balances, base_encoding.ComputeCompressedSerializedUint64ListDiff); err != nil {
			return err
		}
		if err := s.antiquateBytesListDiff(ctx, key, slashingsBytes, s.currentState.RawSlashings(), slashings, base_encoding.ComputeCompressedSerializedUint64ListDiff); err != nil {
			return err
		}
		isEpochCrossed := prevEpoch != state.Epoch(s.currentState)

		if prevValidatorSetLength != s.currentState.ValidatorLength() || isEpochCrossed {
			if err := s.antiquateBytesListDiff(ctx, key, prevValSet, s.currentState.RawValidatorSet(), effectiveBalance, base_encoding.ComputeCompressedSerializedEffectiveBalancesDiff); err != nil {
				return err
			}
			if s.currentState.Version() >= clparams.AltairVersion {
				if err := s.antiquateBytesListDiff(ctx, key, inactivityScores, s.currentState.RawInactivityScores(), inactivityScoresC, base_encoding.ComputeCompressedSerializedUint64ListDiff); err != nil {
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

	rwTx, err := s.mainDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()
	start = time.Now()
	log.Log(logLvl, "Stopped Caplin to load states")
	// Now load.
	if err := effectiveBalance.Load(rwTx, kv.ValidatorEffectiveBalance, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := randaoMixes.Load(rwTx, kv.RandaoMixes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := balances.Load(rwTx, kv.ValidatorBalance, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := proposers.Load(rwTx, kv.Proposers, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := slashings.Load(rwTx, kv.ValidatorSlashings, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := blockRoots.Load(rwTx, kv.BlockRoot, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := stateRoots.Load(rwTx, kv.StateRoot, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := minimalBeaconStates.Load(rwTx, kv.MinimalBeaconState, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := inactivityScoresC.Load(rwTx, kv.InactivityScores, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := intraRandaoMixes.Load(rwTx, kv.IntraRandaoMixes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := checkpoints.Load(rwTx, kv.Checkpoints, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := nextSyncCommittee.Load(rwTx, kv.NextSyncCommittee, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := currentSyncCommittee.Load(rwTx, kv.CurrentSyncCommittee, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := currentEpochAttestations.Load(rwTx, kv.CurrentEpochAttestations, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := previousEpochAttestations.Load(rwTx, kv.PreviousEpochAttestations, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := eth1DataVotes.Load(rwTx, kv.Eth1DataVotes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := stateEvents.Load(rwTx, kv.StateEvents, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
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

	log.Info("Historical antiquated", "slot", s.currentState.Slot(), "latency", time.Since(start))
	return rwTx.Commit()
}

func (s *Antiquary) antiquateField(ctx context.Context, slot uint64, uncompressed []byte, compressor *zstd.Encoder, name string) error {
	folderPath, filePath := clparams.EpochToPaths(slot, s.cfg, name)
	_ = s.fs.MkdirAll(folderPath, 0o755)

	balancesFile, err := s.fs.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer balancesFile.Close()
	compressor.Reset(balancesFile)

	if err := binary.Write(balancesFile, binary.LittleEndian, uint64(len(uncompressed))); err != nil {
		return err
	}

	if _, err := compressor.Write(uncompressed); err != nil {
		return err
	}

	if err := compressor.Close(); err != nil {
		return err
	}
	return balancesFile.Sync()
}

func (s *Antiquary) antiquateEffectiveBalances(ctx context.Context, slot uint64, uncompressed []byte, compressor *zstd.Encoder) error {
	folderPath, filePath := clparams.EpochToPaths(slot, s.cfg, "effective_balances")
	_ = s.fs.MkdirAll(folderPath, 0o755)

	balancesFile, err := s.fs.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer balancesFile.Close()
	compressor.Reset(balancesFile)
	validatorSetSize := 121

	if err := binary.Write(balancesFile, binary.LittleEndian, uint64((len(uncompressed)/validatorSetSize)*8)); err != nil {
		return err
	}

	for i := 0; i < len(uncompressed)/validatorSetSize; i++ {
		// 80:88
		if _, err := compressor.Write(uncompressed[i*validatorSetSize+80 : i*validatorSetSize+88]); err != nil {
			return err
		}
	}

	if err := compressor.Close(); err != nil {
		return err
	}
	return balancesFile.Sync()
}

func (s *Antiquary) antiquateBytesListDiff(ctx context.Context, key []byte, old, new []byte, collector *etl.Collector, diffFn func(w io.Writer, old, new []byte) error) error {
	// create a diff
	diffBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(diffBuffer)
	diffBuffer.Reset()

	if err := diffFn(diffBuffer, old, new); err != nil {
		return err
	}

	return collector.Collect(key, common.Copy(diffBuffer.Bytes()))
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

func (s *Antiquary) collectGenesisState(ctx context.Context, compressor *zstd.Encoder, state *state.CachingBeaconState, slashings, proposersCollector, minimalBeaconStateCollector, stateEvents *etl.Collector, changedValidators map[uint64]struct{}) error {
	var err error
	slot := state.Slot()
	epoch := slot / s.cfg.SlotsPerEpoch
	// Setup state events handlers
	if err := proposersCollector.Collect(base_encoding.Encode64ToBytes4(epoch), getProposerDutiesValue(s.currentState)); err != nil {
		return err
	}

	events := state_accessors.NewStateEvents()

	state.ForEachValidator(func(v solid.Validator, index, total int) bool {
		changedValidators[uint64(index)] = struct{}{}
		if err = s.validatorsTable.AddValidator(v, uint64(index), 0); err != nil {
			return false
		}
		events.AddValidator(uint64(index), v)
		return true
	})
	if err != nil {
		return err
	}
	roundedSlotToDump := slot - (slot % clparams.SlotsPerDump)
	if err := s.antiquateField(ctx, roundedSlotToDump, s.currentState.RawBalances(), compressor, "balances"); err != nil {
		return err
	}
	if err := s.antiquateEffectiveBalances(ctx, roundedSlotToDump, s.currentState.RawValidatorSet(), compressor); err != nil {
		return err
	}
	if s.currentState.Version() >= clparams.AltairVersion {
		if err := s.antiquateField(ctx, roundedSlotToDump, s.currentState.RawInactivityScores(), compressor, "inactivity_scores"); err != nil {
			return err
		}
	}
	var commonBuffer bytes.Buffer
	if err := s.antiquateFullSlashings(slashings, roundedSlotToDump, s.currentState.RawSlashings(), &commonBuffer, compressor); err != nil {
		return err
	}

	if state.Version() >= clparams.AltairVersion {
		if err := s.antiquateField(ctx, roundedSlotToDump, state.RawInactivityScores(), compressor, "inactivity_scores"); err != nil {
			return err
		}

	}

	var b bytes.Buffer
	if err := s.storeMinimalState(&b, state, minimalBeaconStateCollector); err != nil {
		return err
	}

	return stateEvents.Collect(base_encoding.Encode64ToBytes4(slot), events.CopyBytes())
}

func (s *Antiquary) storeMinimalState(buffer *bytes.Buffer, st *state.CachingBeaconState, collector *etl.Collector) error {
	buffer.Reset()
	minimalBeaconState := state_accessors.MinimalBeaconStateFromBeaconState(st.BeaconState)

	if err := minimalBeaconState.WriteTo(buffer); err != nil {
		return err
	}
	return collector.Collect(base_encoding.Encode64ToBytes4(st.Slot()), buffer.Bytes())
}

func (s *Antiquary) dumpPayload(k []byte, v []byte, c *etl.Collector, b *bytes.Buffer, compressor *zstd.Encoder) error {
	if compressor == nil {
		return c.Collect(k, v)
	}
	b.Reset()
	compressor.Reset(b)

	if _, err := compressor.Write(v); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	return c.Collect(k, common.Copy(b.Bytes()))
}

func flattenRandaoMixes(hashes []libcommon.Hash) []byte {
	out := make([]byte, len(hashes)*32)
	for i, h := range hashes {
		copy(out[i*32:(i+1)*32], h[:])
	}
	return out
}

// antiquateFullSlashings goes on mdbx as it is full of common repeated patter always and thus fits with 16KB pages.
func (s *Antiquary) antiquateFullSlashings(collector *etl.Collector, slot uint64, slashings []byte, buffer *bytes.Buffer, compressor *zstd.Encoder) error {
	buffer.Reset()
	compressor.Reset(buffer)
	if _, err := compressor.Write(slashings); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	return collector.Collect(base_encoding.Encode64ToBytes4(slot), common.Copy(buffer.Bytes()))
}
