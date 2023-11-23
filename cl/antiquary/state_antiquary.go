package antiquary

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
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

const slotsPerDumps = 2048 // Dump full balances

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
	reqRetryTimer := time.NewTicker(3 * time.Second)
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
			if progress >= finalized {
				continue
			}
			if err := s.incrementBeaconState(ctx, finalized); err != nil {
				s.logger.Error("Failed to increment beacon state", "err", err)
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

func (s *Antiquary) incrementBeaconState(ctx context.Context, to uint64) error {
	var tx kv.Tx
	tx, err := s.mainDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO(Giulio2002): also store genesis information and resume from state.
	if s.currentState == nil {
		s.currentState, err = initial_state.GetGenesisState(clparams.NetworkType(s.cfg.DepositNetworkID))
		if err != nil {
			return err
		}
	}
	loadfunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return next(k, k, v)
	}
	// Setup ETL collectors for:
	// ValidatorEffectiveBalance,
	// ValidatorSlashed,
	// ValidatorActivationEligibilityEpoch,
	// ValidatorActivationEpoch,
	// ValidatorExitEpoch,
	// ValidatorWithdrawableEpoch,
	// ValidatorWithdrawalCredentials,
	// ValidatorBalance,
	// RandaoMixes,
	// Proposers,
	effectiveBalance := etl.NewCollector(kv.ValidatorEffectiveBalance, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer effectiveBalance.Close()
	slashed := etl.NewCollector(kv.ValidatorSlashed, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer slashed.Close()
	activationEligibilityEpoch := etl.NewCollector(kv.ValidatorActivationEligibilityEpoch, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer activationEligibilityEpoch.Close()
	activationEpoch := etl.NewCollector(kv.ValidatorActivationEpoch, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer activationEpoch.Close()
	exitEpoch := etl.NewCollector(kv.ValidatorExitEpoch, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer exitEpoch.Close()
	withdrawableEpoch := etl.NewCollector(kv.ValidatorWithdrawableEpoch, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer withdrawableEpoch.Close()
	withdrawalCredentials := etl.NewCollector(kv.ValidatorWithdrawalCredentials, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer withdrawalCredentials.Close()

	randaoMixes := etl.NewCollector(kv.RandaoMixes, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer randaoMixes.Close()
	proposers := etl.NewCollector(kv.Proposers, s.dirs.Tmp, etl.NewNewestEntryBuffer(etl.BufferOptimalSize), s.logger)
	defer proposers.Close()
	// Use this as the event slot (it will be incremented by 1 each time we process a block)
	slot := s.currentState.Slot() + 1
	// buffers
	compressedWriter, err := zlib.NewWriterLevel(nil, zlib.BestCompression)
	if err != nil {
		return err
	}
	defer compressedWriter.Close()
	var prevBalances, nextBalances []uint64
	var diff []byte
	// Setup state events handlers
	s.currentState.SetEvents(raw.Events{
		OnRandaoMixChange: func(index int, mix [32]byte) error {
			return randaoMixes.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), mix[:])
		},
		OnNewValidator: func(index int, v solid.Validator, balance uint64) error {
			if err := effectiveBalance.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(v.EffectiveBalance())); err != nil {
				return err
			}
			slashedVal := []byte{0}
			if v.Slashed() {
				slashedVal = []byte{1}
			}
			if err := slashed.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), slashedVal); err != nil {
				return err
			}
			if err := activationEligibilityEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(v.ActivationEligibilityEpoch())); err != nil {
				return err
			}
			if err := activationEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(v.ActivationEpoch())); err != nil {
				return err
			}
			if err := exitEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(v.ExitEpoch())); err != nil {
				return err
			}
			if err := withdrawableEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(v.WithdrawableEpoch())); err != nil {
				return err
			}
			w := v.WithdrawalCredentials()
			return withdrawalCredentials.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), w[:])
		},
		OnNewValidatorEffectiveBalance: func(index int, balance uint64) error {
			return effectiveBalance.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(balance))
		},
		OnNewValidatorActivationEpoch: func(index int, epoch uint64) error {
			return activationEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(epoch))
		},
		OnNewValidatorExitEpoch: func(index int, epoch uint64) error {
			return exitEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(epoch))
		},
		OnNewValidatorWithdrawableEpoch: func(index int, epoch uint64) error {
			return withdrawableEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(epoch))
		},
		OnNewValidatorSlashed: func(index int, newSlashed bool) error {
			slashedVal := []byte{0}
			if newSlashed {
				slashedVal = []byte{1}
			}
			return slashed.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), slashedVal)
		},
		OnNewValidatorActivationEligibilityEpoch: func(index int, epoch uint64) error {
			return activationEligibilityEpoch.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), base_encoding.EncodeCompactUint64(epoch))
		},
		OnNewValidatorWithdrawalCredentials: func(index int, wc []byte) error {
			return withdrawalCredentials.Collect(base_encoding.IndexAndPeriodKey(uint64(index), slot), wc)
		},
		OnEpochBoundary: func(epoch uint64) error {
			// truncate the file
			return proposers.Collect(base_encoding.Encode64ToBytes4(epoch), getProposerDutiesValue(s.currentState))
		},
	})
	log.Info("Starting state processing", "from", slot, "to", to)
	// Set up a timer to log progress
	progressTimer := time.NewTicker(1 * time.Minute)
	defer progressTimer.Stop()
	prevSlot := slot

	for ; slot < to; slot++ {
		block, err := s.snReader.ReadBlockBySlot(ctx, tx, slot)
		if err != nil {
			return err
		}
		if slot%slotsPerDumps == 0 {
			if err := s.antiquateBalances(ctx, slot, s.currentState); err != nil {
				return err
			}
		}

		// If we have a missed block, we just skip it.
		if block == nil {
			continue
		}
		prevBalances = uint64BalancesList(s.currentState, prevBalances)
		// We sanity check the state every 100k slots.
		if err := transition.TransitionState(s.currentState, block, slot%100_000 == 0); err != nil {
			return err
		}
		nextBalances = uint64BalancesList(s.currentState, nextBalances)
		// We now compute the difference between the two balances.
		diff, err = base_encoding.ComputeCompressedSerializedUint64ListDiff(prevBalances, nextBalances, diff)
		if err != nil {
			return err
		}
		// antiquate the balances
		if err := s.antiquateBalancesDiff(ctx, slot, diff); err != nil {
			return err
		}
		// We now do some post-processing on the state.
		select {
		case <-progressTimer.C:
			log.Info("State processing progress", "slot", slot, "blk/sec", fmt.Sprintf("%.2f", float64(slot-prevSlot)/60))
			prevSlot = slot
		default:
		}
	}
	log.Info("State processing finished", "slot", s.currentState.Slot())
	tx.Rollback()
	log.Info("Stopping Caplin to load states")

	rwTx, err := s.mainDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()
	// Now load.
	if err := effectiveBalance.Load(rwTx, kv.ValidatorEffectiveBalance, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := slashed.Load(rwTx, kv.ValidatorSlashed, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := activationEligibilityEpoch.Load(rwTx, kv.ValidatorActivationEligibilityEpoch, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := activationEpoch.Load(rwTx, kv.ValidatorActivationEpoch, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := exitEpoch.Load(rwTx, kv.ValidatorExitEpoch, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := withdrawableEpoch.Load(rwTx, kv.ValidatorWithdrawableEpoch, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := withdrawalCredentials.Load(rwTx, kv.ValidatorWithdrawalCredentials, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := randaoMixes.Load(rwTx, kv.RandaoMixes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := proposers.Load(rwTx, kv.Proposers, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := state_accessors.SetStateProcessingProgress(rwTx, s.currentState.Slot()); err != nil {
		return err
	}
	log.Info("Restarting Caplin")
	return rwTx.Commit()
}

func (s *Antiquary) antiquateBalances(ctx context.Context, slot uint64, state *state.CachingBeaconState) error {
	folderPath, filePath := epochToPaths(slot, s.cfg, "balances")
	_ = s.fs.MkdirAll(folderPath, 0o755)

	balancesFile, err := s.fs.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer balancesFile.Close()

	compressedWriter, err := zlib.NewWriterLevel(balancesFile, zlib.BestCompression)
	if err != nil {
		return err
	}
	defer compressedWriter.Close()

	balances := make([]byte, state.ValidatorLength()*8)
	state.ForEachBalance(func(v uint64, index int, total int) bool {
		binary.BigEndian.PutUint64(balances[index*8:], v)
		return true
	})

	if _, err := compressedWriter.Write(balances); err != nil {
		return err
	}

	if err := compressedWriter.Flush(); err != nil {
		return err
	}
	return balancesFile.Sync()
}

func (s *Antiquary) antiquateBalancesDiff(ctx context.Context, slot uint64, diff []byte) error {
	folderPath, filePath := epochToPaths(slot, s.cfg, "balances_diff")
	_ = s.fs.MkdirAll(folderPath, 0o755)

	balancesFile, err := s.fs.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer balancesFile.Close()

	if err != nil {
		return err
	}
	if _, err := balancesFile.Write(diff); err != nil {
		return err
	}
	return balancesFile.Sync()
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

const subDivisionFolderSize = 10_000

func epochToPaths(slot uint64, config *clparams.BeaconChainConfig, suffix string) (string, string) {
	folderPath := path.Clean(fmt.Sprintf("%d", slot/subDivisionFolderSize))
	return folderPath, path.Clean(fmt.Sprintf("%s/%d.%s.sz", folderPath, slot/config.SlotsPerEpoch, suffix))
}
