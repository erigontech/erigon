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
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
)

// RATIONALE: MDBX locks the entire database when writing to it, so we need to minimize the time spent in the write lock.
// so instead of writing the historical states on write transactions, we accumulate them in memory and write them in a single  write transaction.

// beaconStatesCollector is a collector that collects some of the beacon states fields in sub-collectors.
// these collectors then flush the data to the database.
type beaconStatesCollector struct {
	effectiveBalanceCollector        *etl.Collector
	balancesCollector                *etl.Collector
	randaoMixesCollector             *etl.Collector
	intraRandaoMixesCollector        *etl.Collector
	slashingsCollector               *etl.Collector
	blockRootsCollector              *etl.Collector
	stateRootsCollector              *etl.Collector
	slotDataCollector                *etl.Collector
	epochDataCollector               *etl.Collector
	inactivityScoresCollector        *etl.Collector
	nextSyncCommitteeCollector       *etl.Collector
	currentSyncCommitteeCollector    *etl.Collector
	eth1DataVotesCollector           *etl.Collector
	stateEventsCollector             *etl.Collector
	activeValidatorIndiciesCollector *etl.Collector
	balancesDumpsCollector           *etl.Collector
	effectiveBalancesDumpCollector   *etl.Collector
	// electra -- collectors
	pendingDepositsCollector           *etl.Collector
	pendingConsolidationsCollector     *etl.Collector
	pendingWithdrawalsCollector        *etl.Collector
	pendingDepositsCollectorDump       *etl.Collector
	pendingConsolidationsCollectorDump *etl.Collector
	pendingWithdrawalsCollectorDump    *etl.Collector
	// electra -- diffs data structures
	pendingDepositsWriter       *base_encoding.SSZQueueEncoder[*solid.PendingDeposit]
	pendingConsolidationsWriter *base_encoding.SSZQueueEncoder[*solid.PendingConsolidation]
	pendingWithdrawalsWriter    *base_encoding.SSZQueueEncoder[*solid.PendingPartialWithdrawal]

	buf        *bytes.Buffer
	compressor *zstd.Encoder

	beaconCfg *clparams.BeaconChainConfig
	logger    log.Logger
}

func newBeaconStatesCollector(beaconCfg *clparams.BeaconChainConfig, tmpdir string, logger log.Logger) *beaconStatesCollector {
	buf := &bytes.Buffer{}
	compressor, err := zstd.NewWriter(buf)
	if err != nil {
		panic(err)
	}

	// 18 collectors * 256mb / 8 = 512mb in worst case
	return &beaconStatesCollector{
		effectiveBalanceCollector:        etl.NewCollectorWithAllocator(kv.ValidatorEffectiveBalance, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		balancesCollector:                etl.NewCollectorWithAllocator(kv.ValidatorBalance, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		randaoMixesCollector:             etl.NewCollectorWithAllocator(kv.RandaoMixes, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		intraRandaoMixesCollector:        etl.NewCollectorWithAllocator(kv.IntraRandaoMixes, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		slashingsCollector:               etl.NewCollectorWithAllocator(kv.ValidatorSlashings, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		blockRootsCollector:              etl.NewCollectorWithAllocator(kv.BlockRoot, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		stateRootsCollector:              etl.NewCollectorWithAllocator(kv.StateRoot, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		slotDataCollector:                etl.NewCollectorWithAllocator(kv.SlotData, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		epochDataCollector:               etl.NewCollectorWithAllocator(kv.EpochData, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		inactivityScoresCollector:        etl.NewCollectorWithAllocator(kv.InactivityScores, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		nextSyncCommitteeCollector:       etl.NewCollectorWithAllocator(kv.NextSyncCommittee, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		currentSyncCommitteeCollector:    etl.NewCollectorWithAllocator(kv.CurrentSyncCommittee, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		eth1DataVotesCollector:           etl.NewCollectorWithAllocator(kv.Eth1DataVotes, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		stateEventsCollector:             etl.NewCollectorWithAllocator(kv.StateEvents, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		activeValidatorIndiciesCollector: etl.NewCollectorWithAllocator(kv.ActiveValidatorIndicies, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		balancesDumpsCollector:           etl.NewCollectorWithAllocator(kv.BalancesDump, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		effectiveBalancesDumpCollector:   etl.NewCollectorWithAllocator(kv.EffectiveBalancesDump, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlTrace),
		// electra
		pendingDepositsCollector:           etl.NewCollectorWithAllocator(kv.PendingDeposits, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),
		pendingConsolidationsCollector:     etl.NewCollectorWithAllocator(kv.PendingConsolidations, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),
		pendingWithdrawalsCollector:        etl.NewCollectorWithAllocator(kv.PendingPartialWithdrawals, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),
		pendingDepositsCollectorDump:       etl.NewCollectorWithAllocator(kv.PendingDepositsDump, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),
		pendingConsolidationsCollectorDump: etl.NewCollectorWithAllocator(kv.PendingConsolidationsDump, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),
		pendingWithdrawalsCollectorDump:    etl.NewCollectorWithAllocator(kv.PendingPartialWithdrawalsDump, tmpdir, etl.SmallSortableBuffers, logger).LogLvl(log.LvlInfo),

		pendingDepositsWriter:       base_encoding.NewSSZQueueEncoder[*solid.PendingDeposit](func(a, b *solid.PendingDeposit) bool { return *a == *b }),
		pendingConsolidationsWriter: base_encoding.NewSSZQueueEncoder[*solid.PendingConsolidation](func(a, b *solid.PendingConsolidation) bool { return *a == *b }),
		pendingWithdrawalsWriter:    base_encoding.NewSSZQueueEncoder[*solid.PendingPartialWithdrawal](func(a, b *solid.PendingPartialWithdrawal) bool { return *a == *b }),

		logger:    logger,
		beaconCfg: beaconCfg,

		buf:        buf,
		compressor: compressor,
	}
}

func (i *beaconStatesCollector) addGenesisState(ctx context.Context, state *state.CachingBeaconState) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)

	slot := state.Slot()

	events := state_accessors.NewStateEvents()

	state.ForEachValidator(func(v solid.Validator, index, total int) bool {
		events.AddValidator(uint64(index), v)
		return true
	})
	roundedSlotToDump := slot - (slot % clparams.SlotsPerDump)

	if err := antiquateField(ctx, roundedSlotToDump, state.RawBalances(), i.buf, i.compressor, i.balancesDumpsCollector); err != nil {
		return err
	}

	if err := i.collectEffectiveBalancesDump(slot, state.RawValidatorSet()); err != nil {
		return err
	}
	if err := antiquateFullUint64List(i.slashingsCollector, roundedSlotToDump, state.RawSlashings(), i.buf, i.compressor); err != nil {
		return err
	}

	if err := i.storeEpochData(state); err != nil {
		return err
	}

	if state.Version() >= clparams.AltairVersion {
		// dump inactivity scores
		if err := antiquateFullUint64List(i.inactivityScoresCollector, slot, state.RawInactivityScores(), i.buf, i.compressor); err != nil {
			return err
		}
		committeeSlot := i.beaconCfg.RoundSlotToSyncCommitteePeriod(slot)
		committee := *state.CurrentSyncCommittee()
		if err := i.currentSyncCommitteeCollector.Collect(base_encoding.Encode64ToBytes4(committeeSlot), committee[:]); err != nil {
			return err
		}

		committee = *state.NextSyncCommittee()
		if err := i.nextSyncCommitteeCollector.Collect(base_encoding.Encode64ToBytes4(committeeSlot), committee[:]); err != nil {
			return err
		}
	}
	if state.Version() >= clparams.ElectraVersion {
		if err := antiquateListSSZ(ctx, slot, state.PendingDeposits(), i.buf, i.compressor, i.pendingDepositsCollectorDump); err != nil {
			return err
		}
		if err := antiquateListSSZ(ctx, slot, state.PendingConsolidations(), i.buf, i.compressor, i.pendingConsolidationsCollectorDump); err != nil {
			return err
		}
		if err := antiquateListSSZ(ctx, slot, state.PendingPartialWithdrawals(), i.buf, i.compressor, i.pendingWithdrawalsCollectorDump); err != nil {
			return err
		}
	}

	if err := i.storeSlotData(state, nil); err != nil {
		return err
	}

	return i.stateEventsCollector.Collect(base_encoding.Encode64ToBytes4(slot), events.CopyBytes())
}

func (i *beaconStatesCollector) storeEpochData(st *state.CachingBeaconState) error {
	i.buf.Reset()
	epochData := state_accessors.EpochDataFromBeaconState(st)

	if err := epochData.WriteTo(i.buf); err != nil {
		return err
	}
	roundedSlot := i.beaconCfg.RoundSlotToEpoch(st.Slot())
	return i.epochDataCollector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), i.buf.Bytes())
}

func (i *beaconStatesCollector) storeSlotData(st *state.CachingBeaconState, rewardsCollector *eth2.BlockRewardsCollector) error {
	i.buf.Reset()
	slotData := state_accessors.SlotDataFromBeaconState(st)
	if rewardsCollector != nil {
		slotData.AttestationsRewards = rewardsCollector.Attestations
		slotData.SyncAggregateRewards = rewardsCollector.SyncAggregate
		slotData.AttesterSlashings = rewardsCollector.AttesterSlashings
		slotData.ProposerSlashings = rewardsCollector.ProposerSlashings
	}
	if err := slotData.WriteTo(i.buf); err != nil {
		return err
	}
	return i.slotDataCollector.Collect(base_encoding.Encode64ToBytes4(st.Slot()), i.buf.Bytes())
}

func (i *beaconStatesCollector) collectEffectiveBalancesDump(slot uint64, uncompressed []byte) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)

	validatorSetSize := 121
	for j := 0; j < len(uncompressed)/validatorSetSize; j++ {
		// 80:88
		if _, err := i.compressor.Write(uncompressed[j*validatorSetSize+80 : j*validatorSetSize+88]); err != nil {
			return err
		}
	}

	if err := i.compressor.Close(); err != nil {
		return err
	}
	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	return i.effectiveBalancesDumpCollector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), i.buf.Bytes())
}

func (i *beaconStatesCollector) collectBalancesDump(slot uint64, uncompressed []byte) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)
	return antiquateField(context.Background(), slot, uncompressed, i.buf, i.compressor, i.balancesDumpsCollector)
}

func (i *beaconStatesCollector) collectPendingDepositsDump(slot uint64, pendingDeposits *solid.ListSSZ[*solid.PendingDeposit]) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)
	return antiquateListSSZ(context.Background(), slot, pendingDeposits, i.buf, i.compressor, i.pendingDepositsCollectorDump)
}

func (i *beaconStatesCollector) preStateTransitionHook(preState *state.CachingBeaconState) {
	if preState.Version() >= clparams.ElectraVersion {
		i.pendingDepositsWriter.Initialize(preState.PendingDeposits())
		i.pendingConsolidationsWriter.Initialize(preState.PendingConsolidations())
		i.pendingWithdrawalsWriter.Initialize(preState.PendingPartialWithdrawals())
	}
}

func (i *beaconStatesCollector) collectElectraQueuesDiffs(slot uint64, pendingDeposits *solid.ListSSZ[*solid.PendingDeposit], pendingConsolidations *solid.ListSSZ[*solid.PendingConsolidation], pendingWithdrawals *solid.ListSSZ[*solid.PendingPartialWithdrawal]) error {
	i.buf.Reset()
	if err := i.pendingDepositsWriter.WriteDiff(i.buf, pendingDeposits); err != nil {
		return err
	}
	if err := i.pendingDepositsCollector.Collect(base_encoding.Encode64ToBytes4(slot), i.buf.Bytes()); err != nil {
		return err
	}
	i.buf.Reset()

	if err := i.pendingConsolidationsWriter.WriteDiff(i.buf, pendingConsolidations); err != nil {
		return err
	}
	if err := i.pendingConsolidationsCollector.Collect(base_encoding.Encode64ToBytes4(slot), i.buf.Bytes()); err != nil {
		return err
	}
	i.buf.Reset()

	if err := i.pendingWithdrawalsWriter.WriteDiff(i.buf, pendingWithdrawals); err != nil {
		return err
	}
	return i.pendingWithdrawalsCollector.Collect(base_encoding.Encode64ToBytes4(slot), i.buf.Bytes())
}

func (i *beaconStatesCollector) collectPendingConsolidationsDump(slot uint64, pendingConsolidations *solid.ListSSZ[*solid.PendingConsolidation]) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)
	return antiquateListSSZ(context.Background(), slot, pendingConsolidations, i.buf, i.compressor, i.pendingConsolidationsCollectorDump)
}

func (i *beaconStatesCollector) collectPendingWithdrawalsDump(slot uint64, pendingWithdrawals *solid.ListSSZ[*solid.PendingPartialWithdrawal]) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)
	return antiquateListSSZ(context.Background(), slot, pendingWithdrawals, i.buf, i.compressor, i.pendingWithdrawalsCollectorDump)
}

func (i *beaconStatesCollector) collectIntraEpochRandaoMix(slot uint64, randao common.Hash) error {
	return i.intraRandaoMixesCollector.Collect(base_encoding.Encode64ToBytes4(slot), randao[:])
}

func (i *beaconStatesCollector) collectEpochRandaoMix(epoch uint64, randao common.Hash) error {
	slot := epoch * i.beaconCfg.SlotsPerEpoch
	return i.randaoMixesCollector.Collect(base_encoding.Encode64ToBytes4(slot), randao[:])
}

func (i *beaconStatesCollector) collectStateRoot(slot uint64, stateRoot common.Hash) error {
	return i.stateRootsCollector.Collect(base_encoding.Encode64ToBytes4(slot), stateRoot[:])
}

func (i *beaconStatesCollector) collectBlockRoot(slot uint64, blockRoot common.Hash) error {
	return i.blockRootsCollector.Collect(base_encoding.Encode64ToBytes4(slot), blockRoot[:])
}

func (i *beaconStatesCollector) collectActiveIndices(epoch uint64, activeIndices []uint64) error {
	i.buf.Reset()
	if err := base_encoding.WriteRabbits(activeIndices, i.buf); err != nil {
		return err
	}
	slot := epoch * i.beaconCfg.SlotsPerEpoch
	return i.activeValidatorIndiciesCollector.Collect(base_encoding.Encode64ToBytes4(slot), i.buf.Bytes())
}

func (i *beaconStatesCollector) collectCurrentSyncCommittee(slot uint64, committee *solid.SyncCommittee) error {
	roundedSlot := i.beaconCfg.RoundSlotToSyncCommitteePeriod(slot)
	return i.currentSyncCommitteeCollector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), committee[:])
}

func (i *beaconStatesCollector) collectNextSyncCommittee(slot uint64, committee *solid.SyncCommittee) error {
	roundedSlot := i.beaconCfg.RoundSlotToSyncCommitteePeriod(slot)
	return i.nextSyncCommitteeCollector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), committee[:])
}

func (i *beaconStatesCollector) collectEth1DataVote(slot uint64, eth1Data *cltypes.Eth1Data) error {
	vote, err := eth1Data.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	return i.eth1DataVotesCollector.Collect(base_encoding.Encode64ToBytes4(slot), vote)
}

func (i *beaconStatesCollector) collectSlashings(slot uint64, rawSlashings []byte) error {
	i.buf.Reset()
	i.compressor.Reset(i.buf)
	return antiquateFullUint64List(i.slashingsCollector, slot, rawSlashings, i.buf, i.compressor)
}

func (i *beaconStatesCollector) collectStateEvents(slot uint64, events *state_accessors.StateEvents) error {
	return i.stateEventsCollector.Collect(base_encoding.Encode64ToBytes4(slot), events.CopyBytes())
}

func (i *beaconStatesCollector) collectBalancesDiffs(ctx context.Context, slot uint64, old, new []byte) error {
	return antiquateBytesListDiff(ctx, base_encoding.Encode64ToBytes4(slot), old, new, i.balancesCollector, base_encoding.ComputeCompressedSerializedUint64ListDiff)
}

func (i *beaconStatesCollector) collectEffectiveBalancesDiffs(ctx context.Context, slot uint64, oldValidatorSetSSZ, newValidatorSetSSZ []byte) error {
	return antiquateBytesListDiff(ctx, base_encoding.Encode64ToBytes4(slot), oldValidatorSetSSZ, newValidatorSetSSZ, i.effectiveBalanceCollector, base_encoding.ComputeCompressedSerializedEffectiveBalancesDiff)
}

func (i *beaconStatesCollector) collectInactivityScores(slot uint64, inactivityScores []byte) error {
	return antiquateFullUint64List(i.inactivityScoresCollector, slot, inactivityScores, i.buf, i.compressor)
}

func (i *beaconStatesCollector) flush(ctx context.Context, tx kv.RwTx) error {
	loadfunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return next(k, k, v)
	}

	if err := i.effectiveBalanceCollector.Load(tx, kv.ValidatorEffectiveBalance, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.randaoMixesCollector.Load(tx, kv.RandaoMixes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.balancesCollector.Load(tx, kv.ValidatorBalance, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := i.slashingsCollector.Load(tx, kv.ValidatorSlashings, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.blockRootsCollector.Load(tx, kv.BlockRoot, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.stateRootsCollector.Load(tx, kv.StateRoot, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.activeValidatorIndiciesCollector.Load(tx, kv.ActiveValidatorIndicies, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.slotDataCollector.Load(tx, kv.SlotData, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.inactivityScoresCollector.Load(tx, kv.InactivityScores, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.intraRandaoMixesCollector.Load(tx, kv.IntraRandaoMixes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.epochDataCollector.Load(tx, kv.EpochData, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.nextSyncCommitteeCollector.Load(tx, kv.NextSyncCommittee, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.currentSyncCommitteeCollector.Load(tx, kv.CurrentSyncCommittee, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.eth1DataVotesCollector.Load(tx, kv.Eth1DataVotes, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.stateEventsCollector.Load(tx, kv.StateEvents, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.effectiveBalancesDumpCollector.Load(tx, kv.EffectiveBalancesDump, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingDepositsCollector.Load(tx, kv.PendingDeposits, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingConsolidationsCollector.Load(tx, kv.PendingConsolidations, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingWithdrawalsCollector.Load(tx, kv.PendingPartialWithdrawals, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingDepositsCollectorDump.Load(tx, kv.PendingDepositsDump, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingConsolidationsCollectorDump.Load(tx, kv.PendingConsolidationsDump, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := i.pendingWithdrawalsCollectorDump.Load(tx, kv.PendingPartialWithdrawalsDump, loadfunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return i.balancesDumpsCollector.Load(tx, kv.BalancesDump, loadfunc, etl.TransformArgs{Quit: ctx.Done()})
}

func (i *beaconStatesCollector) close() {
	i.effectiveBalanceCollector.Close()
	i.balancesCollector.Close()
	i.randaoMixesCollector.Close()
	i.intraRandaoMixesCollector.Close()
	i.slashingsCollector.Close()
	i.blockRootsCollector.Close()
	i.stateRootsCollector.Close()
	i.slotDataCollector.Close()
	i.epochDataCollector.Close()
	i.inactivityScoresCollector.Close()
	i.nextSyncCommitteeCollector.Close()
	i.currentSyncCommitteeCollector.Close()
	i.eth1DataVotesCollector.Close()
	i.stateEventsCollector.Close()
	i.activeValidatorIndiciesCollector.Close()
	i.balancesDumpsCollector.Close()
	i.effectiveBalancesDumpCollector.Close()
	i.pendingDepositsCollector.Close()
	i.pendingConsolidationsCollector.Close()
	i.pendingWithdrawalsCollector.Close()
	i.pendingConsolidationsCollectorDump.Close()
	i.pendingDepositsCollectorDump.Close()
	i.pendingWithdrawalsCollectorDump.Close()
}

// antiquateFullUint64List goes on mdbx as it is full of common repeated patter always and thus fits with 16KB pages.
func antiquateFullUint64List(collector *etl.Collector, slot uint64, raw []byte, buffer *bytes.Buffer, compressor *zstd.Encoder) error {
	buffer.Reset()
	compressor.Reset(buffer)
	if _, err := compressor.Write(raw); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	return collector.Collect(base_encoding.Encode64ToBytes4(slot), buffer.Bytes())
}

func antiquateField(ctx context.Context, slot uint64, uncompressed []byte, buffer *bytes.Buffer, compressor *zstd.Encoder, collector *etl.Collector) error {
	buffer.Reset()
	compressor.Reset(buffer)

	if _, err := compressor.Write(uncompressed); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	return collector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), buffer.Bytes())
}

func antiquateListSSZ[T solid.EncodableHashableSSZ](ctx context.Context, slot uint64, l *solid.ListSSZ[T], buffer *bytes.Buffer, compressor *zstd.Encoder, collector *etl.Collector) error {
	buffer.Reset()
	compressor.Reset(buffer)

	uncompressed, err := l.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	if _, err := compressor.Write(uncompressed); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	roundedSlot := slot - (slot % clparams.SlotsPerDump)
	return collector.Collect(base_encoding.Encode64ToBytes4(roundedSlot), buffer.Bytes())
}

func antiquateBytesListDiff(ctx context.Context, key []byte, old, new []byte, collector *etl.Collector, diffFn func(w io.Writer, old, new []byte) error) error {
	// create a diff
	diffBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(diffBuffer)
	diffBuffer.Reset()

	if err := diffFn(diffBuffer, old, new); err != nil {
		return err
	}

	return collector.Collect(key, diffBuffer.Bytes())
}
