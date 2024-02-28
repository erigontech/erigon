package historical_states_reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

type HistoricalStatesReader struct {
	cfg            *clparams.BeaconChainConfig
	validatorTable *state_accessors.StaticValidatorTable // We can save 80% of the I/O by caching the validator table
	blockReader    freezeblocks.BeaconSnapshotReader
	genesisState   *state.CachingBeaconState

	// cache for shuffled sets
	shuffledSetsCache *lru.Cache[uint64, []uint64]
}

func NewHistoricalStatesReader(cfg *clparams.BeaconChainConfig, blockReader freezeblocks.BeaconSnapshotReader, validatorTable *state_accessors.StaticValidatorTable, genesisState *state.CachingBeaconState) *HistoricalStatesReader {

	cache, err := lru.New[uint64, []uint64]("shuffledSetsCache_reader", 125)
	if err != nil {
		panic(err)
	}

	return &HistoricalStatesReader{
		cfg:               cfg,
		blockReader:       blockReader,
		genesisState:      genesisState,
		validatorTable:    validatorTable,
		shuffledSetsCache: cache,
	}
}

func (r *HistoricalStatesReader) ReadHistoricalState(ctx context.Context, tx kv.Tx, slot uint64) (*state.CachingBeaconState, error) {
	ret := state.New(r.cfg)
	latestProcessedState, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}

	// If this happens, we need to update our static tables
	if slot > latestProcessedState || slot > r.validatorTable.Slot() {
		return nil, nil
	}

	if slot == r.genesisState.Slot() {
		return r.genesisState.Copy()
	}
	// Read the current block (we need the block header) + other stuff
	block, err := r.blockReader.ReadBlockBySlot(ctx, tx, slot)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	blockHeader := block.SignedBeaconBlockHeader().Header
	blockHeader.Root = common.Hash{}
	// Read the epoch and per-slot data.
	slotData, err := state_accessors.ReadSlotData(tx, slot)
	if err != nil {
		return nil, err
	}
	if slotData == nil {
		return nil, nil
	}
	roundedSlot := r.cfg.RoundSlotToEpoch(slot)

	epochData, err := state_accessors.ReadEpochData(tx, roundedSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to read epoch data: %w", err)
	}
	if epochData == nil {
		return nil, nil
	}

	// Versioning
	ret.SetVersion(slotData.Version)
	ret.SetGenesisTime(r.genesisState.GenesisTime())
	ret.SetGenesisValidatorsRoot(r.genesisState.GenesisValidatorsRoot())
	ret.SetSlot(slot)
	ret.SetFork(slotData.Fork)
	// History
	stateRoots, blockRoots := solid.NewHashVector(int(r.cfg.SlotsPerHistoricalRoot)), solid.NewHashVector(int(r.cfg.SlotsPerHistoricalRoot))
	ret.SetLatestBlockHeader(blockHeader)

	if err := r.readHistoryHashVector(tx, r.genesisState.BlockRoots(), slot, r.cfg.SlotsPerHistoricalRoot, kv.BlockRoot, blockRoots); err != nil {
		return nil, fmt.Errorf("failed to read block roots: %w", err)
	}
	ret.SetBlockRoots(blockRoots)

	if err := r.readHistoryHashVector(tx, r.genesisState.StateRoots(), slot, r.cfg.SlotsPerHistoricalRoot, kv.StateRoot, stateRoots); err != nil {
		return nil, fmt.Errorf("failed to read state roots: %w", err)
	}
	ret.SetStateRoots(stateRoots)

	historicalRoots := solid.NewHashList(int(r.cfg.HistoricalRootsLimit))
	if err := state_accessors.ReadHistoricalRoots(tx, epochData.HistoricalRootsLength, func(idx int, root common.Hash) error {
		historicalRoots.Append(root)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to read historical roots: %w", err)
	}
	ret.SetHistoricalRoots(historicalRoots)

	// Eth1
	eth1DataVotes := solid.NewStaticListSSZ[*cltypes.Eth1Data](int(r.cfg.Eth1DataVotesLength()), 72)
	if err := r.readEth1DataVotes(tx, slotData.Eth1DataLength, slot, eth1DataVotes); err != nil {
		return nil, fmt.Errorf("failed to read eth1 data votes: %w", err)
	}
	ret.SetEth1DataVotes(eth1DataVotes)
	ret.SetEth1Data(slotData.Eth1Data)
	ret.SetEth1DepositIndex(slotData.Eth1DepositIndex)
	// Registry (Validators + Balances)
	balancesBytes, err := r.reconstructBalances(tx, slotData.ValidatorLength, slot, kv.ValidatorBalance, kv.BalancesDump)
	if err != nil {
		return nil, fmt.Errorf("failed to read validator balances: %w", err)
	}
	balances := solid.NewUint64ListSSZ(int(r.cfg.ValidatorRegistryLimit))
	if err := balances.DecodeSSZ(balancesBytes, 0); err != nil {
		return nil, fmt.Errorf("failed to decode validator balances: %w", err)
	}

	ret.SetBalances(balances)

	validatorSet, err := r.ReadValidatorsForHistoricalState(tx, slot)
	if err != nil {
		return nil, fmt.Errorf("failed to read validators: %w", err)
	}
	ret.SetValidators(validatorSet)
	// Randomness
	randaoMixes := solid.NewHashVector(int(r.cfg.EpochsPerHistoricalVector))
	if err := r.readRandaoMixes(tx, slot, randaoMixes); err != nil {
		return nil, fmt.Errorf("failed to read randao mixes: %w", err)
	}
	ret.SetRandaoMixes(randaoMixes)
	slashingsVector := solid.NewUint64VectorSSZ(int(r.cfg.EpochsPerSlashingsVector))
	// Slashings
	err = r.ReconstructUint64ListDump(tx, slot, kv.ValidatorSlashings, int(r.cfg.EpochsPerSlashingsVector), slashingsVector)
	if err != nil {
		return nil, fmt.Errorf("failed to read slashings: %w", err)
	}
	ret.SetSlashings(slashingsVector)

	// Finality
	currentCheckpoint, previousCheckpoint, finalizedCheckpoint, err := state_accessors.ReadCheckpoints(tx, roundedSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoints: %w", err)
	}
	if currentCheckpoint == nil {
		currentCheckpoint = r.genesisState.CurrentJustifiedCheckpoint()
	}
	if previousCheckpoint == nil {
		previousCheckpoint = r.genesisState.PreviousJustifiedCheckpoint()
	}
	if finalizedCheckpoint == nil {
		finalizedCheckpoint = r.genesisState.FinalizedCheckpoint()
	}
	ret.SetJustificationBits(*epochData.JustificationBits)
	ret.SetPreviousJustifiedCheckpoint(previousCheckpoint)
	ret.SetCurrentJustifiedCheckpoint(currentCheckpoint)
	ret.SetFinalizedCheckpoint(finalizedCheckpoint)
	// Participation
	if ret.Version() == clparams.Phase0Version {
		currentAtts, previousAtts, err := r.readPendingEpochs(tx, slot)
		if err != nil {
			return nil, fmt.Errorf("failed to read pending attestations: %w", err)
		}
		ret.SetCurrentEpochAttestations(currentAtts)
		ret.SetPreviousEpochAttestations(previousAtts)
	} else {
		currentIdxs, previousIdxs, err := r.ReadPartecipations(tx, slot)
		if err != nil {
			return nil, fmt.Errorf("failed to read participations: %w", err)
		}
		ret.SetCurrentEpochParticipation(currentIdxs)
		ret.SetPreviousEpochParticipation(previousIdxs)
	}

	if ret.Version() < clparams.AltairVersion {
		return ret, nil
	}
	inactivityScores := solid.NewUint64ListSSZ(int(r.cfg.ValidatorRegistryLimit))
	// Inactivity
	err = r.ReconstructUint64ListDump(tx, slot, kv.InactivityScores, int(slotData.ValidatorLength), inactivityScores)
	if err != nil {
		return nil, fmt.Errorf("failed to read inactivity scores: %w", err)
	}

	ret.SetInactivityScoresRaw(inactivityScores)
	// Sync
	syncCommitteeSlot := r.cfg.RoundSlotToSyncCommitteePeriod(slot)
	currentSyncCommittee, err := state_accessors.ReadCurrentSyncCommittee(tx, syncCommitteeSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to read current sync committee: %w", err)
	}
	if currentSyncCommittee == nil {
		currentSyncCommittee = r.genesisState.CurrentSyncCommittee()
	}

	nextSyncCommittee, err := state_accessors.ReadNextSyncCommittee(tx, syncCommitteeSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to read next sync committee: %w", err)
	}
	if nextSyncCommittee == nil {
		nextSyncCommittee = r.genesisState.NextSyncCommittee()
	}
	ret.SetCurrentSyncCommittee(currentSyncCommittee)
	ret.SetNextSyncCommittee(nextSyncCommittee)
	// Execution
	if ret.Version() < clparams.BellatrixVersion {
		return ret, nil
	}
	payloadHeader, err := block.Block.Body.ExecutionPayload.PayloadHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to read payload header: %w", err)
	}
	ret.SetLatestExecutionPayloadHeader(payloadHeader)
	if ret.Version() < clparams.CapellaVersion {
		return ret, nil
	}

	// Withdrawals
	ret.SetNextWithdrawalIndex(slotData.NextWithdrawalIndex)
	ret.SetNextWithdrawalValidatorIndex(slotData.NextWithdrawalValidatorIndex)
	// Deep history valid from Capella onwards
	historicalSummaries := solid.NewStaticListSSZ[*cltypes.HistoricalSummary](int(r.cfg.HistoricalRootsLimit), 64)
	if err := state_accessors.ReadHistoricalSummaries(tx, epochData.HistoricalSummariesLength, func(idx int, historicalSummary *cltypes.HistoricalSummary) error {
		historicalSummaries.Append(historicalSummary)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to read historical summaries: %w", err)
	}
	ret.SetHistoricalSummaries(historicalSummaries)
	return ret, nil
}

func (r *HistoricalStatesReader) readHistoryHashVector(tx kv.Tx, genesisVector solid.HashVectorSSZ, slot, size uint64, table string, out solid.HashVectorSSZ) (err error) {
	var needFromGenesis, inserted uint64
	if size > slot || slot-size <= r.genesisState.Slot() {
		needFromGenesis = size - (slot - r.genesisState.Slot())
	}

	needFromDB := size - needFromGenesis
	cursor, err := tx.Cursor(table)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var currKeySlot uint64
	for k, v, err := cursor.Seek(base_encoding.Encode64ToBytes4(slot - needFromDB)); err == nil && k != nil; k, v, err = cursor.Next() {
		if len(v) != 32 {
			return fmt.Errorf("invalid key %x", k)
		}
		currKeySlot = base_encoding.Decode64FromBytes4(k)
		out.Set(int(currKeySlot%size), common.BytesToHash(v))
		inserted++
		if inserted == needFromDB {
			break
		}
	}
	for i := 0; i < int(needFromGenesis); i++ {
		currKeySlot++
		out.Set(int(currKeySlot%size), genesisVector.Get(int(currKeySlot%size)))
	}
	return nil
}

func (r *HistoricalStatesReader) readEth1DataVotes(tx kv.Tx, eth1DataVotesLength, slot uint64, out *solid.ListSSZ[*cltypes.Eth1Data]) error {
	initialSlot := r.cfg.RoundSlotToVotePeriod(slot)
	initialKey := base_encoding.Encode64ToBytes4(initialSlot)
	cursor, err := tx.Cursor(kv.Eth1DataVotes)
	if err != nil {
		return err
	}
	defer cursor.Close()
	k, v, err := cursor.Seek(initialKey)
	if err != nil {
		return err
	}
	if initialSlot <= r.genesisState.Slot() {
		// We need to prepend the genesis votes
		for i := 0; i < r.genesisState.Eth1DataVotes().Len(); i++ {
			out.Append(r.genesisState.Eth1DataVotes().Get(i))
		}
	}

	endSlot := r.cfg.RoundSlotToVotePeriod(slot + r.cfg.SlotsPerEpoch*r.cfg.EpochsPerEth1VotingPeriod)

	for k != nil && base_encoding.Decode64FromBytes4(k) < endSlot {
		if out.Len() >= int(eth1DataVotesLength) {
			break
		}
		eth1Data := &cltypes.Eth1Data{}
		if err := eth1Data.DecodeSSZ(v, 0); err != nil {
			return err
		}
		out.Append(eth1Data)
		k, v, err = cursor.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HistoricalStatesReader) readRandaoMixes(tx kv.Tx, slot uint64, out solid.HashVectorSSZ) error {
	size := r.cfg.EpochsPerHistoricalVector
	genesisVector := r.genesisState.RandaoMixes()
	var needFromGenesis, inserted uint64
	roundedSlot := r.cfg.RoundSlotToEpoch(slot)
	epoch := slot / r.cfg.SlotsPerEpoch
	genesisEpoch := r.genesisState.Slot() / r.cfg.SlotsPerEpoch
	if size > epoch || epoch-size <= genesisEpoch {
		needFromGenesis = size - (epoch - genesisEpoch)
	}

	needFromDB := size - needFromGenesis
	cursor, err := tx.Cursor(kv.RandaoMixes)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var currKeyEpoch uint64
	for k, v, err := cursor.Seek(base_encoding.Encode64ToBytes4(roundedSlot - (needFromDB)*r.cfg.SlotsPerEpoch)); err == nil && k != nil; k, v, err = cursor.Next() {
		if len(v) != 32 {
			return fmt.Errorf("invalid key %x", k)
		}
		currKeyEpoch = base_encoding.Decode64FromBytes4(k) / r.cfg.SlotsPerEpoch
		out.Set(int(currKeyEpoch%size), common.BytesToHash(v))
		inserted++
		if inserted == needFromDB {
			break
		}
	}
	for i := 0; i < int(needFromGenesis); i++ {
		currKeyEpoch++
		out.Set(int(currKeyEpoch%size), genesisVector.Get(int(currKeyEpoch%size)))
	}
	// Now we need to read the intra epoch randao mix.
	intraRandaoMix, err := tx.GetOne(kv.IntraRandaoMixes, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return err
	}
	if len(intraRandaoMix) != 32 {
		return fmt.Errorf("invalid intra randao mix length %d", len(intraRandaoMix))
	}
	out.Set(int(epoch%r.cfg.EpochsPerHistoricalVector), common.BytesToHash(intraRandaoMix))
	return nil
}

func (r *HistoricalStatesReader) reconstructDiffedUint64List(tx kv.Tx, validatorSetLength, slot uint64, diffBucket string, dumpBucket string) ([]byte, error) {
	// Read the file
	remainder := slot % clparams.SlotsPerDump
	freshDumpSlot := slot - remainder

	midpoint := uint64(clparams.SlotsPerDump / 2)
	var compressed []byte
	currentStageProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	forward := remainder <= midpoint || currentStageProgress <= freshDumpSlot+clparams.SlotsPerDump
	if forward {
		compressed, err = tx.GetOne(dumpBucket, base_encoding.Encode64ToBytes4(freshDumpSlot))
		if err != nil {
			return nil, err
		}
	} else {
		compressed, err = tx.GetOne(dumpBucket, base_encoding.Encode64ToBytes4(freshDumpSlot+clparams.SlotsPerDump))
		if err != nil {
			return nil, err
		}
	}
	if len(compressed) == 0 {
		return nil, fmt.Errorf("dump not found for slot %d", freshDumpSlot)
	}

	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	if _, err := buffer.Write(compressed); err != nil {
		return nil, err
	}

	// Read the diff file
	zstdReader, err := zstd.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer zstdReader.Close()

	currentList := make([]byte, validatorSetLength*8)
	if _, err = io.ReadFull(zstdReader, currentList); err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	diffCursor, err := tx.Cursor(diffBucket)
	if err != nil {
		return nil, err
	}
	defer diffCursor.Close()
	if forward {
		for k, v, err := diffCursor.Seek(base_encoding.Encode64ToBytes4(freshDumpSlot)); err == nil && k != nil && base_encoding.Decode64FromBytes4(k) <= slot; k, v, err = diffCursor.Next() {
			if err != nil {
				return nil, err
			}
			if len(k) != 4 {
				return nil, fmt.Errorf("invalid key %x", k)
			}
			currSlot := base_encoding.Decode64FromBytes4(k)
			if currSlot == freshDumpSlot {
				continue
			}
			if currSlot > slot {
				return nil, fmt.Errorf("diff not found for slot %d", slot)
			}
			currentList, err = base_encoding.ApplyCompressedSerializedUint64ListDiff(currentList, currentList, v, false)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for k, v, err := diffCursor.Seek(base_encoding.Encode64ToBytes4(freshDumpSlot + clparams.SlotsPerDump)); err == nil && k != nil && base_encoding.Decode64FromBytes4(k) > slot; k, v, err = diffCursor.Prev() {
			if err != nil {
				return nil, err
			}
			if len(k) != 4 {
				return nil, fmt.Errorf("invalid key %x", k)
			}
			currSlot := base_encoding.Decode64FromBytes4(k)
			if currSlot <= slot || currSlot > freshDumpSlot+clparams.SlotsPerDump {
				continue
			}
			currentList, err = base_encoding.ApplyCompressedSerializedUint64ListDiff(currentList, currentList, v, true)
			if err != nil {
				return nil, err
			}
		}
	}
	currentList = currentList[:validatorSetLength*8]
	return currentList, err
}

func (r *HistoricalStatesReader) reconstructBalances(tx kv.Tx, validatorSetLength, slot uint64, diffBucket, dumpBucket string) ([]byte, error) {
	remainder := slot % clparams.SlotsPerDump
	freshDumpSlot := slot - remainder

	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	var compressed []byte
	currentStageProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	midpoint := uint64(clparams.SlotsPerDump / 2)
	forward := remainder <= midpoint || currentStageProgress <= freshDumpSlot+clparams.SlotsPerDump
	if forward {
		compressed, err = tx.GetOne(dumpBucket, base_encoding.Encode64ToBytes4(freshDumpSlot))
		if err != nil {
			return nil, err
		}
	} else {
		compressed, err = tx.GetOne(dumpBucket, base_encoding.Encode64ToBytes4(freshDumpSlot+clparams.SlotsPerDump))
		if err != nil {
			return nil, err
		}
	}

	if len(compressed) == 0 {
		return nil, fmt.Errorf("dump not found for slot %d", freshDumpSlot)
	}
	if _, err := buffer.Write(compressed); err != nil {
		return nil, err
	}
	zstdReader, err := zstd.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer zstdReader.Close()
	currentList := make([]byte, validatorSetLength*8)
	if _, err = io.ReadFull(zstdReader, currentList); err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	roundedSlot := r.cfg.RoundSlotToEpoch(slot)

	if forward {
		for i := freshDumpSlot; i <= roundedSlot; i += r.cfg.SlotsPerEpoch {
			if i == freshDumpSlot {
				continue
			}
			diff, err := tx.GetOne(diffBucket, base_encoding.Encode64ToBytes4(i))
			if err != nil {
				return nil, err
			}
			if len(diff) == 0 {
				continue
			}
			currentList, err = base_encoding.ApplyCompressedSerializedUint64ListDiff(currentList, currentList, diff, false)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for i := freshDumpSlot + clparams.SlotsPerDump; i > roundedSlot; i -= r.cfg.SlotsPerEpoch {
			diff, err := tx.GetOne(diffBucket, base_encoding.Encode64ToBytes4(i))
			if err != nil {
				return nil, err
			}
			if len(diff) == 0 {
				continue
			}
			currentList, err = base_encoding.ApplyCompressedSerializedUint64ListDiff(currentList, currentList, diff, true)
			if err != nil {
				return nil, err
			}
		}
	}

	diffCursor, err := tx.Cursor(diffBucket)
	if err != nil {
		return nil, err
	}
	defer diffCursor.Close()
	if slot%r.cfg.SlotsPerEpoch == 0 {
		currentList = currentList[:validatorSetLength*8]
		return currentList, nil
	}

	slotDiff, err := tx.GetOne(diffBucket, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return nil, err
	}
	if slotDiff == nil {
		return nil, fmt.Errorf("slot diff not found for slot %d", slot)
	}
	currentList = currentList[:validatorSetLength*8]

	return base_encoding.ApplyCompressedSerializedUint64ListDiff(currentList, currentList, slotDiff, false)
}

func (r *HistoricalStatesReader) ReconstructUint64ListDump(tx kv.Tx, slot uint64, bkt string, size int, out solid.Uint64ListSSZ) error {
	diffCursor, err := tx.Cursor(bkt)
	if err != nil {
		return err
	}
	defer diffCursor.Close()

	k, v, err := diffCursor.Seek(base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return err
	}
	if k == nil {
		return fmt.Errorf("diff not found for slot %d", slot)
	}
	keySlot := base_encoding.Decode64FromBytes4(k)
	if keySlot > slot {
		_, v, err = diffCursor.Prev()
		if err != nil {
			return err
		}
	}
	var b bytes.Buffer
	if _, err := b.Write(v); err != nil {
		return err
	}
	// Read the diff file
	zstdReader, err := zstd.NewReader(&b)
	if err != nil {
		return err
	}
	defer zstdReader.Close()
	currentList := make([]byte, size*8)

	if _, err = io.ReadFull(zstdReader, currentList); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to read dump: %w, len: %d", err, len(v))
	}

	return out.DecodeSSZ(currentList, 0)
}

func (r *HistoricalStatesReader) ReadValidatorsForHistoricalState(tx kv.Tx, slot uint64) (*solid.ValidatorSet, error) {
	// Read the minimal beacon state which have the small fields.
	sd, err := state_accessors.ReadSlotData(tx, slot)
	if err != nil {
		return nil, err
	}
	// State not found
	if sd == nil {
		return nil, nil
	}
	validatorSetLength := sd.ValidatorLength

	out := solid.NewValidatorSetWithLength(int(r.cfg.ValidatorRegistryLimit), int(validatorSetLength))
	// Read the static validator field which are hot in memory (this is > 70% of the whole beacon state)
	r.validatorTable.ForEach(func(validatorIndex uint64, validator *state_accessors.StaticValidator) bool {
		if validatorIndex >= validatorSetLength {
			return false
		}
		validator.ToValidator(out.Get(int(validatorIndex)), slot)
		return true
	})
	// Read the balances

	bytesEffectiveBalances, err := r.reconstructDiffedUint64List(tx, validatorSetLength, slot, kv.ValidatorEffectiveBalance, kv.EffectiveBalancesDump)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(validatorSetLength); i++ {
		out.Get(i).
			SetEffectiveBalanceFromBytes(bytesEffectiveBalances[(i * 8) : (i*8)+8])
	}
	return out, nil
}

func (r *HistoricalStatesReader) readPendingEpochs(tx kv.Tx, slot uint64) (*solid.ListSSZ[*solid.PendingAttestation], *solid.ListSSZ[*solid.PendingAttestation], error) {
	if slot == r.cfg.GenesisSlot {
		return r.genesisState.CurrentEpochAttestations(), r.genesisState.PreviousEpochAttestations(), nil
	}
	epoch, prevEpoch := r.computeRelevantEpochs(slot)
	previousEpochAttestations := solid.NewDynamicListSSZ[*solid.PendingAttestation](int(r.cfg.PreviousEpochAttestationsLength()))
	currentEpochAttestations := solid.NewDynamicListSSZ[*solid.PendingAttestation](int(r.cfg.CurrentEpochAttestationsLength()))
	beginSlot := prevEpoch * r.cfg.SlotsPerEpoch

	for i := beginSlot; i <= slot; i++ {
		// Read the block
		block, err := r.blockReader.ReadBlindedBlockBySlot(context.Background(), tx, i)
		if err != nil {
			return nil, nil, err
		}
		if block == nil {
			continue
		}
		currentEpoch := i / r.cfg.SlotsPerEpoch
		isPreviousPendingAttestations := currentEpoch == prevEpoch

		// Read the participation flags
		block.Block.Body.Attestations.Range(func(index int, attestation *solid.Attestation, length int) bool {
			data := attestation.AttestantionData()
			isCurrentEpoch := data.Target().Epoch() == currentEpoch
			// skip if it is too far behind
			if !isCurrentEpoch && isPreviousPendingAttestations {
				return true
			}
			pendingAttestation := solid.NewPendingAttestionFromParameters(
				attestation.AggregationBits(),
				data,
				i-data.Slot(),
				block.Block.ProposerIndex,
			)

			if data.Target().Epoch() == epoch {
				currentEpochAttestations.Append(pendingAttestation)
			} else {
				previousEpochAttestations.Append(pendingAttestation)
			}
			return true
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return currentEpochAttestations, previousEpochAttestations, nil
}

// readParticipations shuffles active indicies and returns the participation flags for the given epoch.
func (r *HistoricalStatesReader) ReadPartecipations(tx kv.Tx, slot uint64) (*solid.BitList, *solid.BitList, error) {
	var beginSlot uint64
	epoch, prevEpoch := r.computeRelevantEpochs(slot)
	beginSlot = prevEpoch * r.cfg.SlotsPerEpoch

	currentActiveIndicies, err := state_accessors.ReadActiveIndicies(tx, epoch*r.cfg.SlotsPerEpoch)
	if err != nil {
		return nil, nil, err
	}
	var previousActiveIndicies []uint64
	if epoch == 0 {
		previousActiveIndicies = currentActiveIndicies
	} else {
		previousActiveIndicies, err = state_accessors.ReadActiveIndicies(tx, (epoch-1)*r.cfg.SlotsPerEpoch)
		if err != nil {
			return nil, nil, err
		}
	}

	// Read the minimal beacon state which have the small fields.
	sd, err := state_accessors.ReadSlotData(tx, slot)
	if err != nil {
		return nil, nil, err
	}
	// State not found
	if sd == nil {
		return nil, nil, nil
	}
	validatorLength := sd.ValidatorLength

	currentIdxs := solid.NewBitList(int(validatorLength), int(r.cfg.ValidatorRegistryLimit))
	previousIdxs := solid.NewBitList(int(validatorLength), int(r.cfg.ValidatorRegistryLimit))
	if err != nil {
		return nil, nil, err
	}
	// trigger the cache for shuffled sets in parallel
	if err := r.tryCachingEpochsInParallell(tx, [][]uint64{currentActiveIndicies, previousActiveIndicies}, []uint64{epoch, prevEpoch}); err != nil {
		return nil, nil, err
	}
	// Read the previous idxs
	for i := beginSlot; i <= slot; i++ {
		// Read the block
		block, err := r.blockReader.ReadBlindedBlockBySlot(context.Background(), tx, i)
		if err != nil {
			return nil, nil, err
		}
		if block == nil {
			continue
		}
		currentEpoch := i / r.cfg.SlotsPerEpoch

		// Read the participation flags
		block.Block.Body.Attestations.Range(func(index int, attestation *solid.Attestation, length int) bool {
			data := attestation.AttestantionData()
			isCurrentEpoch := data.Target().Epoch() == currentEpoch
			var activeIndicies []uint64
			// This looks horrible
			if isCurrentEpoch {
				if currentEpoch == prevEpoch {
					activeIndicies = previousActiveIndicies
				} else {
					activeIndicies = currentActiveIndicies
				}
			} else {
				if currentEpoch == prevEpoch {
					return true
				}
				activeIndicies = previousActiveIndicies
			}

			attestationEpoch := data.Slot() / r.cfg.SlotsPerEpoch

			mixPosition := (attestationEpoch + r.cfg.EpochsPerHistoricalVector - r.cfg.MinSeedLookahead - 1) % r.cfg.EpochsPerHistoricalVector
			mix, err := r.ReadRandaoMixBySlotAndIndex(tx, data.Slot(), mixPosition)
			if err != nil {
				return false
			}

			var attestingIndicies []uint64
			attestingIndicies, err = r.attestingIndicies(data, attestation.AggregationBits(), true, mix, activeIndicies)
			if err != nil {
				return false
			}
			var participationFlagsIndicies []uint8
			participationFlagsIndicies, err = r.getAttestationParticipationFlagIndicies(tx, block.Version(), i, data, i-data.Slot(), true)
			if err != nil {
				return false
			}
			prevIdxs := isCurrentEpoch && currentEpoch != prevEpoch
			// apply the flags
			for _, idx := range attestingIndicies {
				for _, flagIndex := range participationFlagsIndicies {
					var flagParticipation cltypes.ParticipationFlags
					if prevIdxs {
						flagParticipation = cltypes.ParticipationFlags(currentIdxs.Get(int(idx)))
					} else {
						flagParticipation = cltypes.ParticipationFlags(previousIdxs.Get(int(idx)))
					}
					if flagParticipation.HasFlag(int(flagIndex)) {
						continue
					}
					if prevIdxs {
						currentIdxs.Set(int(idx), byte(flagParticipation.Add(int(flagIndex))))
					} else {
						previousIdxs.Set(int(idx), byte(flagParticipation.Add(int(flagIndex))))
					}
				}
			}
			return true
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return currentIdxs, previousIdxs, nil
}

func (r *HistoricalStatesReader) computeRelevantEpochs(slot uint64) (uint64, uint64) {
	epoch := slot / r.cfg.SlotsPerEpoch
	if epoch == r.cfg.GenesisEpoch {
		return epoch, epoch
	}
	return epoch, epoch - 1
}

func (r *HistoricalStatesReader) tryCachingEpochsInParallell(tx kv.Tx, activeIdxs [][]uint64, epochs []uint64) error {
	var wg sync.WaitGroup
	wg.Add(len(epochs))
	for i, epoch := range epochs {
		mixPosition := (epoch + r.cfg.EpochsPerHistoricalVector - r.cfg.MinSeedLookahead - 1) % r.cfg.EpochsPerHistoricalVector
		mix, err := r.ReadRandaoMixBySlotAndIndex(tx, epochs[0]*r.cfg.SlotsPerEpoch, mixPosition)
		if err != nil {
			return err
		}

		go func(mix libcommon.Hash, epoch uint64, idxs []uint64) {
			defer wg.Done()

			_, _ = r.ComputeCommittee(mix, idxs, epoch*r.cfg.SlotsPerEpoch, r.cfg.TargetCommitteeSize, 0)
		}(mix, epoch, activeIdxs[i])
	}
	wg.Wait()
	return nil
}

func (r *HistoricalStatesReader) ReadValidatorsBalances(tx kv.Tx, slot uint64) (solid.Uint64ListSSZ, error) {
	sd, err := state_accessors.ReadSlotData(tx, slot)
	if err != nil {
		return nil, err
	}
	// State not found
	if sd == nil {
		return nil, nil
	}

	balances, err := r.reconstructBalances(tx, sd.ValidatorLength, slot, kv.ValidatorBalance, kv.BalancesDump)
	if err != nil {
		return nil, err
	}
	balancesList := solid.NewUint64ListSSZ(int(r.cfg.ValidatorRegistryLimit))

	return balancesList, balancesList.DecodeSSZ(balances, 0)
}

func (r *HistoricalStatesReader) ReadRandaoMixBySlotAndIndex(tx kv.Tx, slot, index uint64) (libcommon.Hash, error) {
	epoch := slot / r.cfg.SlotsPerEpoch
	epochSubIndex := epoch % r.cfg.EpochsPerHistoricalVector
	if index == epochSubIndex {
		intraRandaoMix, err := tx.GetOne(kv.IntraRandaoMixes, base_encoding.Encode64ToBytes4(slot))
		if err != nil {
			return libcommon.Hash{}, err
		}
		if len(intraRandaoMix) != 32 {
			return libcommon.Hash{}, fmt.Errorf("invalid intra randao mix length %d", len(intraRandaoMix))
		}
		return libcommon.BytesToHash(intraRandaoMix), nil
	}
	needFromGenesis := true
	var epochLookup uint64
	if index <= epochSubIndex {
		if epoch > (epochSubIndex - index) {
			needFromGenesis = false
			epochLookup = epoch - (epochSubIndex - index)
		}
	} else {
		if epoch > (epochSubIndex + (r.cfg.EpochsPerHistoricalVector - index)) {
			needFromGenesis = false
			epochLookup = epoch - (epochSubIndex + (r.cfg.EpochsPerHistoricalVector - index))
		}
	}
	if epochLookup < r.genesisState.Slot()/r.cfg.SlotsPerEpoch {
		needFromGenesis = true
	}

	if needFromGenesis {
		return r.genesisState.GetRandaoMixes(epoch), nil
	}
	mixBytes, err := tx.GetOne(kv.RandaoMixes, base_encoding.Encode64ToBytes4(epochLookup*r.cfg.SlotsPerEpoch))
	if err != nil {
		return libcommon.Hash{}, err
	}
	if len(mixBytes) != 32 {
		return libcommon.Hash{}, fmt.Errorf("invalid mix length %d", len(mixBytes))
	}
	return libcommon.BytesToHash(mixBytes), nil
}
