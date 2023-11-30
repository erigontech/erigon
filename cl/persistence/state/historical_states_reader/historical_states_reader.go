package historical_states_reader

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/spf13/afero"
)

type HistoricalStatesReader struct {
	cfg            *clparams.BeaconChainConfig
	genesisCfg     *clparams.GenesisConfig
	fs             afero.Fs                              // some data is on filesystem to avoid database fragmentation
	validatorTable *state_accessors.StaticValidatorTable // We can save 80% of the I/O by caching the validator table
	blockReader    freezeblocks.BeaconSnapshotReader
	genesisState   *state.CachingBeaconState
}

// class BeaconState(Container):
//     # Versioning
//     genesis_time: uint64
//     genesis_validators_root: Root
//     slot: Slot
//     fork: Fork
//     # History
//     latest_block_header: BeaconBlockHeader
//     block_roots: Vector[Root, SLOTS_PER_HISTORICAL_ROOT]
//     state_roots: Vector[Root, SLOTS_PER_HISTORICAL_ROOT]
//     historical_roots: List[Root, HISTORICAL_ROOTS_LIMIT]  # Frozen in Capella, replaced by historical_summaries
//     # Eth1
//     eth1_data: Eth1Data
//     eth1_data_votes: List[Eth1Data, EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH]
//     eth1_deposit_index: uint64
//     # Registry
//     validators: List[Validator, VALIDATOR_REGISTRY_LIMIT]
//     balances: List[Gwei, VALIDATOR_REGISTRY_LIMIT]
//     # Randomness
//     randao_mixes: Vector[Bytes32, EPOCHS_PER_HISTORICAL_VECTOR]
//     # Slashings
//     slashings: Vector[Gwei, EPOCHS_PER_SLASHINGS_VECTOR]  # Per-epoch sums of slashed effective balances
//     # Participation
//     previous_epoch_participation: List[ParticipationFlags, VALIDATOR_REGISTRY_LIMIT]
//     current_epoch_participation: List[ParticipationFlags, VALIDATOR_REGISTRY_LIMIT]
//     # Finality
//     justification_bits: Bitvector[JUSTIFICATION_BITS_LENGTH]  # Bit set for every recent justified epoch
//     previous_justified_checkpoint: Checkpoint
//     current_justified_checkpoint: Checkpoint
//     finalized_checkpoint: Checkpoint
//     # Inactivity
//     inactivity_scores: List[uint64, VALIDATOR_REGISTRY_LIMIT]
//     # Sync
//     current_sync_committee: SyncCommittee
//     next_sync_committee: SyncCommittee
//     # Execution
//     latest_execution_payload_header: ExecutionPayloadHeader  # [Modified in Capella]
//     # Withdrawals
//     next_withdrawal_index: WithdrawalIndex  # [New in Capella]
//     next_withdrawal_validator_index: ValidatorIndex  # [New in Capella]
//     # Deep history valid from Capella onwards
//     historical_summaries: List[HistoricalSummary, HISTORICAL_ROOTS_LIMIT]  # [New in Capella]

func NewHistoricalStatesReader(cfg *clparams.BeaconChainConfig, blockReader freezeblocks.BeaconSnapshotReader, validatorTable *state_accessors.StaticValidatorTable, fs afero.Fs, genesisState *state.CachingBeaconState) *HistoricalStatesReader {
	return &HistoricalStatesReader{
		cfg:            cfg,
		fs:             fs,
		blockReader:    blockReader,
		genesisState:   genesisState,
		validatorTable: validatorTable,
	}
}

// previousVersion returns the previous version of the state.
func previousVersion(v clparams.StateVersion) clparams.StateVersion {
	if v == clparams.Phase0Version {
		return clparams.Phase0Version
	}
	return v - 1
}

func (r *HistoricalStatesReader) ReadHistoricalState(ctx context.Context, tx kv.Tx, slot uint64) (*state.CachingBeaconState, error) {
	ret := state.New(r.cfg)

	latestProcessedState, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}

	// If this happens, we need to update our static tables
	if slot > latestProcessedState || slot > r.validatorTable.Slot() {
		return nil, fmt.Errorf("slot %d is greater than latest processed state %d", slot, latestProcessedState)
	}

	if slot == 0 {
		return r.genesisState.Copy()
	}
	// Read the current block (we need the block header) + other stuff
	block, err := r.blockReader.ReadBlockBySlot(ctx, tx, slot)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block at slot %d not found", slot)
	}
	blockHeader := block.SignedBeaconBlockHeader().Header
	blockHeader.Root = common.Hash{}
	// Read the minimal beacon state which have the small fields.
	minimalBeaconState, err := state_accessors.ReadMinimalBeaconState(tx, slot)
	if err != nil {
		return nil, err
	}
	// State not found
	if minimalBeaconState == nil {
		return nil, nil
	}

	// Versioning
	ret.SetVersion(minimalBeaconState.Version)
	ret.SetGenesisTime(r.genesisCfg.GenesisTime)
	ret.SetGenesisValidatorsRoot(r.genesisCfg.GenesisValidatorRoot)
	ret.SetSlot(slot)
	epoch := state.Epoch(ret)
	stateVersion := r.cfg.GetCurrentStateVersion(epoch)
	ret.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(r.cfg.GetForkVersionByVersion(previousVersion(stateVersion))),
		CurrentVersion:  utils.Uint32ToBytes4(r.cfg.GetForkVersionByVersion(stateVersion)),
		Epoch:           r.cfg.GetForkEpochByVersion(stateVersion),
	})
	// History
	stateRoots, blockRoots := solid.NewHashList(int(r.cfg.SlotsPerHistoricalRoot)), solid.NewHashList(int(r.cfg.SlotsPerHistoricalRoot))
	ret.SetLatestBlockHeader(blockHeader)

	if err := r.readHistoryHashVector(tx, r.genesisState.BlockRoots(), slot, r.cfg.SlotsPerHistoricalRoot, kv.BlockRoot, blockRoots); err != nil {
		return nil, err
	}
	ret.SetBlockRoots(blockRoots)

	if err := r.readHistoryHashVector(tx, r.genesisState.StateRoots(), slot, r.cfg.SlotsPerHistoricalRoot, kv.StateRoot, stateRoots); err != nil {
		return nil, err
	}
	ret.SetStateRoots(stateRoots)

	historicalRoots := solid.NewHashList(int(r.cfg.HistoricalRootsLimit))
	if err := state_accessors.ReadHistoricalRoots(tx, minimalBeaconState.HistoricalRootsLength, func(idx int, root common.Hash) error {
		historicalRoots.Append(root)
		return nil
	}); err != nil {
		return nil, err
	}
	ret.SetHistoricalRoots(historicalRoots)

	// Eth1
	eth1DataVotes := solid.NewDynamicListSSZ[*cltypes.Eth1Data](int(r.cfg.Eth1DataVotesLength()))
	if err := r.readEth1DataVotes(tx, slot, eth1DataVotes); err != nil {
		return nil, err
	}
	ret.SetEth1DataVotes(eth1DataVotes)
	ret.SetEth1Data(minimalBeaconState.Eth1Data)
	ret.SetEth1DepositIndex(minimalBeaconState.Eth1DepositIndex)
	// Registry

	// Randomness
	randaoMixes := solid.NewHashList(int(r.cfg.EpochsPerHistoricalVector))
	if err := r.readRandaoMixes(tx, slot, randaoMixes); err != nil {
		return nil, err
	}
	ret.SetRandaoMixes(randaoMixes)
	// Slashings

	// Participation

	// Finality
	currentCheckpoint, previousCheckpoint, finalizedCheckpoint, err := state_accessors.ReadCheckpoints(tx, r.cfg.RoundSlotToEpoch(slot))
	if err != nil {
		return nil, err
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
	ret.SetJustificationBits(*minimalBeaconState.JustificationBits)
	ret.SetPreviousJustifiedCheckpoint(previousCheckpoint)
	ret.SetCurrentJustifiedCheckpoint(currentCheckpoint)
	ret.SetFinalizedCheckpoint(finalizedCheckpoint)
	// Inactivity

	// Sync
	syncCommitteeSlot := r.cfg.RoundSlotToSyncCommitteePeriod(slot)
	currentSyncCommittee, err := state_accessors.ReadCurrentSyncCommittee(tx, syncCommitteeSlot)
	if err != nil {
		return nil, err
	}
	if currentSyncCommittee == nil {
		currentSyncCommittee = r.genesisState.CurrentSyncCommittee()
	}

	nextSyncCommittee, err := state_accessors.ReadNextSyncCommittee(tx, syncCommitteeSlot)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	ret.SetLatestExecutionPayloadHeader(payloadHeader)
	if ret.Version() < clparams.CapellaVersion {
		return ret, nil
	}
	// Withdrawals
	ret.SetNextWithdrawalIndex(minimalBeaconState.NextWithdrawalIndex)
	ret.SetNextWithdrawalValidatorIndex(minimalBeaconState.NextWithdrawalValidatorIndex)
	historicalSummaries := solid.NewDynamicListSSZ[*cltypes.HistoricalSummary](int(r.cfg.HistoricalRootsLimit))
	if err := state_accessors.ReadHistoricalSummaries(tx, minimalBeaconState.HistoricalSummariesLength, func(idx int, historicalSummary *cltypes.HistoricalSummary) error {
		historicalSummaries.Append(historicalSummary)
		return nil
	}); err != nil {
		return nil, err
	}
	ret.SetHistoricalSummaries(historicalSummaries)

	return ret, nil
}

func (r *HistoricalStatesReader) readHistoryHashVector(tx kv.Tx, genesisVector solid.HashVectorSSZ, slot, size uint64, table string, out solid.HashVectorSSZ) (err error) {
	var needFromGenesis, inserted uint64
	if uint64(size) > slot {
		needFromGenesis = size - slot
	}
	needFromDB := size - needFromGenesis
	cursor, err := tx.Cursor(table)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var currKeySlot uint64
	for k, v, err := cursor.Seek(base_encoding.Encode64ToBytes4(slot - needFromDB)); err == nil && k != nil; k, v, err = cursor.Prev() {
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

func (r *HistoricalStatesReader) readEth1DataVotes(tx kv.Tx, slot uint64, out *solid.ListSSZ[*cltypes.Eth1Data]) error {
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
	for initialSlot > base_encoding.Decode64FromBytes4(k) {
		k, v, err = cursor.Next()
		if err != nil {
			return err
		}
		if k == nil {
			return fmt.Errorf("eth1 data votes not found for slot %d", slot)
		}
	}
	endSlot := r.cfg.RoundSlotToVotePeriod(slot + r.cfg.SlotsPerEpoch*r.cfg.EpochsPerEth1VotingPeriod)
	for k != nil && base_encoding.Decode64FromBytes4(k) < endSlot {
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
	slotLookBehind := r.cfg.SlotsPerEpoch * r.cfg.EpochsPerHistoricalVector // how much we must look behind
	slotRoundedToEpoch := r.cfg.RoundSlotToEpoch(slot)
	var initialSlot uint64
	if slotRoundedToEpoch > slotLookBehind {
		initialSlot = slotRoundedToEpoch - slotLookBehind
	}
	initialKey := base_encoding.Encode64ToBytes4(initialSlot)
	cursor, err := tx.Cursor(kv.RandaoMixes)
	if err != nil {
		return err
	}
	defer cursor.Close()
	inserted := 0
	var currEpoch uint64
	for k, v, err := cursor.Seek(initialKey); err == nil && k != nil && slotRoundedToEpoch > base_encoding.Decode64FromBytes4(k); k, v, err = cursor.Next() {
		if len(v) != 32 {
			return fmt.Errorf("invalid key %x", k)
		}
		currKeySlot := base_encoding.Decode64FromBytes4(k)
		currEpoch = r.cfg.RoundSlotToEpoch(currKeySlot)
		inserted++
		out.Set(int(currEpoch%r.cfg.EpochsPerHistoricalVector), common.BytesToHash(v))
	}
	// If we have not enough data, we need to read from genesis
	for i := inserted; i < int(r.cfg.EpochsPerHistoricalVector); i++ {
		currEpoch++
		out.Set(int(currEpoch%r.cfg.EpochsPerHistoricalVector), r.genesisState.RandaoMixes().Get(int(currEpoch%r.cfg.EpochsPerHistoricalVector)))
	}
	// Now we need to read the intra epoch randao mix.
	intraRandaoMix, err := tx.GetOne(kv.IntraRandaoMixes, base_encoding.Encode64ToBytes4(slot))
	if err != nil {
		return err
	}
	if len(intraRandaoMix) != 32 {
		return fmt.Errorf("invalid intra randao mix length %d", len(intraRandaoMix))
	}
	out.Set(int(slot%r.cfg.EpochsPerHistoricalVector), common.BytesToHash(intraRandaoMix))
	return nil

}
