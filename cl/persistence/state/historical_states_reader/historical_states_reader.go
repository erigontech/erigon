package historical_states_reader

import (
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
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
	lock           sync.RWMutex
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

func (r *HistoricalStatesReader) ReadHistoricalState(tx kv.Tx, slot uint64) (*state.CachingBeaconState, error) {
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

	historicalRoots := solid.NewHashList(int(r.cfg.HistoricalRootsLimit))
	if err := state_accessors.ReadHistoricalRootsRange(tx, minimalBeaconState.HistoricalRootsLength, func(idx int, root common.Hash) error {
		historicalRoots.Append(root)
		return nil
	}); err != nil {
		return nil, err
	}
	ret.SetHistoricalRoots(historicalRoots)

	// Eth1
	ret.SetEth1Data(minimalBeaconState.Eth1Data)
	ret.SetEth1DepositIndex(minimalBeaconState.Eth1DepositIndex)
	// Registry

	// Randomness

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
