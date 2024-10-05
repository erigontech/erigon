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

package raw

import (
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const (
	BlockRootsLength = 8192
	StateRootsLength = 8192
	RandoMixesLength = 65536
	SlashingsLength  = 8192
)

type BeaconState struct {
	// State fields
	genesisTime                uint64
	genesisValidatorsRoot      common.Hash
	slot                       uint64
	fork                       *cltypes.Fork
	latestBlockHeader          *cltypes.BeaconBlockHeader
	blockRoots                 solid.HashVectorSSZ
	stateRoots                 solid.HashVectorSSZ
	historicalRoots            solid.HashListSSZ
	eth1Data                   *cltypes.Eth1Data
	eth1DataVotes              *solid.ListSSZ[*cltypes.Eth1Data]
	eth1DepositIndex           uint64
	validators                 *solid.ValidatorSet
	balances                   solid.Uint64ListSSZ
	randaoMixes                solid.HashVectorSSZ
	slashings                  solid.Uint64VectorSSZ
	previousEpochParticipation *solid.BitList
	currentEpochParticipation  *solid.BitList
	justificationBits          cltypes.JustificationBits
	// Altair
	previousJustifiedCheckpoint *solid.Checkpoint
	currentJustifiedCheckpoint  *solid.Checkpoint
	finalizedCheckpoint         *solid.Checkpoint
	inactivityScores            solid.Uint64ListSSZ
	currentSyncCommittee        *solid.SyncCommittee
	nextSyncCommittee           *solid.SyncCommittee
	// Bellatrix
	latestExecutionPayloadHeader *cltypes.Eth1Header
	// Capella
	nextWithdrawalIndex          uint64
	nextWithdrawalValidatorIndex uint64
	historicalSummaries          *solid.ListSSZ[*cltypes.HistoricalSummary]
	// Phase0: genesis fork. these 2 fields replace participation bits.
	previousEpochAttestations *solid.ListSSZ[*solid.PendingAttestation]
	currentEpochAttestations  *solid.ListSSZ[*solid.PendingAttestation]

	//  leaves for computing hashes
	leaves        []byte          // Pre-computed leaves.
	touchedLeaves []atomic.Uint32 // Maps each leaf to whether they were touched or not.

	// cl version
	version      clparams.StateVersion // State version
	beaconConfig *clparams.BeaconChainConfig
	events       Events

	mu sync.Mutex
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		beaconConfig:                 cfg,
		fork:                         &cltypes.Fork{},
		latestBlockHeader:            &cltypes.BeaconBlockHeader{},
		eth1Data:                     &cltypes.Eth1Data{},
		eth1DataVotes:                solid.NewStaticListSSZ[*cltypes.Eth1Data](int(cfg.EpochsPerEth1VotingPeriod)*int(cfg.SlotsPerEpoch), 72),
		historicalSummaries:          solid.NewStaticListSSZ[*cltypes.HistoricalSummary](int(cfg.HistoricalRootsLimit), 64),
		currentSyncCommittee:         &solid.SyncCommittee{},
		nextSyncCommittee:            &solid.SyncCommittee{},
		latestExecutionPayloadHeader: &cltypes.Eth1Header{},
		//inactivityScores: solid.NewSimpleUint64Slice(int(cfg.ValidatorRegistryLimit)),
		inactivityScores:            solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		balances:                    solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		previousEpochParticipation:  solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		currentEpochParticipation:   solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		slashings:                   solid.NewUint64VectorSSZ(SlashingsLength),
		currentEpochAttestations:    solid.NewDynamicListSSZ[*solid.PendingAttestation](int(cfg.CurrentEpochAttestationsLength())),
		previousEpochAttestations:   solid.NewDynamicListSSZ[*solid.PendingAttestation](int(cfg.PreviousEpochAttestationsLength())),
		historicalRoots:             solid.NewHashList(int(cfg.HistoricalRootsLimit)),
		blockRoots:                  solid.NewHashVector(int(cfg.SlotsPerHistoricalRoot)),
		stateRoots:                  solid.NewHashVector(int(cfg.SlotsPerHistoricalRoot)),
		randaoMixes:                 solid.NewHashVector(int(cfg.EpochsPerHistoricalVector)),
		validators:                  solid.NewValidatorSet(int(cfg.ValidatorRegistryLimit)),
		previousJustifiedCheckpoint: &solid.Checkpoint{},
		currentJustifiedCheckpoint:  &solid.Checkpoint{},
		finalizedCheckpoint:         &solid.Checkpoint{},
		leaves:                      make([]byte, 32*32),
	}
	state.init()
	return state
}

func (b *BeaconState) init() error {
	if b.touchedLeaves == nil {
		b.touchedLeaves = make([]atomic.Uint32, StateLeafSize)
	}
	return nil
}

func (b *BeaconState) MarshalJSON() ([]byte, error) {
	obj := map[string]interface{}{
		"genesis_time":                  strconv.FormatInt(int64(b.genesisTime), 10),
		"genesis_validators_root":       b.genesisValidatorsRoot,
		"slot":                          strconv.FormatInt(int64(b.slot), 10),
		"fork":                          b.fork,
		"latest_block_header":           b.latestBlockHeader,
		"block_roots":                   b.blockRoots,
		"state_roots":                   b.stateRoots,
		"historical_roots":              b.historicalRoots,
		"eth1_data":                     b.eth1Data,
		"eth1_data_votes":               b.eth1DataVotes,
		"eth1_deposit_index":            strconv.FormatInt(int64(b.eth1DepositIndex), 10),
		"validators":                    b.validators,
		"balances":                      b.balances,
		"randao_mixes":                  b.randaoMixes,
		"slashings":                     b.slashings,
		"previous_epoch_participation":  b.previousEpochParticipation,
		"current_epoch_participation":   b.currentEpochParticipation,
		"justification_bits":            b.justificationBits,
		"previous_justified_checkpoint": b.previousJustifiedCheckpoint,
		"current_justified_checkpoint":  b.currentJustifiedCheckpoint,
		"finalized_checkpoint":          b.finalizedCheckpoint,
	}
	if b.version == clparams.Phase0Version {
		obj["previous_epoch_attestations"] = b.previousEpochAttestations
		obj["current_epoch_attestations"] = b.currentEpochAttestations
	}

	if b.version >= clparams.AltairVersion {
		obj["inactivity_scores"] = b.inactivityScores
		obj["current_sync_committee"] = b.currentSyncCommittee
		obj["next_sync_committee"] = b.nextSyncCommittee
	}
	if b.version >= clparams.BellatrixVersion {
		obj["latest_execution_payload_header"] = b.latestExecutionPayloadHeader
	}
	if b.version >= clparams.CapellaVersion {
		obj["next_withdrawal_index"] = strconv.FormatInt(int64(b.nextWithdrawalIndex), 10)
		obj["next_withdrawal_validator_index"] = strconv.FormatInt(int64(b.nextWithdrawalValidatorIndex), 10)
		obj["historical_summaries"] = b.historicalSummaries
	}
	return json.Marshal(obj)
}

// Get validators field
func (b *BeaconState) Validators() *solid.ValidatorSet {
	return b.validators
}

func (b *BeaconState) SetEvents(events Events) {
	b.events = events
}

func (b *BeaconState) HistoricalSummariesLength() uint64 {
	return uint64(b.historicalSummaries.Len())
}

func (b *BeaconState) HistoricalRootsLength() uint64 {
	return uint64(b.historicalRoots.Length())
}

// Dangerous
func (b *BeaconState) RawInactivityScores() []byte {
	return b.inactivityScores.Bytes()
}

func (b *BeaconState) RawBalances() []byte {
	return b.balances.Bytes()
}

func (b *BeaconState) RawValidatorSet() []byte {
	return b.validators.Bytes()
}

func (b *BeaconState) RawPreviousEpochParticipation() []byte {
	return b.previousEpochParticipation.Bytes()
}

func (b *BeaconState) RawCurrentEpochParticipation() []byte {
	return b.currentEpochParticipation.Bytes()
}

func (b *BeaconState) HistoricalRoot(index int) common.Hash {
	return b.historicalRoots.Get(index)
}

func (b *BeaconState) HistoricalSummary(index int) *cltypes.HistoricalSummary {
	return b.historicalSummaries.Get(index)
}

func (b *BeaconState) RawSlashings() []byte {
	return b.slashings.Bytes()
}
