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
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
)

var (
	ErrGetBlockRootAtSlotFuture = errors.New("GetBlockRootAtSlot: slot in the future")
)

// Just a bunch of simple getters.

func (b *BeaconState) BeaconConfig() *clparams.BeaconChainConfig {
	return b.beaconConfig
}

func (b *BeaconState) Version() clparams.StateVersion {
	return b.version
}

func (b *BeaconState) GenesisTime() uint64 {
	return b.genesisTime
}

func (b *BeaconState) GenesisValidatorsRoot() common.Hash {
	return b.genesisValidatorsRoot
}

func (b *BeaconState) Slot() uint64 {
	return b.slot
}

func (b *BeaconState) PreviousSlot() uint64 {
	if b.slot == 0 {
		return 0
	}
	return b.slot - 1
}

func (b *BeaconState) Fork() *cltypes.Fork {
	return b.fork
}

func (b *BeaconState) LatestBlockHeader() cltypes.BeaconBlockHeader {
	return *b.latestBlockHeader
}

func (b *BeaconState) BlockRoots() solid.HashVectorSSZ {
	return b.blockRoots
}

func (b *BeaconState) StateRoots() solid.HashVectorSSZ {
	return b.stateRoots
}

func (b *BeaconState) Eth1Data() *cltypes.Eth1Data {
	return b.eth1Data
}

func (b *BeaconState) Eth1DataVotes() *solid.ListSSZ[*cltypes.Eth1Data] {
	return b.eth1DataVotes
}

func (b *BeaconState) Slashings() solid.Uint64VectorSSZ {
	return b.slashings
}

func (b *BeaconState) Balances() solid.Uint64ListSSZ {
	return b.balances
}

func (b *BeaconState) InactivityScores() solid.Uint64ListSSZ {
	return b.inactivityScores
}

func (b *BeaconState) Eth1DepositIndex() uint64 {
	return b.eth1DepositIndex
}

func (b *BeaconState) ValidatorSet() *solid.ValidatorSet {
	return b.validators
}

func (b *BeaconState) PreviousEpochParticipation() *solid.ParticipationBitList {
	return b.previousEpochParticipation
}

func (b *BeaconState) CurrentEpochParticipation() *solid.ParticipationBitList {
	return b.currentEpochParticipation
}

func (b *BeaconState) ValidatorLength() int {
	return b.validators.Length()
}

func (b *BeaconState) AppendValidator(in solid.Validator) {
	b.validators.Append(in)
}

func (b *BeaconState) ForEachValidator(fn func(v solid.Validator, idx int, total int) bool) {
	b.validators.Range(func(index int, value solid.Validator, length int) bool {
		return fn(value, index, length)
	})
}

func (b *BeaconState) ValidatorForValidatorIndex(index int) (solid.Validator, error) {
	if index >= b.validators.Length() {
		return nil, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index), nil
}

func (b *BeaconState) ForEachBalance(fn func(v uint64, idx int, total int) bool) {
	b.balances.Range(func(index int, value uint64, length int) bool {
		return fn(value, index, length)
	})
}

func (b *BeaconState) ValidatorBalance(index int) (uint64, error) {
	if index >= b.balances.Length() {
		return 0, ErrInvalidValidatorIndex
	}
	return b.balances.Get(index), nil
}

func (b *BeaconState) ValidatorPublicKey(index int) (common.Bytes48, error) {
	if index >= b.validators.Length() {
		return common.Bytes48{}, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index).PublicKey(), nil
}

func (b *BeaconState) ValidatorExitEpoch(index int) (uint64, error) {
	if index >= b.validators.Length() {
		return 0, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index).ExitEpoch(), nil
}

func (b *BeaconState) ValidatorWithdrawableEpoch(index int) (uint64, error) {
	if index >= b.validators.Length() {
		return 0, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index).WithdrawableEpoch(), nil
}

func (b *BeaconState) ValidatorEffectiveBalance(index int) (uint64, error) {
	if index >= b.validators.Length() {
		return 0, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index).EffectiveBalance(), nil
}

func (b *BeaconState) ValidatorMinCurrentInclusionDelayAttestation(index int) (*solid.PendingAttestation, error) {
	if index >= b.validators.Length() {
		return nil, ErrInvalidValidatorIndex
	}
	return b.validators.MinCurrentInclusionDelayAttestation(index), nil
}

func (b *BeaconState) ValidatorMinPreviousInclusionDelayAttestation(index int) (*solid.PendingAttestation, error) {
	if index >= b.validators.Length() {
		return nil, ErrInvalidValidatorIndex
	}
	return b.validators.MinPreviousInclusionDelayAttestation(index), nil
}

func (b *BeaconState) ValidatorIsCurrentMatchingSourceAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsCurrentMatchingSourceAttester(idx), nil
}

func (b *BeaconState) ValidatorIsCurrentMatchingTargetAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsCurrentMatchingTargetAttester(idx), nil
}

func (b *BeaconState) ValidatorIsCurrentMatchingHeadAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsCurrentMatchingHeadAttester(idx), nil
}

func (b *BeaconState) ValidatorIsPreviousMatchingSourceAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsPreviousMatchingSourceAttester(idx), nil
}

func (b *BeaconState) ValidatorIsPreviousMatchingTargetAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsPreviousMatchingTargetAttester(idx), nil
}

func (b *BeaconState) ValidatorIsPreviousMatchingHeadAttester(idx int) (bool, error) {
	if idx >= b.validators.Length() {
		return false, ErrInvalidValidatorIndex
	}
	return b.validators.IsPreviousMatchingHeadAttester(idx), nil
}

func (b *BeaconState) RandaoMixes() solid.HashVectorSSZ {
	return b.randaoMixes
}

func (b *BeaconState) GetRandaoMixes(epoch uint64) [32]byte {
	return b.randaoMixes.Get(int(epoch % b.beaconConfig.EpochsPerHistoricalVector))
}

func (b *BeaconState) GetRandaoMix(index int) [32]byte {
	return b.randaoMixes.Get(index)
}

func (b *BeaconState) ForEachSlashingSegment(fn func(idx int, v uint64, total int) bool) {
	b.slashings.Range(fn)
}

func (b *BeaconState) SlashingSegmentAt(pos int) uint64 {
	return b.slashings.Get(pos)
}

func (b *BeaconState) EpochParticipation(currentEpoch bool) *solid.ParticipationBitList {
	if currentEpoch {
		return b.currentEpochParticipation
	}
	return b.previousEpochParticipation
}

func (b *BeaconState) JustificationBits() cltypes.JustificationBits {
	return b.justificationBits
}

func (b *BeaconState) EpochParticipationForValidatorIndex(isCurrentEpoch bool, index int) cltypes.ParticipationFlags {
	if isCurrentEpoch {
		return cltypes.ParticipationFlags(b.currentEpochParticipation.Get(index))
	}
	return cltypes.ParticipationFlags(b.previousEpochParticipation.Get(index))
}

func (b *BeaconState) PreviousJustifiedCheckpoint() solid.Checkpoint {
	return b.previousJustifiedCheckpoint
}

func (b *BeaconState) CurrentJustifiedCheckpoint() solid.Checkpoint {
	return b.currentJustifiedCheckpoint
}

func (b *BeaconState) ValidatorInactivityScore(index int) (uint64, error) {
	if b.inactivityScores.Length() <= index {
		return 0, ErrInvalidValidatorIndex
	}
	return b.inactivityScores.Get(index), nil
}

func (b *BeaconState) FinalizedCheckpoint() solid.Checkpoint {
	return b.finalizedCheckpoint
}

func (b *BeaconState) CurrentSyncCommittee() *solid.SyncCommittee {
	return b.currentSyncCommittee
}

func (b *BeaconState) NextSyncCommittee() *solid.SyncCommittee {
	return b.nextSyncCommittee
}

func (b *BeaconState) LatestExecutionPayloadHeader() *cltypes.Eth1Header {
	return b.latestExecutionPayloadHeader
}

func (b *BeaconState) NextWithdrawalIndex() uint64 {
	return b.nextWithdrawalIndex
}

func (b *BeaconState) CurrentEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation] {
	return b.currentEpochAttestations
}

func (b *BeaconState) CurrentEpochAttestationsLength() int {
	return b.currentEpochAttestations.Len()
}

func (b *BeaconState) PreviousEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation] {
	return b.previousEpochAttestations
}

func (b *BeaconState) PreviousEpochAttestationsLength() int {
	return b.previousEpochAttestations.Len()
}

func (b *BeaconState) NextWithdrawalValidatorIndex() uint64 {
	return b.nextWithdrawalValidatorIndex
}

func (b *BeaconState) DepositRequestsStartIndex() uint64 {
	return b.depositRequestsStartIndex
}

func (b *BeaconState) DepositBalanceToConsume() uint64 {
	return b.depositBalanceToConsume
}

func (b *BeaconState) ConsolidationBalanceToConsume() uint64 {
	return b.consolidationBalanceToConsume
}

func (b *BeaconState) EarliestConsolidationEpoch() uint64 {
	return b.earliestConsolidationEpoch
}

func (b *BeaconState) PendingDeposits() *solid.ListSSZ[*solid.PendingDeposit] {
	return b.pendingDeposits
}

func (b *BeaconState) PendingPartialWithdrawals() *solid.ListSSZ[*solid.PendingPartialWithdrawal] {
	return b.pendingPartialWithdrawals
}

func (b *BeaconState) PendingConsolidations() *solid.ListSSZ[*solid.PendingConsolidation] {
	return b.pendingConsolidations
}

func (b *BeaconState) ProposerLookahead() solid.Uint64VectorSSZ {
	return b.proposerLookahead
}

// more compluicated ones

// GetBlockRootAtSlot returns the block root at a given slot
func (b *BeaconState) GetBlockRootAtSlot(slot uint64) (common.Hash, error) {
	if slot >= b.Slot() {
		return common.Hash{}, ErrGetBlockRootAtSlotFuture
	}
	if b.Slot() > slot+b.BeaconConfig().SlotsPerHistoricalRoot {
		return common.Hash{}, errors.New("GetBlockRootAtSlot: slot too much far behind")
	}
	return b.blockRoots.Get(int(slot % b.BeaconConfig().SlotsPerHistoricalRoot)), nil
}

// GetDomain
func (b *BeaconState) GetDomain(domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch < b.fork.Epoch {
		return fork.ComputeDomain(domainType[:], b.fork.PreviousVersion, b.genesisValidatorsRoot)
	}
	return fork.ComputeDomain(domainType[:], b.fork.CurrentVersion, b.genesisValidatorsRoot)
}

func (b *BeaconState) DebugPrint(prefix string) {
	fmt.Printf("%s: %x\n", prefix, b.currentEpochParticipation)
}

func (b *BeaconState) GetPendingPartialWithdrawals() *solid.ListSSZ[*solid.PendingPartialWithdrawal] {
	return b.pendingPartialWithdrawals
}
