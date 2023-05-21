package raw

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
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

func (b *BeaconState) GenesisValidatorsRoot() libcommon.Hash {
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

func (b *BeaconState) Eth1DepositIndex() uint64 {
	return b.eth1DepositIndex
}

func (b *BeaconState) ValidatorLength() int {
	return b.validators.Length()
}

func (b *BeaconState) AppendValidator(in *cltypes.Validator) {
	b.validators.Append(in)
}

func (b *BeaconState) ForEachValidator(fn func(v *cltypes.Validator, idx int, total int) bool) {
	b.validators.Range(func(index int, value *cltypes.Validator, length int) bool {
		return fn(value, index, length)
	})
}

func (b *BeaconState) ValidatorForValidatorIndex(index int) (*cltypes.Validator, error) {
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
	return b.validators.Get(index).MinCurrentInclusionDelayAttestation, nil
}

func (b *BeaconState) ValidatorMinPreviousInclusionDelayAttestation(index int) (*solid.PendingAttestation, error) {
	if index >= b.validators.Length() {
		return nil, ErrInvalidValidatorIndex
	}
	return b.validators.Get(index).MinPreviousInclusionDelayAttestation, nil
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

func (b *BeaconState) EpochParticipation(currentEpoch bool) *solid.BitList {
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

// more compluicated ones

// GetBlockRootAtSlot returns the block root at a given slot
func (b *BeaconState) GetBlockRootAtSlot(slot uint64) (libcommon.Hash, error) {
	if slot >= b.Slot() {
		return libcommon.Hash{}, ErrGetBlockRootAtSlotFuture
	}
	if b.Slot() > slot+b.BeaconConfig().SlotsPerHistoricalRoot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot too much far behind")
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
