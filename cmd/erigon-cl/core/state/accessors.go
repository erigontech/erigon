package state

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// GetActiveValidatorsIndices returns the list of validator indices active for the given epoch.
func (b *BeaconState) GetActiveValidatorsIndices(epoch uint64) (indicies []uint64) {
	for i, validator := range b.validators {
		if !validator.Active(epoch) {
			continue
		}
		indicies = append(indicies, uint64(i))
	}
	return
}

// Epoch returns current epoch.
func (b *BeaconState) Epoch() uint64 {
	return b.slot / b.beaconConfig.SlotsPerEpoch // Return current state epoch
}

// PreviousEpoch returns previous epoch.
func (b *BeaconState) PreviousEpoch() uint64 {
	epoch := b.Epoch()
	if epoch == 0 {
		return epoch
	}
	return epoch - 1
}

// getUnslashedParticipatingIndices returns set of currently unslashed participating indexes
func (b *BeaconState) GetUnslashedParticipatingIndices(flagIndex int, epoch uint64) (validatorSet []uint64, err error) {
	var participation cltypes.ParticipationFlagsList
	// Must be either previous or current epoch
	switch epoch {
	case b.Epoch():
		participation = b.currentEpochParticipation
	case b.PreviousEpoch():
		participation = b.previousEpochParticipation
	default:
		return nil, fmt.Errorf("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	for i, validator := range b.Validators() {
		if !validator.Active(epoch) ||
			!participation[i].HasFlag(flagIndex) ||
			validator.Slashed {
			continue
		}
		validatorSet = append(validatorSet, uint64(i))
	}
	return
}

// GetTotalBalance return the sum of all balances within the given validator set.
func (b *BeaconState) GetTotalBalance(validatorSet []uint64) (uint64, error) {
	var (
		total          uint64
		validatorsSize = uint64(len(b.validators))
	)
	for _, validatorIndex := range validatorSet {
		// Should be in bounds.
		if validatorIndex >= validatorsSize {
			return 0, fmt.Errorf("GetTotalBalance: out of bounds validator index")
		}
		total += b.validators[validatorIndex].EffectiveBalance
	}
	// Always minimum set to EffectiveBalanceIncrement
	if total < b.beaconConfig.EffectiveBalanceIncrement {
		total = b.beaconConfig.EffectiveBalanceIncrement
	}
	return total, nil
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *BeaconState) GetTotalActiveBalance() (uint64, error) {
	return b.GetTotalBalance(b.GetActiveValidatorsIndices(b.Epoch()))
}

// GetBlockRoot returns blook root at start of a given epoch
func (b *BeaconState) GetBlockRoot(epoch uint64) (libcommon.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.beaconConfig.SlotsPerEpoch)
}

// GetBlockRootAtSlot returns the block root at a given slot
func (b *BeaconState) GetBlockRootAtSlot(slot uint64) (libcommon.Hash, error) {
	if slot >= b.slot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot in the future")
	}
	if b.slot > slot+b.beaconConfig.SlotsPerHistoricalRoot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot too much far behind")
	}
	return b.blockRoots[slot%b.beaconConfig.SlotsPerHistoricalRoot], nil
}
