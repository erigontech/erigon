package raw

import (
	"errors"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// these functions should only use getters

var (
	ErrGetBlockRootAtSlotFuture = errors.New("GetBlockRootAtSlot: slot in the future")
)

// GetBlockRoot returns blook root at start of a given epoch
func (b *BeaconState) GetBlockRoot(epoch uint64) (libcommon.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.BeaconConfig().SlotsPerEpoch)
}

// GetTotalBalance return the sum of all balances within the given validator set.
func (b *BeaconState) GetTotalBalance(validatorSet []uint64) (uint64, error) {
	var (
		total uint64
	)
	for _, validatorIndex := range validatorSet {
		// Should be in bounds.
		delta, err := b.ValidatorEffectiveBalance(int(validatorIndex))
		if err != nil {
			return 0, err
		}
		total += delta
	}
	// Always minimum set to EffectiveBalanceIncrement
	if total < b.BeaconConfig().EffectiveBalanceIncrement {
		total = b.BeaconConfig().EffectiveBalanceIncrement
	}
	return total, nil
}

// GetEpochAtSlot gives the epoch for a certain slot
func (b *BeaconState) GetEpochAtSlot(slot uint64) uint64 {
	return slot / b.BeaconConfig().SlotsPerEpoch
}

// Epoch returns current epoch.
func (b *BeaconState) Epoch() uint64 {
	return b.GetEpochAtSlot(b.Slot())
}
