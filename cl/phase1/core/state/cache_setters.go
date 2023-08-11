package state

import "github.com/ledgerwatch/erigon/cl/cltypes/solid"

// Below are setters.

func (b *CachingBeaconState) SetSlot(slot uint64) {
	b.BeaconState.SetSlot(slot)
	b.proposerIndex = nil
	if slot%b.BeaconConfig().SlotsPerEpoch == 0 {
		b.totalActiveBalanceCache = nil
	}
}

func (b *CachingBeaconState) AddValidator(validator solid.Validator, balance uint64) {
	b.BeaconState.AddValidator(validator, balance)
	b.publicKeyIndicies[validator.PublicKey()] = uint64(b.ValidatorLength()) - 1
	// change in validator set means cache purging
	b.totalActiveBalanceCache = nil
}
