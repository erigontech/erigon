package state

import (
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/raw"
)

func IncreaseBalance(b *raw.BeaconState, index, delta uint64) error {
	currentBalance, err := b.ValidatorBalance(int(index))
	if err != nil {
		return err
	}
	return b.SetValidatorBalance(int(index), currentBalance+delta)
}

func DecreaseBalance(b *raw.BeaconState, index, delta uint64) error {
	currentBalance, err := b.ValidatorBalance(int(index))
	if err != nil {
		return err
	}
	var newBalance uint64
	if currentBalance >= delta {
		newBalance = currentBalance - delta
	}
	return b.SetValidatorBalance(int(index), newBalance)
}
