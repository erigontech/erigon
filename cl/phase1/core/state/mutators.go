package state

import "github.com/ledgerwatch/erigon/cl/abstract"

func IncreaseBalance(b abstract.BeaconState, index, delta uint64) error {
	currentBalance, err := b.ValidatorBalance(int(index))
	if err != nil {
		return err
	}
	return b.SetValidatorBalance(int(index), currentBalance+delta)
}

func DecreaseBalance(b abstract.BeaconState, index, delta uint64) error {
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
