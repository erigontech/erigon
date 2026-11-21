package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func ProcessPendingDeposits(s abstract.BeaconState) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessPendingDepositsTime).End()

	var (
		nextEpoch              = s.Slot()/s.BeaconConfig().SlotsPerEpoch + 1
		availableForProcessing = s.GetDepositBalanceToConsume() + state.GetActivationExitChurnLimit(s)
	)
	if s.Version() >= clparams.GloasVersion {
		availableForProcessing = s.GetDepositBalanceToConsume() + state.GetActivationChurnLimit(s)
	}
	var (
		processAmount       = uint64(0)
		nextDepositIndex    = 0
		depositToPostpone   = []*solid.PendingDeposit{}
		isChurnLimitReached = false
		finalizedSlot       = s.FinalizedCheckpoint().Epoch * s.BeaconConfig().SlotsPerEpoch
		applyErr            error
	)
	s.GetPendingDeposits().Range(func(i int, d *solid.PendingDeposit, length int) bool {
		// Do not process deposit requests if Eth1 bridge deposits are not yet applied.
		if s.Version() < clparams.FuluVersion && d.Slot > s.BeaconConfig().GenesisSlot && s.Eth1DepositIndex() < s.GetDepositRequestsStartIndex() {
			return false
		}
		// Check if deposit has been finalized, otherwise, stop processing.
		if d.Slot > finalizedSlot {
			return false
		}
		// Check if number of processed deposits has not reached the limit, otherwise, stop processing.
		if nextDepositIndex >= int(s.BeaconConfig().MaxPendingDepositsPerEpoch) {
			return false
		}
		isValidatorExited := false
		isValidatorWithdrawn := false
		if vindex, exist := s.ValidatorIndexByPubkey(d.PubKey); exist {
			validator := s.ValidatorSet().Get(int(vindex))
			isValidatorExited = validator.ExitEpoch() < s.BeaconConfig().FarFutureEpoch
			isValidatorWithdrawn = validator.WithdrawableEpoch() < nextEpoch
		}

		switch {
		case isValidatorWithdrawn:
			// Deposited balance will never become active. Increase balance but do not consume churn
			if applyErr = applyPendingDeposit(s, d); applyErr != nil {
				return false
			}
		case isValidatorExited:
			// Validator is exiting, postpone the deposit until after withdrawable epoch
			depositToPostpone = append(depositToPostpone, d)
		default:
			// Check if deposit fits in the churn, otherwise, do no more deposit processing in this epoch.
			isChurnLimitReached = processAmount+d.Amount > availableForProcessing
			if isChurnLimitReached {
				return false
			}
			// Consume churn and apply deposit.
			processAmount += d.Amount
			if applyErr = applyPendingDeposit(s, d); applyErr != nil {
				return false
			}
		}
		nextDepositIndex++
		return true
	})
	if applyErr != nil {
		return applyErr
	}

	// update pending deposits. [nextDepositIndex:] + [postponed]
	newPendingDeposits := s.GetPendingDeposits().ShallowCopy()
	newPendingDeposits.Cut(nextDepositIndex)
	for _, d := range depositToPostpone {
		newPendingDeposits.Append(d)
	}
	s.SetPendingDeposits(newPendingDeposits)

	// Accumulate churn only if the churn limit has been hit.
	if isChurnLimitReached {
		s.SetDepositBalanceToConsume(availableForProcessing - processAmount)
	} else {
		s.SetDepositBalanceToConsume(0)
	}
	return nil
}

func applyPendingDeposit(s abstract.BeaconState, d *solid.PendingDeposit) error {
	vindex, exist := s.ValidatorIndexByPubkey(d.PubKey)
	if !exist {
		if valid, _ := IsValidDepositSignature(&cltypes.DepositData{
			PubKey:                d.PubKey,
			WithdrawalCredentials: d.WithdrawalCredentials,
			Amount:                d.Amount,
			Signature:             d.Signature,
		}, s.BeaconConfig()); valid {
			return AddValidatorToRegistry(s, d.PubKey, d.WithdrawalCredentials, d.Amount)
		}
		return nil
	}
	// increase balance
	return state.IncreaseBalance(s, vindex, d.Amount)
}
