package statechange

import (
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
)

func ProcessPendingConsolidations(s abstract.BeaconState) error {
	nextEpoch := s.Slot()/s.BeaconConfig().SlotsPerEpoch + 1
	nextConsolidationIndex := 0
	var applyErr error
	s.GetPendingConsolidations().Range(func(i int, c *solid.PendingConsolidation, length int) bool {
		sourceValidator, err := s.ValidatorForValidatorIndex(int(c.SourceIndex))
		if err != nil {
			log.Warn("Failed to get source validator for consolidation", "index", c.SourceIndex)
			nextConsolidationIndex++
			return true
		}
		if sourceValidator.Slashed() {
			nextConsolidationIndex++
			return true
		}
		if sourceValidator.WithdrawableEpoch() > nextEpoch {
			return false // stop processing
		}
		// Calculate the consolidated balance
		vBalance, err := s.ValidatorBalance(int(c.SourceIndex))
		if err != nil {
			log.Warn("Failed to get validator balance for consolidation", "index", c.SourceIndex)
			nextConsolidationIndex++
			return true
		}
		sourceEffectiveBalance := min(vBalance, sourceValidator.EffectiveBalance())
		// Move active balance to target. Excess balance is withdrawable.
		if applyErr = state.DecreaseBalance(s, c.SourceIndex, sourceEffectiveBalance); applyErr != nil {
			return false
		}
		if applyErr = state.IncreaseBalance(s, c.TargetIndex, sourceEffectiveBalance); applyErr != nil {
			// Put the source back: a half-applied move destroys balance.
			if err := s.SetValidatorBalance(int(c.SourceIndex), vBalance); err != nil {
				applyErr = fmt.Errorf("%w (source rollback failed: %w)", applyErr, err)
			}
			return false
		}
		nextConsolidationIndex++
		return true
	})
	if applyErr != nil {
		return applyErr
	}
	pendingConsolidations := s.GetPendingConsolidations().ShallowCopy()
	pendingConsolidations.Cut(nextConsolidationIndex)
	s.SetPendingConsolidations(pendingConsolidations)
	return nil
}
