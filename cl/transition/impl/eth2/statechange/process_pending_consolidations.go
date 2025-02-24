package statechange

import (
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func ProcessPendingConsolidations(s abstract.BeaconState) {
	nextEpoch := s.Slot()/s.BeaconConfig().SlotsPerEpoch + 1
	nextConsolidationIndex := 0
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
		state.DecreaseBalance(s, c.SourceIndex, sourceEffectiveBalance)
		state.IncreaseBalance(s, c.TargetIndex, sourceEffectiveBalance)
		nextConsolidationIndex++
		return true
	})
	pendingConsolidations := s.GetPendingConsolidations().ShallowCopy()
	pendingConsolidations.Cut(nextConsolidationIndex)
	s.SetPendingConsolidations(pendingConsolidations)
}
