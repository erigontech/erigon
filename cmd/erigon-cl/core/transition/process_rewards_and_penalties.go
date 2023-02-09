package transition

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func (s *StateTransistor) ProcessRewardsAndPenalties() error {
	// Get deltas for epoch transition.
	deltas, err := s.state.BalanceDeltas()
	if err != nil {
		return err
	}
	// Apply deltas
	for index, delta := range deltas {
		if delta > 0 {
			// Increment
			if err := s.state.IncreaseBalance(index, uint64(delta)); err != nil {
				return err
			}
			continue
		}
		// Decrease balance
		if err := s.state.DecreaseBalance(index, uint64(-delta)); err != nil {
			return err
		}
	}
	return nil
}
