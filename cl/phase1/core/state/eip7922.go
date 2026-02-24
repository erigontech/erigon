package state

// GetExitChurnLimit implements the EIP-7922 dynamic exit churn limit calculation.
// It sums unused churn from past generations and caps the result at
// perEpochChurn * EXIT_CHURN_SLACK_MULTIPLIER.
func GetExitChurnLimit(b *CachingBeaconState) uint64 {
	cfg := b.BeaconConfig()
	currentEpoch := Epoch(b)
	earliestExitEpoch := max(b.EarliestExitEpoch(), ComputeActivationExitEpoch(cfg, currentEpoch))
	perEpochExitChurn := GetActivationExitChurnLimit(b)

	// If the earliest_exit_epoch generation is beyond the lookahead, don't use the slack.
	currentGeneration := currentEpoch / cfg.EpochsPerChurnGeneration
	lookaheadGeneration := currentGeneration + cfg.GenerationsPerExitChurnLookahead
	earliestExitEpochGeneration := earliestExitEpoch / cfg.EpochsPerChurnGeneration
	if earliestExitEpochGeneration > lookaheadGeneration {
		return perEpochExitChurn
	}

	// Compute churn leftover from past generations.
	// The vector has GENERATIONS_PER_EXIT_CHURN_VECTOR entries. Of these,
	// GENERATIONS_PER_EXIT_CHURN_LOOKAHEAD are for the current+lookahead, and
	// the remaining entries are for past generations. We iterate over the past entries
	// in the circular buffer.
	pastGenerations := cfg.GenerationsPerExitChurnVector - cfg.GenerationsPerExitChurnLookahead
	perGenerationExitChurn := perEpochExitChurn * cfg.EpochsPerChurnGeneration
	var totalUnusedExitChurn uint64
	for offset := cfg.GenerationsPerExitChurnLookahead; offset < cfg.GenerationsPerExitChurnVector; offset++ {
		generationIndex := int((currentGeneration + offset) % cfg.GenerationsPerExitChurnVector)
		churnUsage := b.ExitChurnVectorAtIndex(generationIndex)
		if churnUsage < perGenerationExitChurn {
			totalUnusedExitChurn += perGenerationExitChurn - churnUsage
		}
	}
	_ = pastGenerations // used conceptually

	// Cap the churn slack.
	churnWithSlack := totalUnusedExitChurn + perEpochExitChurn
	maxChurn := perEpochExitChurn * cfg.ExitChurnSlackMultiplier
	return min(churnWithSlack, maxChurn)
}

// ProcessHistoricalExitChurnVector is called once per epoch.
// It updates the exit churn vector when crossing a generation boundary.
func ProcessHistoricalExitChurnVector(b *CachingBeaconState) {
	cfg := b.BeaconConfig()
	currentEpoch := Epoch(b)
	nextEpoch := currentEpoch + 1

	currentEpochGeneration := currentEpoch / cfg.EpochsPerChurnGeneration
	nextEpochGeneration := nextEpoch / cfg.EpochsPerChurnGeneration

	// Only update the vector if switching to the next generation.
	if nextEpochGeneration <= currentEpochGeneration {
		return
	}

	earliestExitEpochGeneration := b.EarliestExitEpoch() / cfg.EpochsPerChurnGeneration
	lookaheadGeneration := nextEpochGeneration + cfg.GenerationsPerExitChurnLookahead
	lookaheadGenerationIndex := int(lookaheadGeneration % cfg.GenerationsPerExitChurnVector)

	if earliestExitEpochGeneration < lookaheadGeneration {
		// Reset churn usage to 0.
		b.SetExitChurnVectorAtIndex(lookaheadGenerationIndex, 0)
	} else {
		// Mark as fully consumed.
		b.SetExitChurnVectorAtIndex(lookaheadGenerationIndex, ^uint64(0))
	}
}

// InitializeExitChurnVector initializes the exit churn vector upon EIP-7922 activation.
// All entries are set to UINT64_MAX (fully consumed) since the historical churn
// is unknown at fork activation. Lookahead resets will occur naturally during
// subsequent epoch processing.
func InitializeExitChurnVector(b *CachingBeaconState) {
	cfg := b.BeaconConfig()

	// Mark the churn of each generation as fully consumed.
	for i := 0; i < int(cfg.GenerationsPerExitChurnVector); i++ {
		b.SetExitChurnVectorAtIndex(i, ^uint64(0))
	}

	// Update lookahead generations based on earliest_exit_epoch.
	earliestExitEpochGeneration := b.EarliestExitEpoch() / cfg.EpochsPerChurnGeneration
	currentEpochGeneration := Epoch(b) / cfg.EpochsPerChurnGeneration
	lookaheadGeneration := currentEpochGeneration + cfg.GenerationsPerExitChurnLookahead

	for generation := currentEpochGeneration; generation < lookaheadGeneration; generation++ {
		// Only reset if the exit queue is actively past this generation
		// (earliest_exit_epoch is in a generation strictly after the current one
		// but still before this lookahead generation).
		if earliestExitEpochGeneration > currentEpochGeneration && earliestExitEpochGeneration < generation {
			b.SetExitChurnVectorAtIndex(int(generation%cfg.GenerationsPerExitChurnVector), 0)
		}
	}
}
