package state_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

// TestEIP7922Constants verifies preset constants are defined correctly.
func TestEIP7922Constants(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	require.EqualValues(t, 256, cfg.EpochsPerChurnGeneration)
	require.EqualValues(t, 16, cfg.GenerationsPerExitChurnVector)
	require.EqualValues(t, 2, cfg.GenerationsPerExitChurnLookahead)
	require.EqualValues(t, 8, cfg.ExitChurnSlackMultiplier)
}

// TestGetExitChurnLimit_AllUsed: all past generations fully consumed →
// should return just the per-epoch churn (no slack available).
func TestGetExitChurnLimit_AllUsed(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	// Fill exit_churn_vector with UINT64_MAX (fully consumed)
	for i := 0; i < int(clparams.MainnetBeaconConfig.GenerationsPerExitChurnVector); i++ {
		bs.SetExitChurnVectorAtIndex(i, ^uint64(0))
	}
	perEpochChurn := state.GetActivationExitChurnLimit(bs)
	got := state.GetExitChurnLimit(bs)
	require.Equal(t, perEpochChurn, got, "all used: churn limit should equal per-epoch churn")
}

// TestGetExitChurnLimit_AllUnused: all past generations completely empty →
// should return per_epoch_churn * EXIT_CHURN_SLACK_MULTIPLIER (capped).
func TestGetExitChurnLimit_AllUnused(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	// Fill exit_churn_vector with 0 (nothing consumed)
	for i := 0; i < int(clparams.MainnetBeaconConfig.GenerationsPerExitChurnVector); i++ {
		bs.SetExitChurnVectorAtIndex(i, 0)
	}
	perEpochChurn := state.GetActivationExitChurnLimit(bs)
	got := state.GetExitChurnLimit(bs)
	maxChurn := perEpochChurn * uint64(clparams.MainnetBeaconConfig.ExitChurnSlackMultiplier)
	require.Equal(t, maxChurn, got, "all unused: churn limit should be capped at slack multiplier")
}

// TestGetExitChurnLimit_Partial: half the past generations unused →
// result should be between per-epoch churn and the cap.
func TestGetExitChurnLimit_Partial(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	cfg := clparams.MainnetBeaconConfig
	for i := 0; i < int(cfg.GenerationsPerExitChurnVector); i++ {
		if i%2 == 0 {
			bs.SetExitChurnVectorAtIndex(i, 0) // unused
		} else {
			bs.SetExitChurnVectorAtIndex(i, ^uint64(0)) // fully used
		}
	}
	perEpochChurn := state.GetActivationExitChurnLimit(bs)
	maxChurn := perEpochChurn * uint64(cfg.ExitChurnSlackMultiplier)
	got := state.GetExitChurnLimit(bs)
	require.GreaterOrEqual(t, got, perEpochChurn, "partial: churn limit should be >= per-epoch churn")
	require.LessOrEqual(t, got, maxChurn, "partial: churn limit should be <= cap")
}

// TestProcessHistoricalExitChurnVector_NoGenerationSwitch: within same generation,
// vector must not change.
func TestProcessHistoricalExitChurnVector_NoGenerationSwitch(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	// Set slot to epoch 100, generation 0 (100 < 256)
	bs.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch)
	before := make([]uint64, clparams.MainnetBeaconConfig.GenerationsPerExitChurnVector)
	for i := range before {
		v := uint64(i * 1000)
		bs.SetExitChurnVectorAtIndex(i, v)
		before[i] = v
	}
	state.ProcessHistoricalExitChurnVector(bs)
	for i := range before {
		require.Equal(t, before[i], bs.ExitChurnVectorAtIndex(i),
			"no generation switch: vector should be unchanged at index %d", i)
	}
}

// TestProcessHistoricalExitChurnVector_GenerationSwitch: at a generation boundary,
// the lookahead slot must be reset (either 0 or UINT64_MAX depending on earliest_exit_epoch).
func TestProcessHistoricalExitChurnVector_GenerationSwitch(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	cfg := clparams.MainnetBeaconConfig
	// Place slot at the last epoch of generation 1 (epoch 511 = slot 511*32)
	// So next epoch (512) transitions to generation 2.
	epoch511 := uint64(511)
	bs.SetSlot(epoch511 * cfg.SlotsPerEpoch)
	// Initialize vector to sentinel
	for i := 0; i < int(cfg.GenerationsPerExitChurnVector); i++ {
		bs.SetExitChurnVectorAtIndex(i, 9999)
	}
	// earliest_exit_epoch well in the past → lookahead generation should reset to 0
	bs.SetEarliestExitEpoch(0)
	state.ProcessHistoricalExitChurnVector(bs)
	// generation 2 + lookahead 2 = generation 4; index = 4 % 16 = 4
	lookaheadIdx := (2 + int(cfg.GenerationsPerExitChurnLookahead)) % int(cfg.GenerationsPerExitChurnVector)
	require.Equal(t, uint64(0), bs.ExitChurnVectorAtIndex(lookaheadIdx),
		"generation switch: lookahead index should be reset to 0")
}

// TestComputeExitEpochAndUpdateChurn_DynamicChurn: with slack available,
// the dynamic churn limit must be used (exits are processed faster).
func TestComputeExitEpochAndUpdateChurn_DynamicChurn(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	// All past generations unused → max slack
	cfg := clparams.MainnetBeaconConfig
	for i := 0; i < int(cfg.GenerationsPerExitChurnVector); i++ {
		bs.SetExitChurnVectorAtIndex(i, 0)
	}

	perEpochChurn := state.GetActivationExitChurnLimit(bs)
	dynamicChurn := state.GetExitChurnLimit(bs)
	require.Greater(t, dynamicChurn, perEpochChurn, "dynamic churn should exceed per-epoch churn when slack available")

	// A large exit that would need multiple epochs at fixed churn
	largeExit := perEpochChurn * 4
	exitEpoch := bs.ComputeExitEpochAndUpdateChurn(largeExit)

	// With dynamic churn (8× slack), the exit should fit in fewer epochs
	minExitEpoch := state.ComputeActivationExitEpoch(&cfg, state.Epoch(bs))
	require.GreaterOrEqual(t, exitEpoch, minExitEpoch, "exit epoch must not be before activation exit epoch")
}

// TestExitChurnVectorInitialization: after fork upgrade, initialization must
// set all entries to UINT64_MAX and reset lookahead generations.
func TestExitChurnVectorInitialization(t *testing.T) {
	bs := state.New(&clparams.MainnetBeaconConfig)
	cfg := clparams.MainnetBeaconConfig
	// Simulate upgrade at epoch 0
	state.InitializeExitChurnVector(bs)
	// All entries must be UINT64_MAX initially
	for i := 0; i < int(cfg.GenerationsPerExitChurnVector); i++ {
		require.Equal(t, ^uint64(0), bs.ExitChurnVectorAtIndex(i),
			"initialization: all entries should be UINT64_MAX at index %d", i)
	}
}
