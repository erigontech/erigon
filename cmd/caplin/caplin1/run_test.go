package caplin1

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestValidateGloasExecutionEngine(t *testing.T) {
	t.Run("missing beacon configuration", func(t *testing.T) {
		require.ErrorIs(t, validateGloasExecutionEngine(nil, true), ErrGloasExecutionEngineUnavailable)
	})

	t.Run("scheduled without execution engine", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		cfg.GloasForkEpoch = 0
		require.ErrorIs(t, validateGloasExecutionEngine(&cfg, false), ErrGloasExecutionEngineUnavailable)
	})

	t.Run("scheduled with execution engine", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		cfg.GloasForkEpoch = 0
		require.NoError(t, validateGloasExecutionEngine(&cfg, true))
	})

	t.Run("unscheduled without execution engine", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		cfg.GloasForkEpoch = math.MaxUint64
		require.NoError(t, validateGloasExecutionEngine(&cfg, false))
	})
}
