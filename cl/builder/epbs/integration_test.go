package epbs

import (
	"testing"

	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/stretchr/testify/require"
)

func TestInitBuilderService_Disabled(t *testing.T) {
	cfg := epbscfg.Config{Enabled: false}
	svc, err := InitBuilderService(cfg, BuilderDeps{})
	require.NoError(t, err)
	require.Nil(t, svc)
}

func TestBuilderService_Shutdown_Nil(t *testing.T) {
	// Shutdown on nil should not panic.
	var svc *BuilderService
	svc.Shutdown()
}

func TestBalanceStatus_Zero(t *testing.T) {
	status := BalanceStatus{}
	require.False(t, status.Active)
	require.Equal(t, uint64(0), status.Balance)
}

func TestOnBidWonFunc_NilLoop(t *testing.T) {
	// OnBidWonFunc should not panic when passed a nil context and valid loop
	// (we can't easily test the full flow without real dependencies, but we
	// verify the function returns a non-nil callback).
	loop, _, _, _ := setupBuilderLoop(t)
	fn := OnBidWonFunc(t.Context(), loop)
	require.NotNil(t, fn)
}
