package debug

import (
	"math"
	"os"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
)

// unsetGoMemLimitEnv clears GOMEMLIMIT for one test and restores whatever the
// test binary was launched with.
func unsetGoMemLimitEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GOMEMLIMIT", "") // registers the restore; the unset below is the real setup
	os.Unsetenv("GOMEMLIMIT")
}

// GOMEMLIMIT=off is a deliberate choice, but the runtime reports it exactly like
// an unset variable, so only the environment can tell the two apart.
func TestGoMemLimitInForce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		require.False(t, goMemLimitIsSet(math.MaxInt64))
	})
	t.Run("empty", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "")
		require.False(t, goMemLimitIsSet(math.MaxInt64))
	})
	t.Run("off", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "off")
		require.True(t, goMemLimitIsSet(math.MaxInt64))
	})
	t.Run("set in process", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		require.True(t, goMemLimitIsSet(3<<30))
	})
}

func TestGoMemLimitFor(t *testing.T) {
	const total = int64(10 << 30)
	limit, ok := goMemLimitFor(uint64(total))
	require.True(t, ok)
	// A ceiling, not a reservation: it must leave real headroom without giving
	// away most of the budget.
	require.Greater(t, limit, total/2)
	require.Less(t, limit, total)

	_, ok = goMemLimitFor(0)
	require.False(t, ok)
}

func TestSetGoMemLimitInstallsDerivedLimit(t *testing.T) {
	total := estimate.TotalMemory()
	if total == 0 {
		t.Skip("available memory unknown on this host")
	}
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
	unsetGoMemLimitEnv(t)
	debug.SetMemoryLimit(math.MaxInt64)

	SetGoMemLimit(log.New())

	want, ok := goMemLimitFor(total)
	require.True(t, ok)
	require.Equal(t, want, debug.SetMemoryLimit(-1))
}
