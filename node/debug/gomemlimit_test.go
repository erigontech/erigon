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

func TestGoMemLimitInForce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		require.Equal(t, goMemLimitUnset, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("empty", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "")
		require.Equal(t, goMemLimitUnset, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("off", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "off")
		require.Equal(t, goMemLimitOff, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("set in env", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "4GiB")
		require.Equal(t, goMemLimitSet, goMemLimitInForce(4<<30))
	})
	t.Run("zero is a real limit", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "0")
		require.Equal(t, goMemLimitSet, goMemLimitInForce(0))
	})
	t.Run("max is off in effect", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "9223372036854775807")
		require.Equal(t, goMemLimitOff, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("set in process", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		require.Equal(t, goMemLimitSet, goMemLimitInForce(3<<30))
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
