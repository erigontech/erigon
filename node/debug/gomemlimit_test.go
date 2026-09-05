package debug

import (
	"math"
	"os"
	"runtime/debug"
	"testing"

	"github.com/c2h5oh/datasize"
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

// setProcessGoMemLimit installs a runtime limit for one test and restores it.
func setProcessGoMemLimit(t *testing.T, limit int64) {
	t.Helper()
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
	debug.SetMemoryLimit(limit)
}

func TestGoMemLimitInForce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		setProcessGoMemLimit(t, math.MaxInt64)
		set, _, _ := goMemLimitInForce()
		require.False(t, set)
	})
	t.Run("empty", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "")
		setProcessGoMemLimit(t, math.MaxInt64)
		set, _, _ := goMemLimitInForce()
		require.False(t, set)
	})
	t.Run("off", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "off")
		setProcessGoMemLimit(t, math.MaxInt64)
		set, off, _ := goMemLimitInForce()
		require.True(t, set)
		require.True(t, off)
	})
	t.Run("set in env", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "4GiB")
		setProcessGoMemLimit(t, 4<<30)
		set, off, limit := goMemLimitInForce()
		require.True(t, set)
		require.False(t, off)
		require.Equal(t, datasize.ByteSize(4<<30), limit)
	})
	t.Run("zero is a real limit", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "0")
		setProcessGoMemLimit(t, 0)
		set, off, limit := goMemLimitInForce()
		require.True(t, set)
		require.False(t, off)
		require.Equal(t, datasize.ByteSize(0), limit)
	})
	t.Run("set in process", func(t *testing.T) {
		unsetGoMemLimitEnv(t)
		setProcessGoMemLimit(t, 3<<30)
		set, off, limit := goMemLimitInForce()
		require.True(t, set)
		require.False(t, off)
		require.Equal(t, datasize.ByteSize(3<<30), limit)
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
