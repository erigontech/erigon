package debug

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// An operator-supplied limit must win: the derived one is a fallback, not a policy.
func TestSetGoMemLimitKeepsExistingLimit(t *testing.T) {
	const explicit = int64(3 << 30)
	prev := debug.SetMemoryLimit(explicit)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	SetGoMemLimit(log.New())
	require.Equal(t, explicit, debug.SetMemoryLimit(-1), "an explicit GOMEMLIMIT must not be overridden")
}

// GOMEMLIMIT=off is a deliberate choice, but the runtime reports it exactly like
// an unset variable, so only the environment can tell the two apart.
func TestGoMemLimitInForce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		require.False(t, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("off", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "off")
		require.True(t, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("set in process", func(t *testing.T) {
		require.True(t, goMemLimitInForce(3<<30))
	})
}

// The predicate that decides whether a cgroup constrains us at all, pinned
// independently of the machine the test runs on.
func TestDerivedGoMemLimit(t *testing.T) {
	const gb = uint64(1) << 30
	require.Zero(t, derivedGoMemLimit(0, 16*gb), "no cgroup limit")
	require.Zero(t, derivedGoMemLimit(16*gb, 16*gb), "a cgroup at physical memory constrains nothing")
	require.Zero(t, derivedGoMemLimit(32*gb, 16*gb), "a cgroup above physical memory constrains nothing")
	require.Zero(t, derivedGoMemLimit(math.MaxUint64, 0),
		"an unlimited cgroup constrains nothing even when physical memory is unknown")
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 16*gb))
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 0), "unknown physical memory still honours the cgroup")
}
