package debug

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

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
// independently of the machine the test runs on. The two sentinels are what a
// cgroup reports when it is not limiting memory: v2 "max", and v1's MaxInt64
// rounded down to a page — which no int64 clamp catches on its own.
func TestDerivedGoMemLimit(t *testing.T) {
	const gb = uint64(1) << 30
	require.Zero(t, derivedGoMemLimit(0, 16*gb), "no cgroup limit")
	require.Zero(t, derivedGoMemLimit(16*gb, 16*gb), "a cgroup at physical memory constrains nothing")
	require.Zero(t, derivedGoMemLimit(math.MaxUint64, 16*gb), "cgroup v2 reports unlimited as max")
	require.Zero(t, derivedGoMemLimit(0x7FFFFFFFFFFFF000, 16*gb), "cgroup v1 reports unlimited just under MaxInt64")
	require.Zero(t, derivedGoMemLimit(math.MaxUint64, 0), "unlimited stays unlimited with physical memory unknown")
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 16*gb))
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 0), "unknown physical memory still honours the cgroup")
}
