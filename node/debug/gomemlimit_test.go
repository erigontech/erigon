package debug

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/estimate"
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

// Off a cgroup there is nothing to derive from, so the default must stay.
func TestSetGoMemLimitLeavesUnconfinedProcessAlone(t *testing.T) {
	prev := debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	SetGoMemLimit(log.New())
	got := debug.SetMemoryLimit(-1)
	want := derivedGoMemLimit(estimate.CgroupsMemoryLimit(), estimate.SystemMemory())
	if want == 0 {
		require.Equal(t, int64(math.MaxInt64), got, "unconfined process must keep the default")
	} else {
		require.Equal(t, want, got, "confined process must get the derived ceiling")
	}
}

// The predicate that decides whether a cgroup constrains us at all, pinned
// independently of the machine the test runs on.
func TestDerivedGoMemLimit(t *testing.T) {
	const gb = uint64(1) << 30
	require.Zero(t, derivedGoMemLimit(0, 16*gb), "no cgroup limit")
	require.Zero(t, derivedGoMemLimit(16*gb, 16*gb), "a cgroup at physical memory constrains nothing")
	require.Zero(t, derivedGoMemLimit(32*gb, 16*gb), "a cgroup above physical memory constrains nothing")
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 16*gb))
	require.Equal(t, int64(7*gb), derivedGoMemLimit(10*gb, 0), "unknown physical memory still honours the cgroup")
}
