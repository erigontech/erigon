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

// Off a cgroup there is nothing to derive from, so the default must stay.
func TestSetGoMemLimitLeavesUnconfinedProcessAlone(t *testing.T) {
	prev := debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	SetGoMemLimit(log.New())
	got := debug.SetMemoryLimit(-1)
	if cg := cgroupLimitForTest(); cg == 0 {
		require.Equal(t, int64(math.MaxInt64), got, "unconfined process must keep the default")
	} else {
		require.Less(t, got, int64(math.MaxInt64), "confined process must get a ceiling")
	}
}
