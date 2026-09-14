package estimate

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

// A limit this process derives from TotalMemory must not narrow the budget that
// sized the caches. TotalMemory is process-cached, so this has to be the first
// call in the binary; the totalMemoryCached guard fails loudly if it is not.
func TestTotalMemoryIgnoresLimitInstalledAfterStartup(t *testing.T) {
	require.Zero(t, totalMemoryCached, "must be the first TotalMemory call in this binary")

	const tiny = int64(1 << 20)
	prev := debug.SetMemoryLimit(tiny)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	require.Greater(t, TotalMemory(), uint64(tiny),
		"a limit installed after startup must not shrink the memory budget")
}

func TestMemoryBound(t *testing.T) {
	const gb = uint64(1 << 30)
	for _, tc := range []struct {
		name   string
		bounds []uint64
		want   uint64
	}{
		{"tightest wins", []uint64{8 * gb, 4 * gb, 6 * gb}, 4 * gb},
		{"failed probe is not a bound", []uint64{0, 4 * gb, 0}, 4 * gb},
		{"every probe failed", []uint64{0, 0, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, memoryBound(tc.bounds...))
		})
	}
}
