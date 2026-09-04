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
