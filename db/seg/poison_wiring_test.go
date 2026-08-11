//go:build linux

package seg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
)

// mappedAt reports whether the calling process still owns a mapping covering addr.
func mappedAt(t *testing.T, addr uintptr) bool {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(maps), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		lo, hi, ok := strings.Cut(fields[0], "-")
		if !ok {
			continue
		}
		start, err1 := strconv.ParseUint(lo, 16, 64)
		end, err2 := strconv.ParseUint(hi, 16, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		if addr >= uintptr(start) && addr < uintptr(end) {
			return true
		}
	}
	return false
}

// TestPoisonWiring proves the dbg flag reaches Decompressor.Close: with it set
// the closed file keeps its address range (revoked, never reusable); without it
// the range is released back to the kernel.
func TestPoisonWiring(t *testing.T) {
	d := prepareLoremDict(t)
	addr := uintptr(unsafe.Pointer(&d.mmapHandle1[0]))
	require.True(t, mappedAt(t, addr), "mapped while open")

	d.Close()

	still := mappedAt(t, addr)
	fmt.Printf("MMAP_POISON=%v -> address %#x still mapped after Close: %v\n", dbg.MmapPoison, addr, still)
	require.Equal(t, dbg.MmapPoison, still,
		"poison must retain the range so it cannot be handed to another file; plain Close must release it")
}
