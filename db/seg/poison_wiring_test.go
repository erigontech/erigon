//go:build linux

package seg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
)

// fileStillMapped reports whether this process still holds a mapping of path.
// Asking by file rather than by address matters: a released address is reused
// almost immediately, so "something is mapped here" says nothing about whether
// our file's mapping survived.
func fileStillMapped(t *testing.T, path string) bool {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	base := filepath.Base(path)
	for line := range strings.SplitSeq(string(maps), "\n") {
		if strings.HasSuffix(line, base) || strings.HasSuffix(line, base+" (deleted)") {
			return true
		}
	}
	return false
}

// TestPoisonWiring proves the dbg flag reaches Decompressor.Close: with it set
// the closed file keeps its range (revoked, never reusable); without it the
// range is handed back to the kernel.
func TestPoisonWiring(t *testing.T) {
	d := prepareLoremDict(t)
	path := d.FilePath()
	require.True(t, fileStillMapped(t, path), "mapped while open")

	d.Close()

	require.Equal(t, dbg.MmapPoison, fileStillMapped(t, path),
		"poison must retain the mapping so the address cannot be handed to another "+
			"file; a plain Close must release it")
}
