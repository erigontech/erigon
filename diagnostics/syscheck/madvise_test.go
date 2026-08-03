package syscheck_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/diagnostics/syscheck"
)

// TestMadviseRoundTrip pins what each mmap.Madvise* call actually does to the
// kernel's per-VMA flags, read back through /proc/self/smaps. It is the check
// that a refactor cannot silently leave a snapshot file readahead-enabled.
func TestMadviseRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "madvise.bin")
	require.NoError(t, os.WriteFile(path, make([]byte, 4*1024*1024), 0o600))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	st, err := f.Stat()
	require.NoError(t, err)

	h1, h2, err := mmap.Mmap(f, int(st.Size()))
	require.NoError(t, err)
	defer func() { require.NoError(t, mmap.Munmap(h1, h2)) }()

	advice := func() string {
		all, err := syscheck.FileMappings()
		require.NoError(t, err)
		for _, m := range all {
			if m.Path == path {
				return m.Advice()
			}
		}
		t.Fatalf("mapping for %s not found among %d file mappings", path, len(all))
		return ""
	}

	if runtime.GOOS != "linux" {
		got, err := syscheck.FileMappings()
		require.NoError(t, err)
		require.Nil(t, got, "VmFlags is a Linux-only interface, so nothing is observable here")
		return
	}

	require.Equal(t, "random", advice(), "mmap.Mmap madvises MADV_RANDOM itself")

	require.NoError(t, mmap.MadviseSequential(h1))
	require.Equal(t, "sequential", advice())

	require.NoError(t, mmap.MadviseNormal(h1))
	require.Equal(t, "normal", advice(), "MADV_NORMAL clears both VM_SEQ_READ and VM_RAND_READ")

	require.NoError(t, mmap.MadviseRandom(h1))
	require.Equal(t, "random", advice())

	// WILLNEED is a readahead request, not a flag change — the VMA keeps its advice.
	require.NoError(t, mmap.MadviseWillNeed(h1))
	require.Equal(t, "random", advice())
}

// TestSequentialViewLeavesSharedMappingRandom is the property the whole
// SequentialView design exists for: madvising the scan's own mapping must not
// change the advice on the mapping concurrent random readers use.
func TestSequentialViewLeavesSharedMappingRandom(t *testing.T) {
	if runtime.GOOS != "linux" {
		got, err := syscheck.FileMappings()
		require.NoError(t, err)
		require.Nil(t, got)
		return
	}

	path := filepath.Join(t.TempDir(), "shared.bin")
	require.NoError(t, os.WriteFile(path, make([]byte, 4*1024*1024), 0o600))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	st, err := f.Stat()
	require.NoError(t, err)

	shared1, shared2, err := mmap.Mmap(f, int(st.Size()))
	require.NoError(t, err)
	defer func() { require.NoError(t, mmap.Munmap(shared1, shared2)) }()

	seq1, seq2, err := mmap.Mmap(f, int(st.Size()))
	require.NoError(t, err)
	defer func() { require.NoError(t, mmap.Munmap(seq1, seq2)) }()

	require.NoError(t, mmap.MadviseSequential(seq1))

	all, err := syscheck.FileMappings()
	require.NoError(t, err)

	var advices []string
	for _, m := range all {
		if m.Path == path {
			advices = append(advices, m.Advice())
		}
	}
	require.ElementsMatch(t, []string{"random", "sequential"}, advices,
		"two mmaps of one fd must hold independent advice")
}

// TestMadviseSubPageMapping pins that a mapping shorter than one page still gets
// its advice. mmap hands out whole pages and madvise(2) rounds the length up, so
// trimming the tail down to a page boundary drops the only page there is.
func TestMadviseSubPageMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.bin")
	require.NoError(t, os.WriteFile(path, make([]byte, 300), 0o600))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	st, err := f.Stat()
	require.NoError(t, err)
	require.Less(t, st.Size(), int64(os.Getpagesize()))

	h1, h2, err := mmap.Mmap(f, int(st.Size()))
	require.NoError(t, err)
	defer func() { require.NoError(t, mmap.Munmap(h1, h2)) }()

	if runtime.GOOS != "linux" {
		got, err := syscheck.FileMappings()
		require.NoError(t, err)
		require.Nil(t, got)
		return
	}

	advice := func() (string, int) {
		all, err := syscheck.FileMappings()
		require.NoError(t, err)
		var got string
		var n int
		for _, m := range all {
			if m.Path == path {
				got, n = m.Advice(), n+1
			}
		}
		require.Positive(t, n, "mapping for %s not found", path)
		return got, n
	}

	// Mmap madvises random itself, so start from a state a later call must change.
	got, _ := advice()
	require.Equal(t, "random", got)

	require.NoError(t, mmap.MadviseNormal(h1))
	got, n := advice()
	require.Equal(t, "normal", got, "advice on a sub-page mapping must take effect")
	require.Equal(t, 1, n, "advising a whole mapping must not split its VMA")
}
