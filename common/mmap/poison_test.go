//go:build !windows

package mmap

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
)

// A read through a poisoned range kills the process, so the read has to happen
// in a child. The parent asserts on how the child died.
const poisonChildEnv = "MMAP_POISON_TEST_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(poisonChildEnv) == "" {
		os.Exit(m.Run())
	}
	poisonChild()
}

func poisonChild() {
	f, err := os.CreateTemp("", "poison")
	if err != nil {
		os.Exit(3)
	}
	if err := f.Truncate(4096); err != nil {
		os.Exit(3)
	}
	data, handle, err := Mmap(f, 4096)
	if err != nil {
		os.Exit(3)
	}
	borrowed := data[:8]
	_ = borrowed[0] // readable while the file is open

	// Unlink while mapped, the way a merge disposes of a superseded file.
	if err := dir.RemoveFile(f.Name()); err != nil {
		os.Exit(3)
	}
	if err := Poison(data, handle); err != nil {
		os.Exit(3)
	}
	// The file is "closed" now. A stale borrow must not survive this.
	if borrowed[0] == 0xFF {
		os.Exit(4)
	}
	os.Exit(5) // reached only if the read did not fault
}

func TestPoisonFaultsOnStaleRead(t *testing.T) {
	exe, err := os.Executable()
	require.NoError(t, err)

	cmd := exec.Command(exe, "-test.run", "TestPoisonFaultsOnStaleRead")
	cmd.Env = append(os.Environ(), poisonChildEnv+"=1")
	out, err := cmd.CombinedOutput()

	require.Error(t, err, "reading a poisoned range must kill the child, got clean exit:\n%s", out)
	require.Contains(t, string(out), "fatal error: fault",
		"child should die on a memory fault, not exit normally:\n%s", out)

	// Which signal carries the fault is the OS's choice: Linux reports
	// SIGSEGV/SEGV_ACCERR, Darwin reports SIGBUS for a protection violation on a
	// file-backed mapping. Only Linux runs in production, so pin the code there.
	if runtime.GOOS == "linux" {
		require.Contains(t, string(out), "SIGSEGV")
		require.Contains(t, string(out), "code=0x2",
			"the range must still be mapped (SEGV_ACCERR), not unmapped (SEGV_MAPERR):\n%s", out)
	}
}

// TestPoisonKeepsAddressReserved is the property that makes poisoning useful:
// the range stays owned, so no later file can be handed the same address and
// answer a stale read with plausible bytes.
func TestPoisonKeepsAddressReserved(t *testing.T) {
	dir := t.TempDir()
	first, err := os.Create(filepath.Join(dir, "a.bin"))
	require.NoError(t, err)
	defer first.Close()
	require.NoError(t, first.Truncate(4096))

	data, handle, err := Mmap(first, 4096)
	require.NoError(t, err)
	addr := &data[0]

	require.NoError(t, Poison(data, handle))

	// Map enough further files to make address reuse likely had the range
	// been released.
	for i := range 32 {
		f, err := os.Create(filepath.Join(dir, strings.Repeat("b", i+1)+".bin"))
		require.NoError(t, err)
		require.NoError(t, f.Truncate(4096))
		next, _, err := Mmap(f, 4096)
		require.NoError(t, err)
		require.NotSame(t, addr, &next[0],
			"a later file was handed the poisoned address — a stale read there would "+
				"return this file's bytes instead of faulting")
		f.Close()
	}
}
