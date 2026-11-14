//go:build linux

// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package eliasfano32

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestFallocateAllocatesBlocks(t *testing.T) {
	const size int64 = 8 << 20
	dir := t.TempDir()
	probe, err := os.CreateTemp(dir, "fallocate-probe")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, probe.Close()) })

	probeErr := unix.Fallocate(int(probe.Fd()), 0, 0, size)
	for errors.Is(probeErr, unix.EINTR) {
		probeErr = unix.Fallocate(int(probe.Fd()), 0, 0, size)
	}
	supported := !errors.Is(probeErr, unix.EOPNOTSUPP) && !errors.Is(probeErr, unix.ENOSYS)
	if supported {
		require.NoError(t, probeErr)
	}

	f, err := os.CreateTemp(dir, "fallocate")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	require.NoError(t, fallocate(f, size))
	st, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, size, st.Size())
	lastByte := []byte{1}
	_, err = f.ReadAt(lastByte, size-1)
	require.NoError(t, err)
	require.Equal(t, []byte{0}, lastByte)
	if supported {
		require.GreaterOrEqual(t, st.Sys().(*syscall.Stat_t).Blocks*512, size)
	}
}
