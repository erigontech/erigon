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

package iouring

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestBlockingBatchReadReadsFilePages(t *testing.T) {
	r, err := New(8)
	require.NoError(t, err)
	defer r.Close()
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, []byte("0123456789"), 0o600))
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()

	bufs := [][]byte{make([]byte, 4), make([]byte, 4)}
	require.NoError(t, r.BlockingBatchRead(int(f.Fd()), []int64{0, 6}, []int{4, 4}, bufs))
	require.Equal(t, "0123", string(bufs[0]))
	require.Equal(t, "6789", string(bufs[1]))
}

func TestBlockingBatchReadReportsAFailedCompletion(t *testing.T) {
	r, err := New(8)
	require.NoError(t, err)
	defer r.Close()
	dir, err := os.Open(t.TempDir())
	require.NoError(t, err)
	defer dir.Close()

	// Reading a directory completes with -EISDIR.
	err = r.BlockingBatchRead(int(dir.Fd()), []int64{0}, []int{4}, [][]byte{make([]byte, 4)})
	require.ErrorIs(t, err, unix.EISDIR)
	require.ErrorIs(t, err, ErrReadFailed)

	// The ring still reads after a failed completion.
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, []byte("abcd"), 0o600))
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, 4)
	require.NoError(t, r.BlockingBatchRead(int(f.Fd()), []int64{0}, []int{4}, [][]byte{buf}))
	require.Equal(t, "abcd", string(buf))
}

func TestBlockingBatchReadDrainsTheWaveOfAFailedCompletion(t *testing.T) {
	r, err := New(8)
	require.NoError(t, err)
	defer r.Close()
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, []byte("0123456789abcdefghijklmnopqrstuv"), 0o600))
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	read := func(offsets []int64) ([][]byte, error) {
		lens, bufs := make([]int, len(offsets)), make([][]byte, len(offsets))
		for i := range offsets {
			lens[i], bufs[i] = 4, make([]byte, 4)
		}
		return bufs, r.BlockingBatchRead(int(f.Fd()), offsets, lens, bufs)
	}

	// A negative offset completes with -EINVAL while the other reads of its wave succeed.
	_, err = read([]int64{-2, 0, 4, 8, 12, 16, 20, 24})
	require.ErrorIs(t, err, unix.EINVAL)
	require.ErrorIs(t, err, ErrReadFailed)

	// A completion left over from the failed wave would end the next wave before its reads land.
	bufs, err := read([]int64{28, 24, 20, 16, 12, 8, 4, 0})
	require.NoError(t, err)
	require.Equal(t, []string{"stuv", "opqr", "klmn", "ghij", "cdef", "89ab", "4567", "0123"},
		[]string{string(bufs[0]), string(bufs[1]), string(bufs[2]), string(bufs[3]), string(bufs[4]), string(bufs[5]), string(bufs[6]), string(bufs[7])})
}

func TestBlockingBatchReadResetsRingWithNothingInFlight(t *testing.T) {
	r, err := New(8)
	require.NoError(t, err)
	defer r.Close()

	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	scratch := [][]byte{make([]byte, 4096)}
	ringFd := r.fd
	r.fd = -1 // io_uring_enter fails with EBADF before it submits anything
	err = r.BlockingBatchRead(int(f.Fd()), []int64{0}, []int{4096}, scratch)
	r.fd = ringFd
	require.ErrorIs(t, err, ErrRingReset)
	require.Equal(t, atomic.LoadUint32(r.sqHead), atomic.LoadUint32(r.sqTail), "the unsubmitted SQE is dropped")

	require.NoError(t, r.BlockingBatchRead(int(f.Fd()), []int64{0}, []int{4096}, scratch))
}
