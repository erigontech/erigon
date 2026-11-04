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
