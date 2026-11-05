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
)

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
