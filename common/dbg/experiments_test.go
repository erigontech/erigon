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

package dbg

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBALCommitmentWarmupWorkersDefault(t *testing.T) {
	require.Equal(t, 1, balCommitmentWarmupWorkersDefault(1))
	require.Equal(t, 6, balCommitmentWarmupWorkersDefault(6))
	require.Equal(t, runtime.GOMAXPROCS(-1), balCommitmentWarmupWorkersDefault(runtime.GOMAXPROCS(-1)))
}

func TestBALCommitmentWarmupReaders(t *testing.T) {
	previousEnabled, previousWorkers := ReadAhead, TrieBALWarmupers
	t.Cleanup(func() {
		ReadAhead = previousEnabled
		TrieBALWarmupers = previousWorkers
	})
	ReadAhead = true

	for _, test := range []struct {
		workers int
		want    int
	}{
		{workers: -1, want: 0},
		{workers: 0, want: 0},
		{workers: 6, want: 6},
	} {
		TrieBALWarmupers = test.workers
		require.Equal(t, test.want, BALCommitmentWarmupReaders())
	}
	ReadAhead = false
	TrieBALWarmupers = 6
	require.Zero(t, BALCommitmentWarmupReaders())
}

func TestReadAheadWorkerReaders(t *testing.T) {
	previousEnabled, previousWorkers := ReadAhead, ReadAheadWorkers
	t.Cleanup(func() {
		ReadAhead = previousEnabled
		ReadAheadWorkers = previousWorkers
	})

	ReadAhead, ReadAheadWorkers = false, 6
	require.Zero(t, ReadAheadWorkerReaders())
	ReadAhead, ReadAheadWorkers = true, -1
	require.Equal(t, 1, ReadAheadWorkerReaders())
	ReadAheadWorkers = 0
	require.Equal(t, 1, ReadAheadWorkerReaders())
	ReadAheadWorkers = 6
	require.Equal(t, 6, ReadAheadWorkerReaders())
}
