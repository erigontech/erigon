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

package httpcfg

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadTxLimitCoversExecReaders(t *testing.T) {
	t.Parallel()
	// The limit must exceed every long-lived read tx a parallel batch holds
	// (see execPermanentReadTxs and execReadAheadTxs) even when GOMAXPROCS is
	// set below NumCPU and shrinks the derived default.
	require.Greater(t, RoTxsLimit(0, runtime.NumCPU()), int64(runtime.NumCPU()+execPermanentReadTxs+execReadAheadTxs))
}

func TestRoTxsLimit(t *testing.T) {
	t.Parallel()
	defaultLimit := int64(DefaultDBReadConcurrency())
	floor := func(workers int) int64 {
		return int64(workers + execPermanentReadTxs + execReadAheadTxs + dbReadTxsReserved)
	}
	for _, tc := range []struct {
		name         string
		cfg, workers int
		want         int64
	}{
		{"default passes through when above floor", 0, 4, defaultLimit},
		{"high explicit value passes through", 5000, 8, 5000},
		{"low explicit value raised to floor", 8, 64, floor(64)},
		// pins the census: 8 workers + 5 fixed holders + 2 read-ahead + 16 reserve
		{"explicit value equal to worker count raised", 8, 8, 31},
		{"default floored below worker count", 0, int(defaultLimit) + 1, floor(int(defaultLimit) + 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, RoTxsLimit(tc.cfg, tc.workers))
		})
	}
}
