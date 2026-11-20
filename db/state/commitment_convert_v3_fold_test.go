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

package state_test

import (
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestFoldCommitmentV3BoundsRangeBacklog(t *testing.T) {
	const stepSize, steps, ranges = 4, 2, 64
	t.Cleanup(statecfg.SetCommitmentV3(true))
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	addrs, _ := generateInputData(t, length.Addr, 1, 1024)
	for txNum := range uint64(stepSize * steps) {
		for _, a := range addrs[txNum%stepSize*256 : (txNum%stepSize+1)*256] {
			putAccount(a, txNum)
		}
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(2))
	defer state.SetCommitmentV3FoldRangesForTest(state.SetCommitmentV3FoldRangesForTest(ranges))
	var held atomic.Bool
	var ahead, started atomic.Int64
	held.Store(true)
	defer state.SetFoldCommitmentV3RangeHookForTest(func(seq int) {
		started.Add(1)
		if seq == 0 {
			for deadline := time.Now().Add(500 * time.Millisecond); ahead.Load() < ranges-1 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
			held.Store(false)
			return
		}
		if held.Load() {
			ahead.Add(1)
		}
	})()

	_, err := state.FoldCommitmentV3(t.Context(), agg, []uint64{math.MaxUint64}, nil)
	require.NoError(t, err)
	require.Greater(t, started.Load(), int64(ranges/2), "key ranges folded")
	require.Less(t, ahead.Load(), int64(16), "ranges started while range 0 was held")
}
