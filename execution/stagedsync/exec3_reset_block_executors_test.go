// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestReleaseResidualVersionMaps pins that a "batch full" exit does not strand
// the queued blocks' pooled cells. Only the executor that completes gets released
// on the exec-loop path; every other block still sitting in the map when the next
// batch starts must hand its version map back too.
func TestReleaseResidualVersionMaps(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xab})

	pe := &parallelExecutor{}
	pe.blockExecutors = map[uint64]*blockExecutor{}
	var residual []*blockExecutor
	for blockNum := range uint64(3) {
		vm := state.NewVersionMap(nil)
		vm.WriteBalance(addr, state.Version{TxIndex: 0}, *uint256.NewInt(7), true)
		_, rr, ok := vm.ReadBalance(addr, 1)
		require.True(t, ok, "block %d: setup must land a readable write", blockNum)
		require.Equal(t, state.MVReadResultDone, rr.Status(), "block %d", blockNum)

		be := &blockExecutor{versionMap: vm}
		pe.blockExecutors[blockNum] = be
		residual = append(residual, be)
	}

	pe.releaseResidualVersionMaps()

	require.Empty(t, pe.blockExecutors, "the stale executors must be dropped")
	for i, be := range residual {
		_, rr, ok := be.versionMap.ReadBalance(addr, 1)
		require.False(t, ok, "residual executor %d: version map must be released", i)
		require.Equal(t, state.MVReadResultNone, rr.Status(), "residual executor %d", i)
	}
}
