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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func feeMergeTestWrites(t *testing.T, addr accounts.Address, balance uint64) *state.WriteSet {
	t.Helper()
	ws := &state.WriteSet{}
	ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath},
		Val:         *uint256.NewInt(balance),
	})
	return ws
}

func releaseSuperseded(be *blockExecutor) {
	(&blockResult{supersededWrites: be.supersededWrites}).releaseSupersededWrites()
	be.supersededWrites = nil
}

// TestRecordMergeReleasesSupersededTemp pins which of the three merge rounds a
// tx can go through reclaims its input, and which must leave it alone.
func TestRecordMergeReleasesSupersededTemp(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	be := &blockExecutor{mergedWrites: map[int]*state.WriteSet{}}

	// First round: prev is the worker's TxOut, so nothing may be released.
	txOut := feeMergeTestWrites(t, addr, 1)
	temp1 := feeMergeTestWrites(t, addr, 2)
	be.recordMerge(0, txOut, temp1)
	releaseSuperseded(be)
	require.Same(t, temp1, be.mergedWrites[0])
	require.Equal(t, 1, txOut.Count(), "TxOut must survive the merge")

	// Revalidation round: prev is the temp the first round recorded, so it is
	// superseded and reclaimed.
	temp2 := feeMergeTestWrites(t, addr, 3)
	be.recordMerge(0, temp1, temp2)
	require.Equal(t, 1, temp1.Count(), "superseded temp is reclaimed at the release point, not on the exec loop")
	releaseSuperseded(be)
	require.Same(t, temp2, be.mergedWrites[0])
	require.True(t, temp1.IsEmpty(), "superseded temp must be released")
	require.Equal(t, 1, temp2.Count())

	// After a re-execution the recorded slot is the new TxOut again, so the
	// stale temp does not match prev.
	txOut2 := feeMergeTestWrites(t, addr, 4)
	temp3 := feeMergeTestWrites(t, addr, 5)
	be.recordMerge(0, txOut2, temp3)
	releaseSuperseded(be)
	require.Same(t, temp3, be.mergedWrites[0])
	require.Equal(t, 1, txOut2.Count(), "TxOut must survive the merge")
	require.Equal(t, 1, temp2.Count(), "a temp that is not prev must not be released")
}

// TestRecordMergeReleaseKeepsSharedWrites pins what makes the release safe:
// MergeInto shares VersionedWrite pointers rather than the maps holding them.
func TestRecordMergeReleaseKeepsSharedWrites(t *testing.T) {
	t.Parallel()

	shared := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	fresh := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	be := &blockExecutor{mergedWrites: map[int]*state.WriteSet{}}

	temp1 := feeMergeTestWrites(t, shared, 7)
	be.recordMerge(0, feeMergeTestWrites(t, shared, 1), temp1)

	tipWrites := feeMergeTestWrites(t, fresh, 9)
	merged := temp1.MergeInto(tipWrites)
	require.Same(t, tipWrites, merged)
	be.recordMerge(0, temp1, merged)
	releaseSuperseded(be)

	require.True(t, temp1.IsEmpty())
	vw, ok := merged.GetBalance(shared)
	require.True(t, ok, "entry shared from the released temp must still be reachable")
	require.Equal(t, uint64(7), vw.Val.Uint64())
}

// TestMergeRecordedWritesReclaimsEveryMergeSite pins that the finalize merge
// reclaims what the fee merge left recorded, instead of dropping it stale.
func TestMergeRecordedWritesReclaimsEveryMergeSite(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x4444444444444444444444444444444444444444"))
	be := &blockExecutor{mergedWrites: map[int]*state.WriteSet{}, blockIO: &state.VersionedIO{}}
	txVersion := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, addr, 1)
	be.blockIO.RecordWrites(txVersion, txOut)

	feeMerged := be.mergeRecordedWrites(txVersion, feeMergeTestWrites(t, addr, 2))
	require.Same(t, feeMerged, be.blockIO.WriteSet(0))
	require.Equal(t, 1, txOut.Count(), "TxOut must survive the fee merge")

	finalizeMerged := be.mergeRecordedWrites(txVersion, feeMergeTestWrites(t, addr, 3))
	require.Same(t, finalizeMerged, be.blockIO.WriteSet(0))
	require.Same(t, finalizeMerged, be.mergedWrites[0])
	releaseSuperseded(be)
	require.True(t, feeMerged.IsEmpty(), "the fee merge's set must be reclaimed by the finalize merge")
	require.Equal(t, 1, finalizeMerged.Count())
}
