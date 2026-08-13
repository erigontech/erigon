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

// TestRecordFeeMergeReleasesSupersededTemp pins the fee-merge temp's lifecycle
// across a tx's validation rounds: the first round has nothing to reclaim, a
// same-baseline revalidation reclaims the temp it replaces, a round against a
// new baseline (a re-execution's TxOut) still reclaims the stale temp since
// MergeInto only ever reads from the baseline and never from a temp, and a
// round with no fee contribution reverts the recorded set to the baseline
// itself and reclaims the last temp with nothing left to track.
func TestRecordFeeMergeReleasesSupersededTemp(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	be := &blockExecutor{feeMergeTemp: map[int]*state.WriteSet{}}

	txOutA := feeMergeTestWrites(t, addr, 1)

	temp1 := txOutA.MergeInto(feeMergeTestWrites(t, addr, 2))
	be.recordFeeMerge(0, txOutA, temp1)
	require.Same(t, temp1, be.feeMergeTemp[0])

	temp2 := txOutA.MergeInto(feeMergeTestWrites(t, addr, 3))
	be.recordFeeMerge(0, txOutA, temp2)
	be.awaitMapReleases()
	require.Same(t, temp2, be.feeMergeTemp[0])
	require.Equal(t, 0, temp1.Count(), "superseded fee-merge temp must be released")
	require.Equal(t, 1, txOutA.Count(), "txOut must survive the fee merge")

	txOutB := feeMergeTestWrites(t, addr, 4)
	temp3 := txOutB.MergeInto(feeMergeTestWrites(t, addr, 5))
	be.recordFeeMerge(0, txOutB, temp3)
	be.awaitMapReleases()
	require.Same(t, temp3, be.feeMergeTemp[0])
	require.Equal(t, 0, temp2.Count(), "stale fee-merge temp must be released across a new baseline")
	require.Equal(t, 1, txOutB.Count(), "txOut must survive the fee merge")

	be.recordFeeMerge(0, txOutB, txOutB)
	be.awaitMapReleases()
	require.Nil(t, be.feeMergeTemp[0])
	require.Equal(t, 0, temp3.Count(), "superseded fee-merge temp must be released")
	require.Equal(t, 1, txOutB.Count(), "txOut must survive the fee merge")
}

// TestRecordFeeMergeReleaseKeepsSharedWrites pins what makes the release
// safe: MergeInto shares VersionedWrite pointers with txOut rather than
// cloning them, so pooling a superseded temp's maps must leave both the
// surviving merged set and txOut itself readable.
func TestRecordFeeMergeReleaseKeepsSharedWrites(t *testing.T) {
	t.Parallel()

	shared := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	fresh := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	be := &blockExecutor{feeMergeTemp: map[int]*state.WriteSet{}}

	txOut := feeMergeTestWrites(t, shared, 7)

	temp1 := txOut.MergeInto(feeMergeTestWrites(t, fresh, 1))
	be.recordFeeMerge(0, txOut, temp1)

	merged := txOut.MergeInto(feeMergeTestWrites(t, fresh, 2))
	require.NotSame(t, temp1, merged)
	be.recordFeeMerge(0, txOut, merged)
	be.awaitMapReleases()

	require.Equal(t, 0, temp1.Count(), "superseded fee-merge temp must be released")

	vw, ok := merged.GetBalance(shared)
	require.True(t, ok, "entry shared with txOut must still be reachable after releasing a temp that also shared it")
	require.Equal(t, uint64(7), vw.Val.Uint64())

	txOutVW, ok := txOut.GetBalance(shared)
	require.True(t, ok, "txOut itself must survive the release")
	require.Equal(t, uint64(7), txOutVW.Val.Uint64())
}
