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

func feeMergeTestWrites(t testing.TB, addr accounts.Address, balance uint64) *state.WriteSet {
	t.Helper()
	ws := &state.WriteSet{}
	ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath},
		Val:         *uint256.NewInt(balance),
	})
	return ws
}

func feeMergeTestExecutor(t testing.TB) *blockExecutor {
	t.Helper()
	return &blockExecutor{feeMergeTemp: map[int]feeMerge{}, blockIO: state.NewVersionedIO(2)}
}

func feeMergeTestAddr(hex string) accounts.Address {
	return accounts.InternAddress(common.HexToAddress(hex))
}

// TestRecordFeeMergeReleasesSupersededTemp pins the three transitions the fee
// merge goes through for one tx: the first merge has no temp to reclaim, a
// revalidation round reclaims the temp it replaces, and a round whose input is
// an execResult's TxOut (after a re-execution replaced the recorded slot)
// reclaims nothing.
func TestRecordFeeMergeReleasesSupersededTemp(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x1111111111111111111111111111111111111111")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	// First round: prev is the worker's TxOut, so nothing may be released.
	txOut := feeMergeTestWrites(t, addr, 1)
	tip1 := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, txOut, tip1)
	require.Same(t, tip1, be.feeMergeTemp[0].writes)
	require.Same(t, tip1, be.blockIO.WriteSet(version.TxIndex),
		"the merge product must be the tx's recorded write set")
	require.Equal(t, 1, txOut.Count(), "TxOut must survive the fee merge")

	// Revalidation round: prev is the temp the first round recorded, so it is
	// superseded and reclaimed.
	tip2 := feeMergeTestWrites(t, addr, 3)
	be.recordFeeMerge(version, tip1, tip2)
	be.awaitMapReleases()
	require.Same(t, tip2, be.feeMergeTemp[0].writes)
	require.Equal(t, 0, tip1.Count(), "superseded fee-merge temp must be released")
	require.Equal(t, 1, tip2.Count())

	// After a re-execution the recorded slot is the new TxOut again, so the
	// stale temp does not match prev and stays untouched.
	txOut2 := feeMergeTestWrites(t, addr, 4)
	tip3 := feeMergeTestWrites(t, addr, 5)
	be.recordFeeMerge(version, txOut2, tip3)
	be.awaitMapReleases()
	require.Same(t, tip3, be.feeMergeTemp[0].writes)
	require.Equal(t, 1, txOut2.Count(), "TxOut must survive the fee merge")
	require.Equal(t, 1, tip2.Count(), "a temp that is not prev must not be released")
}

// TestRecordFeeMergeSkipsEmptyTip covers the round the skip produces: with no
// credit to fold in, the recorded set stays the worker's own output — which
// calcFees must keep reading as the pre-credit balance.
func TestRecordFeeMergeSkipsEmptyTip(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x5555555555555555555555555555555555555555")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, addr, 1)
	be.recordWorkerWrites(version, txOut)
	be.recordFeeMerge(version, txOut, nil)

	require.Same(t, txOut, be.blockIO.WriteSet(version.TxIndex))
	require.Nil(t, be.creditedWrites(version, txOut),
		"a skipped credit must not mark the worker's TxOut as carrying one")
}

// TestRecordWorkerWritesDropsCreditedTemp drives the re-execution path through
// recordWorkerWrites: the new TxOut displaces the credited set, so the credit
// must be gone and the set it lived in reclaimed.
func TestRecordWorkerWritesDropsCreditedTemp(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x4444444444444444444444444444444444444444")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, addr, 1)
	be.recordWorkerWrites(version, txOut)
	require.Nil(t, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)),
		"the worker's own output carries no credit")

	tip := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, txOut, tip)
	require.Same(t, tip, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)))

	reTxOut := feeMergeTestWrites(t, addr, 3)
	be.recordWorkerWrites(version, reTxOut)
	be.awaitMapReleases()

	require.Nil(t, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)),
		"a re-executed tx must be credited again, not handed the stale credit")
	require.Equal(t, 0, tip.Count(), "the displaced fee-merge temp must be released")
	require.Equal(t, 1, reTxOut.Count(), "the new TxOut must survive")
}

// TestCreditedWritesPinsVersion pins the credit to the incarnation it was
// computed for, so a re-executed tx cannot inherit it even if the set it lives
// in is still the recorded one.
func TestCreditedWritesPinsVersion(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x6666666666666666666666666666666666666666")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	tip := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, feeMergeTestWrites(t, addr, 1), tip)
	require.Same(t, tip, be.creditedWrites(version, tip))

	reExecuted := version
	reExecuted.Incarnation = 1
	require.Nil(t, be.creditedWrites(reExecuted, tip),
		"a credit computed for an earlier incarnation is not this incarnation's")

	otherTx := state.Version{TxIndex: 1}
	require.Nil(t, be.creditedWrites(otherTx, tip),
		"another tx's fee-merge product says nothing about this tx")
	require.Nil(t, be.creditedWrites(version, nil),
		"a tx with no writes at all must not read as credited")
}

// TestRecordFeeMergeReleaseKeepsSharedWrites pins what makes the release safe:
// MergeInto shares VersionedWrite pointers rather than the maps holding them,
// so pooling the superseded temp's maps leaves the surviving set readable.
func TestRecordFeeMergeReleaseKeepsSharedWrites(t *testing.T) {
	t.Parallel()

	shared := feeMergeTestAddr("0x2222222222222222222222222222222222222222")
	fresh := feeMergeTestAddr("0x3333333333333333333333333333333333333333")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	temp1 := feeMergeTestWrites(t, shared, 7)
	be.recordFeeMerge(version, feeMergeTestWrites(t, shared, 1), temp1)

	tipWrites := feeMergeTestWrites(t, fresh, 9)
	be.recordFeeMerge(version, temp1, tipWrites)
	be.awaitMapReleases()

	require.Equal(t, 0, temp1.Count())
	require.Same(t, tipWrites, be.blockIO.WriteSet(version.TxIndex))
	vw, ok := tipWrites.GetBalance(shared)
	require.True(t, ok, "entry shared from the released temp must still be reachable")
	require.Equal(t, uint64(7), vw.Val.Uint64())
}

// TestCalcFeesRoundThroughBlockExecutor runs the round the way the apply loop
// does — creditedWrites in, recordFeeMerge out — so the skip is exercised
// against the real feeMergeTemp bookkeeping rather than the fixture's stand-in.
func TestCalcFeesRoundThroughBlockExecutor(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	r := newFeeCreditRound(t, s)
	be := feeMergeTestExecutor(t)
	version := r.task.Version()
	be.recordWorkerWrites(version, r.result.TxOut)

	round := func() *state.WriteSet {
		recorded := be.blockIO.WriteSet(version.TxIndex)
		tip, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, be.creditedWrites(version, recorded))
		require.NoError(t, err)
		be.recordFeeMerge(version, recorded, tip)
		return tip
	}

	first := round()
	require.False(t, first.IsEmpty(), "the first round must credit the tip")
	require.Same(t, first, be.blockIO.WriteSet(version.TxIndex))
	require.True(t, round().IsEmpty(), "the recorded set already carries this credit")
	require.Same(t, first, be.blockIO.WriteSet(version.TxIndex),
		"a skipped round must leave the recorded set alone")
}
