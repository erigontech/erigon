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
	return &blockExecutor{
		feeMergeTemp: map[int]feeMerge{},
		blockIO:      state.NewVersionedIO(2),
		versionMap:   state.NewVersionMap(nil),
	}
}

func feeMergeTestAddr(hex string) accounts.Address {
	return accounts.InternAddress(common.HexToAddress(hex))
}

func TestRecordFeeMerge_FirstMergeKeepsWorkerWrites(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x1111111111111111111111111111111111111111")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, addr, 1)
	tip := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, txOut, tip, feeCreditNew)
	be.awaitMapReleases()

	require.Same(t, tip, be.blockIO.WriteSet(version.TxIndex),
		"the merge product must become the tx's recorded write set")
	require.Same(t, tip, be.feeMergeTemp[0].writes)
	require.Equal(t, 1, txOut.Count(), "the worker's TxOut stays live and must not be released")
}

func TestRecordFeeMerge_RevalidationReleasesSupersededTemp(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x1111111111111111111111111111111111111111")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	first := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, feeMergeTestWrites(t, addr, 1), first, feeCreditNew)

	second := feeMergeTestWrites(t, addr, 3)
	be.recordFeeMerge(version, first, second, feeCreditNew)
	be.awaitMapReleases()

	require.Same(t, second, be.feeMergeTemp[0].writes)
	require.Equal(t, 0, first.Count(), "the superseded fee-merge temp must be released")
	require.Equal(t, 1, second.Count())
}

func TestRecordFeeMerge_AfterReExecutionKeepsStaleTemp(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x1111111111111111111111111111111111111111")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	stale := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, feeMergeTestWrites(t, addr, 1), stale, feeCreditNew)

	reTxOut := feeMergeTestWrites(t, addr, 4)
	tip := feeMergeTestWrites(t, addr, 5)
	be.recordFeeMerge(version, reTxOut, tip, feeCreditNew)
	be.awaitMapReleases()

	require.Same(t, tip, be.feeMergeTemp[0].writes)
	require.Equal(t, 1, stale.Count(), "a temp that is not prev must not be released")
	require.Equal(t, 1, reTxOut.Count(), "the worker's TxOut stays live and must not be released")
}

func TestRecordFeeMerge_NoCreditKeepsWorkerWrites(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x5555555555555555555555555555555555555555")
	version := state.Version{TxIndex: 0}

	for _, tc := range []struct {
		name    string
		outcome feeOutcome
	}{
		{"already recorded", feeCreditRecorded},
		{"nothing to emit", feeCreditNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be := feeMergeTestExecutor(t)
			txOut := feeMergeTestWrites(t, addr, 1)
			be.recordWorkerWrites(version, txOut)
			be.recordFeeMerge(version, txOut, nil, tc.outcome)

			require.Same(t, txOut, be.blockIO.WriteSet(version.TxIndex))
			require.Nil(t, be.creditedWrites(version, txOut),
				"calcFees must keep reading the worker's TxOut as the pre-credit balance")
		})
	}
}

func TestRecordWorkerWrites_DropsCreditedTemp(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x4444444444444444444444444444444444444444")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, addr, 1)
	be.recordWorkerWrites(version, txOut)
	require.Nil(t, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)),
		"the worker's own output carries no credit")

	tip := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, txOut, tip, feeCreditNew)
	require.Same(t, tip, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)))

	reTxOut := feeMergeTestWrites(t, addr, 3)
	be.recordWorkerWrites(version, reTxOut)
	be.awaitMapReleases()

	require.Nil(t, be.creditedWrites(version, be.blockIO.WriteSet(version.TxIndex)),
		"a re-executed tx must be credited again, not handed the stale credit")
	require.Equal(t, 0, tip.Count(), "the displaced fee-merge temp must be released")
	require.Equal(t, 1, reTxOut.Count(), "the new TxOut must survive")
}

func TestCreditedWrites_PinsVersion(t *testing.T) {
	t.Parallel()

	addr := feeMergeTestAddr("0x6666666666666666666666666666666666666666")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	tip := feeMergeTestWrites(t, addr, 2)
	be.recordFeeMerge(version, feeMergeTestWrites(t, addr, 1), tip, feeCreditNew)
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

func TestRecordFeeMerge_ReleaseKeepsSharedWrites(t *testing.T) {
	t.Parallel()

	shared := feeMergeTestAddr("0x2222222222222222222222222222222222222222")
	fresh := feeMergeTestAddr("0x3333333333333333333333333333333333333333")
	be := feeMergeTestExecutor(t)
	version := state.Version{TxIndex: 0}

	txOut := feeMergeTestWrites(t, shared, 1)
	temp1 := feeMergeTestWrites(t, fresh, 7)
	be.recordFeeMerge(version, txOut, temp1, feeCreditNew)

	tipWrites := feeMergeTestWrites(t, fresh, 9)
	be.recordFeeMerge(version, temp1, tipWrites, feeCreditNew)
	be.awaitMapReleases()

	require.Equal(t, 0, temp1.Count())
	require.Same(t, tipWrites, be.blockIO.WriteSet(version.TxIndex))
	vw, ok := tipWrites.GetBalance(shared)
	require.True(t, ok, "entry shared with the released temp must still be reachable")
	require.Equal(t, uint64(1), vw.Val.Uint64())
}

func TestRecordFeeMerge_RetractsVanishedCredit(t *testing.T) {
	t.Parallel()
	s := zeroTipEmptyCoinbaseScenario()
	r := newFeeCreditRound(t, s)
	version := r.task.Version()

	require.NotNil(t, r.run(t), "an emptied coinbase must be touched")
	merged := r.recorded()
	sd, ok := merged.GetSelfDestruct(s.coinbase)
	require.True(t, ok, "the credit is a SelfDestructPath delete")
	require.True(t, sd.Val)

	r.setPreCreditBalance(s.coinbase, 9_000)
	require.Nil(t, r.run(t), "a funded coinbase leaves this round no adjustment to emit")

	restored := r.recorded()
	require.Same(t, r.result.TxOut, restored,
		"with no adjustment left the recorded set must fall back to the worker's own writes")
	_, ok = restored.GetSelfDestruct(s.coinbase)
	require.False(t, ok, "a delete the round no longer emits must not stay recorded")
	_, _, ok = r.vm.ReadSelfDestruct(s.coinbase, version.TxIndex+1)
	require.False(t, ok, "a delete the round no longer emits must not stay in the version map")
	require.Nil(t, r.credited())

	r.be.awaitMapReleases()
	require.Equal(t, 0, merged.Count(), "the retracted merge product must be released")
}
