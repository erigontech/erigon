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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func armAssert(t *testing.T) {
	prev := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = prev })
}

func TestValidationSkipOracleOnReplayedBlock(t *testing.T) {
	armAssert(t)
	fx, err := blockreplay.Load(fixturePath(t))
	require.NoError(t, err)
	require.NotNil(t, fx.Outputs)

	r, closeFn := setupEphemeralReplay(t, fx)
	defer closeFn()
	tx, doms := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()

	require.NoError(t, r.exec(tx, doms))
	require.NoError(t, r.verify(tx, doms, fx.Outputs))
}

const arrivalWriter, arrivalReader = 1, 3

var arrivalAddr = accounts.InternAddress(common.HexToAddress("0x0a"))

// newArrivalExecutor returns an executor whose reader validated a balance read of the
// writer's value, and that result with its stamp.
func newArrivalExecutor(t *testing.T) (*blockExecutor, *exec.TxResult) {
	vm := state.NewVersionMap(nil)
	vm.InitStamps(arrivalReader + 1)
	vm.WriteBalance(arrivalAddr, state.Version{TxIndex: arrivalWriter}, *uint256.NewInt(1), true)

	var rs state.ReadSet
	rs.SetBalance(arrivalAddr, state.VersionedRead[uint256.Int]{
		ReadHeader: state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: arrivalWriter}},
		Val:        *uint256.NewInt(1),
	})
	io := state.NewVersionedIO(arrivalReader + 2)
	io.RecordReads(state.Version{TxIndex: arrivalReader}, rs)
	be := &blockExecutor{
		versionMap: vm,
		blockIO:    io,
		tasks:      []*execTask{{Task: &exec.TxTask{TxIndex: -1}, index: 0}},
	}
	tv := &taskVersion{execTask: &execTask{index: arrivalReader + 1}, version: state.Version{TxIndex: arrivalReader}}
	res := &exec.TxResult{TxIn: rs}
	valid, _, _ := be.selfLoopEvaluate(tv, res)
	require.True(t, valid)
	require.NotZero(t, res.ValStamp, "the worker stamps before validating")
	return be, res
}

func writeBalanceBelowReader(vm *state.VersionMap) {
	ws := &state.WriteSet{}
	ws.SetBalance(arrivalAddr, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: arrivalAddr, Version: state.Version{TxIndex: arrivalReader - 1}, Path: state.BalancePath},
		Val:         *uint256.NewInt(2),
	})
	vm.FlushVersionedWrites(ws, true)
}

func TestArrivalValidSkipsOnlyWhenNoLowerWrite(t *testing.T) {
	be, res := newArrivalExecutor(t)
	require.True(t, be.arrivalValid(arrivalReader, res.ValStamp))
	require.Equal(t, 1, be.cntValidationSkip)

	writeBalanceBelowReader(be.versionMap)
	require.False(t, be.arrivalValid(arrivalReader, res.ValStamp),
		"a lower write after the stamp forces re-validation, which sees the stale read")
	require.Equal(t, 1, be.cntValidationSkip)
}

func TestArrivalValidOracleIgnoresLowerWriteAfterSkip(t *testing.T) {
	armAssert(t)
	be, res := newArrivalExecutor(t)
	be.oracleHook = func() { writeBalanceBelowReader(be.versionMap) }
	require.NotPanics(t, func() { require.True(t, be.arrivalValid(arrivalReader, res.ValStamp)) },
		"a lower write landing after the skip decision is finalize's to catch, not the oracle's")
}

func TestDeliveredStampedResultSkipsArrivalValidation(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xa0"))
	b := accounts.InternAddress(common.HexToAddress("0xb1"))
	c := accounts.InternAddress(common.HexToAddress("0xc2"))
	d := accounts.InternAddress(common.HexToAddress("0xd3"))
	s := newDepsTest(t, valueTransfers([]accounts.Address{a, b}, []accounts.Address{c, d}))
	s.stamp = true
	s.deliverTx(0, 0, state.ReadSet{})
	require.True(t, s.be.validateTasks.checkComplete(0))
	require.Equal(t, 1, s.be.cntValidationSkip, "a stamped result with no lower write since must skip arrival validation")
}

func TestArrivalValidOraclePanicsOnStaleSkip(t *testing.T) {
	armAssert(t)
	be, _ := newArrivalExecutor(t)
	writeBalanceBelowReader(be.versionMap)
	require.Panics(t, func() { be.arrivalValid(arrivalReader, be.versionMap.WriteSeq()) },
		"a stamp that misses a lower write must not commit a stale read under ERIGON_ASSERT")
}
