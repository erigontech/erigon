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
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const arrivalWriter, arrivalReader = 1, 3

var arrivalAddr = accounts.InternAddress(common.HexToAddress("0x0a"))

func newArrivalExecutor(t *testing.T) (*blockExecutor, *exec.TxResult) {
	vm := state.NewVersionMap(nil)
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

// A stale read must not commit on arrival, with or without asserts.
func TestArrivalValidRejectsStaleRead(t *testing.T) {
	prev := dbg.AssertEnabled
	dbg.AssertEnabled = false
	t.Cleanup(func() { dbg.AssertEnabled = prev })
	be, _ := newArrivalExecutor(t)
	writeBalanceBelowReader(be.versionMap)
	require.False(t, be.validateNow(arrivalReader))
}
