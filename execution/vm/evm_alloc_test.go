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

package vm_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestEVMCallDoesNotHeapAllocatePerFrame(t *testing.T) {
	versionMap := state.NewVersionMap(nil)
	reader := state.NewVersionedStateReader(0, state.ReadSet{}, versionMap, state.NewNoopReader())
	statedb := state.NewWithVersionMap(reader, versionMap)
	defer statedb.Close()
	statedb.SetTxContext(1, -1)

	caller := accounts.InternAddress(common.HexToAddress("0xcafe"))
	codeless := accounts.InternAddress(common.HexToAddress("0xdeadbeef"))
	vmenv := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, statedb, chain.AllProtocolChanges, vm.Config{})

	allocs := testing.AllocsPerRun(200, func() {
		_, _, _, _ = vmenv.Call(caller, codeless, nil, mdgas.MdGas{Execution: 50_000}, uint256.Int{}, false)
	})

	require.Zero(t, allocs,
		"a pointer kept into evm.call's named returns moves them to the heap on every EVM call, "+
			"so the cost lands on all four of Call, CallCode, DelegateCall and StaticCall")
}
