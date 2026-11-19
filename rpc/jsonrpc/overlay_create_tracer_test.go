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

package jsonrpc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestOverlayCreateTracerRecordsError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		runtimeCode []byte
		wantErr     error
	}{
		{name: "successful creation", runtimeCode: []byte{0xaa}},
		{name: "invalid deployed code", runtimeCode: []byte{0xef}, wantErr: vm.ErrInvalidCode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := accounts.InternAddress(common.Address{0x01})
			target := accounts.InternAddress(types.CreateAddress(caller.Value(), 0))
			statedb := state.New(state.NewNoopReader())
			defer statedb.Close()
			tracer := OverlayCreateTracer{contractAddress: target, statedb: statedb}
			evm := vm.NewEVM(
				evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer},
				evmtypes.TxContext{},
				statedb,
				chain.AllProtocolChanges,
				vm.Config{Tracer: tracer.Tracer().Hooks},
			)
			_, _, _, _, err := evm.Create(caller, program.New().ReturnData(tc.runtimeCode).Bytes(), mdgas.MdGas{Execution: 1_000_000}, uint256.Int{}, nil, false)
			require.ErrorIs(t, err, tc.wantErr)
			require.ErrorIs(t, tracer.err, tc.wantErr)
		})
	}
}
