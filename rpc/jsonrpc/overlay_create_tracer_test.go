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
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestOverlayCreateTracerCapturesResultCode(t *testing.T) {
	t.Parallel()
	caller := accounts.InternAddress(common.Address{0x01})
	target := accounts.InternAddress(common.Address{0x02})
	evm := vm.NewEVM(
		evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer},
		evmtypes.TxContext{},
		state.New(state.NewNoopReader()),
		chain.AllProtocolChanges,
		vm.Config{},
	)
	tracer := OverlayCreateTracer{
		contractAddress: target,
		code:            program.New().ReturnData([]byte{0xaa}).Bytes(),
		gasCap:          1_000_000,
		evm:             evm,
	}
	tracer.OnEnter(
		0,
		byte(vm.CREATE2),
		caller,
		target,
		false,
		nil,
		tracer.gasCap,
		uint256.Int{},
		nil,
	)
	require.NoError(t, tracer.err)
	require.Equal(t, []byte{0xaa}, tracer.resultCode)
}
