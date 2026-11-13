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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestTxTraceNonceChanges(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	address := accounts.InternAddress(common.Address{19: 1})
	require.NoError(t, ibs.SetNonce(address, 7, tracing.NonceChangeUnspecified))
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	evm.FrameContext = ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameAssert}}}, uint256.Int{})
	require.NoError(t, ibs.SetNonce(address, 8, tracing.NonceChangeUnspecified))
	for _, test := range []struct {
		name  string
		param uint64
		want  uint256.Int
	}{
		{"count", 3, uint256.Int{1}},
		{"address", 14, uint256.Int{1}},
		{"before", 15, uint256.Int{7}},
		{"after", 16, uint256.Int{8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			call := &CallContext{}
			call.Stack.push(uint256.Int{})
			call.Stack.push(uint256.Int{test.param})
			_, _, err := opTxTrace(0, evm, call)
			require.NoError(t, err)
			require.Equal(t, test.want, *call.Stack.peek())
		})
	}
}

func TestTxTraceCodeChanges(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	address := accounts.InternAddress(common.Address{19: 1})
	before, after := []byte{byte(STOP)}, []byte{byte(PUSH0), byte(STOP)}
	require.NoError(t, ibs.SetCode(address, before, tracing.CodeChangeUnspecified))
	beforeHash, err := ibs.GetCodeHash(address)
	require.NoError(t, err)
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	evm.FrameContext = ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameAssert}}}, uint256.Int{})
	require.NoError(t, ibs.SetCode(address, after, tracing.CodeChangeUnspecified))
	afterHash, err := ibs.GetCodeHash(address)
	require.NoError(t, err)
	beforeValue, afterValue := beforeHash.Value(), afterHash.Value()
	for _, test := range []struct {
		name  string
		param uint64
		want  uint256.Int
	}{
		{"count", 2, uint256.Int{1}},
		{"address", 11, uint256.Int{1}},
		{"before", 12, *new(uint256.Int).SetBytes(beforeValue[:])},
		{"after", 13, *new(uint256.Int).SetBytes(afterValue[:])},
	} {
		t.Run(test.name, func(t *testing.T) {
			call := &CallContext{}
			call.Stack.push(uint256.Int{})
			call.Stack.push(uint256.Int{test.param})
			_, _, err := opTxTrace(0, evm, call)
			require.NoError(t, err)
			require.Equal(t, test.want, *call.Stack.peek())
		})
	}
}
