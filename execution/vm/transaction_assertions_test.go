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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestTxDiffChargesBeforeDynamicWork(t *testing.T) {
	for _, traced := range []bool{false, true} {
		name := "untraced"
		if traced {
			name = "traced"
		}
		t.Run(name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			t.Cleanup(ibs.Close)
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			config := Config{}
			if traced {
				config.Tracer = &tracing.Hooks{OnOpcode: func(uint64, byte, uint64, uint64, tracing.OpContext, []byte, int, error) {}}
			}
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, cfg, config)
			table := *evm.jt
			evm.jt = &table
			called := false
			dynamicGas := table[TXDIFF].dynamicGas
			table[TXDIFF].dynamicGas = func(evm *EVM, call *CallContext, gas mdgas.MdGas, memory uint64) (mdgas.MdGasCost, error) {
				called = true
				return dynamicGas(evm, call, gas, memory)
			}
			contract := Contract{Code: []byte{byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(TXDIFF)}}
			_, _, _, err := evm.Run(contract, mdgas.MdGas{Execution: 3*GasQuickStep + params.WarmStorageReadCostEIP2929 - 1}, nil, true)
			require.ErrorIs(t, err, ErrOutOfGas)
			require.False(t, called, "insufficient gas must reject TXDIFF before interning operands")
		})
	}
}

func TestTxDiffAccountReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		param uint64
	}{
		{"balance before", 2},
		{"balance after", 3},
		{"code hash before", 4},
		{"code hash after", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address := common.HexToAddress("0x2000")
			readErr := errors.New("account read failed")
			ibs := state.New(beneficiaryErrReader{
				StateReader: state.NewNoopReader(), fail: accounts.InternAddress(address), err: readErr,
			})
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			evm.FrameContext = ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FramePostTx}}}, uint256.Int{})
			call := &CallContext{}
			call.Stack.push(uint256.Int{})
			call.Stack.push(*new(uint256.Int).SetBytes(address[:]))
			call.Stack.push(*uint256.NewInt(tc.param))

			_, _, err := opTxDiff(0, evm, call)
			require.ErrorIs(t, err, readErr)
			require.ErrorIs(t, err, ErrIntraBlockStateFailed)
		})
	}
}
