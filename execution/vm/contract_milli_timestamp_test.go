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

const milliTimestampJennerTime = 1_800_000_000

var milliTimestampAddr = accounts.InternAddress(common.BytesToAddress([]byte{0x70}))

func newMilliTimestampEVM(t *testing.T, time uint64, milliseconds uint16) *vm.EVM {
	t.Helper()
	cfg := &chain.Config{
		ChainID:               uint256.NewInt(97),
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		JennerTime:            common.NewUint64(milliTimestampJennerTime),
		Parlia:                &chain.ParliaConfig{},
	}
	blockContext := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
		BlockNumber:  1,
		Time:         time,
		Milliseconds: milliseconds,
	}
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	return vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, cfg, vm.Config{})
}

func TestMilliTimestampBeforeJennerIsAnEmptyAccount(t *testing.T) {
	evm := newMilliTimestampEVM(t, milliTimestampJennerTime-1, 750)
	require.NotContains(t, vm.ActivePrecompiles(evm.ChainRules()), milliTimestampAddr)

	ret, gas, _, err := evm.Call(accounts.InternAddress(common.Address{1}), milliTimestampAddr, nil, mdgas.MdGas{Execution: 100_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Empty(t, ret)
	require.Equal(t, uint64(100_000), gas.Execution)
}

func TestMilliTimestampReturnsTheBlockMilliTimestamp(t *testing.T) {
	const want = milliTimestampJennerTime*1000 + 750
	evm := newMilliTimestampEVM(t, milliTimestampJennerTime, 750)
	require.Contains(t, vm.ActivePrecompiles(evm.ChainRules()), milliTimestampAddr)

	caller := accounts.InternAddress(common.Address{1})
	input := []byte{0xde, 0xad, 0xbe, 0xef}
	for name, call := range map[string]func(gas mdgas.MdGas) ([]byte, mdgas.MdGas, error){
		"CALL": func(gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
			ret, left, _, err := evm.Call(caller, milliTimestampAddr, input, gas, uint256.Int{}, false)
			return ret, left, err
		},
		"CALLCODE": func(gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
			ret, left, _, err := evm.CallCode(caller, milliTimestampAddr, input, gas, uint256.Int{})
			return ret, left, err
		},
		"DELEGATECALL": func(gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
			ret, left, _, err := evm.DelegateCall(caller, caller, milliTimestampAddr, input, uint256.Int{}, gas)
			return ret, left, err
		},
		"STATICCALL": func(gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
			ret, left, _, err := evm.StaticCall(caller, milliTimestampAddr, input, gas)
			return ret, left, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			ret, gas, err := call(mdgas.MdGas{Execution: 100_000})
			require.NoError(t, err)
			require.Equal(t, uint256.NewInt(want).Bytes32(), [32]byte(ret))
			require.Equal(t, uint64(100_000-20), gas.Execution)
		})
	}
}

func TestMilliTimestampOutOfGas(t *testing.T) {
	evm := newMilliTimestampEVM(t, milliTimestampJennerTime, 0)

	_, gas, _, err := evm.StaticCall(accounts.InternAddress(common.Address{1}), milliTimestampAddr, nil, mdgas.MdGas{Execution: 19})
	require.ErrorIs(t, err, vm.ErrOutOfGas)
	require.Zero(t, gas.Execution)
}
