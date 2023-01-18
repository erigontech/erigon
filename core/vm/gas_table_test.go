// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

func TestMemoryGasCost(t *testing.T) {
	tests := []struct {
		size     uint64
		cost     uint64
		overflow bool
	}{
		{0x1fffffffe0, 36028809887088637, false},
		{0x1fffffffe1, 0, true},
	}
	for i, tt := range tests {
		v, err := memoryGasCost(&Memory{}, tt.size)
		if (err == ErrGasUintOverflow) != tt.overflow {
			t.Errorf("test %d: overflow mismatch: have %v, want %v", i, err == ErrGasUintOverflow, tt.overflow)
		}
		if v != tt.cost {
			t.Errorf("test %d: gas cost mismatch: have %v, want %v", i, v, tt.cost)
		}
	}
}

var eip2200Tests = []struct {
	original byte
	gaspool  uint64
	input    string
	used     uint64
	refund   uint64
	failure  error
}{
	{0, math.MaxUint64, "0x60006000556000600055", 1612, 0, nil},                // 0 -> 0 -> 0
	{0, math.MaxUint64, "0x60006000556001600055", 20812, 0, nil},               // 0 -> 0 -> 1
	{0, math.MaxUint64, "0x60016000556000600055", 20812, 19200, nil},           // 0 -> 1 -> 0
	{0, math.MaxUint64, "0x60016000556002600055", 20812, 0, nil},               // 0 -> 1 -> 2
	{0, math.MaxUint64, "0x60016000556001600055", 20812, 0, nil},               // 0 -> 1 -> 1
	{1, math.MaxUint64, "0x60006000556000600055", 5812, 15000, nil},            // 1 -> 0 -> 0
	{1, math.MaxUint64, "0x60006000556001600055", 5812, 4200, nil},             // 1 -> 0 -> 1
	{1, math.MaxUint64, "0x60006000556002600055", 5812, 0, nil},                // 1 -> 0 -> 2
	{1, math.MaxUint64, "0x60026000556000600055", 5812, 15000, nil},            // 1 -> 2 -> 0
	{1, math.MaxUint64, "0x60026000556003600055", 5812, 0, nil},                // 1 -> 2 -> 3
	{1, math.MaxUint64, "0x60026000556001600055", 5812, 4200, nil},             // 1 -> 2 -> 1
	{1, math.MaxUint64, "0x60026000556002600055", 5812, 0, nil},                // 1 -> 2 -> 2
	{1, math.MaxUint64, "0x60016000556000600055", 5812, 15000, nil},            // 1 -> 1 -> 0
	{1, math.MaxUint64, "0x60016000556002600055", 5812, 0, nil},                // 1 -> 1 -> 2
	{1, math.MaxUint64, "0x60016000556001600055", 1612, 0, nil},                // 1 -> 1 -> 1
	{0, math.MaxUint64, "0x600160005560006000556001600055", 40818, 19200, nil}, // 0 -> 1 -> 0 -> 1
	{1, math.MaxUint64, "0x600060005560016000556000600055", 10818, 19200, nil}, // 1 -> 0 -> 1 -> 0
	{1, 2306, "0x6001600055", 2306, 0, ErrOutOfGas},                            // 1 -> 1 (2300 sentry + 2xPUSH)
	{1, 2307, "0x6001600055", 806, 0, nil},                                     // 1 -> 1 (2301 sentry + 2xPUSH)
}

func TestEIP2200(t *testing.T) {

	for i, tt := range eip2200Tests {
		tt := tt
		i := i

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			address := libcommon.BytesToAddress([]byte("contract"))
			_, tx := memdb.NewTestTx(t)

			s := state.New(state.NewPlainStateReader(tx))
			s.CreateAccount(address, true)
			s.SetCode(address, hexutil.MustDecode(tt.input))
			s.SetState(address, &libcommon.Hash{}, *uint256.NewInt(uint64(tt.original)))

			_ = s.CommitBlock(params.AllProtocolChanges.Rules(0, 0), state.NewPlainStateWriter(tx, tx, 0))
			vmctx := evmtypes.BlockContext{
				CanTransfer: func(evmtypes.IntraBlockState, libcommon.Address, *uint256.Int) bool { return true },
				Transfer:    func(evmtypes.IntraBlockState, libcommon.Address, libcommon.Address, *uint256.Int, bool) {},
			}
			vmenv := NewEVM(vmctx, evmtypes.TxContext{}, s, params.AllProtocolChanges, Config{ExtraEips: []int{2200}})

			_, gas, err := vmenv.Call(AccountRef(libcommon.Address{}), address, nil, tt.gaspool, new(uint256.Int), false /* bailout */)
			if !errors.Is(err, tt.failure) {
				t.Errorf("test %d: failure mismatch: have %v, want %v", i, err, tt.failure)
			}
			if used := tt.gaspool - gas; used != tt.used {
				t.Errorf("test %d: gas used mismatch: have %v, want %v", i, used, tt.used)
			}
			if refund := vmenv.IntraBlockState().GetRefund(); refund != tt.refund {
				t.Errorf("test %d: gas refund mismatch: have %v, want %v", i, refund, tt.refund)
			}
		})
	}
}

var createGasTests = []struct {
	code    string
	eip3860 bool
	gasUsed uint64
}{
	// create(0, 0, 0xc000)
	{"0x61C00060006000f0", false, 41225},
	// create(0, 0, 0xc000)
	{"0x61C00060006000f0", true, 44297},
	// create2(0, 0, 0xc000, 0)
	{"0x600061C00060006000f5", false, 50444},
	// create2(0, 0, 0xc000, 0)
	{"0x600061C00060006000f5", true, 53516},
}

func TestCreateGas(t *testing.T) {
	for i, tt := range createGasTests {
		address := libcommon.BytesToAddress([]byte("contract"))
		_, tx := memdb.NewTestTx(t)

		s := state.New(state.NewPlainStateReader(tx))
		s.CreateAccount(address, true)
		s.SetCode(address, hexutil.MustDecode(tt.code))
		_ = s.CommitBlock(params.TestChainConfig.Rules(0, 0), state.NewPlainStateWriter(tx, tx, 0))

		vmctx := evmtypes.BlockContext{
			CanTransfer: func(evmtypes.IntraBlockState, libcommon.Address, *uint256.Int) bool { return true },
			Transfer:    func(evmtypes.IntraBlockState, libcommon.Address, libcommon.Address, *uint256.Int, bool) {},
		}
		config := Config{}
		if tt.eip3860 {
			config.ExtraEips = []int{3860}
		}

		vmenv := NewEVM(vmctx, evmtypes.TxContext{}, s, params.TestChainConfig, config)

		var startGas uint64 = math.MaxUint64
		_, gas, err := vmenv.Call(AccountRef(libcommon.Address{}), address, nil, startGas, new(uint256.Int), false /* bailout */)
		if err != nil {
			t.Errorf("test %d execution failed: %v", i, err)
		}
		if gasUsed := startGas - gas; gasUsed != tt.gasUsed {
			t.Errorf("test %d: gas used mismatch: have %v, want %v", i, gasUsed, tt.gasUsed)
		}
	}
}
