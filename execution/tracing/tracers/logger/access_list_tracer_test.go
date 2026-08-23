// Copyright 2024 The Erigon Authors
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

package logger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var (
	addr = common.BytesToAddress([]byte{0x01, 0x71})

	slot1 = common.BytesToHash([]byte{0x01})
	slot2 = common.BytesToHash([]byte{0x02})
	slot3 = common.BytesToHash([]byte{0x03})
	slot4 = common.BytesToHash([]byte{0x04})

	ordered = types.AccessList{{
		Address: addr,
		StorageKeys: []common.Hash{
			slot1,
			slot2,
			slot3,
			slot4,
		},
	}}
)

func TestTracer_AccessList_Order(t *testing.T) {
	al := newAccessList()
	al.addAddress(addr)
	al.addSlot(addr, slot1)
	al.addSlot(addr, slot4)
	al.addSlot(addr, slot3)
	al.addSlot(addr, slot2)
	require.NotEqual(t, ordered, al.accessList())
	require.Equal(t, ordered, al.accessListSorted())
	require.True(t, al.Equal(al)) //nolint:gocritic
}

func TestNewAccessListTracerExcludedAddress(t *testing.T) {
	excluded := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.HexToHash("0x01")
	prelude := types.AccessList{{
		Address:     excluded,
		StorageKeys: []common.Hash{slot},
	}}
	excl := map[common.Address]struct{}{excluded: {}}
	tracer := NewAccessListTracer(prelude, excl, nil)
	got := tracer.AccessList()
	if len(got) != 0 {
		t.Fatalf("excluded prelude address must not contribute tuples, got %+v", got)
	}
}

type testOpContext struct {
	tracing.OpContext
	stack   []uint256.Int
	address accounts.Address
}

func (c *testOpContext) StackData() []uint256.Int  { return c.stack }
func (c *testOpContext) Address() accounts.Address { return c.address }
func (c *testOpContext) MemoryData() []byte        { return nil }

// countingOpContext counts stack lookups. Kept out of the benchmark: the counter
// runs once per opcode before this change and once per stack-using opcode after,
// so timing it would credit the change with removing its own instrumentation.
type countingOpContext struct {
	testOpContext
	stackReads int
}

func (c *countingOpContext) StackData() []uint256.Int { c.stackReads++; return c.stack }

func TestOnOpcodeReadsStackOnlyWhenUsed(t *testing.T) {
	for _, tc := range []struct {
		op    vm.OpCode
		reads int
	}{
		{vm.ADD, 0},
		{vm.PUSH1, 0},
		{vm.MSTORE, 0},
		{vm.JUMPDEST, 0},
		{vm.POP, 0},
		{vm.CREATE, 0}, // reads the account nonce, not the stack
		{vm.SLOAD, 1},
		{vm.SSTORE, 1},
		{vm.BALANCE, 1},
		{vm.EXTCODESIZE, 1},
		{vm.EXTCODEHASH, 1},
		{vm.EXTCODECOPY, 1},
		{vm.SELFDESTRUCT, 1},
		{vm.CALL, 1},
		{vm.CALLCODE, 1},
		{vm.DELEGATECALL, 1},
		{vm.STATICCALL, 1},
		{vm.CREATE2, 1},
	} {
		t.Run(tc.op.String(), func(t *testing.T) {
			scope := &countingOpContext{testOpContext: testOpContext{
				address: accounts.InternAddress(addr),
				stack:   make([]uint256.Int, 8),
			}}
			NewAccessListTracer(nil, nil, nil).OnOpcode(0, byte(tc.op), 100, 3, scope, nil, 1, nil)
			require.Equal(t, tc.reads, scope.stackReads)
		})
	}
}

// Storage and calls are a small minority of what executes.
var benchOpcodes = func() []byte {
	ops := make([]byte, 0, 256)
	for range 40 {
		ops = append(ops,
			byte(vm.PUSH1), byte(vm.PUSH1), byte(vm.DUP1), byte(vm.SWAP1),
			byte(vm.ADD), byte(vm.MSTORE), byte(vm.JUMPDEST), byte(vm.POP))
	}
	ops = append(ops, byte(vm.SLOAD), byte(vm.SSTORE), byte(vm.CALL), byte(vm.BALANCE))
	return ops
}()

func BenchmarkAccessListTracerOnOpcode(b *testing.B) {
	scope := &testOpContext{
		address: accounts.InternAddress(addr),
		stack:   make([]uint256.Int, 8),
	}
	tracer := NewAccessListTracer(nil, nil, nil)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		tracer.OnOpcode(uint64(i), benchOpcodes[i%len(benchOpcodes)], 100, 3, scope, nil, 1, nil)
		i++
	}
}
