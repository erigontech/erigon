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
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

func BenchmarkAccessListTracerSeed(b *testing.B) {
	// Real eth_createAccessList lists are small: a handful of addresses with a
	// few slots each. The wide shapes are here for scale.
	for _, shape := range []struct{ nAddrs, nSlots int }{
		{1, 1}, {1, 5}, {1, 17}, {3, 5}, {5, 20}, {30, 20},
	} {
		prev := NewAccessListTracer(nil, nil, nil)
		for a := range shape.nAddrs {
			address := common.BytesToAddress([]byte{byte(a + 1)})
			for s := range shape.nSlots {
				prev.list.addSlot(address, common.BytesToHash([]byte{byte(s + 1)}))
			}
		}
		// AccessList() is built either way, so only the seeding half differs.
		acl := prev.AccessList()
		name := fmt.Sprintf("%dx%d", shape.nAddrs, shape.nSlots)

		b.Run(name+"/roundTrip", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = NewAccessListTracer(acl, nil, nil)
			}
		})
		b.Run(name+"/seedNew", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = prev.SeedNew(nil)
			}
		})
	}
}

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
