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
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func BenchmarkJsonStreamLogger_OnOpcode(b *testing.B) {
	key := common.BigToHash(common.Big1)
	val := common.BigToHash(common.Big2)
	scope := &mockOpContext{
		memory: bytes.Repeat([]byte{0xab}, 256),
		stack:  []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])},
	}

	stream := jsonstream.New(io.Discard)
	l := NewJsonStreamLogger(&LogConfig{EnableMemory: true}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
		i++
	}
}

// BenchmarkOnOpcodeStackDepth shows the scaling: the saved allocation is per
// stack slot per step, and a real trace is not two slots deep.
func BenchmarkOnOpcodeStackDepth(b *testing.B) {
	for _, depth := range []int{2, 8, 16, 32} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			stack := make([]uint256.Int, depth)
			for i := range stack {
				stack[i].SetUint64(uint64(i)*0x0123456789abcdef + 1)
			}
			scope := &mockOpContext{memory: bytes.Repeat([]byte{0xab}, 256), stack: stack}
			l := NewJsonStreamLogger(&LogConfig{}, context.Background(), jsonstream.New(io.Discard))
			l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

			b.ReportAllocs()
			i := 0
			for b.Loop() {
				l.OnOpcode(uint64(i), byte(vm.ADD), 100, 3, scope, nil, 1, nil)
				i++
				// Nothing else drains this stream, and every iteration appends to it.
				_ = l.stream.Flush()
			}
		})
	}
}

func BenchmarkStackValueWrite(b *testing.B) {
	vals := make([]uint256.Int, 16)
	for i := range vals {
		vals[i].SetUint64(uint64(i)*0x0123456789abcdef + 1)
	}

	b.Run("WriteString_Hex", func(b *testing.B) {
		s := jsonstream.New(io.Discard)
		b.ReportAllocs()
		for b.Loop() {
			for i := range vals {
				s.WriteString(vals[i].Hex())
			}
			_ = s.Flush()
		}
	})
	b.Run("WriteRaw_hexQuoted", func(b *testing.B) {
		l := &JsonStreamLogger{stream: jsonstream.New(io.Discard)}
		b.ReportAllocs()
		for b.Loop() {
			for i := range vals {
				l.stream.WriteRaw(l.hexQuoted(&vals[i]))
			}
			_ = l.stream.Flush()
		}
	})
}

// BenchmarkOnOpcodeStorage covers the shape debug_traceTransaction takes by
// default: two hex strings per touched slot, accumulating across steps.
func BenchmarkOnOpcodeStorage(b *testing.B) {
	for _, slots := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			l := NewJsonStreamLogger(&LogConfig{}, context.Background(), jsonstream.New(io.Discard))
			l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}
			scope := &mockOpContext{}
			for i := range slots {
				key := common.BigToHash(big.NewInt(int64(i + 1)))
				val := common.BigToHash(big.NewInt(int64(1000 + i)))
				scope.stack = []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])}
				l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
				i++
				// Nothing else drains this stream, and every iteration appends to it.
				_ = l.stream.Flush()
			}
		})
	}
}
