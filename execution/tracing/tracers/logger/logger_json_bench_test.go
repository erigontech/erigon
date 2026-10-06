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

package logger

import (
	"io"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
)

func benchJSONLoggerStep(b *testing.B, stackDepth int, memSize int) {
	stack := make([]uint256.Int, stackDepth)
	for i := range stack {
		stack[i] = *uint256.NewInt(uint64(i) * 0x1234_5678)
	}
	scope := &mockOpContext{memory: make([]byte, memSize), stack: stack}
	l := NewJSONLogger(&LogConfig{EnableMemory: true, EnableReturnData: true}, io.Discard)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}
	rData := []byte{0x01, 0x02, 0x03, 0x04}

	b.ReportAllocs()
	for b.Loop() {
		l.OnOpcode(42, byte(vm.SSTORE), 1_000_000, 2100, scope, rData, 3, nil)
	}
}

func BenchmarkJSONLoggerStep(b *testing.B) {
	b.Run("stack=4/mem=64", func(b *testing.B) { benchJSONLoggerStep(b, 4, 64) })
	b.Run("stack=16/mem=256", func(b *testing.B) { benchJSONLoggerStep(b, 16, 256) })
	b.Run("stack=64/mem=1024", func(b *testing.B) { benchJSONLoggerStep(b, 64, 1024) })
}
