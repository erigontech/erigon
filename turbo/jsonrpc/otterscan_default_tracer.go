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

package jsonrpc

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/vm"
)

// Helper implementation of vm.Tracer; since the interface is big and most
// custom tracers implement just a few of the methods, this is a base struct
// to avoid lots of empty boilerplate code
type DefaultTracer struct {
}

func (t *DefaultTracer) CaptureTxStart(gasLimit uint64) {}

func (t *DefaultTracer) CaptureTxEnd(restGas uint64) {}

func (t *DefaultTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *DefaultTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *DefaultTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}

func (t *DefaultTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}
