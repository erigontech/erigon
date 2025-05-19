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
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
)

type OverlayCreateTracer struct {
	contractAddress common.Address
	isCapturing     bool
	code            []byte
	gasCap          uint64
	err             error
	resultCode      []byte
	evm             *vm.EVM
}

func (ct *OverlayCreateTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: nil,
			OnEnter:   ct.OnEnter,
		},
	}
}

// Top call frame
func (ct *OverlayCreateTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.evm = env
}
func (ct *OverlayCreateTracer) CaptureEnd(output []byte, usedGas uint64, err error) {}

// Rest of the frames
func (ct *OverlayCreateTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.isCapturing {
		return
	}

	if (vm.OpCode(typ) == vm.CREATE || vm.OpCode(typ) == vm.CREATE2) && to == ct.contractAddress {
		ct.isCapturing = true
		_, _, _, err := ct.evm.OverlayCreate(vm.AccountRef(from), vm.NewCodeAndHash(ct.code), ct.gasCap, value, to, vm.OpCode(typ), true /* incrementNonce */)
		if err != nil {
			ct.err = err
		} else {
			if result, err := ct.evm.IntraBlockState().GetCode(ct.contractAddress); err != nil {
				ct.resultCode = result
			} else {
				ct.err = err
			}
		}
	}
}
