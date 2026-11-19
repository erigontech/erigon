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

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type OverlayCreateTracer struct {
	contractAddress accounts.Address
	isCapturing     bool
	created         bool // A later collision must not replace a successful empty-code deployment.
	depth           int
	err             error
}

func (ct *OverlayCreateTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnEnterV2: ct.OnEnterV2,
			OnExitV2:  ct.OnExitV2,
		},
	}
}

func (ct *OverlayCreateTracer) canCapture(target accounts.Address) bool {
	return target == ct.contractAddress && !ct.isCapturing && !ct.created
}

func (ct *OverlayCreateTracer) onInitCodeOverrideError(target accounts.Address, err error) {
	if ct.canCapture(target) {
		ct.err = err
	}
}

func (ct *OverlayCreateTracer) OnEnterV2(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas mdgas.MdGas, value uint256.Int, code []byte) {
	if (vm.OpCode(typ) == vm.CREATE || vm.OpCode(typ) == vm.CREATE2) && ct.canCapture(to) {
		ct.isCapturing = true
		ct.depth = depth
	}
}

func (ct *OverlayCreateTracer) OnExitV2(depth int, _ []byte, _ mdgas.MdGasUsage, err error, _ bool) {
	if ct.isCapturing && depth == ct.depth {
		ct.err = err
		ct.created = err == nil
		ct.isCapturing = false
	}
}
