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
	"context"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
)

type CreateTracer struct {
	ctx     context.Context
	target  common.Address
	found   bool
	Creator common.Address
	Tx      types.Transaction
	hooks   *tracing.Hooks
}

func NewCreateTracer(ctx context.Context, target common.Address) *CreateTracer {
	tracer := &CreateTracer{
		ctx:    ctx,
		target: target,
		found:  false,
	}
	tracer.hooks = &tracing.Hooks{
		OnEnter: tracer.OnEnter,
	}

	return tracer
}

func (t *CreateTracer) TracingHooks() *tracing.Hooks {
	return t.hooks
}

func (t *CreateTracer) SetTransaction(tx types.Transaction) {
	t.Tx = tx
}

func (t *CreateTracer) Found() bool {
	return t.found
}

func (t *CreateTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	if t.found {
		return
	}
	if vm.OpCode(typ) != vm.CREATE && vm.OpCode(typ) != vm.CREATE2 {
		return
	}
	if to != t.target {
		return
	}

	t.found = true
	t.Creator = from
}
