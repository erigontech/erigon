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

	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/core/vm"
)

type CreateTracer struct {
	DefaultTracer
	ctx     context.Context
	target  common.Address
	found   bool
	Creator common.Address
	Tx      types.Transaction
}

func NewCreateTracer(ctx context.Context, target common.Address) *CreateTracer {
	return &CreateTracer{
		ctx:    ctx,
		target: target,
		found:  false,
	}
}

func (t *CreateTracer) SetTransaction(tx types.Transaction) {
	t.Tx = tx
}

func (t *CreateTracer) Found() bool {
	return t.found
}

func (t *CreateTracer) captureStartOrEnter(from, to common.Address, create bool) {
	if t.found {
		return
	}
	if !create {
		return
	}
	if to != t.target {
		return
	}

	t.found = true
	t.Creator = from
}

func (t *CreateTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to, create)
}

func (t *CreateTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to, create)
}
