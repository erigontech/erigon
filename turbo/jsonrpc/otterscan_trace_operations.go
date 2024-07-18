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
	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon/core/vm"
)

type OperationType int

const (
	OP_TRANSFER      OperationType = 0
	OP_SELF_DESTRUCT OperationType = 1
	OP_CREATE        OperationType = 2
	OP_CREATE2       OperationType = 3
)

type InternalOperation struct {
	Type  OperationType  `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
}

type OperationsTracer struct {
	DefaultTracer
	ctx     context.Context
	Results []*InternalOperation
}

func NewOperationsTracer(ctx context.Context) *OperationsTracer {
	return &OperationsTracer{
		ctx:     ctx,
		Results: make([]*InternalOperation, 0),
	}
}

func (t *OperationsTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if typ == vm.CALL && value.Uint64() != 0 {
		t.Results = append(t.Results, &InternalOperation{OP_TRANSFER, from, to, (*hexutil.Big)(value.ToBig())})
		return
	}
	if typ == vm.CREATE {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE, from, to, (*hexutil.Big)(value.ToBig())})
	}
	if typ == vm.CREATE2 {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE2, from, to, (*hexutil.Big)(value.ToBig())})
	}
	if typ == vm.SELFDESTRUCT {
		t.Results = append(t.Results, &InternalOperation{OP_SELF_DESTRUCT, from, to, (*hexutil.Big)(value.ToBig())})
	}
}
