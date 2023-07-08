package jsonrpc

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
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
