package commands

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
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

func (t *OperationsTracer) CaptureStart(env *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if depth == 0 {
		return
	}

	if calltype == vm.CALLT && value.Uint64() != 0 {
		t.Results = append(t.Results, &InternalOperation{OP_TRANSFER, from, to, (*hexutil.Big)(value.ToBig())})
		return
	}
	if calltype == vm.CREATET {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE, from, to, (*hexutil.Big)(value.ToBig())})
	}
	if calltype == vm.CREATE2T {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE2, from, to, (*hexutil.Big)(value.ToBig())})
	}
}

func (l *OperationsTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *uint256.Int) {
	l.Results = append(l.Results, &InternalOperation{OP_SELF_DESTRUCT, from, to, (*hexutil.Big)(value.ToBig())})
}
