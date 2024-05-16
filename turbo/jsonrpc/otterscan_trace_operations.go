package jsonrpc

import (
	"context"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
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
	ctx     context.Context
	Results []*InternalOperation
}

func NewOperationsTracer(ctx context.Context) *OperationsTracer {
	return &OperationsTracer{
		ctx:     ctx,
		Results: make([]*InternalOperation, 0),
	}
}

func (t *OperationsTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnEnter: t.OnEnter,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}
}

func (t *OperationsTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if vm.OpCode(typ) == vm.CALL && value.Uint64() != 0 {
		t.Results = append(t.Results, &InternalOperation{OP_TRANSFER, from, to, (*hexutil.Big)(value.ToBig())})
		return
	}
	if vm.OpCode(typ) == vm.CREATE {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE, from, to, (*hexutil.Big)(value.ToBig())})
	}
	if vm.OpCode(typ) == vm.CREATE2 {
		t.Results = append(t.Results, &InternalOperation{OP_CREATE2, from, to, (*hexutil.Big)(value.ToBig())})
	}
	if vm.OpCode(typ) == vm.SELFDESTRUCT {
		t.Results = append(t.Results, &InternalOperation{OP_SELF_DESTRUCT, from, to, (*hexutil.Big)(value.ToBig())})
	}
}

func (t *OperationsTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

func (t *OperationsTracer) Stop(err error) {}
