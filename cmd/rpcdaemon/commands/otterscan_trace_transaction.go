package commands

import (
	"context"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
)

func (api *OtterscanAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash) ([]*TraceEntry, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tracer := NewTransactionTracer(ctx)
	if _, err := api.runTracer(ctx, tx, hash, tracer); err != nil {
		return nil, err
	}

	return tracer.Results, nil
}

type TraceEntry struct {
	Type  string         `json:"type"`
	Depth int            `json:"depth"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
	Input hexutil.Bytes  `json:"input"`
}

type TransactionTracer struct {
	DefaultTracer
	ctx     context.Context
	Results []*TraceEntry
	depth   int // computed from CaptureStart, CaptureEnter, and CaptureExit calls
}

func NewTransactionTracer(ctx context.Context) *TransactionTracer {
	return &TransactionTracer{
		ctx:     ctx,
		Results: make([]*TraceEntry, 0),
	}
}

func (t *TransactionTracer) captureStartOrEnter(from, to common.Address, precompile bool, callType vm.CallType, input []byte, value *uint256.Int) {
	if precompile {
		return
	}

	inputCopy := make([]byte, len(input))
	copy(inputCopy, input)
	_value := new(big.Int)
	_value.Set(value.ToBig())
	if callType == vm.CALLT {
		t.Results = append(t.Results, &TraceEntry{"CALL", t.depth, from, to, (*hexutil.Big)(_value), inputCopy})
		return
	}
	if callType == vm.STATICCALLT {
		t.Results = append(t.Results, &TraceEntry{"STATICCALL", t.depth, from, to, nil, inputCopy})
		return
	}
	if callType == vm.DELEGATECALLT {
		t.Results = append(t.Results, &TraceEntry{"DELEGATECALL", t.depth, from, to, nil, inputCopy})
		return
	}
	if callType == vm.CALLCODET {
		t.Results = append(t.Results, &TraceEntry{"CALLCODE", t.depth, from, to, (*hexutil.Big)(_value), inputCopy})
		return
	}
	if callType == vm.CREATET {
		t.Results = append(t.Results, &TraceEntry{"CREATE", t.depth, from, to, (*hexutil.Big)(value.ToBig()), inputCopy})
		return
	}
	if callType == vm.CREATE2T {
		t.Results = append(t.Results, &TraceEntry{"CREATE2", t.depth, from, to, (*hexutil.Big)(value.ToBig()), inputCopy})
		return
	}
}

func (t *TransactionTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, callType vm.CallType, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.depth = 0
	t.captureStartOrEnter(from, to, precompile, callType, input, value)
}

func (t *TransactionTracer) CaptureEnter(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, callType vm.CallType, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.depth++
	t.captureStartOrEnter(from, to, precompile, callType, input, value)
}

func (t *TransactionTracer) CaptureExit(output []byte, startGas, endGas uint64, d time.Duration, err error) {
	t.depth--
}

func (l *TransactionTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *uint256.Int) {
	last := l.Results[len(l.Results)-1]
	l.Results = append(l.Results, &TraceEntry{"SELFDESTRUCT", last.Depth + 1, from, to, (*hexutil.Big)(value.ToBig()), nil})
}
