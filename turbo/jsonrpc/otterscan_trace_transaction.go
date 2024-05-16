package jsonrpc

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func (api *OtterscanAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash) ([]*TraceEntry, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tracer := NewTransactionTracer(ctx)
	if _, err := api.runTracer(ctx, tx, hash, &tracers.Tracer{
		Hooks: tracer.TracingHooks(),
	}); err != nil {
		return nil, err
	}

	return tracer.Results, nil
}

type TraceEntry struct {
	Type   string           `json:"type"`
	Depth  int              `json:"depth"`
	From   common.Address   `json:"from"`
	To     common.Address   `json:"to"`
	Value  *hexutil.Big     `json:"value"`
	Input  hexutility.Bytes `json:"input"`
	Output hexutility.Bytes `json:"output"`
}

type TransactionTracer struct {
	ctx     context.Context
	Results []*TraceEntry
	depth   int // computed from CaptureStart, CaptureEnter, and CaptureExit calls
	stack   []*TraceEntry
	hooks   *tracing.Hooks
}

func NewTransactionTracer(ctx context.Context) *TransactionTracer {
	tracer := &TransactionTracer{
		ctx:     ctx,
		Results: make([]*TraceEntry, 0),
		stack:   make([]*TraceEntry, 0),
	}
	tracer.hooks = &tracing.Hooks{
		OnEnter: tracer.OnEnter,
		OnExit:  tracer.OnExit,
	}

	return tracer
}

func (t *TransactionTracer) TracingHooks() *tracing.Hooks {
	return t.hooks
}

func (t *TransactionTracer) captureStartOrEnter(typ vm.OpCode, from, to common.Address, precompile bool, input []byte, value *uint256.Int) {

}

func (t *TransactionTracer) OnEnter(depth int, typRaw byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.depth = depth
	typ := vm.OpCode(typRaw)
	// t.captureStartOrEnter(vm.OpCode(typ), from, to, precompile, input, value)

	inputCopy := make([]byte, len(input))
	copy(inputCopy, input)
	_value := new(big.Int)
	if value != nil {
		_value.Set(value.ToBig())
	}

	var entry *TraceEntry
	if typ == vm.CALL {
		entry = &TraceEntry{"CALL", t.depth, from, to, (*hexutil.Big)(_value), inputCopy, nil}
	} else if typ == vm.STATICCALL {
		entry = &TraceEntry{"STATICCALL", t.depth, from, to, nil, inputCopy, nil}
	} else if typ == vm.DELEGATECALL {
		entry = &TraceEntry{"DELEGATECALL", t.depth, from, to, nil, inputCopy, nil}
	} else if typ == vm.CALLCODE {
		entry = &TraceEntry{"CALLCODE", t.depth, from, to, (*hexutil.Big)(_value), inputCopy, nil}
	} else if typ == vm.CREATE {
		entry = &TraceEntry{"CREATE", t.depth, from, to, (*hexutil.Big)(value.ToBig()), inputCopy, nil}
	} else if typ == vm.CREATE2 {
		entry = &TraceEntry{"CREATE2", t.depth, from, to, (*hexutil.Big)(value.ToBig()), inputCopy, nil}
	} else if typ == vm.SELFDESTRUCT {
		last := t.Results[len(t.Results)-1]
		entry = &TraceEntry{"SELFDESTRUCT", last.Depth + 1, from, to, (*hexutil.Big)(value.ToBig()), nil, nil}
	} else {
		// safeguard in case new CALL-like opcodes are introduced but not handled,
		// otherwise CaptureExit/stack will get out of sync
		entry = &TraceEntry{"UNKNOWN", t.depth, from, to, (*hexutil.Big)(value.ToBig()), inputCopy, nil}
	}

	// Ignore precompiles in the returned trace (maybe we shouldn't?)
	if !precompile {
		t.Results = append(t.Results, entry)
	}

	// stack precompiles in order to match captureEndOrExit
	t.stack = append(t.stack, entry)
}

func (t *TransactionTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	t.depth = depth

	lastIdx := len(t.stack) - 1
	pop := t.stack[lastIdx]
	t.stack = t.stack[:lastIdx]

	outputCopy := make([]byte, len(output))
	copy(outputCopy, output)
	pop.Output = outputCopy
}
