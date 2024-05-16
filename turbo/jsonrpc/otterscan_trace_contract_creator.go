package jsonrpc

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
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

func (t *CreateTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.found {
		return
	}
	if vm.OpCode(typ) != vm.CREATE {
		return
	}
	if to != t.target {
		return
	}

	t.found = true
	t.Creator = from
}
