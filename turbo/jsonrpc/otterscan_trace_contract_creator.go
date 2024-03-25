package jsonrpc

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
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

func (t *CreateTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnEnter: t.OnEnter,
		},
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

func (t *CreateTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to, vm.OpCode(typ) == vm.CREATE)
}
