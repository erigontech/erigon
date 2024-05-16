package jsonrpc

import (
	"bytes"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/tracing"
)

type TouchTracer struct {
	searchAddr common.Address
	Found      bool
	hooks      *tracing.Hooks
}

func NewTouchTracer(searchAddr common.Address) *TouchTracer {
	tracer := &TouchTracer{
		searchAddr: searchAddr,
	}
	tracer.hooks = &tracing.Hooks{
		OnEnter: tracer.OnEnter,
	}

	return tracer
}

func (t *TouchTracer) TracingHooks() *tracing.Hooks {
	return t.hooks
}

func (t *TouchTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if !t.Found && (bytes.Equal(t.searchAddr.Bytes(), from.Bytes()) || bytes.Equal(t.searchAddr.Bytes(), to.Bytes())) {
		t.Found = true
	}
}
