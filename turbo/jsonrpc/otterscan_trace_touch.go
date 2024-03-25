package jsonrpc

import (
	"bytes"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

type TouchTracer struct {
	DefaultTracer
	searchAddr common.Address
	Found      bool
}

func NewTouchTracer(searchAddr common.Address) *TouchTracer {
	return &TouchTracer{
		searchAddr: searchAddr,
	}
}

func (t *TouchTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnEnter: t.OnEnter,
		},
	}
}

func (t *TouchTracer) captureStartOrEnter(from, to common.Address) {
	if !t.Found && (bytes.Equal(t.searchAddr.Bytes(), from.Bytes()) || bytes.Equal(t.searchAddr.Bytes(), to.Bytes())) {
		t.Found = true
	}
}

func (t *TouchTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to)
}
