package commands

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/vm"
)

// Helper implementation of vm.Tracer; since the interface is big and most
// custom tracers implement just a few of the methods, this is a base struct
// to avoid lots of empty boilerplate code
type DefaultTracer struct {
}

func (t *DefaultTracer) CaptureTxStart(gasLimit uint64) {}

func (t *DefaultTracer) CaptureTxEnd(restGas uint64) {}

func (t *DefaultTracer) CaptureStart(env vm.VMInterface, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *DefaultTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *DefaultTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}

func (t *DefaultTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}
