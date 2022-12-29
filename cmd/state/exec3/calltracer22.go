package exec3

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
)

type CallTracer struct {
	froms map[common.Address]struct{}
	tos   map[common.Address]struct{}
}

func NewCallTracer() *CallTracer {
	return &CallTracer{
		froms: map[common.Address]struct{}{},
		tos:   map[common.Address]struct{}{},
	}
}
func (ct *CallTracer) Froms() map[common.Address]struct{} { return ct.froms }
func (ct *CallTracer) Tos() map[common.Address]struct{}   { return ct.tos }

func (ct *CallTracer) CaptureTxStart(gasLimit uint64) {}
func (ct *CallTracer) CaptureTxEnd(restGas uint64)    {}
func (ct *CallTracer) captureStartOrEnter(from, to common.Address) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = struct{}{}
}
func (ct *CallTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.captureStartOrEnter(from, to)
}
func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.captureStartOrEnter(from, to)
}
func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}
func (ct *CallTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}
