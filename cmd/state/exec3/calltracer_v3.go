package exec3

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm"
)

type CallTracer struct {
	froms map[libcommon.Address]struct{}
	tos   map[libcommon.Address]struct{}
}

func NewCallTracer() *CallTracer {
	return &CallTracer{}
}
func (ct *CallTracer) Reset() {
	ct.froms, ct.tos = nil, nil
}
func (ct *CallTracer) Froms() map[libcommon.Address]struct{} { return ct.froms }
func (ct *CallTracer) Tos() map[libcommon.Address]struct{}   { return ct.tos }

func (ct *CallTracer) CaptureTxStart(gasLimit uint64) {}
func (ct *CallTracer) CaptureTxEnd(restGas uint64)    {}
func (ct *CallTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.froms == nil {
		ct.froms = map[libcommon.Address]struct{}{}
		ct.tos = map[libcommon.Address]struct{}{}
	}
	ct.froms[from], ct.tos[to] = struct{}{}, struct{}{}
}
func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.froms == nil {
		ct.froms = map[libcommon.Address]struct{}{}
		ct.tos = map[libcommon.Address]struct{}{}
	}
	ct.froms[from], ct.tos[to] = struct{}{}, struct{}{}
}
func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}
func (ct *CallTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}
