package exec3

import (
	"math/big"
	"time"

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

func (ct *CallTracer) CaptureStart(env *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, callType vm.CallType, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = struct{}{}
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) {
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = struct{}{}
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
