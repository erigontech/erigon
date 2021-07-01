package otterscan

import (
	"bytes"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
)

type TouchTracer struct {
	searchAddr common.Address
	Found      bool
}

func NewTouchTracer(searchAddr common.Address) *TouchTracer {
	return &TouchTracer{
		searchAddr: searchAddr,
	}
}

func (l *TouchTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, codeHash common.Hash) error {
	if !l.Found && (bytes.Equal(l.searchAddr.Bytes(), from.Bytes()) || bytes.Equal(l.searchAddr.Bytes(), to.Bytes())) {
		l.Found = true
	}
	return nil
}

func (l *TouchTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TouchTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TouchTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}

func (l *TouchTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}

func (l *TouchTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *TouchTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
