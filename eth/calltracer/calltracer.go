package calltracer

import (
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
)

type CallTracer struct {
	Froms   map[common.Address]struct{}
	Tos     map[common.Address]bool // address -> isCreated
	HasTEVM func(contractHash common.Hash) (bool, error)
}

func NewCallTracer(hasTEVM func(contractHash common.Hash) (bool, error)) *CallTracer {
	return &CallTracer{
		Froms:   make(map[common.Address]struct{}),
		Tos:     make(map[common.Address]bool),
		HasTEVM: hasTEVM,
	}
}

func (ct *CallTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	ct.Froms[from] = struct{}{}

	created, ok := ct.Tos[to]
	if !ok {
		ct.Tos[to] = false
	}

	if !created && create {
		if len(code) > 0 && ct.HasTEVM != nil {
			has, err := ct.HasTEVM(common.BytesToHash(crypto.Keccak256(code)))
			if !has {
				ct.Tos[to] = true
			}

			if err != nil {
				log.Warn("while CaptureStart", "error", err)
			}
		}
	}
	return nil
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	ct.Froms[from] = struct{}{}
	ct.Tos[to] = false
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
