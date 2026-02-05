// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"hash"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	NoRecursion   bool // Disables call, callcode, delegate call and create
	NoBaseFee     bool // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	TraceJumpDest bool // Print transaction hashes where jumpdest analysis was useful
	NoReceipts    bool // Do not calculate receipts
	ReadOnly      bool // Do no perform any block finalisation
	StatelessExec bool // true is certain conditions (like state trie root hash matching) need to be relaxed for stateless EVM execution
	RestoreState  bool // Revert all changes made to the state (useful for constant system calls)

	ExtraEips []int // Additional EIPS that are to be enabled
}

func (vmConfig *Config) HasEip3860(rules *chain.Rules) bool {
	return slices.Contains(vmConfig.ExtraEips, 3860) || rules.IsShanghai
}

// CallContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type CallContext struct {
	gas      uint64
	input    []byte
	Memory   Memory
	Stack    Stack
	Contract *Contract
}

var contextPool = sync.Pool{
	New: func() any {
		return &CallContext{
			Stack: Stack{data: make([]uint256.Int, 0, 16)},
		}
	},
}

func getCallContext(contract *Contract, input []byte, gas uint64) *CallContext {
	ctx, ok := contextPool.Get().(*CallContext)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}

	ctx.gas = gas
	ctx.input = input
	ctx.Contract = contract
	return ctx
}

func (c *CallContext) put() {
	c.Memory.reset()
	c.Stack.Reset()
	c.input = nil
	if c.Contract != nil {
		putContract(c.Contract)
		c.Contract = nil
	}
	contextPool.Put(c)
}

// UseGas attempts the use gas and subtracts it and returns true on success
// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
func (c *CallContext) useGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	if remaining, ok := useGas(c.gas, gas, tracer, reason); ok {
		c.gas = remaining
		return true
	}
	return false
}

func useGas(initial uint64, gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (remaining uint64, ok bool) {
	if initial < gas {
		return initial, false
	}

	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(initial, initial-gas, reason)
	}

	return initial - gas, true
}

// RefundGas refunds gas to the contract
func (c *CallContext) refundGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
	_ = reason

	if gas == 0 {
		return
	}
	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(c.gas, c.gas+gas, reason)
	}
	c.gas += gas
}

// MemoryData returns the underlying memory slice. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) MemoryData() []byte {
	return ctx.Memory.Data()
}

// StackData returns the stack data. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) StackData() []uint256.Int {
	return ctx.Stack.data
}

// Caller returns the current caller.
func (ctx *CallContext) Caller() accounts.Address {
	return ctx.Contract.Caller()
}

// Address returns the address where this scope of execution is taking place.
func (ctx *CallContext) Address() accounts.Address {
	return ctx.Contract.Address()
}

// CallValue returns the value supplied with this call.
func (ctx *CallContext) CallValue() uint256.Int {
	return ctx.Contract.Value()
}

// CallInput returns the input/calldata with this call. Callers must not modify
// the contents of the returned data.
func (ctx *CallContext) CallInput() []byte {
	return ctx.input
}

func (ctx *CallContext) Code() []byte {
	return ctx.Contract.Code
}

func (ctx *CallContext) CodeHash() accounts.CodeHash {
	return ctx.Contract.CodeHash
}

func (ctx *CallContext) Gas() uint64 {
	return ctx.gas
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

func copyJumpTable(jt *JumpTable) *JumpTable {
	var copy JumpTable
	for i, op := range jt {
		if op != nil {
			opCopy := *op
			copy[i] = &opCopy
		}
	}
	return &copy
}

func jumpTable(chainRules *chain.Rules, cfg Config) *JumpTable {
	var jt *JumpTable
	switch {
	case chainRules.IsAmsterdam:
		jt = &amsterdamInstructionSet
	case chainRules.IsOsaka:
		jt = &osakaInstructionSet
	case chainRules.IsBhilai:
		jt = &bhilaiInstructionSet
	case chainRules.IsPrague:
		jt = &pragueInstructionSet
	case chainRules.IsCancun:
		jt = &cancunInstructionSet
	case chainRules.IsNapoli:
		jt = &napoliInstructionSet
	case chainRules.IsShanghai:
		jt = &shanghaiInstructionSet
	case chainRules.IsLondon:
		jt = &londonInstructionSet
	case chainRules.IsBerlin:
		jt = &berlinInstructionSet
	case chainRules.IsIstanbul:
		jt = &istanbulInstructionSet
	case chainRules.IsConstantinople:
		jt = &constantinopleInstructionSet
	case chainRules.IsByzantium:
		jt = &byzantiumInstructionSet
	case chainRules.IsSpuriousDragon:
		jt = &spuriousDragonInstructionSet
	case chainRules.IsTangerineWhistle:
		jt = &tangerineWhistleInstructionSet
	case chainRules.IsHomestead:
		jt = &homesteadInstructionSet
	default:
		jt = &frontierInstructionSet
	}
	if len(cfg.ExtraEips) > 0 {
		jt = copyJumpTable(jt)
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	return jt
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas uint64, input []byte, readOnly bool) (_ []byte, _ uint64, err error) {
	exec := newExecutor(evm)
	return exec.run(contract, gas, input, readOnly)
}
