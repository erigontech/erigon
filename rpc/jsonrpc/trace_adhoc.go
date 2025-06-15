// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	math2 "github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/eth/tracers/config"
	ptracer "github.com/erigontech/erigon/polygon/tracer"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/transactions"
)

const (
	CALL               = "call"
	CALLCODE           = "callcode"
	DELEGATECALL       = "delegatecall"
	STATICCALL         = "staticcall"
	CREATE             = "create"
	SUICIDE            = "suicide"
	REWARD             = "reward"
	TraceTypeTrace     = "trace"
	TraceTypeStateDiff = "stateDiff"
	TraceTypeVmTrace   = "vmTrace"
)

// TraceCallParam (see SendTxArgs -- this allows optional params plus don't use MixedcaseAddress
type TraceCallParam struct {
	From                 *common.Address   `json:"from"`
	To                   *common.Address   `json:"to"`
	Gas                  *hexutil.Uint64   `json:"gas"`
	GasPrice             *hexutil.Big      `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big      `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big      `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.Big      `json:"maxFeePerBlobGas"`
	Value                *hexutil.Big      `json:"value"`
	Data                 hexutil.Bytes     `json:"data"`
	AccessList           *types.AccessList `json:"accessList"`
	txHash               *common.Hash
	traceTypes           []string
	isBorStateSyncTxn    bool
}

// TraceCallResult is the response to `trace_call` method
type TraceCallResult struct {
	Output          hexutil.Bytes                        `json:"output"`
	StateDiff       map[common.Address]*StateDiffAccount `json:"stateDiff"`
	Trace           []*ParityTrace                       `json:"trace"`
	VmTrace         *VmTrace                             `json:"vmTrace"`
	TransactionHash *common.Hash                         `json:"transactionHash,omitempty"`
}

// StateDiffAccount is the part of `trace_call` response that is under "stateDiff" tag
type StateDiffAccount struct {
	Balance interface{}                            `json:"balance"` // Can be either string "=" or mapping "*" => {"from": "hex", "to": "hex"}
	Code    interface{}                            `json:"code"`
	Nonce   interface{}                            `json:"nonce"`
	Storage map[common.Hash]map[string]interface{} `json:"storage"`
}

type StateDiffBalance struct {
	From *hexutil.Big `json:"from"`
	To   *hexutil.Big `json:"to"`
}

type StateDiffCode struct {
	From hexutil.Bytes `json:"from"`
	To   hexutil.Bytes `json:"to"`
}

type StateDiffNonce struct {
	From hexutil.Uint64 `json:"from"`
	To   hexutil.Uint64 `json:"to"`
}

type StateDiffStorage struct {
	From common.Hash `json:"from"`
	To   common.Hash `json:"to"`
}

// VmTrace is the part of `trace_call` response that is under "vmTrace" tag
type VmTrace struct {
	Code hexutil.Bytes `json:"code"`
	Ops  []*VmTraceOp  `json:"ops"`
}

// VmTraceOp is one element of the vmTrace ops trace
type VmTraceOp struct {
	Cost int        `json:"cost"`
	Ex   *VmTraceEx `json:"ex"`
	Pc   int        `json:"pc"`
	Sub  *VmTrace   `json:"sub"`
	Op   string     `json:"op,omitempty"`
	Idx  string     `json:"idx,omitempty"`
}

type VmTraceEx struct {
	Mem   *VmTraceMem   `json:"mem"`
	Push  []string      `json:"push"`
	Store *VmTraceStore `json:"store"`
	Used  int           `json:"used"`
}

type VmTraceMem struct {
	Data string `json:"data"`
	Off  int    `json:"off"`
}

type VmTraceStore struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

// ToMessage converts CallArgs to the Message type used by the core evm
func (args *TraceCallParam) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (*types.Message, error) {
	// Set sender address or use zero address if none specified.
	var addr common.Address
	if args.From != nil {
		addr = *args.From
	}

	// Set default gas & gas price if none were set
	gas := globalGasCap
	if gas == 0 {
		gas = uint64(math.MaxUint64 / 2)
	}
	if args.Gas != nil {
		gas = uint64(*args.Gas)
	}
	if globalGasCap != 0 && globalGasCap < gas {
		log.Warn("Caller gas above allowance, capping", "requested", gas, "cap", globalGasCap)
		gas = globalGasCap
	}
	var (
		gasPrice         *uint256.Int
		gasFeeCap        *uint256.Int
		gasTipCap        *uint256.Int
		maxFeePerBlobGas *uint256.Int
	)
	if baseFee == nil {
		// If there's no basefee, then it must be a non-1559 execution
		gasPrice = new(uint256.Int)
		if args.GasPrice != nil {
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return nil, errors.New("args.GasPrice higher than 2^256-1")
			}
		}
		gasFeeCap, gasTipCap = gasPrice, gasPrice
	} else {
		// A basefee is provided, necessitating 1559-type execution
		if args.GasPrice != nil {
			var overflow bool
			// User specified the legacy gas field, convert to 1559 gas typing
			gasPrice, overflow = uint256.FromBig(args.GasPrice.ToInt())
			if overflow {
				return nil, errors.New("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas fields (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return nil, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return nil, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			// Backfill the legacy gasPrice for EVM execution, unless we're all zeroes
			gasPrice = new(uint256.Int)
			if !gasFeeCap.IsZero() || !gasTipCap.IsZero() {
				gasPrice = math2.U256Min(new(uint256.Int).Add(gasTipCap, baseFee), gasFeeCap)
			} else {
				// This means gasFeeCap == 0, gasTipCap == 0
				gasPrice.Set(baseFee)
				gasFeeCap, gasTipCap = gasPrice, gasPrice
			}
		}
		if args.MaxFeePerBlobGas != nil {
			maxFeePerBlobGas = uint256.MustFromBig(args.MaxFeePerBlobGas.ToInt())
		}
	}
	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			return nil, errors.New("args.Value higher than 2^256-1")
		}
	}
	var data []byte
	if args.Data != nil {
		data = args.Data
	}
	var accessList types.AccessList
	if args.AccessList != nil {
		accessList = *args.AccessList
	}
	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, gasFeeCap, gasTipCap, data, accessList, false /* checkNonce */, false /* isFree */, maxFeePerBlobGas)
	return msg, nil
}

func parseOeTracerConfig(traceConfig *config.TraceConfig) (OeTracerConfig, error) {
	if traceConfig == nil || traceConfig.TracerConfig == nil || *traceConfig.TracerConfig == nil {
		return OeTracerConfig{}, nil
	}

	var config OeTracerConfig
	if err := json.Unmarshal(*traceConfig.TracerConfig, &config); err != nil {
		return OeTracerConfig{}, err
	}

	return config, nil
}

type OeTracerConfig struct {
	IncludePrecompiles bool `json:"includePrecompiles"` // by default Parity/OpenEthereum format does not include precompiles
}

// OeTracer is an OpenEthereum-style tracer
type OeTracer struct {
	r            *TraceCallResult
	traceAddr    []int
	traceStack   []*ParityTrace
	precompile   bool // Whether the last CaptureStart was called with `precompile = true`
	compat       bool // Bug for bug compatibility mode
	lastVmOp     *VmTraceOp
	lastOp       vm.OpCode
	lastMemOff   uint64
	lastMemLen   uint64
	memOffStack  []uint64
	memLenStack  []uint64
	lastOffStack *VmTraceOp
	vmOpStack    []*VmTraceOp // Stack of vmTrace operations as call depth increases
	idx          []string     // Prefix for the "idx" inside operations, for easier navigation
	config       OeTracerConfig
}

// ToTransaction converts CallArgs to the Transaction type used by the core evm
func (args *TraceCallParam) ToTransaction(globalGasCap uint64, baseFee *uint256.Int) (types.Transaction, error) {
	msg, err := args.ToMessage(globalGasCap, baseFee)
	if err != nil {
		return nil, err
	}

	var tx types.Transaction
	switch {
	case args.MaxFeePerGas != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		tx = &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    msg.Nonce(),
				GasLimit: msg.Gas(),
				To:       args.To,
				Value:    msg.Value(),
				Data:     msg.Data(),
			},
			FeeCap:     msg.FeeCap(),
			TipCap:     msg.TipCap(),
			AccessList: al,
		}
	case args.AccessList != nil:
		tx = &types.AccessListTx{
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    msg.Value(),
					Data:     msg.Data(),
				},
				GasPrice: msg.GasPrice(),
			},
			AccessList: *args.AccessList,
		}
	default:
		tx = &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    msg.Nonce(),
				GasLimit: msg.Gas(),
				To:       args.To,
				Value:    msg.Value(),
				Data:     msg.Data(),
			},
			GasPrice: msg.GasPrice(),
		}
	}
	return tx, nil
}

func (ot *OeTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnEnter:  ot.OnEnter,
			OnExit:   ot.OnExit,
			OnOpcode: ot.OnOpcode,
		},
		GetResult: ot.GetResult,
		Stop:      ot.Stop,
	}
}

func (ot *OeTracer) captureStartOrEnter(deep bool, typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ot.r.VmTrace != nil {
		var vmTrace *VmTrace
		if deep {
			var vmT *VmTrace
			if len(ot.vmOpStack) > 0 {
				vmT = ot.vmOpStack[len(ot.vmOpStack)-1].Sub
			} else {
				vmT = ot.r.VmTrace
			}
			if !ot.compat {
				ot.idx = append(ot.idx, fmt.Sprintf("%d-", len(vmT.Ops)-1))
			}
		}
		if ot.lastVmOp != nil {
			vmTrace = &VmTrace{Ops: []*VmTraceOp{}}
			ot.lastVmOp.Sub = vmTrace
			ot.vmOpStack = append(ot.vmOpStack, ot.lastVmOp)
		} else {
			vmTrace = ot.r.VmTrace
		}
		if create {
			vmTrace.Code = common.CopyBytes(input)
			if ot.lastVmOp != nil {
				ot.lastVmOp.Cost += int(gas)
			}
		} else {
			vmTrace.Code = code
		}
	}
	if precompile && deep && (value == nil || value.IsZero()) {
		ot.precompile = true
		if !ot.config.IncludePrecompiles {
			return
		}
	}
	if gas > 500000000 {
		gas = 500000001 - (0x8000000000000000 - gas)
	}
	trace := &ParityTrace{}
	if create {
		trResult := &CreateTraceResult{}
		trace.Type = CREATE
		trResult.Address = new(common.Address)
		copy(trResult.Address[:], to.Bytes())
		trace.Result = trResult
	} else {
		trace.Result = &TraceResult{}
		trace.Type = CALL
	}
	if deep {
		topTrace := ot.traceStack[len(ot.traceStack)-1]
		traceIdx := topTrace.Subtraces
		ot.traceAddr = append(ot.traceAddr, traceIdx)
		topTrace.Subtraces++
		if typ == vm.DELEGATECALL {
			switch action := topTrace.Action.(type) {
			case *CreateTraceAction:
				value, _ = uint256.FromBig(action.Value.ToInt())
			case *CallTraceAction:
				value, _ = uint256.FromBig(action.Value.ToInt())
			}
		}
		if typ == vm.STATICCALL {
			value = uint256.NewInt(0)
		}
	}
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	if create {
		action := CreateTraceAction{}
		action.From = from
		action.Gas.ToInt().SetUint64(gas)
		action.Init = common.CopyBytes(input)
		action.Value.ToInt().Set(value.ToBig())
		trace.Action = &action
	} else if typ == vm.SELFDESTRUCT {
		trace.Type = SUICIDE
		trace.Result = nil
		action := &SuicideTraceAction{}
		action.Address = from
		action.RefundAddress = to
		action.Balance.ToInt().Set(value.ToBig())
		trace.Action = action
	} else {
		action := CallTraceAction{}
		switch typ {
		case vm.CALL:
			action.CallType = CALL
		case vm.CALLCODE:
			action.CallType = CALLCODE
		case vm.DELEGATECALL:
			action.CallType = DELEGATECALL
		case vm.STATICCALL:
			action.CallType = STATICCALL
		}
		action.From = from
		action.To = to
		action.Gas.ToInt().SetUint64(gas)
		action.Input = common.CopyBytes(input)
		action.Value.ToInt().Set(value.ToBig())
		trace.Action = &action
	}
	ot.r.Trace = append(ot.r.Trace, trace)
	ot.traceStack = append(ot.traceStack, trace)
}

func (ot *OeTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	isCreate := vm.OpCode(typ) == vm.CREATE || vm.OpCode(typ) == vm.CREATE2
	ot.captureStartOrEnter(depth != 0 /* deep */, vm.OpCode(typ), from, to, precompile, isCreate, input, gas, &value, code)
}

func (ot *OeTracer) captureEndOrExit(deep bool, output []byte, gasUsed uint64, err error) {
	if ot.r.VmTrace != nil {
		if len(ot.vmOpStack) > 0 {
			ot.lastOffStack = ot.vmOpStack[len(ot.vmOpStack)-1]
			ot.vmOpStack = ot.vmOpStack[:len(ot.vmOpStack)-1]
		}
		if !ot.compat && deep {
			ot.idx = ot.idx[:len(ot.idx)-1]
		}
		if deep {
			ot.lastMemOff = ot.memOffStack[len(ot.memOffStack)-1]
			ot.memOffStack = ot.memOffStack[:len(ot.memOffStack)-1]
			ot.lastMemLen = ot.memLenStack[len(ot.memLenStack)-1]
			ot.memLenStack = ot.memLenStack[:len(ot.memLenStack)-1]
		}
	}
	if ot.precompile {
		ot.precompile = false
		if !ot.config.IncludePrecompiles {
			return
		}
	}
	if !deep {
		ot.r.Output = common.CopyBytes(output)
	}
	ignoreError := false
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	if ot.compat {
		ignoreError = !deep && topTrace.Type == CREATE
	}
	if err != nil && !ignoreError {
		if errors.Is(err, vm.ErrExecutionReverted) {
			topTrace.Error = "Reverted"
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).GasUsed = new(hexutil.Big)
				topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(gasUsed)
				topTrace.Result.(*TraceResult).Output = common.CopyBytes(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
				topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(gasUsed)
				topTrace.Result.(*CreateTraceResult).Code = common.CopyBytes(output)
			}
		} else {
			topTrace.Result = nil
			topTrace.Error = err.Error()
		}
	} else {
		if len(output) > 0 {
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).Output = common.CopyBytes(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).Code = common.CopyBytes(output)
			}
		}
		switch topTrace.Type {
		case CALL:
			topTrace.Result.(*TraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(gasUsed)
		case CREATE:
			topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(gasUsed)
		}
	}
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if deep {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
}

func (ot *OeTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	ot.captureEndOrExit(depth != 0 /* deep */, output, gasUsed, err)
}

func (ot *OeTracer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	memory := scope.MemoryData()
	st := scope.StackData()

	if ot.r.VmTrace != nil {
		var vmTrace *VmTrace
		if len(ot.vmOpStack) > 0 {
			vmTrace = ot.vmOpStack[len(ot.vmOpStack)-1].Sub
		} else {
			vmTrace = ot.r.VmTrace
		}
		if ot.lastVmOp != nil && ot.lastVmOp.Ex != nil {
			// Set the "push" of the last operation
			var showStack int
			switch {
			case ot.lastOp >= vm.PUSH0 && ot.lastOp <= vm.PUSH32:
				showStack = 1
			case ot.lastOp >= vm.SWAP1 && ot.lastOp <= vm.SWAP16:
				showStack = int(ot.lastOp-vm.SWAP1) + 2
			case ot.lastOp >= vm.DUP1 && ot.lastOp <= vm.DUP16:
				showStack = int(ot.lastOp-vm.DUP1) + 2
			}
			switch ot.lastOp {
			case vm.CALLDATALOAD, vm.SLOAD, vm.MLOAD, vm.CALLDATASIZE, vm.LT, vm.GT, vm.DIV, vm.SDIV, vm.SAR, vm.AND, vm.EQ, vm.CALLVALUE, vm.ISZERO,
				vm.ADD, vm.EXP, vm.CALLER, vm.KECCAK256, vm.SUB, vm.ADDRESS, vm.GAS, vm.MUL, vm.RETURNDATASIZE, vm.NOT, vm.SHR, vm.SHL,
				vm.EXTCODESIZE, vm.SLT, vm.OR, vm.NUMBER, vm.PC, vm.TIMESTAMP, vm.BALANCE, vm.SELFBALANCE, vm.MULMOD, vm.ADDMOD, vm.BASEFEE,
				vm.BLOCKHASH, vm.BYTE, vm.XOR, vm.ORIGIN, vm.CODESIZE, vm.MOD, vm.SIGNEXTEND, vm.GASLIMIT, vm.DIFFICULTY, vm.SGT, vm.GASPRICE,
				vm.MSIZE, vm.EXTCODEHASH, vm.SMOD, vm.CHAINID, vm.COINBASE:
				showStack = 1
			}
			for i := showStack - 1; i >= 0; i-- {
				if len(st) > i {
					ot.lastVmOp.Ex.Push = append(ot.lastVmOp.Ex.Push, tracers.StackBack(st, i).Hex())
				}
			}
			// Set the "mem" of the last operation
			var setMem bool
			switch ot.lastOp {
			case vm.MSTORE, vm.MSTORE8, vm.MLOAD, vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY, vm.EXTCODECOPY:
				setMem = true
			}
			if setMem && ot.lastMemLen > 0 {
				// TODO: error handling
				cpy, _ := tracers.GetMemoryCopyPadded(memory, int64(ot.lastMemOff), int64(ot.lastMemLen))
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastVmOp.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
		}
		if ot.lastOffStack != nil {
			ot.lastOffStack.Ex.Used = int(gas)
			if len(st) > 0 {
				ot.lastOffStack.Ex.Push = []string{tracers.StackBack(st, 0).Hex()}
			} else {
				ot.lastOffStack.Ex.Push = []string{}
			}
			if ot.lastMemLen > 0 && memory != nil {
				cpy, _ := tracers.GetMemoryCopyPadded(memory, int64(ot.lastMemOff), int64(ot.lastMemLen))
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastOffStack.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
			ot.lastOffStack = nil
		}
		if ot.lastOp == vm.STOP && vm.OpCode(op) == vm.STOP && len(ot.vmOpStack) == 0 {
			// Looks like OE is "optimising away" the second STOP
			return
		}
		ot.lastVmOp = &VmTraceOp{Ex: &VmTraceEx{}}
		vmTrace.Ops = append(vmTrace.Ops, ot.lastVmOp)
		if !ot.compat {
			var sb strings.Builder
			sb.Grow(len(ot.idx))
			for _, idx := range ot.idx {
				sb.WriteString(idx)
			}
			ot.lastVmOp.Idx = fmt.Sprintf("%s%d", sb.String(), len(vmTrace.Ops)-1)
		}
		ot.lastOp = vm.OpCode(op)
		ot.lastVmOp.Cost = int(cost)
		ot.lastVmOp.Pc = int(pc)
		ot.lastVmOp.Ex.Push = []string{}
		ot.lastVmOp.Ex.Used = int(gas) - int(cost)
		if !ot.compat {
			ot.lastVmOp.Op = vm.OpCode(op).String()
		}
		switch vm.OpCode(op) {
		case vm.MSTORE, vm.MLOAD:
			if len(st) > 0 {
				ot.lastMemOff = tracers.StackBack(st, 0).Uint64()
				ot.lastMemLen = 32
			}
		case vm.MSTORE8:
			if len(st) > 0 {
				ot.lastMemOff = tracers.StackBack(st, 0).Uint64()
				ot.lastMemLen = 1
			}
		case vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY:
			if len(st) > 2 {
				ot.lastMemOff = tracers.StackBack(st, 0).Uint64()
				ot.lastMemLen = tracers.StackBack(st, 2).Uint64()
			}
		case vm.EXTCODECOPY:
			if len(st) > 3 {
				ot.lastMemOff = tracers.StackBack(st, 1).Uint64()
				ot.lastMemLen = tracers.StackBack(st, 3).Uint64()
			}
		case vm.STATICCALL, vm.DELEGATECALL:
			if len(st) > 5 {
				ot.memOffStack = append(ot.memOffStack, tracers.StackBack(st, 4).Uint64())
				ot.memLenStack = append(ot.memLenStack, tracers.StackBack(st, 5).Uint64())
			}
		case vm.CALL, vm.CALLCODE:
			if len(st) > 6 {
				ot.memOffStack = append(ot.memOffStack, tracers.StackBack(st, 5).Uint64())
				ot.memLenStack = append(ot.memLenStack, tracers.StackBack(st, 6).Uint64())
			}
		case vm.CREATE, vm.CREATE2, vm.SELFDESTRUCT:
			// Effectively disable memory output
			ot.memOffStack = append(ot.memOffStack, 0)
			ot.memLenStack = append(ot.memLenStack, 0)
		case vm.SSTORE:
			if len(st) > 1 {
				ot.lastVmOp.Ex.Store = &VmTraceStore{Key: tracers.StackBack(st, 0).Hex(), Val: tracers.StackBack(st, 1).Hex()}
			}
		}
		if ot.lastVmOp.Ex.Used < 0 {
			ot.lastVmOp.Ex = nil
		}
	}
}

func (ot *OeTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

func (ot *OeTracer) Stop(err error) {}

// Implements core/state/StateWriter to provide state diffs
type StateDiff struct {
	sdMap map[common.Address]*StateDiffAccount
}

func (sd *StateDiff) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) DeleteAccount(address common.Address, original *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	accountDiff := sd.sdMap[address]
	if accountDiff == nil {
		accountDiff = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
		sd.sdMap[address] = accountDiff
	}
	m := make(map[string]interface{})
	m["*"] = &StateDiffStorage{From: common.BytesToHash(original.Bytes()), To: common.BytesToHash(value.Bytes())}
	accountDiff.Storage[key] = m
	return nil
}

func (sd *StateDiff) CreateContract(address common.Address) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

// CompareStates uses the addresses accumulated in the sdMap and compares balances, nonces, and codes of the accounts, and fills the rest of the sdMap
func (sd *StateDiff) CompareStates(initialIbs, ibs *state.IntraBlockState) error {
	var toRemove []common.Address
	for addr, accountDiff := range sd.sdMap {
		initialExist, err := initialIbs.Exist(addr)
		if err != nil {
			return err
		}
		exist, err := ibs.Exist(addr)
		if err != nil {
			return err
		}
		if initialExist {
			if exist {
				var allEqual = len(accountDiff.Storage) == 0
				ifromBalance, err := initialIbs.GetBalance(addr)
				if err != nil {
					return err
				}
				fromBalance := ifromBalance.ToBig()
				itoBalance, err := ibs.GetBalance(addr)
				if err != nil {
					return err
				}
				toBalance := itoBalance.ToBig()
				if fromBalance.Cmp(toBalance) == 0 {
					accountDiff.Balance = "="
				} else {
					m := make(map[string]*StateDiffBalance)
					m["*"] = &StateDiffBalance{From: (*hexutil.Big)(fromBalance), To: (*hexutil.Big)(toBalance)}
					accountDiff.Balance = m
					allEqual = false
				}
				fromCode, err := initialIbs.GetCode(addr)
				if err != nil {
					return err
				}
				toCode, err := ibs.GetCode(addr)
				if err != nil {
					return err
				}
				if bytes.Equal(fromCode, toCode) {
					accountDiff.Code = "="
				} else {
					m := make(map[string]*StateDiffCode)
					m["*"] = &StateDiffCode{From: fromCode, To: toCode}
					accountDiff.Code = m
					allEqual = false
				}
				fromNonce, err := initialIbs.GetNonce(addr)
				if err != nil {
					return err
				}
				toNonce, err := ibs.GetNonce(addr)
				if err != nil {
					return err
				}
				if fromNonce == toNonce {
					accountDiff.Nonce = "="
				} else {
					m := make(map[string]*StateDiffNonce)
					m["*"] = &StateDiffNonce{From: hexutil.Uint64(fromNonce), To: hexutil.Uint64(toNonce)}
					accountDiff.Nonce = m
					allEqual = false
				}
				if allEqual {
					toRemove = append(toRemove, addr)
				}
			} else {
				{
					balance, err := initialIbs.GetBalance(addr)
					if err != nil {
						return err
					}
					m := make(map[string]*hexutil.Big)
					m["-"] = (*hexutil.Big)(balance.ToBig())
					accountDiff.Balance = m
				}
				{
					code, err := initialIbs.GetCode(addr)
					if err != nil {
						return err
					}
					m := make(map[string]hexutil.Bytes)
					m["-"] = code
					accountDiff.Code = m
				}
				{
					nonce, err := initialIbs.GetNonce(addr)
					if err != nil {
						return err
					}
					m := make(map[string]hexutil.Uint64)
					m["-"] = hexutil.Uint64(nonce)
					accountDiff.Nonce = m
				}
			}
		} else if exist {
			{
				balance, err := ibs.GetBalance(addr)
				if err != nil {
					return err
				}
				m := make(map[string]*hexutil.Big)
				m["+"] = (*hexutil.Big)(balance.ToBig())
				accountDiff.Balance = m
			}
			{
				code, err := ibs.GetCode(addr)
				if err != nil {
					return err
				}
				m := make(map[string]hexutil.Bytes)
				m["+"] = code
				accountDiff.Code = m
			}
			{
				nonce, err := ibs.GetNonce(addr)
				if err != nil {
					return err
				}
				m := make(map[string]hexutil.Uint64)
				m["+"] = hexutil.Uint64(nonce)
				accountDiff.Nonce = m
			}
			// Transform storage
			for _, sm := range accountDiff.Storage {
				str := sm["*"].(*StateDiffStorage)
				delete(sm, "*")
				sm["+"] = &str.To
			}
		} else {
			toRemove = append(toRemove, addr)
		}
	}
	for _, addr := range toRemove {
		delete(sd.sdMap, addr)
	}
	return nil
}

func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) (*TraceCallResult, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	var isBorStateSyncTxn bool
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, err
	}

	if !ok {
		if chainConfig.Bor == nil {
			return nil, nil
		}

		// otherwise this may be a bor state sync transaction - check
		if api.useBridgeReader {
			blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, txHash)
		} else {
			blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, txHash)
		}

		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		isBorStateSyncTxn = true
	}

	header, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(blockNum), tx)
	if err != nil {
		return nil, err
	}

	txNumMin, err := api._txNumReader.Min(tx, blockNum)
	if err != nil {
		return nil, err
	}

	if txNumMin+1 > txNum && !isBorStateSyncTxn {
		return nil, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNum)
	}

	var txnIndex = int(txNum - txNumMin - 1)

	if isBorStateSyncTxn {
		txnIndex = -1
	}

	signer := types.MakeSigner(chainConfig, blockNum, header.Time)
	// Returns an array of trace arrays, one trace array for each transaction
	trace, err := api.callTransaction(ctx, tx, header, traceTypes, txnIndex, *gasBailOut, signer, chainConfig, traceConfig)
	if err != nil {
		return nil, err
	}

	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	for _, traceType := range traceTypes {
		switch traceType {
		case TraceTypeTrace:
			traceTypeTrace = true
		case TraceTypeStateDiff:
			traceTypeStateDiff = true
		case TraceTypeVmTrace:
			traceTypeVmTrace = true
		default:
			return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
		}
	}
	result := &TraceCallResult{}

	result.Output = trace.Output
	if traceTypeTrace {
		result.Trace = trace.Trace
	}
	if traceTypeStateDiff {
		result.StateDiff = trace.StateDiff
	}
	if traceTypeVmTrace {
		result.VmTrace = trace.VmTrace
	}

	return trace, nil
}

func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) ([]*TraceCallResult, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	blockNumber, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	// Extract transactions from block
	block, bErr := api.blockWithSenders(ctx, tx, blockHash, blockNumber)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", blockNumber)
	}
	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	for _, traceType := range traceTypes {
		switch traceType {
		case TraceTypeTrace:
			traceTypeTrace = true
		case TraceTypeStateDiff:
			traceTypeStateDiff = true
		case TraceTypeVmTrace:
			traceTypeVmTrace = true
		default:
			return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
		}
	}

	signer := types.MakeSigner(chainConfig, blockNumber, block.Time())
	// Returns an array of trace arrays, one trace array for each transaction
	traces, _, err := api.callBlock(ctx, tx, block, traceTypes, *gasBailOut, signer, chainConfig, traceConfig)
	if err != nil {
		return nil, err
	}

	result := make([]*TraceCallResult, len(traces))
	for i, trace := range traces {
		tr := &TraceCallResult{}
		tr.Output = trace.Output
		if traceTypeTrace {
			tr.Trace = trace.Trace
		} else {
			tr.Trace = []*ParityTrace{}
		}
		if traceTypeStateDiff {
			tr.StateDiff = trace.StateDiff
		}
		if traceTypeVmTrace {
			tr.VmTrace = trace.VmTrace
		}
		tr.TransactionHash = trace.TransactionHash
		result[i] = tr
	}

	return result, nil
}

// Call implements trace_call.
func (api *TraceAPIImpl) Call(ctx context.Context, args TraceCallParam, traceTypes []string, blockNrOrHash *rpc.BlockNumberOrHash, traceConfig *config.TraceConfig) (*TraceCallResult, error) {
	tx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if blockNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}

	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, *blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, *blockNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	ibs := state.New(stateReader)

	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	header := block.Header()

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if api.evmCallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, api.evmCallTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	for _, traceType := range traceTypes {
		switch traceType {
		case TraceTypeTrace:
			traceTypeTrace = true
		case TraceTypeStateDiff:
			traceTypeStateDiff = true
		case TraceTypeVmTrace:
			traceTypeVmTrace = true
		default:
			return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
		}
	}
	if traceTypeVmTrace {
		traceResult.VmTrace = &VmTrace{Ops: []*VmTraceOp{}}
	}
	var ot OeTracer
	ot.config, err = parseOeTracerConfig(traceConfig)
	if err != nil {
		return nil, err
	}
	ot.compat = api.compatibility
	if traceTypeTrace || traceTypeVmTrace {
		ot.r = traceResult
		ot.traceAddr = []int{}
	}

	// Get a new instance of the EVM.
	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, errors.New("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(api.gasCap, baseFee)
	if err != nil {
		return nil, err
	}
	txn, err := args.ToTransaction(api.gasCap, baseFee)
	if err != nil {
		return nil, err
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
	txCtx := core.NewEVMTxContext(msg)

	blockCtx.GasLimit = math.MaxUint64
	blockCtx.MaxGasLimit = true

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Tracer: ot.Tracer().Hooks})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	var execResult *evmtypes.ExecutionResult
	ibs.SetTxContext(blockCtx.BlockNumber, 0)
	ibs.SetHooks(ot.Tracer().Hooks)

	if ot.Tracer() != nil && ot.Tracer().Hooks.OnTxStart != nil {
		ot.Tracer().OnTxStart(evm.GetVMContext(), txn, msg.From())
	}
	execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */, engine)
	if err != nil {
		if ot.Tracer() != nil && ot.Tracer().Hooks.OnTxEnd != nil {
			ot.Tracer().OnTxEnd(nil, err)
		}
		return nil, err
	}
	if ot.Tracer() != nil && ot.Tracer().Hooks.OnTxEnd != nil {
		ot.Tracer().OnTxEnd(&types.Receipt{GasUsed: execResult.GasUsed}, nil)
	}
	traceResult.Output = common.CopyBytes(execResult.ReturnData)
	if traceTypeStateDiff {
		sdMap := make(map[common.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd := &StateDiff{sdMap: sdMap}
		if err = ibs.FinalizeTx(evm.ChainRules(), sd); err != nil {
			return nil, err
		}
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)
		initialIbs := state.New(stateReader)
		sd.CompareStates(initialIbs, ibs)
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", api.evmCallTimeout)
	}

	return traceResult, nil
}

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls json.RawMessage, parentNrOrHash *rpc.BlockNumberOrHash, traceConfig *config.TraceConfig) ([]*TraceCallResult, error) {
	dbtx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	var callParams []TraceCallParam
	dec := json.NewDecoder(bytes.NewReader(calls))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim('[') {
		return nil, errors.New("expected array of [callparam, tracetypes]")
	}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if tok != json.Delim('[') {
			return nil, errors.New("expected [callparam, tracetypes]")
		}
		callParams = append(callParams, TraceCallParam{})
		args := &callParams[len(callParams)-1]
		if err = dec.Decode(args); err != nil {
			return nil, err
		}
		if err = dec.Decode(&args.traceTypes); err != nil {
			return nil, err
		}
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if tok != json.Delim(']') {
			return nil, errors.New("expected end of [callparam, tracetypes]")
		}
	}
	tok, err = dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim(']') {
		return nil, errors.New("expected end of array of [callparam, tracetypes]")
	}
	var baseFee *uint256.Int
	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, *parentNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	// TODO: can read here only parent header
	parentBlock, err := api.blockWithSenders(ctx, dbtx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if parentBlock == nil {
		return nil, fmt.Errorf("parent block %d(%x) not found", blockNumber, hash)
	}
	parentHeader := parentBlock.Header()
	if parentHeader != nil && parentHeader.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(parentHeader.BaseFee)
		if overflow {
			return nil, errors.New("header.BaseFee uint256 overflow")
		}
	}
	msgs := make([]*types.Message, len(callParams))
	txns := make([]types.Transaction, len(callParams))
	for i, args := range callParams {
		msgs[i], err = args.ToMessage(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to msg: %w", err)
		}

		txns[i], err = args.ToTransaction(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to txn: %w", err)
		}
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, dbtx, api._blockReader, *parentNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}
	stateCache := shards.NewStateCache(
		32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)
	ibs := state.New(cachedReader)

	trace, _, err := api.doCallBlock(ctx, dbtx, stateReader, stateCache, cachedWriter, ibs,
		txns, msgs, callParams, parentNrOrHash, parentHeader, true /* gasBailout */, traceConfig)

	return trace, err
}

func (api *TraceAPIImpl) doCallBlock(ctx context.Context, dbtx kv.Tx, stateReader state.StateReader,
	stateCache *shards.StateCache, cachedWriter state.StateWriter, ibs *state.IntraBlockState,
	txns []types.Transaction, msgs []*types.Message, callParams []TraceCallParam,
	parentNrOrHash *rpc.BlockNumberOrHash, header *types.Header, gasBailout bool,
	traceConfig *config.TraceConfig,
) ([]*TraceCallResult, *tracing.Hooks, error) {
	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, nil, err
	}
	engine := api.engine()

	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	parentBlockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, *parentNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return nil, nil, err
	}
	noop := state.NewNoopWriter()

	parentHeader, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(parentBlockNumber), dbtx)
	if err != nil {
		return nil, nil, err
	}
	if parentHeader == nil {
		return nil, nil, fmt.Errorf("parent header %d(%x) not found", parentBlockNumber, hash)
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if api.evmCallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, api.evmCallTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()
	results := make([]*TraceCallResult, 0, len(msgs))

	useParent := false
	if header == nil {
		header = parentHeader
		useParent = true
	}

	var baseTxNum uint64
	historicalStateReader, isHistoricalStateReader := stateReader.(state.HistoricalStateReader)
	if isHistoricalStateReader {
		baseTxNum = historicalStateReader.GetTxNum()
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, parentNrOrHash.RequireCanonical, dbtx, api._blockReader, chainConfig)
	var tracer *tracers.Tracer
	var tracingHooks *tracing.Hooks

	for txIndex, msg := range msgs {
		if isHistoricalStateReader {
			historicalStateReader.SetTxNum(baseTxNum + uint64(txIndex))
		}
		if err := common.Stopped(ctx.Done()); err != nil {
			return nil, nil, err
		}

		var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
		args := callParams[txIndex]
		for _, traceType := range args.traceTypes {
			switch traceType {
			case TraceTypeTrace:
				traceTypeTrace = true
			case TraceTypeStateDiff:
				traceTypeStateDiff = true
			case TraceTypeVmTrace:
				traceTypeVmTrace = true
			default:
				return nil, nil, fmt.Errorf("unrecognized trace type: %s", traceType)
			}
		}

		traceResult := &TraceCallResult{Trace: []*ParityTrace{}, TransactionHash: args.txHash}
		vmConfig := vm.Config{}
		if traceTypeTrace || traceTypeVmTrace {
			var ot OeTracer
			ot.config, err = parseOeTracerConfig(traceConfig)
			if err != nil {
				return nil, nil, err
			}
			ot.compat = api.compatibility
			ot.r = traceResult
			ot.idx = []string{fmt.Sprintf("%d-", txIndex)}
			if traceTypeTrace {
				ot.traceAddr = []int{}
			}
			if traceTypeVmTrace {
				traceResult.VmTrace = &VmTrace{Ops: []*VmTraceOp{}}
			}
			vmConfig.Tracer = ot.Tracer().Hooks
			tracingHooks = ot.Tracer().Hooks
			tracer = ot.Tracer()
		}

		if useParent {
			blockCtx.GasLimit = math.MaxUint64
			blockCtx.MaxGasLimit = true
		}

		// Clone the state cache before applying the changes for diff after transaction execution, clone is discarded
		var cloneReader state.StateReader
		var sd *StateDiff
		if traceTypeStateDiff {
			cloneCache := stateCache.Clone()
			cloneReader = state.NewCachedReader(stateReader, cloneCache)
			//cloneReader = stateReader
			if isHistoricalStateReader {
				historicalStateReader.SetTxNum(baseTxNum + uint64(txIndex))
			}
			sdMap := make(map[common.Address]*StateDiffAccount)
			traceResult.StateDiff = sdMap
			sd = &StateDiff{sdMap: sdMap}
		}

		ibs.Reset()
		var finalizeTxStateWriter state.StateWriter
		if sd != nil {
			finalizeTxStateWriter = sd
		} else {
			finalizeTxStateWriter = noop
		}

		var txFinalized bool
		var execResult *evmtypes.ExecutionResult
		if args.isBorStateSyncTxn {
			txFinalized = true
			var stateSyncEvents []*types.Message
			stateSyncEvents, err = api.stateSyncEvents(ctx, dbtx, header.Hash(), parentBlockNumber+1, chainConfig)
			if err != nil {
				return nil, nil, err
			}

			execResult, err = ptracer.TraceBorStateSyncTxnTraceAPI(
				ctx,
				&vmConfig,
				chainConfig,
				ibs,
				finalizeTxStateWriter,
				blockCtx,
				header.Hash(),
				header.Number.Uint64(),
				header.Time,
				stateSyncEvents,
				tracer,
			)
		} else {
			ibs.SetTxContext(blockCtx.BlockNumber, txIndex)
			if tracer != nil {
				ibs.SetHooks(tracer.Hooks)
			}
			txCtx := core.NewEVMTxContext(msg)
			evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
			gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())

			if tracer != nil && tracer.Hooks.OnTxStart != nil {
				tracer.Hooks.OnTxStart(evm.GetVMContext(), txns[txIndex], msg.From())
			}
			execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /* gasBailout */, engine)
		}
		if err != nil {
			if tracer != nil && tracer.Hooks.OnTxEnd != nil {
				tracer.Hooks.OnTxEnd(nil, err)
			}
			return nil, nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
		}

		if tracer != nil && tracer.Hooks.OnTxEnd != nil {
			tracer.Hooks.OnTxEnd(&types.Receipt{GasUsed: execResult.GasUsed}, nil)
		}

		chainRules := chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time)
		traceResult.Output = common.CopyBytes(execResult.ReturnData)
		if traceTypeStateDiff {
			initialIbs := state.New(cloneReader)
			if !txFinalized {
				if err = ibs.FinalizeTx(chainRules, sd); err != nil {
					return nil, nil, err
				}
			}
			if sd != nil {
				if err = sd.CompareStates(initialIbs, ibs); err != nil {
					return nil, nil, err
				}
			}
			if err = ibs.CommitBlock(chainRules, cachedWriter); err != nil {
				return nil, nil, err
			}
		} else {
			if !txFinalized {
				if err = ibs.FinalizeTx(chainRules, noop); err != nil {
					return nil, nil, err
				}
			}
			if err = ibs.CommitBlock(chainRules, cachedWriter); err != nil {
				return nil, nil, err
			}
		}
		if !traceTypeTrace {
			traceResult.Trace = []*ParityTrace{}
		}
		results = append(results, traceResult)
	}

	return results, tracingHooks, nil
}

func (api *TraceAPIImpl) doCall(ctx context.Context, dbtx kv.Tx, stateReader state.StateReader,
	stateCache *shards.StateCache, cachedWriter state.StateWriter, ibs *state.IntraBlockState,
	msg *types.Message, callParam TraceCallParam,
	parentNrOrHash *rpc.BlockNumberOrHash, header *types.Header, gasBailout bool, txIndex int,
	traceConfig *config.TraceConfig,
) (*TraceCallResult, error) {
	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	parentBlockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, *parentNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	noop := state.NewNoopWriter()

	parentHeader, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(parentBlockNumber), dbtx)
	if err != nil {
		return nil, err
	}
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %d(%x) not found", parentBlockNumber, hash)
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if api.evmCallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, api.evmCallTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	useParent := false
	if header == nil {
		header = parentHeader
		useParent = true
	}

	var baseTxNum uint64
	historicalStateReader, isHistoricalStateReader := stateReader.(state.HistoricalStateReader)
	if isHistoricalStateReader {
		baseTxNum = historicalStateReader.GetTxNum()
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, parentNrOrHash.RequireCanonical, dbtx, api._blockReader, chainConfig)

	if isHistoricalStateReader {
		historicalStateReader.SetTxNum(baseTxNum + uint64(txIndex))
	}
	if err := common.Stopped(ctx.Done()); err != nil {
		return nil, err
	}

	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	args := callParam
	for _, traceType := range args.traceTypes {
		switch traceType {
		case TraceTypeTrace:
			traceTypeTrace = true
		case TraceTypeStateDiff:
			traceTypeStateDiff = true
		case TraceTypeVmTrace:
			traceTypeVmTrace = true
		default:
			return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
		}
	}

	traceResult := &TraceCallResult{Trace: []*ParityTrace{}, TransactionHash: args.txHash}
	vmConfig := vm.Config{}
	var tracer *tracers.Tracer
	if traceTypeTrace || traceTypeVmTrace {
		var ot OeTracer
		ot.config, err = parseOeTracerConfig(traceConfig)
		if err != nil {
			return nil, err
		}
		ot.compat = api.compatibility
		ot.r = traceResult
		ot.idx = []string{fmt.Sprintf("%d-", txIndex)}
		if traceTypeTrace {
			ot.traceAddr = []int{}
		}
		if traceTypeVmTrace {
			traceResult.VmTrace = &VmTrace{Ops: []*VmTraceOp{}}
		}
		vmConfig.Tracer = ot.Tracer().Hooks
		tracer = ot.Tracer()
	}

	if useParent {
		blockCtx.GasLimit = math.MaxUint64
		blockCtx.MaxGasLimit = true
	}

	// Clone the state cache before applying the changes for diff after transaction execution, clone is discarded
	var cloneReader state.StateReader
	var sd *StateDiff
	if traceTypeStateDiff {
		cloneCache := stateCache.Clone()
		cloneReader = state.NewCachedReader(stateReader, cloneCache)
		//cloneReader = stateReader
		if isHistoricalStateReader {
			historicalStateReader.SetTxNum(baseTxNum + uint64(txIndex))
		}
		sdMap := make(map[common.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd = &StateDiff{sdMap: sdMap}
	}

	ibs.Reset()
	var finalizeTxStateWriter state.StateWriter
	if sd != nil {
		finalizeTxStateWriter = sd
	} else {
		finalizeTxStateWriter = noop
	}

	var txFinalized bool
	var execResult *evmtypes.ExecutionResult
	if args.isBorStateSyncTxn {
		txFinalized = true
		var stateSyncEvents []*types.Message
		stateSyncEvents, err = api.stateSyncEvents(ctx, dbtx, header.Hash(), parentBlockNumber+1, chainConfig)
		if err != nil {
			return nil, err
		}

		execResult, err = ptracer.TraceBorStateSyncTxnTraceAPI(
			ctx,
			&vmConfig,
			chainConfig,
			ibs,
			finalizeTxStateWriter,
			blockCtx,
			header.Hash(),
			header.Number.Uint64(),
			header.Time,
			stateSyncEvents,
			tracer,
		)
	} else {
		ibs.SetTxContext(blockCtx.BlockNumber, txIndex)
		txCtx := core.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())

		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /*gasBailout*/, engine)
	}
	if err != nil {
		return nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
	}

	chainRules := chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time)
	traceResult.Output = common.CopyBytes(execResult.ReturnData)
	if traceTypeStateDiff {
		initialIbs := state.New(cloneReader)
		if !txFinalized {
			if err = ibs.FinalizeTx(chainRules, sd); err != nil {
				return nil, err
			}
		}

		if sd != nil {
			if err = sd.CompareStates(initialIbs, ibs); err != nil {
				return nil, err
			}
		}

		if err = ibs.CommitBlock(chainRules, cachedWriter); err != nil {
			return nil, err
		}
	} else {
		if !txFinalized {
			if err = ibs.FinalizeTx(chainRules, noop); err != nil {
				return nil, err
			}
		}
		if err = ibs.CommitBlock(chainRules, cachedWriter); err != nil {
			return nil, err
		}
	}
	if !traceTypeTrace {
		traceResult.Trace = []*ParityTrace{}
	}

	return traceResult, nil
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_rawTransaction")
}
