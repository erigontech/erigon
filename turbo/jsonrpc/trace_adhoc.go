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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	math2 "github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/polygon/tracer"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
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

// TraceCallParam (see SendTxArgs -- this allows optional prams plus don't use MixedcaseAddress
type TraceCallParam struct {
	From                 *libcommon.Address `json:"from"`
	To                   *libcommon.Address `json:"to"`
	Gas                  *hexutil.Uint64    `json:"gas"`
	GasPrice             *hexutil.Big       `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big       `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big       `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.Big       `json:"maxFeePerBlobGas"`
	Value                *hexutil.Big       `json:"value"`
	Data                 hexutility.Bytes   `json:"data"`
	AccessList           *types2.AccessList `json:"accessList"`
	txHash               *libcommon.Hash
	traceTypes           []string
	isBorStateSyncTxn    bool
}

// TraceCallResult is the response to `trace_call` method
type TraceCallResult struct {
	Output          hexutility.Bytes                        `json:"output"`
	StateDiff       map[libcommon.Address]*StateDiffAccount `json:"stateDiff"`
	Trace           []*ParityTrace                          `json:"trace"`
	VmTrace         *VmTrace                                `json:"vmTrace"`
	TransactionHash *libcommon.Hash                         `json:"transactionHash,omitempty"`
}

// StateDiffAccount is the part of `trace_call` response that is under "stateDiff" tag
type StateDiffAccount struct {
	Balance interface{}                               `json:"balance"` // Can be either string "=" or mapping "*" => {"from": "hex", "to": "hex"}
	Code    interface{}                               `json:"code"`
	Nonce   interface{}                               `json:"nonce"`
	Storage map[libcommon.Hash]map[string]interface{} `json:"storage"`
}

type StateDiffBalance struct {
	From *hexutil.Big `json:"from"`
	To   *hexutil.Big `json:"to"`
}

type StateDiffCode struct {
	From hexutility.Bytes `json:"from"`
	To   hexutility.Bytes `json:"to"`
}

type StateDiffNonce struct {
	From hexutil.Uint64 `json:"from"`
	To   hexutil.Uint64 `json:"to"`
}

type StateDiffStorage struct {
	From libcommon.Hash `json:"from"`
	To   libcommon.Hash `json:"to"`
}

// VmTrace is the part of `trace_call` response that is under "vmTrace" tag
type VmTrace struct {
	Code hexutility.Bytes `json:"code"`
	Ops  []*VmTraceOp     `json:"ops"`
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
func (args *TraceCallParam) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (types.Message, error) {
	// Set sender address or use zero address if none specified.
	var addr libcommon.Address
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
				return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
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
				return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas feilds (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
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
			maxFeePerBlobGas.SetFromBig(args.MaxFeePerBlobGas.ToInt())
		}
	}
	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			return types.Message{}, errors.New("args.Value higher than 2^256-1")
		}
	}
	var data []byte
	if args.Data != nil {
		data = args.Data
	}
	var accessList types2.AccessList
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

func (ot *OeTracer) CaptureTxStart(gasLimit uint64) {}

func (ot *OeTracer) CaptureTxEnd(restGas uint64) {}

func (ot *OeTracer) captureStartOrEnter(deep bool, typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	//fmt.Printf("captureStartOrEnter deep %t, typ %s, from %x, to %x, create %t, input %x, gas %d, value %d, precompile %t\n", deep, typ.String(), from, to, create, input, gas, value, precompile)
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
			vmTrace.Code = libcommon.CopyBytes(input)
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
		trResult.Address = new(libcommon.Address)
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
		action.Init = libcommon.CopyBytes(input)
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
		action.Input = libcommon.CopyBytes(input)
		action.Value.ToInt().Set(value.ToBig())
		trace.Action = &action
	}
	ot.r.Trace = append(ot.r.Trace, trace)
	ot.traceStack = append(ot.traceStack, trace)
}

func (ot *OeTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ot.captureStartOrEnter(false /* deep */, vm.CALL, from, to, precompile, create, input, gas, value, code)
}

func (ot *OeTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ot.captureStartOrEnter(true /* deep */, typ, from, to, precompile, create, input, gas, value, code)
}

func (ot *OeTracer) captureEndOrExit(deep bool, output []byte, usedGas uint64, err error) {
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
		ot.r.Output = libcommon.CopyBytes(output)
	}
	ignoreError := false
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	if ot.compat {
		ignoreError = !deep && topTrace.Type == CREATE
	}
	if err != nil && !ignoreError {
		if err == vm.ErrExecutionReverted {
			topTrace.Error = "Reverted"
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).GasUsed = new(hexutil.Big)
				topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(usedGas)
				topTrace.Result.(*TraceResult).Output = libcommon.CopyBytes(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
				topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(usedGas)
				topTrace.Result.(*CreateTraceResult).Code = libcommon.CopyBytes(output)
			}
		} else {
			topTrace.Result = nil
			topTrace.Error = err.Error()
		}
	} else {
		if len(output) > 0 {
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).Output = libcommon.CopyBytes(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).Code = libcommon.CopyBytes(output)
			}
		}
		switch topTrace.Type {
		case CALL:
			topTrace.Result.(*TraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(usedGas)
		case CREATE:
			topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(usedGas)
		}
	}
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if deep {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
}

func (ot *OeTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	ot.captureEndOrExit(false /* deep */, output, usedGas, err)
}

func (ot *OeTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	ot.captureEndOrExit(true /* deep */, output, usedGas, err)
}

func (ot *OeTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, opDepth int, err error) {
	memory := scope.Memory
	st := scope.Stack

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
				if st.Len() > i {
					ot.lastVmOp.Ex.Push = append(ot.lastVmOp.Ex.Push, st.Back(i).Hex())
				}
			}
			// Set the "mem" of the last operation
			var setMem bool
			switch ot.lastOp {
			case vm.MSTORE, vm.MSTORE8, vm.MLOAD, vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY, vm.EXTCODECOPY:
				setMem = true
			}
			if setMem && ot.lastMemLen > 0 {
				cpy := memory.GetCopy(int64(ot.lastMemOff), int64(ot.lastMemLen))
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastVmOp.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
		}
		if ot.lastOffStack != nil {
			ot.lastOffStack.Ex.Used = int(gas)
			if st.Len() > 0 {
				ot.lastOffStack.Ex.Push = []string{st.Back(0).Hex()}
			} else {
				ot.lastOffStack.Ex.Push = []string{}
			}
			if ot.lastMemLen > 0 && memory != nil {
				cpy := memory.GetCopy(int64(ot.lastMemOff), int64(ot.lastMemLen))
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastOffStack.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
			ot.lastOffStack = nil
		}
		if ot.lastOp == vm.STOP && op == vm.STOP && len(ot.vmOpStack) == 0 {
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
		ot.lastOp = op
		ot.lastVmOp.Cost = int(cost)
		ot.lastVmOp.Pc = int(pc)
		ot.lastVmOp.Ex.Push = []string{}
		ot.lastVmOp.Ex.Used = int(gas) - int(cost)
		if !ot.compat {
			ot.lastVmOp.Op = op.String()
		}
		switch op {
		case vm.MSTORE, vm.MLOAD:
			if st.Len() > 0 {
				ot.lastMemOff = st.Back(0).Uint64()
				ot.lastMemLen = 32
			}
		case vm.MSTORE8:
			if st.Len() > 0 {
				ot.lastMemOff = st.Back(0).Uint64()
				ot.lastMemLen = 1
			}
		case vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY:
			if st.Len() > 2 {
				ot.lastMemOff = st.Back(0).Uint64()
				ot.lastMemLen = st.Back(2).Uint64()
			}
		case vm.EXTCODECOPY:
			if st.Len() > 3 {
				ot.lastMemOff = st.Back(1).Uint64()
				ot.lastMemLen = st.Back(3).Uint64()
			}
		case vm.STATICCALL, vm.DELEGATECALL:
			if st.Len() > 5 {
				ot.memOffStack = append(ot.memOffStack, st.Back(4).Uint64())
				ot.memLenStack = append(ot.memLenStack, st.Back(5).Uint64())
			}
		case vm.CALL, vm.CALLCODE:
			if st.Len() > 6 {
				ot.memOffStack = append(ot.memOffStack, st.Back(5).Uint64())
				ot.memLenStack = append(ot.memLenStack, st.Back(6).Uint64())
			}
		case vm.CREATE, vm.CREATE2, vm.SELFDESTRUCT:
			// Effectively disable memory output
			ot.memOffStack = append(ot.memOffStack, 0)
			ot.memLenStack = append(ot.memLenStack, 0)
		case vm.SSTORE:
			if st.Len() > 1 {
				ot.lastVmOp.Ex.Store = &VmTraceStore{Key: st.Back(0).Hex(), Val: st.Back(1).Hex()}
			}
		}
		if ot.lastVmOp.Ex.Used < 0 {
			ot.lastVmOp.Ex = nil
		}
	}
}

func (ot *OeTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, opDepth int, err error) {
}

// Implements core/state/StateWriter to provide state diffs
type StateDiff struct {
	sdMap map[libcommon.Address]*StateDiffAccount
}

func (sd *StateDiff) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[libcommon.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[libcommon.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[libcommon.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	accountDiff := sd.sdMap[address]
	if accountDiff == nil {
		accountDiff = &StateDiffAccount{Storage: make(map[libcommon.Hash]map[string]interface{})}
		sd.sdMap[address] = accountDiff
	}
	m := make(map[string]interface{})
	m["*"] = &StateDiffStorage{From: libcommon.BytesToHash(original.Bytes()), To: libcommon.BytesToHash(value.Bytes())}
	accountDiff.Storage[*key] = m
	return nil
}

func (sd *StateDiff) CreateContract(address libcommon.Address) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[libcommon.Hash]map[string]interface{})}
	}
	return nil
}

// CompareStates uses the addresses accumulated in the sdMap and compares balances, nonces, and codes of the accounts, and fills the rest of the sdMap
func (sd *StateDiff) CompareStates(initialIbs, ibs *state.IntraBlockState) error {
	var toRemove []libcommon.Address
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
					m := make(map[string]hexutility.Bytes)
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
				balance, err := initialIbs.GetBalance(addr)
				if err != nil {
					return err
				}
				m := make(map[string]*hexutil.Big)
				m["+"] = (*hexutil.Big)(balance.ToBig())
				accountDiff.Balance = m
			}
			{
				code, err := initialIbs.GetCode(addr)
				if err != nil {
					return err
				}
				m := make(map[string]hexutility.Bytes)
				m["+"] = code
				accountDiff.Code = m
			}
			{
				nonce, err := initialIbs.GetNonce(addr)
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

func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, txHash libcommon.Hash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) (*TraceCallResult, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	var isBorStateSyncTxn bool
	blockNum, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		if chainConfig.Bor == nil {
			return nil, nil
		}

		// otherwise this may be a bor state sync transaction - check
		if api.bridgeReader != nil {
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

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	var txnIndex int
	for idx := 0; idx < block.Transactions().Len() && !isBorStateSyncTxn; idx++ {
		txn := block.Transactions()[idx]
		if txn.Hash() == txHash {
			txnIndex = idx
			break
		}
	}

	if isBorStateSyncTxn {
		txnIndex = block.Transactions().Len()
	}

	signer := types.MakeSigner(chainConfig, blockNum, block.Time())
	// Returns an array of trace arrays, one trace array for each transaction
	traces, _, err := api.callManyTransactions(ctx, tx, block, traceTypes, txnIndex, *gasBailOut, signer, chainConfig, traceConfig)
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

	for txno, trace := range traces {
		// We're only looking for a specific transaction
		if txno == txnIndex {
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
	}

	return result, nil
}

func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) ([]*TraceCallResult, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginRo(ctx)
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
	traces, _, err := api.callManyTransactions(ctx, tx, block, traceTypes, -1 /* all txn indices */, *gasBailOut, signer, chainConfig, traceConfig)
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
	tx, err := api.kv.BeginRo(ctx)
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

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, *blockNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
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

	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
	txCtx := core.NewEVMTxContext(msg)

	blockCtx.GasLimit = math.MaxUint64
	blockCtx.MaxGasLimit = true

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: traceTypeTrace, Tracer: &ot})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	var execResult *evmtypes.ExecutionResult
	ibs.SetTxContext(0)
	execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
	if err != nil {
		return nil, err
	}
	traceResult.Output = libcommon.CopyBytes(execResult.ReturnData)
	if traceTypeStateDiff {
		sdMap := make(map[libcommon.Address]*StateDiffAccount)
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
	dbtx, err := api.kv.BeginRo(ctx)
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
	msgs := make([]types.Message, len(callParams))
	for i, args := range callParams {
		msgs[i], err = args.ToMessage(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to msg: %w", err)
		}
	}
	results, _, err := api.doCallMany(ctx, dbtx, msgs, callParams, parentNrOrHash, nil, true /* gasBailout */, -1 /* all txn indices */, traceConfig)
	return results, err
}

func (api *TraceAPIImpl) doCallMany(ctx context.Context, dbtx kv.Tx, msgs []types.Message, callParams []TraceCallParam,
	parentNrOrHash *rpc.BlockNumberOrHash, header *types.Header, gasBailout bool, txIndexNeeded int,
	traceConfig *config.TraceConfig,
) ([]*TraceCallResult, *state.IntraBlockState, error) {
	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, nil, err
	}
	engine := api.engine()

	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, *parentNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return nil, nil, err
	}
	stateReader, err := rpchelper.CreateStateReader(ctx, dbtx, api._blockReader, *parentNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return nil, nil, err
	}
	stateCache := shards.NewStateCache(
		32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
	//cachedReader := stateReader
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	//cachedWriter := noop
	cachedWriter := state.NewCachedWriter(noop, stateCache)
	ibs := state.New(cachedReader)

	parentHeader, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(blockNumber), dbtx)
	if err != nil {
		return nil, nil, err
	}
	if parentHeader == nil {
		return nil, nil, fmt.Errorf("parent header %d(%x) not found", blockNumber, hash)
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

	for txIndex, msg := range msgs {
		if isHistoricalStateReader {
			historicalStateReader.SetTxNum(baseTxNum + uint64(txIndex))
		}
		if err := libcommon.Stopped(ctx.Done()); err != nil {
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
		if (traceTypeTrace && (txIndexNeeded == -1 || txIndex == txIndexNeeded)) || traceTypeVmTrace {
			var ot OeTracer
			ot.config, err = parseOeTracerConfig(traceConfig)
			if err != nil {
				return nil, nil, err
			}
			ot.compat = api.compatibility
			ot.r = traceResult
			ot.idx = []string{fmt.Sprintf("%d-", txIndex)}
			if traceTypeTrace && (txIndexNeeded == -1 || txIndex == txIndexNeeded) {
				ot.traceAddr = []int{}
			}
			if traceTypeVmTrace {
				traceResult.VmTrace = &VmTrace{Ops: []*VmTraceOp{}}
			}
			vmConfig.Debug = true
			vmConfig.Tracer = &ot
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
			sdMap := make(map[libcommon.Address]*StateDiffAccount)
			traceResult.StateDiff = sdMap
			sd = &StateDiff{sdMap: sdMap}
		}

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
			stateSyncEvents, err = api.stateSyncEvents(ctx, dbtx, header.Hash(), blockNumber, chainConfig)
			if err != nil {
				return nil, nil, err
			}

			execResult, err = tracer.TraceBorStateSyncTxnTraceAPI(
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
			)
		} else {
			ibs.SetTxContext(txIndex)
			txCtx := core.NewEVMTxContext(msg)
			evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
			gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())

			execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /*gasBailout*/)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
		}

		chainRules := chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time)
		traceResult.Output = libcommon.CopyBytes(execResult.ReturnData)
		if traceTypeStateDiff {
			initialIbs := state.New(cloneReader)
			if !txFinalized {
				if err = ibs.FinalizeTx(chainRules, sd); err != nil {
					return nil, nil, err
				}
			}
			sd.CompareStates(initialIbs, ibs)
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
		// When txIndexNeeded is not -1, we are tracing specific transaction in the block and not the entire block, so we stop after we've traced
		// the required transaction
		if txIndexNeeded != -1 && txIndex == txIndexNeeded {
			break
		}
	}

	return results, ibs, nil
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, txHash libcommon.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_rawTransaction")
}
