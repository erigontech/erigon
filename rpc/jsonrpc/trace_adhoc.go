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
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
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
	GasPrice             *hexutil.U256     `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.U256     `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.U256     `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.U256     `json:"maxFeePerBlobGas"`
	Value                *hexutil.U256     `json:"value"`
	Data                 *hexutil.Bytes    `json:"data"`
	Input                *hexutil.Bytes    `json:"input"`
	AccessList           *types.AccessList `json:"accessList"`
	txHash               *common.Hash
	traceTypes           []string

	// Converted as ethapi.CallArgs converts them for eth_call.
	Nonce               *hexutil.Uint64           `json:"nonce"`
	ChainID             *hexutil.U256             `json:"chainId"`
	BlobVersionedHashes []common.Hash             `json:"blobVersionedHashes"`
	AuthorizationList   []types.JsonAuthorization `json:"authorizationList"`
}

// UnmarshalJSON decodes a call object and rejects one whose data and input disagree, as
// ethapi.CallArgs does.
func (args *TraceCallParam) UnmarshalJSON(raw []byte) error {
	type traceCallParam TraceCallParam
	if err := json.Unmarshal(raw, (*traceCallParam)(args)); err != nil {
		return err
	}
	return ethapi.CheckCallData(args.Data, args.Input)
}

// TraceCallResult is the response to `trace_call` method
type TraceCallResult struct {
	Output          hexutil.Bytes                          `json:"output"`
	StateDiff       map[accounts.Address]*StateDiffAccount `json:"stateDiff"`
	Trace           []*ParityTrace                         `json:"trace"`
	VmTrace         *VmTrace                               `json:"vmTrace"`
	TransactionHash *common.Hash                           `json:"transactionHash,omitempty"`
}

// StateDiffAccount is the part of `trace_call` response that is under "stateDiff" tag
type StateDiffAccount struct {
	Balance any                            `json:"balance"` // Can be either string "=" or mapping "*" => {"from": "hex", "to": "hex"}
	Code    any                            `json:"code"`
	Nonce   any                            `json:"nonce"`
	Storage map[common.Hash]map[string]any `json:"storage"`
}

type StateDiffBalance struct {
	From *hexutil.U256 `json:"from"`
	To   *hexutil.U256 `json:"to"`
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
	Cost         int        `json:"cost"`
	StateGasCost int64      `json:"stateGasCost,omitempty"`
	Ex           *VmTraceEx `json:"ex"`
	Pc           int        `json:"pc"`
	Sub          *VmTrace   `json:"sub"`
	Op           string     `json:"op,omitempty"`
	Idx          string     `json:"idx,omitempty"`
}

type VmTraceEx struct {
	Mem               *VmTraceMem   `json:"mem"`
	Push              []string      `json:"push"`
	Store             *VmTraceStore `json:"store"`
	GasRemaining      int           `json:"used"`                // legacy "used" means remaining execution gas.
	StateGasRemaining uint64        `json:"stateUsed,omitempty"` // mirrors legacy "used" naming for remaining state gas.
}

type VmTraceMem struct {
	Data string `json:"data"`
	Off  int    `json:"off"`
}

type VmTraceStore struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

// toCallArgs returns args as the eth_call arguments they describe.
func (args *TraceCallParam) toCallArgs() ethapi.CallArgs {
	return ethapi.CallArgs{
		From:                 args.From,
		To:                   args.To,
		Gas:                  args.Gas,
		GasPrice:             args.GasPrice,
		MaxPriorityFeePerGas: args.MaxPriorityFeePerGas,
		MaxFeePerGas:         args.MaxFeePerGas,
		MaxFeePerBlobGas:     args.MaxFeePerBlobGas,
		Value:                args.Value,
		Data:                 args.Data,
		Input:                args.Input,
		Nonce:                args.Nonce,
		AccessList:           args.AccessList,
		ChainID:              args.ChainID,
		BlobVersionedHashes:  args.BlobVersionedHashes,
		AuthorizationList:    args.AuthorizationList,
	}
}

// ToMessage converts args to the Message eth_call would run for them: omitted fees are zero,
// and a call with no gas price is not repriced to the base fee.
func (args *TraceCallParam) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (*types.Message, error) {
	callArgs := args.toCallArgs()
	return callArgs.ToMessage(globalGasCap, baseFee)
}

// callValidationError is a call rejected before execution, with the code eth_simulateV1 gives it.
type callValidationError struct {
	err  error
	code int
}

func (e *callValidationError) Error() string  { return e.err.Error() }
func (e *callValidationError) ErrorCode() int { return e.code }
func (e *callValidationError) Unwrap() error  { return e.err }

// callError gives a call that fails validation the error code eth_simulateV1 gives it, and
// returns any other error unchanged.
func callError(err error) error {
	var coded *rpc.CustomError
	if errors.As(txValidationError(err), &coded) && coded.Code != rpc.ErrCodeInternalError {
		return &callValidationError{err: err, code: coded.Code}
	}
	return err
}

// overrideBaseFee is a nil-safe wrapper around (*ethapi.BlockOverrides).OverrideBaseFee.
func overrideBaseFee(traceConfig *config.TraceConfig, baseFee *uint256.Int) *uint256.Int {
	if traceConfig == nil {
		return baseFee
	}
	return traceConfig.BlockOverrides.OverrideBaseFee(baseFee)
}

// overrideHeader is a nil-safe wrapper around (*ethapi.BlockOverrides).OverrideHeader.
func overrideHeader(traceConfig *config.TraceConfig, header *types.Header) *types.Header {
	if traceConfig == nil {
		return header
	}
	return traceConfig.BlockOverrides.OverrideHeader(header)
}

// overrideBlockContext applies traceConfig's BlockOverrides (if any) to blockCtx.
func overrideBlockContext(traceConfig *config.TraceConfig, blockCtx *evmtypes.BlockContext) error {
	if traceConfig == nil {
		return nil
	}
	return traceConfig.BlockOverrides.Override(blockCtx)
}

// checkOverriddenSigner recovers txn's sender with the overridden block's signer: a stored sender
// was derived with the real block's signer, which may accept a txn the overridden one rejects.
// Only a number or time override changes the signer.
func checkOverriddenSigner(traceConfig *config.TraceConfig, signer *types.Signer, txn types.Transaction) error {
	if traceConfig == nil || traceConfig.BlockOverrides == nil ||
		(traceConfig.BlockOverrides.Number == nil && traceConfig.BlockOverrides.Time == nil) {
		return nil
	}
	_, err := signer.Sender(txn)
	return err
}

func parseOeTracerConfig(traceConfig *config.TraceConfig) (OeTracerConfig, error) {
	if traceConfig != nil && traceConfig.Tracer != nil && *traceConfig.Tracer != "" {
		return OeTracerConfig{}, errors.New("trace_* does not support custom tracers; use debug_* (e.g. debug_traceTransaction) for named or JS tracers")
	}
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
	builtin      bool // Whether the last frame entered is a precompile; it has no subframes, so its exit comes next
	compat       bool // Bug for bug compatibility mode
	isAmsterdam  bool
	lastVmOp     *VmTraceOp
	lastOp       vm.OpCode
	lastMemOff   uint64
	lastMemLen   uint64
	callMemOff   uint64 // Output window of the last operation, pushed onto memOffStack/memLenStack if it enters a frame
	callMemLen   uint64
	memOffStack  []uint64
	memLenStack  []uint64
	lastOffStack *VmTraceOp
	vmOpStack    []*VmTraceOp // Stack of vmTrace operations as call depth increases
	idx          []string     // Prefix for the "idx" inside operations, for easier navigation
	config       OeTracerConfig
}

func (args *TraceCallParam) zeroUnpricedBlobBaseFee(blockCtx *evmtypes.BlockContext) {
	callArgs := ethapi.CallArgs{MaxFeePerBlobGas: args.MaxFeePerBlobGas, BlobVersionedHashes: args.BlobVersionedHashes}
	callArgs.ZeroUnpricedBlobBaseFee(blockCtx)
}

// ToTransaction converts CallArgs to the Transaction type used by the core evm
func (args *TraceCallParam) ToTransaction(globalGasCap uint64, baseFee *uint256.Int) (types.Transaction, error) {
	var chainID uint256.Int
	if args.ChainID != nil {
		chainID = uint256.Int(*args.ChainID)
	}

	msg, err := args.ToMessage(globalGasCap, baseFee)
	if err != nil {
		return nil, err
	}

	var tx types.Transaction
	switch {
	case args.AuthorizationList != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		tx = &types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				ChainID:    chainID,
				FeeCap:     *msg.FeeCap(),
				TipCap:     *msg.TipCap(),
				AccessList: al,
			},
			Authorizations: msg.Authorizations(),
		}
	case args.BlobVersionedHashes != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		var maxFeePerBlobGas uint256.Int
		if args.MaxFeePerBlobGas != nil {
			maxFeePerBlobGas = uint256.Int(*args.MaxFeePerBlobGas)
		}
		tx = &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				ChainID:    chainID,
				FeeCap:     *msg.FeeCap(),
				TipCap:     *msg.TipCap(),
				AccessList: al,
			},
			MaxFeePerBlobGas:    maxFeePerBlobGas,
			BlobVersionedHashes: args.BlobVersionedHashes,
		}
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
				Value:    *msg.Value(),
				Data:     msg.Data(),
			},
			ChainID:    chainID,
			FeeCap:     *msg.FeeCap(),
			TipCap:     *msg.TipCap(),
			AccessList: al,
		}
	case args.AccessList != nil:
		tx = &types.AccessListTx{
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				GasPrice: *msg.GasPrice(),
			},
			ChainID:    chainID,
			AccessList: *args.AccessList,
		}
	default:
		tx = &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    msg.Nonce(),
				GasLimit: msg.Gas(),
				To:       args.To,
				Value:    *msg.Value(),
				Data:     msg.Data(),
			},
			GasPrice: *msg.GasPrice(),
		}
	}
	return tx, nil
}

func (ot *OeTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart:  ot.OnTxStart,
			OnEnterV2:  ot.OnEnterV2,
			OnExitV2:   ot.OnExitV2,
			OnOpcodeV2: ot.OnOpcodeV2,
			OnFaultV2:  ot.OnFaultV2,
		},
		GetResult: ot.GetResult,
		Stop:      ot.Stop,
	}
}

func (ot *OeTracer) OnTxStart(env *tracing.VMContext, _ types.Transaction, _ accounts.Address) {
	ot.isAmsterdam = env.Rules.IsAmsterdam
}

func (ot *OeTracer) captureStartOrEnter(deep bool, typ vm.OpCode, from accounts.Address, to accounts.Address, precompile bool, create bool, input []byte, gas mdgas.MdGas, value *uint256.Int, code []byte) {
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
			ot.memOffStack = append(ot.memOffStack, ot.callMemOff)
			ot.memLenStack = append(ot.memLenStack, ot.callMemLen)
		}
		if ot.lastVmOp != nil {
			vmTrace = &VmTrace{Ops: []*VmTraceOp{}}
			// SELFDESTRUCT enters a frame that runs no code, so it gets no sub.
			if typ != vm.SELFDESTRUCT {
				ot.lastVmOp.Sub = vmTrace
			}
			ot.vmOpStack = append(ot.vmOpStack, ot.lastVmOp)
		} else {
			vmTrace = ot.r.VmTrace
		}
		if create {
			vmTrace.Code = bytes.Clone(input)
			if ot.lastVmOp != nil {
				ot.lastVmOp.Cost += int(gas.Execution)
			}
		} else {
			vmTrace.Code = code
		}
	}
	ot.builtin = precompile
	if precompile && deep && (value == nil || value.IsZero()) {
		ot.precompile = true
		if !ot.config.IncludePrecompiles {
			return
		}
	}
	if gas.Execution > 500000000 {
		gas.Execution = 500000001 - (0x8000000000000000 - gas.Execution)
	}
	trace := &ParityTrace{}
	if create {
		trResult := &CreateTraceResult{}
		trace.Type = CREATE
		trResult.Address = new(common.Address)
		toVal := to.Value()
		copy(trResult.Address[:], toVal[:])
		trace.Result = trResult
	} else {
		trace.Result = &TraceResult{GasUsed: new(hexutil.U256)}
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
				v := uint256.Int(action.Value)
				value = &v
			case *CallTraceAction:
				v := uint256.Int(action.Value)
				value = &v
			}
		}
		if typ == vm.STATICCALL {
			value = uint256.NewInt(0)
		}
	}
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	switch {
	case create:
		action := CreateTraceAction{}
		action.From = from.Value()
		action.CreationMethod = strings.ToLower(typ.String())
		action.Gas = hexutil.U256(*uint256.NewInt(gas.Execution))
		if ot.isAmsterdam {
			action.StateGas = (*hexutil.Uint64)(&gas.State)
		}
		action.Init = bytes.Clone(input)
		action.Value = hexutil.U256(*value)
		trace.Action = &action
	case typ == vm.SELFDESTRUCT:
		trace.Type = SUICIDE
		trace.Result = nil
		action := &SuicideTraceAction{}
		action.Address = from.Value()
		action.RefundAddress = to.Value()
		action.Balance = hexutil.U256(*value)
		trace.Action = action
	default:
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
		action.From = from.Value()
		action.To = to.Value()
		action.Gas = hexutil.U256(*uint256.NewInt(gas.Execution))
		action.Input = bytes.Clone(input)
		action.Value = hexutil.U256(*value)
		trace.Action = &action
	}
	ot.r.Trace = append(ot.r.Trace, trace)
	ot.traceStack = append(ot.traceStack, trace)
}

func (ot *OeTracer) OnEnterV2(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas mdgas.MdGas, value uint256.Int, code []byte) {
	isCreate := vm.OpCode(typ) == vm.CREATE || vm.OpCode(typ) == vm.CREATE2
	ot.captureStartOrEnter(depth != 0 /* deep */, vm.OpCode(typ), from, to, precompile, isCreate, input, gas, &value, code)
}

func (ot *OeTracer) captureEndOrExit(deep bool, output []byte, gasUsed mdgas.MdGasUsage, err error) {
	if ot.r.VmTrace != nil {
		if len(ot.vmOpStack) > 0 {
			ot.lastOffStack = ot.vmOpStack[len(ot.vmOpStack)-1]
			ot.vmOpStack = ot.vmOpStack[:len(ot.vmOpStack)-1]
			// A call or create that fails its depth, balance or nonce check runs no code, so it gets no sub.
			if errors.Is(err, vm.ErrDepth) || errors.Is(err, vm.ErrInsufficientBalance) || errors.Is(err, vm.ErrNonceUintOverflow) {
				ot.lastOffStack.Sub = nil
			}
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
	builtin := ot.builtin
	ot.builtin = false
	if ot.precompile {
		ot.precompile = false
		if !ot.config.IncludePrecompiles {
			return
		}
	}
	if !deep {
		ot.r.Output = bytes.Clone(output)
	}
	ignoreError := false
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	if ot.compat {
		ignoreError = !deep && topTrace.Type == CREATE
	}
	if err != nil && !ignoreError {
		topTrace.Error = parityTraceError(err, builtin)
		if errors.Is(err, vm.ErrExecutionReverted) {
			// A reverted CREATE deploys nothing, so it reports its revert data as a call does,
			// without the address it would have occupied.
			topTrace.Result = &TraceResult{GasUsed: (*hexutil.U256)(uint256.NewInt(gasUsed.Execution)), Output: bytes.Clone(output)}
		} else {
			topTrace.Result = nil
		}
	} else {
		if len(output) > 0 {
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).Output = bytes.Clone(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).Code = bytes.Clone(output)
			}
		}
		switch topTrace.Type {
		case CALL:
			topTrace.Result.(*TraceResult).GasUsed = (*hexutil.U256)(uint256.NewInt(gasUsed.Execution))
		case CREATE:
			topTrace.Result.(*CreateTraceResult).GasUsed = (*hexutil.U256)(uint256.NewInt(gasUsed.Execution))
		}
	}
	if ot.isAmsterdam {
		switch result := topTrace.Result.(type) {
		case *TraceResult:
			result.StateGasUsed = (*hexutil.Int64)(&gasUsed.State)
		case *CreateTraceResult:
			result.StateGasUsed = (*hexutil.Int64)(&gasUsed.State)
		}
	}
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if deep {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
}

// parityTraceError returns the Parity trace label for a frame's error. An error without a label
// keeps its own text.
func parityTraceError(err error, builtin bool) string {
	var (
		stackUnderflow *vm.ErrStackUnderflow
		stackOverflow  *vm.ErrStackOverflow
		invalidOpCode  *vm.ErrInvalidOpCode
	)
	switch {
	case errors.Is(err, vm.ErrExecutionReverted):
		return "Reverted"
	// Before out of gas: the interpreter wraps a dynamic gas error in vm.ErrOutOfGas.
	case errors.Is(err, vm.ErrWriteProtection):
		return "Mutable Call In Static Context"
	// EIP-170 names a code deposit failure, the code size limit included, as out of gas.
	case errors.Is(err, vm.ErrOutOfGas), errors.Is(err, vm.ErrCodeStoreOutOfGas), errors.Is(err, vm.ErrMaxCodeSizeExceeded),
		errors.Is(err, vm.ErrMaxInitCodeSizeExceeded), errors.Is(err, vm.ErrGasUintOverflow):
		return "Out of gas"
	case errors.Is(err, vm.ErrInvalidJump):
		return "Bad jump destination"
	case errors.As(err, &invalidOpCode):
		return "Bad instruction"
	case errors.As(err, &stackUnderflow):
		return "Stack underflow"
	case errors.As(err, &stackOverflow):
		return "Out of stack"
	case errors.Is(err, vm.ErrReturnDataOutOfBounds):
		return "Out of bounds"
	case errors.Is(err, vm.ErrInvalidCode):
		return "Invalid code"
	case errors.Is(err, vm.ErrContractAddressCollision):
		return "Contract address collision"
	case errors.Is(err, vm.ErrNonceUintOverflow):
		return "Nonce overflow"
	case errors.Is(err, vm.ErrInsufficientBalance):
		return "Insufficient balance for transfer"
	case errors.Is(err, vm.ErrDepth):
		return "Max call depth exceeded"
	case builtin:
		return "Built-in failed"
	default:
		return err.Error()
	}
}

func (ot *OeTracer) OnExitV2(depth int, output []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
	ot.captureEndOrExit(depth != 0 /* deep */, output, gasUsed, err)
}

func (ot *OeTracer) OnOpcodeV2(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, scope tracing.OpContext, rData []byte, depth int, err error) {
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
				vm.ADD, vm.EXP, vm.CALLER, vm.KECCAK256, vm.SUB, vm.ADDRESS, vm.GAS, vm.MUL, vm.RETURNDATASIZE, vm.NOT, vm.SHR, vm.SHL, vm.CLZ,
				vm.EXTCODESIZE, vm.SLT, vm.OR, vm.NUMBER, vm.SLOTNUM, vm.PC, vm.TIMESTAMP, vm.BALANCE, vm.SELFBALANCE, vm.MULMOD, vm.ADDMOD, vm.BASEFEE,
				vm.BLOBHASH, vm.BLOBBASEFEE, vm.BLOCKHASH, vm.BYTE, vm.XOR, vm.ORIGIN, vm.CODESIZE, vm.MOD, vm.SIGNEXTEND, vm.GASLIMIT, vm.DIFFICULTY,
				vm.SGT, vm.GASPRICE, vm.MSIZE, vm.EXTCODEHASH, vm.SMOD, vm.CHAINID, vm.COINBASE, vm.TLOAD, vm.DUPN:
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
			case vm.MSTORE, vm.MSTORE8, vm.MLOAD, vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY, vm.EXTCODECOPY, vm.MCOPY:
				setMem = true
			}
			if setMem && ot.lastMemLen > 0 {
				cpy, err := tracers.GetMemoryCopyPadded(memory, int64(ot.lastMemOff), int64(ot.lastMemLen))
				if err != nil {
					log.Warn("Failed to copy memory for trace output; this may happen with invalid offset/length",
						"off", ot.lastMemOff,
						"len", ot.lastMemLen,
						"err", err,
						"hint", "May affect trace completeness; consider enabling debug logs for deeper insight")
					cpy = make([]byte, ot.lastMemLen)
				}
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastVmOp.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
		}
		if ot.lastOffStack != nil {
			ot.lastOffStack.Ex.GasRemaining = int(gas.Execution)
			ot.lastOffStack.Ex.StateGasRemaining = gas.State
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
		// The interpreter reports running off the end of the code as a STOP at or beyond len(code),
		// and reports an operation it rejects before execution with that error. Neither executed.
		if pc >= uint64(len(scope.Code())) || rejectedBeforeExecution(err) {
			ot.lastVmOp = nil
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
		ot.lastVmOp.Cost = int(cost.Execution)
		ot.lastVmOp.StateGasCost = cost.State
		ot.lastVmOp.Pc = int(pc)
		if !ot.compat {
			ot.lastVmOp.Op = vm.OpCode(op).String()
		}
		if err != nil {
			// The operation began executing and halted exceptionally, so it has no effects.
			ot.lastVmOp.Ex = nil
			return
		}
		ot.lastVmOp.Ex.Push = []string{}
		gasRemaining := scope.Gas()
		ot.lastVmOp.Ex.StateGasRemaining = gasRemaining.State
		ot.lastVmOp.Ex.GasRemaining = int(gasRemaining.Execution)
		ot.callMemOff, ot.callMemLen = 0, 0
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
		case vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY, vm.MCOPY:
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
				ot.callMemOff = tracers.StackBack(st, 4).Uint64()
				ot.callMemLen = tracers.StackBack(st, 5).Uint64()
			}
		case vm.CALL, vm.CALLCODE:
			if len(st) > 6 {
				ot.callMemOff = tracers.StackBack(st, 5).Uint64()
				ot.callMemLen = tracers.StackBack(st, 6).Uint64()
			}
		case vm.SSTORE:
			if len(st) > 1 {
				ot.lastVmOp.Ex.Store = &VmTraceStore{Key: tracers.StackBack(st, 0).Hex(), Val: tracers.StackBack(st, 1).Hex()}
			}
		}
	}
}

// OnFaultV2 is called when an operation reported by OnOpcodeV2 fails during execution.
func (ot *OeTracer) OnFaultV2(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, scope tracing.OpContext, depth int, err error) {
	if ot.r.VmTrace == nil || ot.lastVmOp == nil || errors.Is(err, vm.ErrExecutionReverted) {
		return
	}
	// Stack bounds are checked before the opcode hook, so an undefined opcode is the only fault
	// here for an operation that did not execute.
	var invalid *vm.ErrInvalidOpCode
	if errors.As(err, &invalid) && invalid.Undefined() {
		vmTrace := ot.r.VmTrace
		if len(ot.vmOpStack) > 0 {
			vmTrace = ot.vmOpStack[len(ot.vmOpStack)-1].Sub
		}
		vmTrace.Ops = vmTrace.Ops[:len(vmTrace.Ops)-1]
		ot.lastVmOp = nil
		return
	}
	ot.lastVmOp.Ex = nil
}

// rejectedBeforeExecution reports whether err rejects an operation before it executes:
// a stack underflow or overflow.
func rejectedBeforeExecution(err error) bool {
	var underflow *vm.ErrStackUnderflow
	var overflow *vm.ErrStackOverflow
	return errors.As(err, &underflow) || errors.As(err, &overflow)
}

func (ot *OeTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage{}, nil
}

func (ot *OeTracer) Stop(err error) {}

// Implements execution/state/StateWriter to provide state diffs
type StateDiff struct {
	sdMap map[accounts.Address]*StateDiffAccount
}

func (sd *StateDiff) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]any)}
	}
	return nil
}

func (sd *StateDiff) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]any)}
	}
	return nil
}

func (sd *StateDiff) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]any)}
	}
	return nil
}

func (sd *StateDiff) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	accountDiff := sd.sdMap[address]
	if accountDiff == nil {
		accountDiff = &StateDiffAccount{Storage: make(map[common.Hash]map[string]any)}
		sd.sdMap[address] = accountDiff
	}
	m := make(map[string]any)
	m["*"] = &StateDiffStorage{From: common.BytesToHash(original.Bytes()), To: common.BytesToHash(value.Bytes())}
	accountDiff.Storage[key.Value()] = m
	return nil
}

func (sd *StateDiff) CreateContract(address accounts.Address) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]any)}
	}
	return nil
}

// CompareStates uses the addresses accumulated in the sdMap and compares balances, nonces, and codes of the accounts, and fills the rest of the sdMap
func (sd *StateDiff) CompareStates(initialIbs, ibs *state.IntraBlockState) error {
	var toRemove []accounts.Address
	for addr, accountDiff := range sd.sdMap {
		initialExist, err := initialIbs.Exist(addr)
		if err != nil {
			return err
		}
		exist, err := ibs.Exist(addr)
		if err != nil {
			return err
		}
		switch {
		case initialExist:
			if exist {
				allEqual := len(accountDiff.Storage) == 0
				ifromBalance, err := initialIbs.GetBalance(addr)
				if err != nil {
					return err
				}
				itoBalance, err := ibs.GetBalance(addr)
				if err != nil {
					return err
				}
				if ifromBalance.Eq(&itoBalance) {
					accountDiff.Balance = "="
				} else {
					m := make(map[string]*StateDiffBalance)
					m["*"] = &StateDiffBalance{From: (*hexutil.U256)(&ifromBalance), To: (*hexutil.U256)(&itoBalance)}
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
					m := make(map[string]*hexutil.U256)
					m["-"] = (*hexutil.U256)(&balance)
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
		case exist:
			{
				balance, err := ibs.GetBalance(addr)
				if err != nil {
					return err
				}
				m := make(map[string]*hexutil.U256)
				m["+"] = (*hexutil.U256)(&balance)
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
		default:
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

	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	err = api.BaseAPI.checkBlockHistoryAvailable(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	header, err := api.canonicalHeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	txnIndex, err := api.txnIndexInBlock(ctx, tx, blockNum, txNum)
	if err != nil {
		return nil, err
	}

	// Returns an array of trace arrays, one trace array for each transaction
	trace, err := api.callTransaction(ctx, tx, header, traceTypes, txnIndex, *gasBailOut, chainConfig, traceConfig)
	if err != nil {
		return nil, err
	}

	return trace, nil
}

func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) ([]*TraceCallResult, error) {
	if err := rejectPending(blockNrOrHash); err != nil {
		return nil, err
	}
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

	blockNumber, blockHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkBlockHistoryAvailable(ctx, tx, blockNumber)
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

	// Returns an array of trace arrays, one trace array for each transaction
	traces, wdiffs, err := api.callBlock(ctx, tx, block, traceTypes, *gasBailOut, chainConfig, traceConfig, nil /* withSyscall */)
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

	// Withdrawals are surfaced only via stateDiff; trace_block emits them as reward traces
	// in its flat output, but this per-tx result structure has no block-level slot for them.
	if traceTypeStateDiff && traceConfig.IncludeWithdrawalsEnabled() && len(wdiffs) > 0 {
		sdMap := make(map[accounts.Address]*StateDiffAccount, len(wdiffs))
		for _, wd := range wdiffs {
			addr := accounts.InternAddress(wd.address)
			if entry, ok := sdMap[addr]; ok {
				if wd.existed {
					bal := entry.Balance.(map[string]*StateDiffBalance)["*"]
					cur := uint256.Int(*bal.To)
					cur.Add(&cur, &wd.amount)
					bal.To = (*hexutil.U256)(&cur)
				} else {
					balMap := entry.Balance.(map[string]*hexutil.U256)
					cur := uint256.Int(*balMap["+"])
					cur.Add(&cur, &wd.amount)
					balMap["+"] = (*hexutil.U256)(&cur)
				}
			} else {
				var to uint256.Int
				to.Add(&wd.prev, &wd.amount)
				if wd.existed {
					sdMap[addr] = &StateDiffAccount{
						Balance: map[string]*StateDiffBalance{
							"*": {
								From: (*hexutil.U256)(&wd.prev),
								To:   (*hexutil.U256)(&to),
							},
						},
						Code:    "=",
						Nonce:   "=",
						Storage: map[common.Hash]map[string]any{},
					}
				} else {
					sdMap[addr] = &StateDiffAccount{
						Balance: map[string]*hexutil.U256{"+": (*hexutil.U256)(&to)},
						Code:    map[string]hexutil.Bytes{"+": {}},
						Nonce:   map[string]hexutil.Uint64{"+": 0},
						Storage: map[common.Hash]map[string]any{},
					}
				}
			}
		}
		result = append(result, &TraceCallResult{ //nolint:makezero
			Trace:     []*ParityTrace{},
			StateDiff: sdMap,
		})
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
	if err := ethapi.CheckChainID(args.ChainID, chainConfig.ChainID); err != nil {
		return nil, err
	}
	engine := api.engine()

	if blockNrOrHash == nil {
		num := rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	if err := rejectPending(*blockNrOrHash); err != nil {
		return nil, err
	}

	blockNumber, hash, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, *blockNrOrHash, tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}

	header, err := api.headerByHashAndNumber(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}

	err = rpchelper.CheckBlockExecuted(tx, blockNumber)
	if err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, -1, api._txNumReader)
	if err != nil {
		return nil, err
	}

	ibs := state.New(stateReader)
	defer ibs.Close()

	_, storeEVM, cleanup := setupEVMTimeout(ctx, api.evmCallTimeout)
	defer cleanup()

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
	vmConfig := vm.Config{NoBaseFee: true}
	if traceTypeTrace || traceTypeVmTrace {
		ot.r = traceResult
		ot.traceAddr = []int{}
		vmConfig.Tracer = ot.Tracer().Hooks
	}

	// Get a new instance of the EVM.
	var blockOverrides *ethapi.BlockOverrides
	if traceConfig != nil {
		blockOverrides = traceConfig.BlockOverrides
	}
	effectiveHeader := blockOverrides.OverrideHeader(header)
	blockCtx := transactions.NewEVMBlockContext(engine, effectiveHeader, blockNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
	if err := blockOverrides.Override(&blockCtx); err != nil {
		return nil, err
	}

	baseFee := effectiveHeader.BaseFee
	msg, err := args.ToMessage(api.gasCap, baseFee)
	if err != nil {
		return nil, err
	}
	txn, err := args.ToTransaction(api.gasCap, baseFee)
	if err != nil {
		return nil, err
	}
	txCtx := protocol.NewEVMTxContext(msg)

	var precompiles vm.PrecompiledContracts
	if traceConfig != nil && traceConfig.StateOverrides != nil {
		precompiles, err = applyStateOverrides(ibs, traceConfig.StateOverrides, blockCtx.Rules(chainConfig))
		if err != nil {
			return nil, err
		}
	}

	args.zeroUnpricedBlobBaseFee(&blockCtx)
	evm := vm.NewEVM(vm.ZeroUnpricedBaseFee(blockCtx, txCtx, vmConfig), txCtx, ibs, chainConfig, vmConfig)
	if precompiles != nil {
		evm.SetPrecompiles(precompiles)
	}
	storeEVM(evm)

	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	var execResult *evmtypes.ExecutionResult
	ibs.SetTxContext(blockCtx.BlockNumber, 0)
	ibs.SetHooks(vmConfig.Tracer)

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnTxStart != nil {
		vmConfig.Tracer.OnTxStart(evm.GetVMContext(), txn, msg.From())
	}
	execResult, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		vmConfig.Tracer.EmitTxEnd(nil, mdgas.TxnGasUsage{}, err)
		return nil, callError(err)
	}
	if vmConfig.Tracer.HasTxEndHook() {
		vmConfig.Tracer.EmitTxEnd(&types.Receipt{GasUsed: execResult.ReceiptGasUsed}, execResult.TxnGasUsage, nil)
	}
	traceResult.Output = bytes.Clone(execResult.ReturnData)
	if traceTypeStateDiff {
		sdMap := make(map[accounts.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd := &StateDiff{sdMap: sdMap}
		if err := ibs.FinalizeTx(evm.ChainRules(), sd); err != nil {
			return nil, err
		}
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)
		initialIbs := state.New(stateReader)
		defer initialIbs.Close()
		if traceConfig != nil && traceConfig.StateOverrides != nil {
			if _, err := applyStateOverrides(initialIbs, traceConfig.StateOverrides, blockCtx.Rules(chainConfig)); err != nil {
				return nil, err
			}
		}
		if err := sd.CompareStates(initialIbs, ibs); err != nil {
			return nil, err
		}
	}

	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", api.evmCallTimeout)
	}

	if !traceTypeTrace {
		traceResult.Trace = []*ParityTrace{}
	}

	return traceResult, nil
}

// applyStateOverrides applies overrides as synthetic pre-state. Each call needs its own
// precompile set: Override consumes it via MovePrecompileTo, so a reused set fails.
func applyStateOverrides(ibs *state.IntraBlockState, overrides *ethapi.StateOverrides, rules *chain.Rules) (vm.PrecompiledContracts, error) {
	precompiles := vm.ActivePrecompiledContracts(rules)
	if err := overrides.Override(ibs, precompiles, rules); err != nil {
		return nil, err
	}
	return precompiles, nil
}

// overriddenStorageReader serves the storage of accounts with a full `state`
// override, so slots missing from the override read as zero instead of falling
// through to the database.
type overriddenStorageReader struct {
	state.StateReader
	storage map[accounts.Address]map[common.Hash]common.Hash
}

func withOverriddenStorage(r state.StateReader, overrides *ethapi.StateOverrides) state.StateReader {
	storage := make(map[accounts.Address]map[common.Hash]common.Hash)
	for addr, account := range *overrides {
		if account.State != nil {
			storage[addr] = *account.State
		}
	}
	if len(storage) == 0 {
		return r
	}
	return &overriddenStorageReader{StateReader: r, storage: storage}
}

func (r *overriddenStorageReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	slots, ok := r.storage[address]
	if !ok {
		return r.StateReader.ReadAccountStorage(address, key)
	}
	value, ok := slots[key.Value()]
	return *new(uint256.Int).SetBytes32(value[:]), ok, nil
}

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls json.RawMessage, parentNrOrHash *rpc.BlockNumberOrHash, traceConfig *config.TraceConfig) ([]*TraceCallResult, error) {
	tx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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
		if err := dec.Decode(args); err != nil {
			return nil, err
		}
		if err := dec.Decode(&args.traceTypes); err != nil {
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
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	for i := range callParams {
		if err := ethapi.CheckChainID(callParams[i].ChainID, chainConfig.ChainID); err != nil {
			return nil, fmt.Errorf("call %d: %w", i, err)
		}
	}
	var baseFee *uint256.Int
	if parentNrOrHash == nil {
		num := rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	if err := rejectPending(*parentNrOrHash); err != nil {
		return nil, err
	}
	blockNumber, hash, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, *parentNrOrHash, tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}

	parentHeader, err := api.headerByHashAndNumber(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if parentHeader == nil {
		return nil, fmt.Errorf("parent block %d(%x) not found", blockNumber, hash)
	}
	if parentHeader.BaseFee != nil {
		baseFee = parentHeader.BaseFee
	}
	baseFee = overrideBaseFee(traceConfig, baseFee)
	msgs := make([]*types.Message, len(callParams))
	txns := make([]types.Transaction, len(callParams))
	for i := range callParams {
		args := &callParams[i]
		msgs[i], err = args.ToMessage(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to msg: %w", err)
		}

		txns[i], err = args.ToTransaction(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to txn: %w", err)
		}
	}

	err = rpchelper.CheckBlockExecuted(tx, blockNumber)
	if err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, -1, api._txNumReader)
	if err != nil {
		return nil, err
	}
	var overrides *ethapi.StateOverrides
	if traceConfig != nil && traceConfig.StateOverrides != nil {
		overrides = traceConfig.StateOverrides
		stateReader = withOverriddenStorage(stateReader, overrides)
	}
	stateCache := shards.NewStateCache(
		32, 0, /* no limit */
	) // this cache living only during current RPC call, but required to store state writes
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)
	ibs := state.New(cachedReader)
	defer ibs.Close()

	trace, _, err := api.doCallBlock(ctx, tx, stateReader, stateCache, cachedWriter, ibs,
		txns, msgs, callParams, overrideHeader(traceConfig, parentHeader), parentNrOrHash.RequireCanonical, false /* gasBailout */, false /* advanceTxNum */, true /* noBaseFee */, traceConfig, overrides)
	if err != nil {
		return nil, callError(err)
	}
	return trace, nil
}

// advanceTxNum moves the history reader with the transaction index. Block
// replay needs it; an ad-hoc bundle must not, or a call reads state left by a
// real transaction it never executed.
func (api *TraceAPIImpl) doCallBlock(ctx context.Context, dbtx kv.Tx, stateReader state.StateReader,
	stateCache *shards.StateCache, cachedWriter state.StateWriter, ibs *state.IntraBlockState,
	txns []types.Transaction, msgs []*types.Message, callParams []TraceCallParam,
	header *types.Header, requireCanonical, gasBailout, advanceTxNum, noBaseFee bool,
	traceConfig *config.TraceConfig, overrides *ethapi.StateOverrides,
) ([]*TraceCallResult, *tracing.Hooks, error) {
	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, nil, err
	}
	engine := api.engine()

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

	var baseTxNum uint64
	historicalStateReader, isHistoricalStateReader := stateReader.(state.HistoricalStateReader)
	if isHistoricalStateReader {
		baseTxNum = historicalStateReader.GetTxNum()
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, requireCanonical, dbtx, api._blockReader, chainConfig)
	if err := overrideBlockContext(traceConfig, &blockCtx); err != nil {
		return nil, nil, err
	}
	var precompiles vm.PrecompiledContracts
	if overrides != nil {
		rules := blockCtx.Rules(chainConfig)
		if precompiles, err = applyStateOverrides(ibs, overrides, rules); err != nil {
			return nil, nil, err
		}
		// Committed to the cache because a stateDiff call resets ibs. The reset
		// here drops the fake storage, whose committed value tracks every write.
		if err := ibs.CommitOverrideDirtyAccounts(rules, cachedWriter, ibs.ExtractAndClearDirty()); err != nil {
			return nil, nil, err
		}
		ibs.Reset()
	}
	var tracer *tracers.Tracer
	var tracingHooks *tracing.Hooks

	for txIndex, msg := range msgs {
		if isHistoricalStateReader && advanceTxNum {
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
		vmConfig := vm.Config{NoBaseFee: noBaseFee}
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

		// Reset and clone only needed when stateDiff is requested:
		// stateDiff requires per-tx isolation to compute before/after state.
		// For trace/vmTrace only, skip Reset to match whole-block replay semantics (issue #12607).
		var cloneReader state.StateReader
		var sd *StateDiff
		if traceTypeStateDiff {
			ibs.Reset()
			cloneCache := stateCache.Clone()
			cloneReader = state.NewCachedReader(stateReader, cloneCache)
			sdMap := make(map[accounts.Address]*StateDiffAccount)
			traceResult.StateDiff = sdMap
			sd = &StateDiff{sdMap: sdMap}
		}

		ibs.SetTxContext(blockCtx.BlockNumber, txIndex)
		if tracer != nil {
			ibs.SetHooks(tracer.Hooks)
		}
		txCtx := protocol.NewEVMTxContext(msg)
		txBlockCtx := blockCtx
		if noBaseFee {
			args.zeroUnpricedBlobBaseFee(&txBlockCtx)
		}
		evm := vm.NewEVM(vm.ZeroUnpricedBaseFee(txBlockCtx, txCtx, vmConfig), txCtx, ibs, chainConfig, vmConfig)
		if precompiles != nil {
			evm.SetPrecompiles(precompiles)
		}
		gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())

		if tracer != nil && tracer.Hooks.OnTxStart != nil {
			tracer.Hooks.OnTxStart(evm.GetVMContext(), txns[txIndex], msg.From())
		}
		execResult, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /* gasBailout */, engine)
		if err != nil {
			if tracer != nil {
				tracer.Hooks.EmitTxEnd(nil, mdgas.TxnGasUsage{}, err)
			}
			return nil, nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
		}

		if tracer != nil && tracer.Hooks.HasTxEndHook() {
			tracer.Hooks.EmitTxEnd(&types.Receipt{GasUsed: execResult.ReceiptGasUsed}, execResult.TxnGasUsage, nil)
		}

		chainRules := blockCtx.Rules(chainConfig)
		traceResult.Output = bytes.Clone(execResult.ReturnData)
		if traceTypeStateDiff {
			// Closure so the per-tx state is closed on every exit path.
			if err := func() error {
				initialIbs := state.New(cloneReader)
				defer initialIbs.Close()
				if err := ibs.FinalizeTx(chainRules, sd); err != nil {
					return err
				}
				if sd != nil {
					return sd.CompareStates(initialIbs, ibs)
				}
				return nil
			}(); err != nil {
				return nil, nil, err
			}
		} else {
			// Write into stateCache even when no stateDiff is requested: a later
			// stateDiff call resets ibs and rebuilds its state from the cache.
			if err := ibs.FinalizeTx(chainRules, cachedWriter); err != nil {
				return nil, nil, err
			}
		}
		if traceTypeStateDiff {
			// CommitBlock after each tx to flush ibs changes into stateCache,
			// so the next tx's cloneReader captures the correct "before" state
			if err := ibs.CommitBlock(chainRules, cachedWriter); err != nil {
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
	txn types.Transaction, callParam TraceCallParam,
	header *types.Header, requireCanonical, gasBailout bool, txIndex int,
	traceConfig *config.TraceConfig,
) (*TraceCallResult, error) {
	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()
	noop := state.NewNoopWriter()

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

	var baseTxNum uint64
	historicalStateReader, isHistoricalStateReader := stateReader.(state.HistoricalStateReader)
	if isHistoricalStateReader {
		baseTxNum = historicalStateReader.GetTxNum()
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, requireCanonical, dbtx, api._blockReader, chainConfig)
	if err := overrideBlockContext(traceConfig, &blockCtx); err != nil {
		return nil, err
	}
	chainRules := blockCtx.Rules(chainConfig)
	signer := types.MakeSigner(chainConfig, blockCtx.BlockNumber, blockCtx.Time)
	if err := checkOverriddenSigner(traceConfig, signer, txn); err != nil {
		return nil, fmt.Errorf("convert txn into msg: %w", err)
	}
	msg, err := txn.AsMessage(*signer, &blockCtx.BaseFee, chainRules)
	if err != nil {
		return nil, fmt.Errorf("convert txn into msg: %w", err)
	}

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
		sdMap := make(map[accounts.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd = &StateDiff{sdMap: sdMap}
	}

	ibs.Reset()
	ibs.SetTxContext(blockCtx.BlockNumber, txIndex)
	txCtx := protocol.NewEVMTxContext(msg)
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnTxStart != nil {
		vmConfig.Tracer.OnTxStart(evm.GetVMContext(), txn, msg.From())
	}
	execResult, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /*gasBailout*/, engine)
	if err != nil {
		return nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
	}

	traceResult.Output = bytes.Clone(execResult.ReturnData)
	if traceTypeStateDiff {
		initialIbs := state.New(cloneReader)
		defer initialIbs.Close()
		if err := ibs.FinalizeTx(chainRules, sd); err != nil {
			return nil, err
		}

		if sd != nil {
			if err := sd.CompareStates(initialIbs, ibs); err != nil {
				return nil, err
			}
		}

		if err := ibs.CommitBlock(chainRules, cachedWriter); err != nil {
			return nil, err
		}
	} else {
		if err := ibs.FinalizeTx(chainRules, noop); err != nil {
			return nil, err
		}
		if err := ibs.CommitBlock(chainRules, cachedWriter); err != nil {
			return nil, err
		}
	}
	if !traceTypeTrace {
		traceResult.Trace = []*ParityTrace{}
	}

	return traceResult, nil
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, encodedTx hexutil.Bytes, traceTypes []string) (*TraceCallResult, error) {
	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return nil, err
	}
	if api.gasCap != 0 && txn.GetGasLimit() > api.gasCap {
		return nil, clientLimitExceededError(fmt.Sprintf("transaction gas limit %d exceeds the RPC gas cap %d", txn.GetGasLimit(), api.gasCap))
	}

	dbtx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	num := rpc.LatestBlockNumber
	blockNrOrHash := rpc.BlockNumberOrHash{BlockNumber: &num}

	blockNumber, hash, latest, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, dbtx, api._blockReader)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, dbtx, blockNumber)
	if err != nil {
		return nil, err
	}

	header, err := api.headerByHashAndNumber(ctx, dbtx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}

	err = rpchelper.CheckBlockExecuted(dbtx, blockNumber)
	if err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, dbtx, blockNumber, latest, 0, api._txNumReader)
	if err != nil {
		return nil, err
	}

	_, storeEVM, cleanup := setupEVMTimeout(ctx, api.evmCallTimeout)
	defer cleanup()

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

	ibs := state.New(stateReader)
	defer ibs.Close()

	var ot OeTracer
	ot.config, err = parseOeTracerConfig(nil)
	if err != nil {
		return nil, err
	}
	ot.compat = api.compatibility
	vmConfig := vm.Config{}
	if traceTypeTrace || traceTypeVmTrace {
		ot.r = traceResult
		ot.traceAddr = []int{}
		vmConfig.Tracer = ot.Tracer().Hooks
	}

	signer := types.MakeSigner(chainConfig, header.Number.Uint64(), header.Time)
	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, dbtx, api._blockReader, chainConfig)
	rules := blockCtx.Rules(chainConfig)

	// Keep the nonce, EIP-3607 sender-code and EIP-7825 gas-limit checks that
	// AsMessage enables: a signed transaction is traced only if it is valid at
	// the latest state.
	msg, err := txn.AsMessage(*signer, header.BaseFee, rules)
	if err != nil {
		return nil, err
	}

	txCtx := protocol.NewEVMTxContext(msg)

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
	storeEVM(evm)

	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	var execResult *evmtypes.ExecutionResult
	ibs.SetTxContext(blockCtx.BlockNumber, 0)
	ibs.SetHooks(vmConfig.Tracer)

	if vmConfig.Tracer != nil && vmConfig.Tracer.OnTxStart != nil {
		vmConfig.Tracer.OnTxStart(evm.GetVMContext(), txn, msg.From())
	}
	// A signed transaction pays for its own gas, so no gas bailout: the sender
	// is charged for value and gas as it would be in a block.
	execResult, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		vmConfig.Tracer.EmitTxEnd(nil, mdgas.TxnGasUsage{}, err)
		return nil, err
	}
	if vmConfig.Tracer.HasTxEndHook() {
		vmConfig.Tracer.EmitTxEnd(&types.Receipt{GasUsed: execResult.ReceiptGasUsed}, execResult.TxnGasUsage, nil)
	}

	traceResult.Output = bytes.Clone(execResult.ReturnData)

	if traceTypeStateDiff {
		sdMap := make(map[accounts.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd := &StateDiff{sdMap: sdMap}
		if err := ibs.FinalizeTx(evm.ChainRules(), sd); err != nil {
			return nil, err
		}
		initialIbs := state.New(stateReader)
		defer initialIbs.Close()
		if err := sd.CompareStates(initialIbs, ibs); err != nil {
			return nil, err
		}
	}

	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", api.evmCallTimeout)
	}

	if !traceTypeTrace {
		traceResult.Trace = []*ParityTrace{}
	}

	return traceResult, nil
}
