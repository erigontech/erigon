package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	math2 "github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/log/v3"
)

const callTimeout = 5 * time.Minute

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
	From                 *common.Address   `json:"from"`
	To                   *common.Address   `json:"to"`
	Gas                  *hexutil.Uint64   `json:"gas"`
	GasPrice             *hexutil.Big      `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big      `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big      `json:"maxFeePerGas"`
	Value                *hexutil.Big      `json:"value"`
	Data                 hexutil.Bytes     `json:"data"`
	AccessList           *types.AccessList `json:"accessList"`
	txHash               *common.Hash
	traceTypes           []string
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
func (args *TraceCallParam) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (types.Message, error) {
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
		gasPrice  *uint256.Int
		gasFeeCap *uint256.Int
		gasTipCap *uint256.Int
	)
	if baseFee == nil {
		// If there's no basefee, then it must be a non-1559 execution
		gasPrice = new(uint256.Int)
		if args.GasPrice != nil {
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
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
				return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas feilds (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
				}
			}
			// Backfill the legacy gasPrice for EVM execution, unless we're all zeroes
			gasPrice = new(uint256.Int)
			if gasFeeCap.BitLen() > 0 || gasTipCap.BitLen() > 0 {
				gasPrice = math2.U256Min(new(uint256.Int).Add(gasTipCap, baseFee), gasFeeCap)
			}
			// Still zero
			if gasPrice.IsZero() {
				gasPrice.Set(baseFee)
			}
		}
	}
	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			panic(fmt.Errorf("args.Value higher than 2^256-1"))
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
	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, gasFeeCap, gasTipCap, data, accessList, false /* checkNonce */)
	return msg, nil
}

// OpenEthereum-style tracer
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
}

func (ot *OeTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	if ot.r.VmTrace != nil {
		var vmTrace *VmTrace
		if depth > 0 {
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
	if precompile {
		ot.precompile = true
		return nil
	}
	if gas > 500000000 {
		gas = 500000001 - (0x8000000000000000 - gas)
	}
	//fmt.Printf("CaptureStart depth %d, from %x, to %x, create %t, input %x, gas %d, value %d\n", depth, from, to, create, input, gas, value)
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
	if depth > 0 {
		topTrace := ot.traceStack[len(ot.traceStack)-1]
		traceIdx := topTrace.Subtraces
		ot.traceAddr = append(ot.traceAddr, traceIdx)
		topTrace.Subtraces++
		if calltype == vm.DELEGATECALLT {
			switch action := topTrace.Action.(type) {
			case *CreateTraceAction:
				value = action.Value.ToInt()
			case *CallTraceAction:
				value = action.Value.ToInt()
			}
		}
		if calltype == vm.STATICCALLT {
			value = big.NewInt(0)
		}
	}
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	if create {
		action := CreateTraceAction{}
		action.From = from
		action.Gas.ToInt().SetUint64(gas)
		action.Init = common.CopyBytes(input)
		action.Value.ToInt().Set(value)
		trace.Action = &action
	} else {
		action := CallTraceAction{}
		switch calltype {
		case vm.CALLT:
			action.CallType = CALL
		case vm.CALLCODET:
			action.CallType = CALLCODE
		case vm.DELEGATECALLT:
			action.CallType = DELEGATECALL
		case vm.STATICCALLT:
			action.CallType = STATICCALL
		}
		action.From = from
		action.To = to
		action.Gas.ToInt().SetUint64(gas)
		action.Input = common.CopyBytes(input)
		action.Value.ToInt().Set(value)
		trace.Action = &action
	}
	ot.r.Trace = append(ot.r.Trace, trace)
	ot.traceStack = append(ot.traceStack, trace)
	return nil
}

func (ot *OeTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) error {
	if ot.r.VmTrace != nil {
		if len(ot.vmOpStack) > 0 {
			ot.lastOffStack = ot.vmOpStack[len(ot.vmOpStack)-1]
			ot.vmOpStack = ot.vmOpStack[:len(ot.vmOpStack)-1]
		}
		if !ot.compat && depth > 0 {
			ot.idx = ot.idx[:len(ot.idx)-1]
		}
		if depth > 0 {
			ot.lastMemOff = ot.memOffStack[len(ot.memOffStack)-1]
			ot.memOffStack = ot.memOffStack[:len(ot.memOffStack)-1]
			ot.lastMemLen = ot.memLenStack[len(ot.memLenStack)-1]
			ot.memLenStack = ot.memLenStack[:len(ot.memLenStack)-1]
		}
	}
	if ot.precompile {
		ot.precompile = false
		return nil
	}
	if depth == 0 {
		ot.r.Output = common.CopyBytes(output)
	}
	ignoreError := false
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	if ot.compat {
		ignoreError = depth == 0 && topTrace.Type == CREATE
	}
	if err != nil && !ignoreError {
		switch err {
		case vm.ErrInvalidJump:
			topTrace.Error = "Bad jump destination"
		case vm.ErrContractAddressCollision, vm.ErrCodeStoreOutOfGas, vm.ErrOutOfGas, vm.ErrGasUintOverflow:
			topTrace.Error = "Out of gas"
		case vm.ErrExecutionReverted:
			topTrace.Error = "Reverted"
		case vm.ErrWriteProtection:
			topTrace.Error = "Mutable Call In Static Context"
		default:
			switch err.(type) {
			case *vm.ErrStackUnderflow:
				topTrace.Error = "Stack underflow"
			case *vm.ErrInvalidOpCode:
				topTrace.Error = "Bad instruction"
			default:
				topTrace.Error = err.Error()
			}
		}
		topTrace.Result = nil
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
			topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(startGas - endGas)
		case CREATE:
			topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(startGas - endGas)
		}
	}
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if depth > 0 {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
	return nil
}

func (ot *OeTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, st *stack.Stack, rData []byte, contract *vm.Contract, opDepth int, err error) error {
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
			case ot.lastOp >= vm.PUSH1 && ot.lastOp <= vm.PUSH32:
				showStack = 1
			case ot.lastOp >= vm.SWAP1 && ot.lastOp <= vm.SWAP16:
				showStack = int(ot.lastOp-vm.SWAP1) + 2
			case ot.lastOp >= vm.DUP1 && ot.lastOp <= vm.DUP16:
				showStack = int(ot.lastOp-vm.DUP1) + 2
			}
			switch ot.lastOp {
			case vm.CALLDATALOAD, vm.SLOAD, vm.MLOAD, vm.CALLDATASIZE, vm.LT, vm.GT, vm.DIV, vm.SDIV, vm.SAR, vm.AND, vm.EQ, vm.CALLVALUE, vm.ISZERO,
				vm.ADD, vm.EXP, vm.CALLER, vm.SHA3, vm.SUB, vm.ADDRESS, vm.GAS, vm.MUL, vm.RETURNDATASIZE, vm.NOT, vm.SHR, vm.SHL,
				vm.EXTCODESIZE, vm.SLT, vm.OR, vm.NUMBER, vm.PC, vm.TIMESTAMP, vm.BALANCE, vm.SELFBALANCE, vm.MULMOD, vm.ADDMOD, vm.BASEFEE,
				vm.BLOCKHASH, vm.BYTE, vm.XOR, vm.ORIGIN, vm.CODESIZE, vm.MOD, vm.SIGNEXTEND, vm.GASLIMIT, vm.DIFFICULTY, vm.SGT, vm.GASPRICE,
				vm.MSIZE, vm.EXTCODEHASH:
				showStack = 1
			}
			for i := showStack - 1; i >= 0; i-- {
				ot.lastVmOp.Ex.Push = append(ot.lastVmOp.Ex.Push, st.Back(i).String())
			}
			// Set the "mem" of the last operation
			var setMem bool
			switch ot.lastOp {
			case vm.MSTORE, vm.MSTORE8, vm.MLOAD, vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY:
				setMem = true
			}
			if setMem && ot.lastMemLen > 0 {
				cpy := memory.GetCopy(ot.lastMemOff, ot.lastMemLen)
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastVmOp.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
		}
		if ot.lastOffStack != nil {
			ot.lastOffStack.Ex.Used = int(gas)
			ot.lastOffStack.Ex.Push = []string{st.Back(0).String()}
			if ot.lastMemLen > 0 && memory != nil {
				cpy := memory.GetCopy(ot.lastMemOff, ot.lastMemLen)
				if len(cpy) == 0 {
					cpy = make([]byte, ot.lastMemLen)
				}
				ot.lastOffStack.Ex.Mem = &VmTraceMem{Data: fmt.Sprintf("0x%0x", cpy), Off: int(ot.lastMemOff)}
			}
			ot.lastOffStack = nil
		}
		if ot.lastOp == vm.STOP && op == vm.STOP && len(ot.vmOpStack) == 0 {
			// Looks like OE is "optimising away" the second STOP
			return nil
		}
		ot.lastVmOp = &VmTraceOp{Ex: &VmTraceEx{}}
		vmTrace.Ops = append(vmTrace.Ops, ot.lastVmOp)
		if !ot.compat {
			var sb strings.Builder
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
			ot.lastMemOff = st.Back(0).Uint64()
			ot.lastMemLen = 32
		case vm.MSTORE8:
			ot.lastMemOff = st.Back(0).Uint64()
			ot.lastMemLen = 1
		case vm.RETURNDATACOPY, vm.CALLDATACOPY, vm.CODECOPY:
			ot.lastMemOff = st.Back(0).Uint64()
			ot.lastMemLen = st.Back(2).Uint64()
		case vm.STATICCALL, vm.DELEGATECALL:
			ot.memOffStack = append(ot.memOffStack, st.Back(4).Uint64())
			ot.memLenStack = append(ot.memLenStack, st.Back(5).Uint64())
		case vm.CALL, vm.CALLCODE:
			ot.memOffStack = append(ot.memOffStack, st.Back(5).Uint64())
			ot.memLenStack = append(ot.memLenStack, st.Back(6).Uint64())
		case vm.CREATE, vm.CREATE2:
			// Effectively disable memory output
			ot.memOffStack = append(ot.memOffStack, 0)
			ot.memLenStack = append(ot.memLenStack, 0)
		case vm.SSTORE:
			ot.lastVmOp.Ex.Store = &VmTraceStore{Key: st.Back(0).String(), Val: st.Back(1).String()}
		}
		if ot.lastVmOp.Ex.Used < 0 {
			ot.lastVmOp.Ex = nil
		}
	}
	return nil
}

func (ot *OeTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, opDepth int, err error) error {
	return nil
}

func (ot *OeTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	trace := &ParityTrace{}
	trace.Type = SUICIDE
	action := &SuicideTraceAction{}
	action.Address = from
	action.RefundAddress = to
	action.Balance.ToInt().Set(value)
	trace.Action = action
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	traceIdx := topTrace.Subtraces
	ot.traceAddr = append(ot.traceAddr, traceIdx)
	topTrace.Subtraces++
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	ot.r.Trace = append(ot.r.Trace, trace)
}

func (ot *OeTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ot *OeTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

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

func (sd *StateDiff) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	accountDiff := sd.sdMap[address]
	if accountDiff == nil {
		accountDiff = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
		sd.sdMap[address] = accountDiff
	}
	m := make(map[string]interface{})
	m["*"] = &StateDiffStorage{From: common.BytesToHash(original.Bytes()), To: common.BytesToHash(value.Bytes())}
	accountDiff.Storage[*key] = m
	return nil
}

func (sd *StateDiff) CreateContract(address common.Address) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

// CompareStates uses the addresses accumulated in the sdMap and compares balances, nonces, and codes of the accounts, and fills the rest of the sdMap
func (sd *StateDiff) CompareStates(initialIbs, ibs *state.IntraBlockState) {
	var toRemove []common.Address
	for addr, accountDiff := range sd.sdMap {
		initialExist := initialIbs.Exist(addr)
		exist := ibs.Exist(addr)
		if initialExist {
			if exist {
				var allEqual = len(accountDiff.Storage) == 0
				fromBalance := initialIbs.GetBalance(addr).ToBig()
				toBalance := ibs.GetBalance(addr).ToBig()
				if fromBalance.Cmp(toBalance) == 0 {
					accountDiff.Balance = "="
				} else {
					m := make(map[string]*StateDiffBalance)
					m["*"] = &StateDiffBalance{From: (*hexutil.Big)(fromBalance), To: (*hexutil.Big)(toBalance)}
					accountDiff.Balance = m
					allEqual = false
				}
				fromCode := initialIbs.GetCode(addr)
				toCode := ibs.GetCode(addr)
				if bytes.Equal(fromCode, toCode) {
					accountDiff.Code = "="
				} else {
					m := make(map[string]*StateDiffCode)
					m["*"] = &StateDiffCode{From: fromCode, To: toCode}
					accountDiff.Code = m
					allEqual = false
				}
				fromNonce := initialIbs.GetNonce(addr)
				toNonce := ibs.GetNonce(addr)
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
					m := make(map[string]*hexutil.Big)
					m["-"] = (*hexutil.Big)(initialIbs.GetBalance(addr).ToBig())
					accountDiff.Balance = m
				}
				{
					m := make(map[string]hexutil.Bytes)
					m["-"] = initialIbs.GetCode(addr)
					accountDiff.Code = m
				}
				{
					m := make(map[string]hexutil.Uint64)
					m["-"] = hexutil.Uint64(initialIbs.GetNonce(addr))
					accountDiff.Nonce = m
				}
			}
		} else if exist {
			{
				m := make(map[string]*hexutil.Big)
				m["+"] = (*hexutil.Big)(ibs.GetBalance(addr).ToBig())
				accountDiff.Balance = m
			}
			{
				m := make(map[string]hexutil.Bytes)
				m["+"] = ibs.GetCode(addr)
				accountDiff.Code = m
			}
			{
				m := make(map[string]hexutil.Uint64)
				m["+"] = hexutil.Uint64(ibs.GetNonce(addr))
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
}

func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) (*TraceCallResult, error) {
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	blockNumber, err := rawdb.ReadTxLookupEntry(tx, txHash)
	if err != nil {
		return nil, err
	}
	if blockNumber == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	// Extract transactions from block
	block, _, bErr := rawdb.ReadBlockByNumberWithSenders(tx, *blockNumber)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", *blockNumber)
	}
	var txIndex int
	for idx, txn := range block.Transactions() {
		if txn.Hash() == txHash {
			txIndex = idx
			break
		}
	}
	bn := hexutil.Uint64(*blockNumber)

	parentNr := bn
	if parentNr > 0 {
		parentNr -= 1
	}

	// Returns an array of trace arrays, one trace array for each transaction
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), traceTypes, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), txIndex, types.MakeSigner(chainConfig, *blockNumber))
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
		if txno == txIndex {
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

func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, traceTypes []string) ([]*TraceCallResult, error) {
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	parentNr := blockNumber
	if parentNr > 0 {
		parentNr -= 1
	}
	// Extract transactions from block
	block, _, bErr := rawdb.ReadBlockByNumberWithSenders(tx, blockNumber)
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
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), traceTypes, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), -1 /* all tx indices */, types.MakeSigner(chainConfig, blockNumber))
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
		result[i] = tr
		txhash := block.Transactions()[i].Hash()
		tr.TransactionHash = &txhash
	}

	return result, nil
}

// Call implements trace_call.
func (api *TraceAPIImpl) Call(ctx context.Context, args TraceCallParam, traceTypes []string, blockNrOrHash *rpc.BlockNumberOrHash) (*TraceCallResult, error) {
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	if blockNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(tx)
	} else {
		stateReader = state.NewPlainState(tx, blockNumber)
	}
	ibs := state.New(stateReader)

	header := rawdb.ReadHeader(tx, hash, blockNumber)
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if callTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
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
			return nil, fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(api.gasCap, baseFee)
	if err != nil {
		return nil, err
	}

	blockCtx, txCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, tx, ethdb.GetHasTEVM(tx))
	blockCtx.GasLimit = math.MaxUint64
	blockCtx.MaxGasLimit = true

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: traceTypeTrace, Tracer: &ot})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas())
	var execResult *core.ExecutionResult
	ibs.Prepare(common.Hash{}, common.Hash{}, 0)
	execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
	if err != nil {
		return nil, err
	}
	traceResult.Output = common.CopyBytes(execResult.ReturnData)
	if traceTypeStateDiff {
		sdMap := make(map[common.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd := &StateDiff{sdMap: sdMap}
		if err = ibs.FinalizeTx(evm.ChainRules, sd); err != nil {
			return nil, err
		}
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)
		initialIbs := state.New(stateReader)
		sd.CompareStates(initialIbs, ibs)
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}

	return traceResult, nil
}

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls json.RawMessage, parentNrOrHash *rpc.BlockNumberOrHash) ([]*TraceCallResult, error) {
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
		return nil, fmt.Errorf("expected array of [callparam, tracetypes]")
	}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if tok != json.Delim('[') {
			return nil, fmt.Errorf("expected [callparam, tracetypes]")
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
			return nil, fmt.Errorf("expected end of [callparam, tracetypes]")
		}
	}
	tok, err = dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim(']') {
		return nil, fmt.Errorf("expected end of array of [callparam, tracetypes]")
	}
	var baseFee *uint256.Int
	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*parentNrOrHash, dbtx, api.filters)
	if err != nil {
		return nil, err
	}
	parentHeader := rawdb.ReadHeader(dbtx, hash, blockNumber)
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %d(%x) not found", blockNumber, hash)
	}
	if parentHeader != nil && parentHeader.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(parentHeader.BaseFee)
		if overflow {
			return nil, fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msgs := make([]types.Message, len(callParams))
	for i, args := range callParams {
		msgs[i], err = args.ToMessage(api.gasCap, baseFee)
		if err != nil {
			return nil, fmt.Errorf("convert callParam to msg: %w", err)
		}
	}
	return api.doCallMany(ctx, dbtx, msgs, callParams, parentNrOrHash, nil, true /* gasBailout */, -1 /* all tx indices */)
}

func (api *TraceAPIImpl) doCallMany(ctx context.Context, dbtx kv.Tx, msgs []types.Message, callParams []TraceCallParam, parentNrOrHash *rpc.BlockNumberOrHash, header *types.Header,
	gasBailout bool, txIndexNeeded int) ([]*TraceCallResult, error) {
	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	if parentNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		parentNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*parentNrOrHash, dbtx, api.filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := parentNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainState(dbtx, blockNumber)
	}
	stateCache := shards.NewStateCache(32, 0 /* no limit */)
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)

	parentHeader := rawdb.ReadHeader(dbtx, hash, blockNumber)
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %d(%x) not found", blockNumber, hash)
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if callTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()
	results := []*TraceCallResult{}

	useParent := false
	if header == nil {
		header = parentHeader
		useParent = true
	}
	for txIndex, msg := range msgs {
		if err := common.Stopped(ctx.Done()); err != nil {
			return nil, err
		}
		traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
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
				return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
			}
		}
		vmConfig := vm.Config{}
		if (traceTypeTrace && (txIndexNeeded == -1 || txIndex == txIndexNeeded)) || traceTypeVmTrace {
			var ot OeTracer
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

		// Get a new instance of the EVM.
		blockCtx, txCtx := transactions.GetEvmContext(msg, header, parentNrOrHash.RequireCanonical, dbtx, ethdb.GetHasTEVM(dbtx))
		if useParent {
			blockCtx.GasLimit = math.MaxUint64
			blockCtx.MaxGasLimit = true
		}
		ibs := state.New(cachedReader)
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)

		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)

		gp := new(core.GasPool).AddGas(msg.Gas())
		var execResult *core.ExecutionResult
		// Clone the state cache before applying the changes, clone is discarded
		var cloneReader state.StateReader
		if traceTypeStateDiff {
			cloneCache := stateCache.Clone()
			cloneReader = state.NewCachedReader(stateReader, cloneCache)
		}
		if args.txHash != nil {
			ibs.Prepare(*args.txHash, header.Hash(), txIndex)
		} else {
			ibs.Prepare(common.Hash{}, header.Hash(), txIndex)
		}
		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailout /* gasBailout */)
		if err != nil {
			return nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
		}
		traceResult.Output = common.CopyBytes(execResult.ReturnData)
		if traceTypeStateDiff {
			initialIbs := state.New(cloneReader)
			sdMap := make(map[common.Address]*StateDiffAccount)
			traceResult.StateDiff = sdMap
			sd := &StateDiff{sdMap: sdMap}
			if err = ibs.FinalizeTx(evm.ChainRules, sd); err != nil {
				return nil, err
			}
			sd.CompareStates(initialIbs, ibs)
			if err = ibs.CommitBlock(evm.ChainRules, cachedWriter); err != nil {
				return nil, err
			}
		} else {
			if err = ibs.FinalizeTx(evm.ChainRules, noop); err != nil {
				return nil, err
			}
			if err = ibs.CommitBlock(evm.ChainRules, cachedWriter); err != nil {
				return nil, err
			}
		}
		if !traceTypeTrace {
			traceResult.Trace = []*ParityTrace{}
		}
		results = append(results, traceResult)
	}
	return results, nil
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_rawTransaction")
}
