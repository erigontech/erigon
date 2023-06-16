package sentio

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type Trace struct {
	//	only in debug mode
	Name string `json:"name,omitempty"`

	Type string `json:"type"`
	Pc   uint64 `json:"pc"`
	// Global index of the trace
	Index int `json:"index"`

	// Gas remaining before the OP
	Gas math.HexOrDecimal64 `json:"gas"`
	// Gas for the entire call
	GasUsed math.HexOrDecimal64 `json:"gasUsed"`
	// Gas cost for the OP, just help the computation
	gasCost uint64

	From *libcommon.Address `json:"from,omitempty"`
	// Used by call
	To *libcommon.Address `json:"to,omitempty"`
	// Input
	Input string `json:"input,omitempty"` // TODO better struct it and make it bytes
	// Ether transfered
	Value hexutil.Bytes `json:"value,omitempty"`
	// Return for calls
	Output   hexutil.Bytes `json:"output,omitempty"`
	Error    string        `json:"error,omitempty"`
	Revertal string        `json:"revertReason,omitempty"`

	// Used by jump
	InputStack   []uint256.Int `json:"inputStack,omitempty"`
	InputMemory  *[]string     `json:"inputMemory,omitempty"`
	OutputStack  []uint256.Int `json:"outputStack,omitempty"`
	OutputMemory *[]string     `json:"outputMemory,omitempty"`
	FunctionPc   uint64        `json:"functionPc,omitempty"`

	// Used by log
	Address *libcommon.Address `json:"address,omitempty"`
	Data    hexutil.Bytes      `json:"data,omitempty"`
	Topics  []hexutil.Bytes    `json:"topics,omitempty"`

	// Only used by root
	Traces []Trace `json:"traces,omitempty"`

	// Only set in debug mode
	TracerConfig *sentioTracerConfig `json:"tracerConfig,omitempty"`

	// Use for internal call stack organization
	// The jump to go into the function
	//enterPc uint64
	exitPc uint64

	// the function get called
	function *functionInfo
}

type sentioTracer struct {
	config      sentioTracerConfig
	env         vm.VMInterface
	functionMap map[string]map[uint64]functionInfo
	callMap     map[string]map[uint64]uint64

	previousJump *Trace
	index        int
	entryPc      map[uint64]bool

	callstack []Trace
	gasLimit  uint64
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *sentioTracer) CaptureTxEnd(restGas uint64) {
	t.callstack[0].GasUsed = math.HexOrDecimal64(t.gasLimit - restGas)
	if t.callstack[0].Index == -1 {
		// It's possible that we can't correctly locate the PC that match the entry function (check why), in this case we need to 0 for the user
		t.callstack[0].Index = 0
	}
}

func (t *sentioTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env

	root := Trace{
		Index: -1,
		Type:  vm.CALL.String(),
		From:  &from,
		To:    &to,
		Gas:   math.HexOrDecimal64(gas),
		Input: hexutil.Bytes(input).String(),
	}
	if value != nil {
		root.Value = value.Bytes()
	}
	if create {
		root.Type = vm.CREATE.String()
	}

	if !create && !precompile && len(input) >= 4 {
		m, ok := t.functionMap[to.String()]
		if ok {
			sigHash := "0x" + common.Bytes2Hex(input[0:4])
			for pc, fn := range m {
				if fn.SignatureHash == sigHash {
					t.entryPc[pc] = true
				}
			}
			log.Info(fmt.Sprintf("entry pc match %s (%d times) ", sigHash, len(t.entryPc)))
		}
	}
	t.callstack = append(t.callstack, root)
}

func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.callstack[0].GasUsed = math.HexOrDecimal64(usedGas)
	t.callstack[0].Output = common.CopyBytes(output)

	stackSize := len(t.callstack)
	currentGas := uint64(t.callstack[stackSize-1].Gas) - usedGas
	for j := stackSize - 1; j > 0; j-- {
		t.callstack[j].Output = common.CopyBytes(output)
		t.callstack[j].GasUsed = math.HexOrDecimal64(uint64(t.callstack[j].Gas) - currentGas + t.callstack[j].gasCost)
		t.callstack[j-1].Traces = append(t.callstack[j-1].Traces, t.callstack[j])
	}
	t.callstack = t.callstack[:1]
}

func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	size := len(t.callstack)

	t.callstack[size-1].From = &from
	t.callstack[size-1].To = &to
	t.callstack[size-1].Input = hexutil.Bytes(input).String()
	t.callstack[size-1].Gas = math.HexOrDecimal64(gas)

	if value != nil {
		t.callstack[size-1].Value = value.Bytes()
	}

func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	size := len(t.callstack)
	if size <= 1 {
		return
	}

	//log.Info(fmt.Sprintf("CaptureExit pop frame %s", t.callstack[size-1].Type))

	stackSize := len(t.callstack)
	for i := stackSize - 1; i >= 0; i-- {
		if t.callstack[i].function != nil {
			continue
		}

		if stackSize-i > 1 {
			log.Info(fmt.Sprintf("tail call optimization [external] size %d", stackSize-i))
		}

		call := t.callstack[i]
		call.GasUsed = math.HexOrDecimal64(usedGas)
		currentGas := uint64(call.Gas) - usedGas + call.gasCost
		for j := stackSize - 1; j >= i; j-- {
			t.callstack[j].Output = common.CopyBytes(output)
			t.callstack[j].GasUsed = math.HexOrDecimal64(uint64(t.callstack[j].Gas) - currentGas + t.callstack[j].gasCost)
			t.callstack[j-1].Traces = append(t.callstack[j-1].Traces, t.callstack[j])
		}
		call.GasUsed = math.HexOrDecimal64(usedGas)

		t.callstack = t.callstack[:i]

		return
	}

	log.Error(fmt.Sprintf("failed to pop stack"))

}

func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	t.index++

	if t.callstack[0].Index == -1 && t.entryPc[pc] {
		//fillback the index and PC for root
		t.callstack[0].Pc = pc
		t.callstack[0].DefPc = pc
		t.callstack[0].Index = t.index - 1
		t.previousJump = nil
		return
	}

	var mergeBase = func(trace Trace) Trace {
		trace.Pc = pc
		trace.Type = op.String()
		trace.Gas = math.HexOrDecimal64(gas)
		trace.gasCost = cost
		trace.Index = t.index - 1

		// Assume it's single instruction, adjust it for jump and call
		trace.GasUsed = math.HexOrDecimal64(cost)
		if err != nil {
			// set error for instruction
			trace.Error = err.Error()
		}
		return trace
	}

	var copyMemory = func(offset *uint256.Int, size *uint256.Int) hexutil.Bytes {
		// it's important to get copy
		return scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	}

	var formatMemory = func() *[]string {
		memory := make([]string, 0, (scope.Memory.Len()+31)/32)
		for i := 0; i+32 <= scope.Memory.Len(); i += 32 {
			memory = append(memory, fmt.Sprintf("%x", scope.Memory.GetPtr(int64(i), 32)))
		}
		return &memory
	}

	switch op {
	case vm.CREATE, vm.CREATE2, vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL, vm.SELFDESTRUCT:
		// more info to be add at CaptureEnter
		call := mergeBase(Trace{
			Type: op.String(),
		})
		t.callstack = append(t.callstack, call)
	case vm.JUMP:
		from := scope.Contract.CodeAddr
		jump := mergeBase(Trace{
			From: from,
			//InputStack: append([]uint256.Int(nil), scope.Stack.Data...), // TODO only need partial
		})
		if t.previousJump != nil {
			log.Error("Unexpected previous jump", t.previousJump)
		}
		if err == nil {
			t.previousJump = &jump
		} else {
			log.Error("error in jump", "err", err)
			// error happend, attach to current frame
			t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, jump)
		}
	case vm.JUMPDEST:
		from := scope.Contract.CodeAddr
		fromStr := from.String()

		if t.previousJump != nil { // vm.JumpDest and match with a previous jump (otherwise it's a jumpi)
			t.previousJump.InputStack = append([]uint256.Int(nil), scope.Stack.Data...)
			// Check if this is return
			// TODO pontentially maintain a map for fast filtering
			//log.Info("fromStr" + fromStr + ", callstack size" + fmt.Sprint(len(t.callStack)))
			stackSize := len(t.callstack)

			for i := stackSize - 1; i >= 0; i-- {
				//log.Info("callstack" + fmt.Sprint(t.callStack[i]))
				functionInfo := t.callstack[i].function
				if functionInfo == nil {
					break
				}

				if functionInfo.address != fromStr {
					break
				}

				if t.callstack[i].exitPc == pc {
					// find a match, pop the stack, copy memory if needed

					if stackSize-i > 1 {
						log.Info(fmt.Sprintf("tail call optimization size %d", stackSize-i))
					}

					// TODO maybe don't need return all
					for j := stackSize - 1; j >= i; j-- {
						t.callstack[j].GasUsed = math.HexOrDecimal64(uint64(t.callstack[j].Gas) - gas + t.callstack[j].gasCost)
						t.callstack[j].OutputStack = append([]uint256.Int(nil), scope.Stack.Data...)
						if functionInfo.OutputMemory {
							t.callstack[j].OutputMemory = formatMemory()
						}
						t.callstack[j-1].Traces = append(t.callstack[j-1].Traces, t.callstack[j])
					}
					t.callstack = t.callstack[:i]
					t.previousJump = nil
					return
				}
			}

			funcInfo := t.getFunctionInfo(fromStr, pc)
			//log.Info("function info" + fmt.Sprint(funcInfo))

			if funcInfo != nil {
				// filter those jump are not call site
				refId := t.refId(t.previousJump.From.String(), t.previousJump.Pc)
				if refId != funcInfo.Id {
					t.previousJump = nil
					return
				}

				if funcInfo.InputSize >= scope.Stack.Len() {
					// TODO this check should not needed after frist check
					log.Error("Unexpected stack size" + "function:" + fmt.Sprint(funcInfo) + ", stack" + fmt.Sprint(scope.Stack.Data))
					log.Error("previous jump" + fmt.Sprint(*t.previousJump))
					t.previousJump = nil
					return
				}

				// confirmed that we are in an internal call
				//t.internalCallStack = append(t.internalCallStack, internalCallStack{
				//	enterPc:  t.previousJump.Pc,
				//	exitPc:   scope.Stack.Back(funcInfo.InputSize).Uint64(),
				//	function: funcInfo,
				//})
				//jump.enterPc = t.previousJump.Pc
				t.previousJump.exitPc = scope.Stack.Back(funcInfo.InputSize).Uint64()
				t.previousJump.function = funcInfo
				t.previousJump.FunctionPc = pc
				if t.config.Debug {
					t.previousJump.Name = funcInfo.Name
				}
				if funcInfo.InputMemory {
					t.previousJump.InputMemory = formatMemory()
				}
				t.callstack = append(t.callstack, *t.previousJump)
				//t.callstack = append(t.callstack, callStack{
			}

			// reset previous jump regardless
			t.previousJump = nil
		}
	case vm.LOG0, vm.LOG1, vm.LOG2, vm.LOG3, vm.LOG4:
		topicCount := int(op - vm.LOG0)
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		data := copyMemory(logOffset, logSize)
		var topics []hexutil.Bytes
		//stackLen := scope.Stack.Len()
		for i := 0; i < topicCount; i++ {
			topics = append(topics, scope.Stack.Back(2+i).Bytes())
		}
		addr := scope.Contract.Address()
		l := mergeBase(Trace{
			Address: &addr,
			Data:    data,
			Topics:  topics,
		})
		t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, l)
	case vm.REVERT:
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		output := scope.Memory.GetPtr(int64(logOffset.Uint64()), int64(logSize.Uint64()))
		//data := copyMemory(logOffset, logSize)

		trace := mergeBase(Trace{
			Error: "execution reverted",
		})
		if unpacked, err := abi.UnpackRevert(output); err == nil {
			trace.Revertal = unpacked
		}
		t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, trace)
	default:
		if err != nil {
			// Error happen, attach the error OP if not already processed
			t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, mergeBase(Trace{}))
		}
	}
}

func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	if t.config.Debug {
		t.callstack[0].TracerConfig = &t.config
	}

	if len(t.callstack) != 1 {
		log.Error("callstack length is not 1, is " + fmt.Sprint(len(t.callstack)))
	}

	return json.Marshal(t.callstack[0])
}

func (t *sentioTracer) Stop(err error) {

}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentioTracer" {
		return nil, errors.New("no tracer found")
	}

	var config sentioTracerConfig
	functionMap := map[string]map[uint64]functionInfo{}
	callMap := map[string]map[uint64]uint64{}

	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}

		for address, functions := range config.Functions {
			checkSumAddress := libcommon.HexToAddress(address).String()
			functionMap[checkSumAddress] = make(map[uint64]functionInfo)

			for _, function := range functions {
				function.address = checkSumAddress
				functionMap[checkSumAddress][function.Pc] = function
			}
		}

		for address, calls := range config.Calls {
			checkSumAddress := libcommon.HexToAddress(address).String()
			callMap[checkSumAddress] = make(map[uint64]uint64)

			for _, call := range calls {
				callMap[checkSumAddress][call[0]] = call[1]
			}
		}

		log.Info(fmt.Sprintf("create sentioTracer config with %d functions, %d calls", len(functionMap), len(callMap)))
	}

	return &sentioTracer{
		config:      config,
		functionMap: functionMap,
		callMap:     callMap,
		entryPc:     map[uint64]bool{},
	}, nil
}

//func (t *sentioTracer) isPrecompiled(addr libcommon.Address) bool {
//	for _, p := range t.activePrecompiles {
//		if p == addr {
//			return true
//		}
//	}
//	return false
//}

func (t *sentioTracer) getFunctionInfo(address string, pc uint64) *functionInfo {
	m, ok := t.functionMap[address]
	if !ok || m == nil {
		return nil
	}
	info, ok := m[pc]
	if ok {
		return &info
	}

	return nil
}

func (t *sentioTracer) refId(address string, pc uint64) uint64 {
	m, ok := t.callMap[address]
	if !ok || m == nil {
		return -1
	}
	info, ok := m[pc]
	if ok {
		return info
	}
	return -1
}
