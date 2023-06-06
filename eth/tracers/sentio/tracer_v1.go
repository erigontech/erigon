package sentio

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type sentioTracer struct {
	env               vm.VMInterface
	activePrecompiles []libcommon.Address

	traces      []Trace
	descended   bool
	index       int
	currentGas  math.HexOrDecimal64
	callsNumber int
	rootTrace   Trace
	gasLimit    uint64
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *sentioTracer) CaptureTxEnd(restGas uint64) {
	t.rootTrace.GasUsed = math.HexOrDecimal64(t.gasLimit - restGas)
}

func (t *sentioTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	// Update list of precompiles based on current block
	rules := env.ChainConfig().Rules(env.Context().BlockNumber, env.Context().Time)
	t.activePrecompiles = vm.ActivePrecompiles(rules)

	t.rootTrace = Trace{
		Index: 0,
		//Type:  typ.String(),
		From:  &from,
		To:    &to,
		Gas:   math.HexOrDecimal64(gas),
		Input: hexutil.Bytes(input).String(),
		Value: value.Bytes(),
	}
}
func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.rootTrace.GasUsed = math.HexOrDecimal64(usedGas)
	t.rootTrace.Output = output
	t.rootTrace.Traces = t.traces
}
func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.rootTrace.Type == "" {
		t.rootTrace.Type = typ.String()
	}
}
func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {}

func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Capture any errors immediately
	if err != nil {
		t.traces = append(t.traces, Trace{
			Type:        "ERROR",
			ErrorString: err.Error(), //TODO ask pengcheng
		})
		return
	}

	t.index++
	var mergeBase = func(trace Trace) Trace {
		//trace.op = op
		trace.Pc = pc
		trace.Type = op.String()
		trace.Index = t.index - 1
		trace.GasIn = math.HexOrDecimal64(gas)
		trace.GasCost = math.HexOrDecimal64(cost)
		return trace
	}

	var copyMemory = func(offset *uint256.Int, size *uint256.Int) hexutil.Bytes {
		// TODO check if we should use getPtr or getCopy
		return scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	}

	switch op {
	case vm.RETURN:
		outputOffset := scope.Stack.Peek()
		outputSize := scope.Stack.Back(1)
		trace := mergeBase(Trace{
			Value: copyMemory(outputOffset, outputSize),
		})
		t.traces = append(t.traces, trace)
		return
	case vm.CREATE, vm.CREATE2:
		// If a new contract is being created, add to the call stack
		inputOffset := scope.Stack.Back(1)
		inputSize := scope.Stack.Back(2)
		// TODO calculate to
		from := scope.Contract.Address()
		trace := mergeBase(Trace{
			From:  &from,
			Input: copyMemory(inputOffset, inputSize).String(),
			Value: scope.Stack.Peek().Bytes(),
		})
		t.traces = append(t.traces, trace)
		t.callsNumber++
		t.descended = true
		return
	case vm.SELFDESTRUCT:
		// If a contract is being self destructed, gather that as a subcall too
		from := scope.Contract.Address()
		to := libcommon.BytesToAddress(scope.Stack.Peek().Bytes())
		trace := mergeBase(Trace{
			From:  &from,
			To:    &to,
			Value: t.env.IntraBlockState().GetBalance(from).Bytes(),
		})
		t.traces = append(t.traces, trace)
		return
	case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL:
		// If a new method invocation is being done, add to the call stack
		to := libcommon.BytesToAddress(scope.Stack.Back(1).Bytes())
		if t.isPrecompiled(to) {
			//log.Warn("precompiled", "index: ", t.index)
			return
		}
		offset := 1
		if op == vm.DELEGATECALL || op == vm.STATICCALL {
			offset = 0
		}
		inputOffset := scope.Stack.Back(offset + 2)
		inputSize := scope.Stack.Back(offset + 3)
		from := scope.Contract.Address()
		trace := mergeBase(Trace{
			From:  &from,
			To:    &to,
			Input: copyMemory(inputOffset, inputSize).String(),
		})
		if op == vm.CALL || op == vm.CALLCODE {
			trace.Value = scope.Stack.Back(2).Bytes()
		}
		t.traces = append(t.traces, trace)
		t.callsNumber++
		t.descended = true
		return
	case vm.JUMP, vm.JUMPDEST:
		from := scope.Contract.Address()
		jump := mergeBase(Trace{
			From:  &from,
			Stack: append([]uint256.Int(nil), scope.Stack.Data...), // TODO only need partial
		})
		t.traces = append(t.traces, jump)
		return
	case vm.LOG0, vm.LOG1, vm.LOG2, vm.LOG3, vm.LOG4:
		topicCount := int(op - vm.LOG0)
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		data := copyMemory(logOffset, logSize)
		var topics []hexutil.Bytes
		//stackLen := scope.Stack.Len()
		for i := 0; i < int(topicCount); i++ {
			topics = append(topics, scope.Stack.Back(2+i).Bytes())
		}
		addr := scope.Contract.Address()
		l := mergeBase(Trace{
			Address: &addr,
			Data:    data,
			Topics:  topics,
		})
		t.traces = append(t.traces, l)
		return
	}

	// If we've just descended into an inner call, retrieve it's true allowance. We
	// need to extract if from within the call as there may be funky gas dynamics
	// with regard to requested and actually given gas (2300 stipend, 63/64 rule).
	if t.descended {
		if depth >= t.callsNumber { // how about currentDepth?
			t.currentGas = math.HexOrDecimal64(gas)
			//t.traces[topIdx].Gas = gas
		} else {
			// TODO(karalabe): The call was made to a plain account. We currently don't
			// have access to the true gas amount inside the call and so any amount will
			// mostly be wrong since it depends on a lot of input args. Skip gas for now.
		}
		t.descended = false
	}
	if op == vm.REVERT {
		trace := mergeBase(Trace{
			ErrorString: "execution reverted",
		})
		t.traces = append(t.traces, trace)
		return
	}

	if depth == t.callsNumber-1 {
		t.callsNumber--
		trace := Trace{
			Type:  "CALLEND",
			GasIn: math.HexOrDecimal64(gas),
			Value: scope.Stack.Peek().Bytes(),
		}
		if t.currentGas != 0 {
			trace.Gas = t.currentGas
			t.currentGas = 0
		}
		t.traces = append(t.traces, trace)
	}
}

func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	return json.Marshal(t.rootTrace)
	//return json.RawMessage(`{}`), nil
}

func (t *sentioTracer) Stop(err error) {

}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentioTracer" {
		return nil, errors.New("no tracer found")
	}

	return &sentioTracer{
		callsNumber: 1,
	}, nil
}

func (t *sentioTracer) isPrecompiled(addr libcommon.Address) bool {
	for _, p := range t.activePrecompiles {
		if p == addr {
			return true
		}
	}
	return false
}
