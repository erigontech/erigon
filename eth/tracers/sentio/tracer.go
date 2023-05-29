package sentio

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type sentioTracer struct {
	//internalCall bool
	//stream       *jsoniter.Stream
	env               vm.VMInterface
	activePrecompiles []libcommon.Address

	callStack  []Call
	returnData hexutil.Bytes
	descended  bool
	index      uint
}

type Common struct {
	Op    vm.OpCode `json:"type"` //  this will be differ than js version
	Pc    uint64    `json:"pc"`
	Index uint      `json:"index"`
}

type CallCommon struct {
	Common
	GasIn   uint64        `json:"gasIn"` // TODO no json?
	Gas     uint64        `json:"gas"`
	GasCost uint64        `json:"gasCost"` // TODO no json?
	GasUsed uint64        `json:"gasUsed"`
	Output  hexutil.Bytes `json:"output"`

	From libcommon.Address `json:"from"`
}

type Call struct {
	CallCommon

	From        libcommon.Address `json:"from"`
	To          libcommon.Address `json:"to"`
	Input       hexutil.Bytes     `json:"input"`
	Value       hexutil.Bytes     `joson:"value"`
	ErrorString string            `json:"error"`
	Calls       []Call            `json:"calls"`
	Jumps       []Jump            `json:"jumps"`
	Logs        []Log             `json:"logs"`
}

type Jump struct {
	CallCommon

	From  libcommon.Address `json:"from"`
	Stack []uint256.Int
}

// TODO use shared structure?
type Log struct {
	Common
	Address libcommon.Address `json:"address"`
	Data    hexutil.Bytes     `json:"data"`
	Topics  []hexutil.Bytes   `json:"topics"`
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {}
func (t *sentioTracer) CaptureTxEnd(restGas uint64)    {}
func (t *sentioTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	// Update list of precompiles based on current block
	rules := env.ChainConfig().Rules(env.Context().BlockNumber, env.Context().Time)
	t.activePrecompiles = vm.ActivePrecompiles(rules)
}
func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {}
func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}
func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {}

func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Capture any errors immediately
	// TODO
	//var error = log.getError();
	//if (error !== undefined) {
	//	this.fault(log, db);
	//	return;
	//}
	if err != nil {
		t.Fault(err)
	}

	common := Common{
		Pc:    pc,
		Op:    op,
		Index: t.index,
	}
	callCommon := CallCommon{
		Common:  common,
		GasIn:   gas,
		GasCost: cost,
	}
	t.index++
	topIdx := len(t.callStack) - 1

	switch op {
	case vm.RETURN:
		outputOffset := scope.Stack.Peek()
		outputSize := scope.Stack.Back(1)
		// TODO check if we should use getPtr or getCopy
		t.returnData = scope.Memory.GetPtr(int64(outputOffset.Uint64()), int64(outputSize.Uint64()))
		return
	case vm.CREATE:
		fallthrough
	case vm.CREATE2:
		inputOffset := scope.Stack.Back(1)
		inputSize := uint256.NewInt(0).Add(inputOffset, scope.Stack.Back(2))
		// TODO calculate to
		call := Call{
			CallCommon: callCommon,
			From:       scope.Contract.Address(),
			Input:      scope.Memory.GetPtr(int64(inputOffset.Uint64()), int64(inputSize.Uint64())),
			Value:      scope.Stack.Peek().Bytes(),
		}
		t.callStack = append(t.callStack, call)
		return
	case vm.SELFDESTRUCT:
		from := scope.Contract.Address()
		call := Call{
			CallCommon: callCommon,
			From:       from,
			To:         libcommon.BytesToAddress(scope.Stack.Peek().Bytes()),
			Value:      t.env.IntraBlockState().GetBalance(from).Bytes(),
		}
		t.callStack[topIdx].Calls = append(t.callStack[topIdx].Calls, call)
		return
	case vm.CALL:
		fallthrough
	case vm.CALLCODE:
		fallthrough
	case vm.DELEGATECALL:
		fallthrough
	case vm.STATICCALL:
		to := libcommon.BytesToAddress(scope.Stack.Back(1).Bytes())
		if t.isPrecompiled(to) {
			return
		}
		offset := 1
		if op == vm.DELEGATECALL || op == vm.STATICCALL {
			offset = 0
		}
		inputOffset := scope.Stack.Back(offset + 2)
		inputSize := scope.Stack.Back(offset + 3)
		call := Call{
			CallCommon: callCommon,
			From:       scope.Contract.Address(),
			To:         to,
			Input:      scope.Memory.GetPtr(int64(inputOffset.Uint64()), int64(inputSize.Uint64())),

			// outOff
			// outLen
		}
		if op == vm.CALL || op == vm.CALLCODE {
			call.Value = scope.Stack.Back(2).Bytes()
		}
		t.callStack = append(t.callStack, call)
		t.descended = true // TODO do we need this?
		return
	case vm.JUMP:
		fallthrough
	case vm.JUMPI:
		fallthrough
	case vm.JUMPDEST:
		jump := Jump{
			CallCommon: callCommon,
			Stack:      append([]uint256.Int(nil), scope.Stack.Data...),
		}
		t.callStack[topIdx].Jumps = append(t.callStack[topIdx].Jumps, jump)
		return
	case vm.LOG0:
		fallthrough
	case vm.LOG1:
		fallthrough
	case vm.LOG2:
		fallthrough
	case vm.LOG3:
		fallthrough
	case vm.LOG4:
		// TODO log process
		topicCount := 0xf & op
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		data := scope.Memory.GetPtr(int64(logOffset.Uint64()), int64(logSize.Uint64()))
		var topics []hexutil.Bytes
		//stackLen := scope.Stack.Len()
		for i := 0; i < int(topicCount); i++ {
			topics = append(topics, scope.Stack.Back(2+i).Bytes())
		}
		log := Log{
			Common: common,
			Data:   data,
			Topics: topics,
		}
		t.callStack[topIdx].Logs = append(t.callStack[topIdx].Logs, log)
		return
	}

	// If we've just descended into an inner call, retrieve it's true allowance. We
	// need to extract if from within the call as there may be funky gas dynamics
	// with regard to requested and actually given gas (2300 stipend, 63/64 rule).
	if t.descended {
		if depth >= len(t.callStack) {
			t.callStack[topIdx].Gas = gas
		} else {
			// TODO(karalabe): The call was made to a plain account. We currently don't
			// have access to the true gas amount inside the call and so any amount will
			// mostly be wrong since it depends on a lot of input args. Skip gas for now.
		}
		t.descended = false
	}
	if op == vm.REVERT {
		// TODO is it possible to get full error?
		t.callStack[topIdx].ErrorString = "execution reverted"
		return
	}

	if depth != topIdx {
		return
	}

	call := t.callStack[topIdx]
	t.callStack = t.callStack[0:topIdx]
	topIdx--

	if call.Op == vm.CREATE || call.Op == vm.CREATE2 {
		if call.GasIn > 0 {
			call.GasUsed = call.GasIn - call.GasCost + gas // TODO double check this
		}

		ret := scope.Stack.Peek()
		if ret.Uint64() != 0 {
			addr := libcommon.BytesToAddress(scope.Stack.Peek().Bytes())
			call.To = addr
			call.Output = t.env.IntraBlockState().GetCode(addr)
		} else if err == nil {
			call.ErrorString = "internal failure" // TODO(karalabe): surface these faults somehow
		}
	} else {
		call.GasUsed = call.GasIn - call.GasCost + call.Gas - gas
		ret := scope.Stack.Peek()
		if ret.Uint64() != 0 {
			call.Output = t.returnData
		} else if err == nil {
			call.ErrorString = "internal failure" // TODO(karalabe): surface these faults somehow
		}

		// Inject the call into the previous one
		t.callStack[topIdx].Calls = append(t.callStack[topIdx].Calls, call)
		t.returnData = nil
	}

}

func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {

}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {

}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (t *sentioTracer) Stop(err error) {

}

func (t *sentioTracer) Fault(err error) {
	topIdx := len(t.callStack) - 1
	// If the topmost call already reverted, don't handle the additional fault again
	if t.callStack[topIdx].ErrorString != "" {
		return
	}

	call := t.callStack[topIdx]
	t.callStack = t.callStack[0:topIdx]
	topIdx--

	// Consume all available gas and clean any leftovers
	if call.Gas != 0 {
		//call.gas = '0x' + bigInt(call.gas).toString(16)
		call.GasUsed = call.Gas
	}

	if topIdx >= 0 {
		t.callStack[topIdx].Calls = append(t.callStack[topIdx].Calls, call)
		return
	}
	//// Last call failed too, leave it in the stack
	t.callStack = append(t.callStack, call)
}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentio" {
		return nil, errors.New("no tracer found")
	}

	// TODO add configures
	return &sentioTracer{
		//internalCall: true,
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
