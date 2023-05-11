package calltracer

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

type TransactionRoot struct {
	Opcode    vm.OpCode            `json:"opcode"`
	CloseCode vm.OpCode            `json:"closeCode"`
	Calls     []*InnerCall         `json:"calls"`
	Logs      []LogEntry           `json:"logs"`
	Storage   []StorageInteraction `json:"storage"`

	IntrisictGas uint64 `json:"intrisictGas"`
	GasUsed      uint64 `json:"gasUsed"`
	GasLimit     uint64 `json:"gasLimit"`
	GasRest      uint64 `json:"gasRest"`

	Salt   string             `json:"salt"`
	From   *libcommon.Address `json:"from"`
	To     *libcommon.Address `json:"to"`
	Value  *uint256.Int       `json:"value"`
	Input  string             `json:"input"`
	Output string             `json:"output"`

	DestructTo     *libcommon.Address `json:"destructTo"`
	CreatedAddress *libcommon.Address `json:"createdAddress"`
	Success        uint64             `json:"success"`

	IsCreate bool     `json:"isCreate"`
	Err      error    `json:"error"`
	CodeLen  int      `json:"codeLen"`
	Dbg      []string `json:"dbg"`
}

type StorageInteractionType uint8

const (
	Save StorageInteractionType = 0
	Load StorageInteractionType = 1
)

type StorageInteraction struct {
	InteractionType StorageInteractionType `json:"type"`
	Key             string                 `json:"key"`
	Value           string                 `json:"value"`
}

type InnerCall struct {
	Opcode    vm.OpCode            `json:"opcode"`
	CloseCode vm.OpCode            `json:"closeCode"`
	Calls     []*InnerCall         `json:"calls"`
	Logs      []LogEntry           `json:"logs"`
	Storage   []StorageInteraction `json:"storage"`

	IntrisictGas uint64 `json:"intrisictGas"`
	GasUsed      uint64 `json:"gasUsed"`
	Gas          uint64 `json:"gas"`
	GasPassed    uint64 `json:"gasPassed"`
	GasCost      uint64 `json:"gasCost"`

	Salt   string            `json:"salt"`
	From   libcommon.Address `json:"from"`
	To     libcommon.Address `json:"to"`
	Value  uint256.Int       `json:"value"`
	Input  string            `json:"input"`
	Output string            `json:"output"`

	DestructTo     *libcommon.Address `json:"destructTo"`
	CreatedAddress *libcommon.Address `json:"createdAddress"`
	Success        uint64             `json:"success"`

	Depth  int    `json:"depth"`
	Result string `json:"result"`
	Error  string `json:"error"`
}

type LogEntry struct {
	Contract libcommon.Address `json:"address"`
	Topics   []string          `json:"topics"`
	Data     string            `json:"data"`
}

type BlockTrace struct {
	Hash  libcommon.Hash  `json:"hash"`
	Trace TransactionRoot `json:"trace"`
}
type BlockTraces struct {
	BlockNumber uint64       `json:"blockNumber"`
	Traces      []BlockTrace `json:"traces"`
}

type OInchCallTracer struct {
	roots []TransactionRoot

	currentRoot  *TransactionRoot
	currentStack []*InnerCall
	prevOpcode   vm.OpCode

	// Holds information about last finished call,
	// setup in finishCall and clear in exit context,
	// because context exit hool called after state processing
	lastFinishedCall *InnerCall
}

func (ct *OInchCallTracer) GetResult() (json.RawMessage, error) {
	res, err := json.Marshal(ct.roots)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (ct *OInchCallTracer) Stop(err error) {
	log.Error("[1inch-tracer] error during trace via handler", "err", err)
	// panic("[1inch-tracer] implement me")
}

func New1InchCallTracer() *OInchCallTracer {
	log.Info("[1inch-tracer] new instace created")
	return &OInchCallTracer{
		roots:        []TransactionRoot{},
		currentRoot:  nil,
		currentStack: nil,
	}
}

// CaptureTxStart Tx start but before CaptureStart
func (ct *OInchCallTracer) CaptureTxStart(gasLimit uint64) {
	log.Info("[1inch-tracer] Capture transaction start")
	newRoot := TransactionRoot{
		From:     nil,
		To:       nil,
		GasLimit: gasLimit,
		GasUsed:  0,
	}

	root := InnerCall{
		Opcode:  vm.CALL,
		Calls:   []*InnerCall{},
		Logs:    []LogEntry{},
		Storage: []StorageInteraction{},
	}
	ct.currentStack = []*InnerCall{&root}
	ct.currentRoot = &newRoot
	ct.prevOpcode = 0
}

// CaptureTxEnd Tx end but after CaptureEnd
func (ct *OInchCallTracer) CaptureTxEnd(restGas uint64) {
	log.Info("[1inch-tracer] Capture transaction end")
	ct.currentRoot.GasUsed = ct.currentRoot.GasLimit - restGas
	ct.currentRoot.GasRest = restGas

	ct.roots = append(ct.roots, *ct.currentRoot)

	ct.currentStack = nil
	ct.currentRoot = nil
	ct.prevOpcode = 0
}

//// CaptureStart and CaptureEnter also capture SELFDESTRUCT opcode invocations
//func (ct *OInchCallTracer) captureStartOrEnter(From, to libcommon.Address, create bool, code []byte) {
//	ct.froms[From] = struct{}{}
//
//	created, ok := ct.tos[to]
//	if !ok {
//		ct.tos[to] = false
//	}
//
//	if !created && create {
//		if len(code) > 0 {
//			ct.tos[to] = true
//		}
//	}
//}

// CaptureStart capture tx start
func (ct *OInchCallTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	log.Info("[1inch-tracer] Capture start", "from", from, "to", to, "gas", gas)
	ct.currentRoot.From = &from
	ct.currentRoot.To = &to
	ct.currentRoot.IsCreate = create
	ct.currentRoot.Value = value
	ct.currentRoot.Input = "0x" + hex.EncodeToString(input)
	ct.currentRoot.CodeLen = len(code)
}

// CaptureEnter Capture context enter (calls) called after state hook
func (ct *OInchCallTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	log.Info("[1inch-tracer] Capture context enter", "from", from, "to", to, "opcode", typ.String())
	if typ != vm.SELFDESTRUCT {
		call := ct.peekCall()
		// Process root call
		if call.Opcode == 0 {
			call.Opcode = typ
			call.Value = *value
			call.From = from
			call.To = to
			call.Gas = gas
			return
		}
		if call.Opcode != typ {
			panic(fmt.Sprintf("[1inch-tracer] Wrong opcode: ", call.Opcode, typ))
		}

		if value == nil {
			log.Trace("[1inch-tracer] Capture context enter with nil value", "opcode", typ)
		}

		// Some opcodes have nil value (e.g. DELEGATECALL, STATICCALL)
		if value != nil && call.Value != *value {
			panic(fmt.Sprintf("[1inch-tracer] Wrong Value: ", call.Value, *value))
		}

		if call.From != from {
			panic(fmt.Sprintf("[1inch-tracer] Wrong from: ", call.From, from))
		}

		if call.Opcode != vm.CREATE && call.Opcode != vm.CREATE2 && call.To != to {
			panic(fmt.Sprintf("[1inch-tracer] Wrong to: ", call.To, to))
		}

		if call.Opcode == vm.CREATE || call.Opcode == vm.CREATE2 {
			call.To = to
		}

		call.GasCost = call.IntrisictGas - gas
	}
}

// CaptureState Called for each opcode
func (ct *OInchCallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	log.Trace("[1inch-tracer] Capture opcode", "op", op, "gas", gas, "cost", cost)

	// cost for CALL's means different, actually it is intrisict gasLimit inside call context

	{
		switch ct.prevOpcode {
		case vm.SLOAD:
			call := ct.peekCall()
			if len(scope.Stack.Data) > 0 && len(call.Storage) > 0 {
				// Pop loaded data from storage
				stackLastPos := len(scope.Stack.Data) - 1
				value := hex.EncodeToString(scope.Stack.Data[stackLastPos-0].Bytes())
				s := &call.Storage[len(call.Storage)-1]
				s.Value = value
			}
		}

		// Ether transfer
		if ct.prevOpcode == vm.CALL || ct.prevOpcode == vm.CALLCODE || ct.prevOpcode == vm.DELEGATECALL || ct.prevOpcode == vm.STATICCALL {
			lastCall := ct.peekCall()
			if lastCall.Depth == depth {
				c := ct.popCall()
				c.GasUsed = c.Gas - gas
				c.CloseCode = vm.CHAINID // Fake close code NO_CONTRACT
				ct.finishCall(c)
			}
		}
		ct.prevOpcode = op
	}

	// Success codes and addresses for CREATE's
	if ct.lastFinishedCall != nil && ct.lastFinishedCall.Depth == depth {
		if len(scope.Stack.Data) > 0 {
			switch ct.lastFinishedCall.Opcode {
			case vm.CREATE:
				stackLastPos := len(scope.Stack.Data) - 1
				address := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-0].Bytes())
				(*ct.lastFinishedCall).CreatedAddress = &address
			case vm.CREATE2:
				stackLastPos := len(scope.Stack.Data) - 1

				address := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-0].Bytes())
				ct.lastFinishedCall.CreatedAddress = &address

			case vm.CALL:
				stackLastPos := len(scope.Stack.Data) - 1
				success := scope.Stack.Data[stackLastPos-0].Uint64()
				ct.lastFinishedCall.Success = success
			case vm.CALLCODE:
				stackLastPos := len(scope.Stack.Data) - 1
				success := scope.Stack.Data[stackLastPos-0].Uint64()
				ct.lastFinishedCall.Success = success
			case vm.DELEGATECALL:
				stackLastPos := len(scope.Stack.Data) - 1
				success := scope.Stack.Data[stackLastPos-0].Uint64()
				ct.lastFinishedCall.Success = success
			case vm.STATICCALL:
				stackLastPos := len(scope.Stack.Data) - 1
				success := scope.Stack.Data[stackLastPos-0].Uint64()
				ct.lastFinishedCall.Success = success
			}
		} else {
			log.Info("[1inch-tracer] Empty stack after opcode: ", ct.lastFinishedCall.Opcode.String())
		}
	}

	// Stack unwinding (TODO: peekCall should etunr nil if stack is empty, but for now let it be error)
	lastCall := ct.peekCall()
	if lastCall != nil && lastCall.Depth >= depth {
		diff := lastCall.Depth - depth
		for i := 0; i <= diff; i++ {
			c := ct.popCall()
			c.GasUsed = c.Gas - gas
			c.CloseCode = op // TODO: close code STACK_UNWINDED
			ct.finishCall(c)
		}
	}
	switch op {
	case vm.SSTORE:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "SSTORE: empty_stack"
			return
		}

		stackLastPos := stackLen - 1

		key := scope.Stack.Data[stackLastPos-0].Bytes32()
		value := hex.EncodeToString(scope.Stack.Data[stackLastPos-1].Bytes())

		storageKeyStr := hex.EncodeToString(key[:])

		call.Storage = append(call.Storage, StorageInteraction{
			InteractionType: Save,
			Key:             storageKeyStr,
			Value:           value,
		})
	case vm.SLOAD:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "SLOAD: empty_stack"
			return
		}

		stackLastPos := stackLen - 1
		key := scope.Stack.Data[stackLastPos-0].Bytes32()

		storageKeyStr := hex.EncodeToString(key[:])

		call.Storage = append(call.Storage, StorageInteraction{
			InteractionType: Load,
			Key:             storageKeyStr,
		})
	case vm.CREATE:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: gas,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "CREATE: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := stackLen - 1
		val := scope.Stack.Data[stackLastPos-0]
		inOff := scope.Stack.Data[stackLastPos-1]
		inLen := scope.Stack.Data[stackLastPos-2]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			IntrisictGas: gas,
			From:         scope.Contract.Address(),
			Value:        val,
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.CREATE2:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: gas,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "CREATE2: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := len(scope.Stack.Data) - 1
		val := scope.Stack.Data[stackLastPos-0]
		inOff := scope.Stack.Data[stackLastPos-1]
		inLen := scope.Stack.Data[stackLastPos-2]
		salt := hex.EncodeToString(scope.Stack.Data[stackLastPos-3].Bytes())
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			IntrisictGas: gas,
			From:         scope.Contract.Address(),
			Value:        val,
			Salt:         salt,
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.CALLCODE:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: cost,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "CALLCODE: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := stackLen - 1
		gasPassed := scope.Stack.Data[stackLastPos-0].Uint64()
		to := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-1].Bytes())
		val := scope.Stack.Data[stackLastPos-2]
		inOff := scope.Stack.Data[stackLastPos-3]
		inLen := scope.Stack.Data[stackLastPos-4]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			GasPassed:    gasPassed,
			IntrisictGas: cost,
			From:         scope.Contract.Address(),
			To:           to,
			Value:        val,
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.CALL:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: cost,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "CALL: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := stackLen - 1
		gasPassed := scope.Stack.Data[stackLastPos-0].Uint64()
		to := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-1].Bytes())
		val := scope.Stack.Data[stackLastPos-2]
		inOff := scope.Stack.Data[stackLastPos-3]
		inLen := scope.Stack.Data[stackLastPos-4]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			GasPassed:    gasPassed,
			IntrisictGas: cost,
			From:         scope.Contract.Address(),
			To:           to,
			Value:        val,
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.DELEGATECALL:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: cost,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "DELEGATECALL: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := stackLen - 1
		gasPassed := scope.Stack.Data[stackLastPos-0].Uint64()
		to := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-1].Bytes())
		inOff := scope.Stack.Data[stackLastPos-2]
		inLen := scope.Stack.Data[stackLastPos-3]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			GasPassed:    gasPassed,
			IntrisictGas: cost,
			From:         scope.Contract.Address(),
			To:           to,
			Value:        uint256.Int{0},
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.STATICCALL:
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call := InnerCall{
				Opcode:       op,
				Calls:        []*InnerCall{},
				Logs:         []LogEntry{},
				Storage:      []StorageInteraction{},
				Gas:          gas,
				GasPassed:    0,
				IntrisictGas: cost,
				From:         scope.Contract.Address(),
				Input:        "0x",
				Depth:        depth,
			}
			call.Error = "STATICCALL: empty_stack"
			ct.pushCall(&call, depth)
			return
		}

		stackLastPos := stackLen - 1
		gasPassed := scope.Stack.Data[stackLastPos-0].Uint64()
		to := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-1].Bytes())
		inOff := scope.Stack.Data[stackLastPos-2]
		inLen := scope.Stack.Data[stackLastPos-3]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
		)

		call := InnerCall{
			Opcode:       op,
			Calls:        []*InnerCall{},
			Logs:         []LogEntry{},
			Storage:      []StorageInteraction{},
			Gas:          gas,
			GasPassed:    gasPassed,
			IntrisictGas: cost,
			From:         scope.Contract.Address(),
			To:           to,
			Value:        uint256.Int{0},
			Input:        data,
			Depth:        depth,
		}
		ct.pushCall(&call, depth)
	case vm.SELFDESTRUCT:
		call := ct.popCall()
		call.CloseCode = op
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			ct.finishCall(call)
			return
		}
		stackLastPos := stackLen - 1
		to := libcommon.BytesToAddress(scope.Stack.Data[stackLastPos-0].Bytes())
		call.DestructTo = &to
		ct.finishCall(call)
	case vm.STOP:
		call := ct.popCall()
		call.CloseCode = op
		call.GasUsed = call.IntrisictGas - gas
		ct.finishCall(call)
	case vm.REVERT:
		call := ct.popCall()
		call.CloseCode = op
		call.GasUsed = call.IntrisictGas - gas

		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "REVERT: empty_stack"
			ct.finishCall(call)
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		res := "0x"
		if inLen.GtUint64(0) {
			res = "0x" + hex.EncodeToString(
				scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
			)
		}
		call.Result = res
		ct.finishCall(call)
	case vm.INVALID:
		call := ct.popCall()
		call.CloseCode = op
		call.GasUsed = call.IntrisictGas - gas
		ct.finishCall(call)
	case vm.RETURN:
		call := ct.popCall()
		call.CloseCode = op
		call.GasUsed = call.IntrisictGas - gas
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "RET: empty_stack"
			ct.finishCall(call)
			return
		}

		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		res := "0x"
		if inLen.GtUint64(0) {
			res = "0x" + hex.EncodeToString(
				scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())),
			)
		}
		call.Result = res
		ct.finishCall(call)
	case vm.LOG0:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "LOG0: empty_stack"
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())))
		logEntry := LogEntry{
			Contract: scope.Contract.Address(),
			Topics:   []string{},
			Data:     data,
		}
		call.Logs = append(call.Logs, logEntry)
	case vm.LOG1:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "LOG1: empty_stack"
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())))
		topic1 := scope.Stack.Data[stackLastPos-2].Bytes()
		logEntry := LogEntry{
			Contract: scope.Contract.Address(),
			Topics: []string{
				"0x" + hex.EncodeToString(topic1),
			},
			Data: data,
		}
		call.Logs = append(call.Logs, logEntry)
	case vm.LOG2:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "LOG2: empty_stack"
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())))
		topic1 := scope.Stack.Data[stackLastPos-2].Bytes()
		topic2 := scope.Stack.Data[stackLastPos-3].Bytes()
		logEntry := LogEntry{
			Contract: scope.Contract.Address(),
			Topics: []string{
				"0x" + hex.EncodeToString(topic1),
				"0x" + hex.EncodeToString(topic2),
			},
			Data: data,
		}
		call.Logs = append(call.Logs, logEntry)
	case vm.LOG3:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "LOG3: empty_stack"
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())))
		topic1 := scope.Stack.Data[stackLastPos-2].Bytes()
		topic2 := scope.Stack.Data[stackLastPos-3].Bytes()
		topic3 := scope.Stack.Data[stackLastPos-4].Bytes()
		logEntry := LogEntry{
			Contract: scope.Contract.Address(),
			Topics: []string{
				"0x" + hex.EncodeToString(topic1),
				"0x" + hex.EncodeToString(topic2),
				"0x" + hex.EncodeToString(topic3),
			},
			Data: data,
		}
		call.Logs = append(call.Logs, logEntry)
	case vm.LOG4:
		call := ct.peekCall()
		stackLen := len(scope.Stack.Data)
		if stackLen == 0 {
			call.Error = "LOG4: empty_stack"
			return
		}
		stackLastPos := stackLen - 1
		inOff := scope.Stack.Data[stackLastPos-0]
		inLen := scope.Stack.Data[stackLastPos-1]
		data := "0x" + hex.EncodeToString(
			scope.Memory.GetPtr(int64(inOff.Uint64()), int64(inLen.Uint64())))
		topic1 := scope.Stack.Data[stackLastPos-2].Bytes()
		topic2 := scope.Stack.Data[stackLastPos-3].Bytes()
		topic3 := scope.Stack.Data[stackLastPos-4].Bytes()
		topic4 := scope.Stack.Data[stackLastPos-5].Bytes()
		logEntry := LogEntry{
			Contract: scope.Contract.Address(),
			Topics: []string{
				"0x" + hex.EncodeToString(topic1),
				"0x" + hex.EncodeToString(topic2),
				"0x" + hex.EncodeToString(topic3),
				"0x" + hex.EncodeToString(topic4),
			},
			Data: data,
		}
		call.Logs = append(call.Logs, logEntry)
	}
}

// CaptureFault Called after reverts but before exit
func (ct *OInchCallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
	log.Info("[1inch-tracer] Capture revert", "opcode", op.String(), "gas", gas, "cost", cost, "err", err)
}

// CaptureEnd capture tx end
func (ct *OInchCallTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	log.Info("[1inch-tracer] Capture end", "gasUsed", usedGas, "err", err)

	ct.currentRoot.Output = "0x" + hex.EncodeToString(output)
	ct.currentRoot.Err = err

	if ct.currentRoot.CodeLen == 0 && len(ct.currentRoot.Calls) == 0 {
		// Empty transaction, ether transfer
		return
	}

	if len(ct.currentRoot.Calls) != 1 {
		log.Error("[1inch-tracer] Capture end: Error, root call isnt single")

		for len(ct.currentStack) > 1 {
			c := ct.popCall()
			ct.finishCall(c)
		}
		ct.currentRoot.Calls = ct.currentStack
		ct.currentRoot.Err = errors.New("Not single root")
		return
	}

	rootCall := ct.currentRoot.Calls[0]
	ct.currentRoot.Salt = rootCall.Salt
	ct.currentRoot.Calls = rootCall.Calls
	ct.currentRoot.IntrisictGas = rootCall.IntrisictGas
	ct.currentRoot.Logs = rootCall.Logs
	ct.currentRoot.Storage = rootCall.Storage
	ct.currentRoot.Opcode = rootCall.Opcode
	ct.currentRoot.CloseCode = rootCall.CloseCode
	ct.currentRoot.Success = rootCall.Success
	ct.currentRoot.CreatedAddress = rootCall.CreatedAddress
	ct.currentRoot.DestructTo = rootCall.DestructTo

	ct.currentRoot.Dbg = append(ct.currentRoot.Dbg, fmt.Sprintf("GasUsed: %d %d %d", rootCall.Gas, rootCall.GasUsed, rootCall.GasPassed))
}

// CaptureExit Capture context exit (STOP/REVERT/etc) called after state hook
func (ct *OInchCallTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	if ct.lastFinishedCall == nil {
		log.Error("[1inch-tracer] Capture context exit: Error, no last finished call")
		return
	}
	if output != nil && len(output) > 0 {
		ct.lastFinishedCall.Output = "0x" + hex.EncodeToString(output)
	}
	log.Info("[1inch-tracer] Capture context exit", "gasUsed", usedGas, "err", err)

	ct.lastFinishedCall = nil
}

// WriteToDb Called at the end of the block
func (ct *OInchCallTracer) WriteToDb(tx kv.StatelessWriteTx, block *types.Block, vmConfig vm.Config) error {
	log.Info("[1inch-tracer] Write to DB", "blockNo", *block.Number(), "txCount", len(block.Transactions()))
	if len(block.Transactions()) != len(ct.roots) {
		panic("[1inch-tracer] Transaction amount in block and in traces doesn't match")
	}

	traces := make([]BlockTrace, len(ct.roots))
	for i := 0; i < len(ct.roots); i++ {
		t := block.Transactions()[i]
		traces[i] = BlockTrace{
			Hash:  t.Hash(),
			Trace: ct.roots[i],
		}
	}
	// blockTrace := BlockTraces{
	// 	BlockNumber: block.Number().Uint64(),
	// 	Traces:      traces,
	// }

	return nil
}

func prettyPrint(i *TransactionRoot) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

func (ct *OInchCallTracer) pushCall(call *InnerCall, depth int) {
	ct.currentStack = append(ct.currentStack, call)
}

func (ct *OInchCallTracer) finishCall(call *InnerCall) {
	ct.lastFinishedCall = call
	if len(ct.currentStack) == 0 {
		ct.currentRoot.Calls = append(ct.currentRoot.Calls, call)
		return
	}
	preLastCall := ct.currentStack[len(ct.currentStack)-1]
	preLastCall.Calls = append(preLastCall.Calls, call)
}

func (ct *OInchCallTracer) popCall() *InnerCall {
	stackLen := len(ct.currentStack)
	lastCall := ct.currentStack[stackLen-1]
	ct.currentStack = ct.currentStack[:stackLen-1]
	return lastCall
}

func (ct *OInchCallTracer) peekCall() *InnerCall {
	stackLen := len(ct.currentStack)
	lastCall := ct.currentStack[stackLen-1]
	return lastCall
}
