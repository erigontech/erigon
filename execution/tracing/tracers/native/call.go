// Copyright 2021 The go-ethereum Authors
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

package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func init() {
	register("callTracer", newCallTracer)
}

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type callLog -out gen_calllog_fastjson.go
type callLog struct {
	Index    hexutil.Uint64 `json:"index" ethjson:"quantity"`
	Address  common.Address `json:"address" ethjson:"data"`
	Topics   []common.Hash  `json:"topics" ethjson:"datalist"`
	Data     hexutil.Bytes  `json:"data" ethjson:"data"`
	Position hexutil.Uint   `json:"position" ethjson:"quantity"`
}

type callLogs []callLog

func (ls callLogs) MarshalFastJSONTo(s *jsonstream.Stream) error { return writeObjects(s, ls) }

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type callFrame -out gen_callframe_fastjson.go

type callFrame struct {
	Type           vm.OpCode       `json:"-"`
	From           common.Address  `json:"from" ethjson:"data"`
	Gas            hexutil.Uint64  `json:"gas" ethjson:"quantity"`
	StateGas       hexutil.Uint64  `json:"stateGasReservoir,omitempty" ethjson:"quantity"`
	GasUsed        hexutil.Uint64  `json:"gasUsed" ethjson:"quantity"`                  // root frame: receipt gas after refund and floor; child frame: execution gas.
	RegularGasUsed *hexutil.Uint64 `json:"regularGasUsed,omitempty" ethjson:"quantity"` // amsterdam root frame: execution block contribution before refunds, with calldata floor.
	StateGasUsed   *hexutil.Int64  `json:"stateGasUsed,omitempty" ethjson:"quantity"`   // amsterdam root frame: nonnegative block contribution; child frame: signed net state usage.
	GasRefund      *hexutil.Uint64 `json:"gasRefund,omitempty" ethjson:"quantity"`
	To             *common.Address `json:"to,omitempty" ethjson:"data"`
	Input          hexutil.Bytes   `json:"input" ethjson:"data"`
	Output         hexutil.Bytes   `json:"output,omitempty" ethjson:"data"`
	Error          string          `json:"error,omitempty" ethjson:"string"`
	Revertal       string          `json:"revertReason,omitempty" ethjson:"string"`
	Calls          callFrames      `json:"calls,omitempty" ethjson:"objects"`
	Logs           callLogs        `json:"logs,omitempty" ethjson:"objects"`
	Value          *hexutil.U256   `json:"value,omitempty" ethjson:"quantity"`
	TypeStr        string          `json:"type" ethjson:"string"`
}

type callFrames []callFrame

func (fs callFrames) MarshalFastJSONTo(s *jsonstream.Stream) error { return writeObjects(s, fs) }

func writeObjects[E any, P interface {
	*E
	jsonstream.Marshaler
}](s *jsonstream.Stream, items []E) error {
	s.WriteArrayStart()
	for i := range items {
		if err := P(&items[i]).MarshalFastJSONTo(s); err != nil {
			return err
		}
	}
	s.WriteArrayEnd()
	return nil
}

// setType keeps the opcode and its wire spelling in step.
func (f *callFrame) setType(op vm.OpCode) {
	f.Type, f.TypeStr = op, op.String()
}

func (f *callFrame) failed() bool {
	return len(f.Error) > 0
}

func (f *callFrame) processOutput(output []byte, err error) {
	output = bytes.Clone(output)
	if err == nil {
		f.Output = output
		return
	}
	f.Error = err.Error()
	if f.Type == vm.CREATE || f.Type == vm.CREATE2 {
		f.To = nil
	}
	if !errors.Is(err, vm.ErrExecutionReverted) || len(output) == 0 {
		return
	}
	f.Output = output
	if len(output) < 4 {
		return
	}
	if unpacked, err := abi.UnpackRevert(output); err == nil {
		f.Revertal = unpacked
	}
}

type callTracer struct {
	callstack   []callFrame
	config      callTracerConfig
	gasLimit    uint64
	isAmsterdam bool
	depth       int
	interrupt   atomic.Bool           // Atomic flag to signal execution interruption
	reason      atomic.Pointer[error] // Reason for the interruption, populated by Stop
	precompiles []bool                // keep track of whether scopes are for pre-compiles or not
}

func defaultCallTracerConfig() callTracerConfig {
	return callTracerConfig{
		IncludePrecompiles: true,
	}
}

type callTracerConfig struct {
	OnlyTopCall        bool `json:"onlyTopCall"`        // If true, call tracer won't collect any subcalls
	WithLog            bool `json:"withLog"`            // If true, call tracer will collect event logs
	IncludePrecompiles bool `json:"includePrecompiles"` // If true, call tracer will collect calls to precompiles (true by default)
}

// newCallTracer returns a native go tracer which tracks
// call frames of a tx, and implements vm.EVMLogger.
func newCallTracer(ctx *tracers.Context, cfg json.RawMessage) (*tracers.Tracer, error) {
	config := defaultCallTracerConfig()
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}
	}
	// First callframe contains txn context info
	// and is populated on start and end.
	t := &callTracer{callstack: make([]callFrame, 0, 1), config: config}
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: t.OnTxStart,
			OnTxEndV2: t.OnTxEndV2,
			OnEnterV2: t.OnEnterV2,
			OnExitV2:  t.OnExitV2,
			OnLog:     t.OnLog,
		},
		GetResult:         t.GetResult,
		MarshalFastJSONTo: t.MarshalFastJSONTo,
		Stop:              t.Stop,
	}, nil
}

func (t *callTracer) OnEnterV2(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas mdgas.MdGas, value uint256.Int, code []byte) {
	t.depth = depth
	t.precompiles = append(t.precompiles, precompile)
	if t.config.OnlyTopCall && depth > 0 {
		return
	}
	if precompile && !t.config.IncludePrecompiles {
		return
	}
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}

	var toValue *common.Address
	if !to.IsNil() {
		v := to.Value()
		toValue = &v
	}
	call := callFrame{
		From:     from.Value(),
		To:       toValue,
		Input:    bytes.Clone(input),
		Gas:      hexutil.Uint64(gas.Execution),
		StateGas: hexutil.Uint64(gas.State),
	}

	call.setType(vm.OpCode(typ))
	if call.Type != vm.STATICCALL {
		call.Value = (*hexutil.U256)(&value)
	}

	if depth == 0 {
		call.Gas = hexutil.Uint64(t.gasLimit)
	}
	t.callstack = append(t.callstack, call)
}

func (t *callTracer) OnExitV2(depth int, output []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
	if depth == 0 {
		t.captureEnd(output, err)
		return
	}

	t.depth = depth - 1

	if t.config.OnlyTopCall {
		return
	}
	size := len(t.callstack)
	if size <= 1 {
		return
	}
	precompilesLastIdx := len(t.precompiles) - 1
	if precompilesLastIdx < 0 {
		return
	}
	// pop precompile
	precompile := t.precompiles[precompilesLastIdx]
	t.precompiles = t.precompiles[:precompilesLastIdx]
	if precompile && !t.config.IncludePrecompiles {
		return
	}
	// pop call
	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	size -= 1

	call.GasUsed = hexutil.Uint64(gasUsed.Execution)
	if t.isAmsterdam {
		call.StateGasUsed = (*hexutil.Int64)(&gasUsed.State)
	}
	call.processOutput(output, err)
	t.callstack[size-1].Calls = append(t.callstack[size-1].Calls, call)
}

func (t *callTracer) captureEnd(output []byte, err error) {
	if len(t.callstack) != 1 {
		return
	}
	t.callstack[0].processOutput(output, err)
}

func (t *callTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	t.gasLimit = tx.GetGasLimit()
	t.isAmsterdam = env.Rules.IsAmsterdam
}

func (t *callTracer) OnTxEndV2(receipt *types.Receipt, txnGasUsage mdgas.TxnGasUsage, err error) {
	// Error happened during tx validation.
	if err != nil {
		return
	}

	if len(t.callstack) == 0 {
		// can happen if top-level is a call to precompile
		// and includePrecompiles is false
		return
	}

	t.callstack[0].GasUsed = hexutil.Uint64(receipt.GasUsed)
	if t.isAmsterdam {
		t.callstack[0].RegularGasUsed = toHexUint64Ptr(txnGasUsage.BlockExecutionGasUsed)
		stateGasUsed := hexutil.Int64(txnGasUsage.BlockStateGasUsed)
		t.callstack[0].StateGasUsed = &stateGasUsed
		t.callstack[0].GasRefund = toHexUint64Ptr(txnGasUsage.GasRefund)
	}
	if t.config.WithLog {
		// Logs are not emitted when the call fails
		clearFailedLogs(&t.callstack[0], false)
	}
}

func (t *callTracer) OnLog(log *types.Log) {
	// Only logs need to be captured via opcode processing
	if !t.config.WithLog {
		return
	}
	// Avoid processing nested calls when only caring about top call
	if t.config.OnlyTopCall && t.depth > 0 {
		return
	}
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}
	frame := &t.callstack[len(t.callstack)-1]
	frame.Logs = append(frame.Logs, callLog{
		Address: log.Address, Topics: log.Topics, Data: log.Data,
		Index: hexutil.Uint64(log.Index), Position: hexutil.Uint(len(frame.Calls)),
	})
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *callTracer) GetResult() (json.RawMessage, error) {
	root, err := t.root()
	if root == nil || err != nil {
		return nil, err
	}
	res, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	if p := t.reason.Load(); p != nil {
		return res, *p
	}
	return res, nil
}

func (t *callTracer) MarshalFastJSONTo(s *jsonstream.Stream) error {
	root, err := t.root()
	if err != nil {
		return err
	}
	if root == nil {
		s.WriteNil()
		return nil
	}
	if p := t.reason.Load(); p != nil {
		return *p
	}
	return root.MarshalFastJSONTo(s)
}

// root is nil without an error when the top-level call went to a precompile and includePrecompiles is false.
func (t *callTracer) root() (*callFrame, error) {
	if len(t.callstack) == 0 && !t.config.IncludePrecompiles {
		return nil, nil
	}
	if len(t.callstack) != 1 {
		return nil, errors.New("incorrect number of top-level calls")
	}
	return &t.callstack[0], nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *callTracer) Stop(err error) {
	t.reason.Store(&err)
	t.interrupt.Store(true)
}

// clearFailedLogs clears the logs of a callframe and all its children in case
// of execution failure. Revert gave those indices back, so no renumbering.
func clearFailedLogs(cf *callFrame, parentFailed bool) {
	failed := cf.failed() || parentFailed
	if failed {
		cf.Logs = nil
	}
	for i := range cf.Calls {
		clearFailedLogs(&cf.Calls[i], failed)
	}
}
