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
	"encoding/json"
	"errors"
	"math/big"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
)

//go:generate gencodec -type callFrame -field-override callFrameMarshaling -out gen_callframe_json.go

func init() {
	register("callTracer", newCallTracer)
}

type callLog struct {
	Index    uint64         `json:"index"`
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	Position hexutil.Uint   `json:"position"`
}

type callFrame struct {
	Type     vm.OpCode      `json:"-"`
	From     common.Address `json:"from"`
	Gas      uint64         `json:"gas"`
	GasUsed  uint64         `json:"gasUsed"`
	To       common.Address `json:"to,omitempty" rlp:"optional"`
	Input    []byte         `json:"input" rlp:"optional"`
	Output   []byte         `json:"output,omitempty" rlp:"optional"`
	Error    string         `json:"error,omitempty" rlp:"optional"`
	Revertal string         `json:"revertReason,omitempty"`
	Calls    []callFrame    `json:"calls,omitempty" rlp:"optional"`
	Logs     []callLog      `json:"logs,omitempty" rlp:"optional"`
	// Placed at end on purpose. The RLP will be decoded to 0 instead of
	// nil if there are non-empty elements after in the struct.
	Value *big.Int `json:"value,omitempty" rlp:"optional"`
}

func (f *callFrame) TypeString() string {
	return f.Type.String()
}

func (f *callFrame) failed() bool {
	return len(f.Error) > 0
}

func (f *callFrame) processOutput(output []byte, err error) {
	output = common.CopyBytes(output)
	if err == nil {
		f.Output = output
		return
	}
	f.Error = err.Error()
	if f.Type == vm.CREATE || f.Type == vm.CREATE2 {
		f.To = common.Address{}
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

type callFrameMarshaling struct {
	TypeString string `json:"type"`
	Gas        hexutil.Uint64
	GasUsed    hexutil.Uint64
	Value      *hexutil.Big
	Input      hexutil.Bytes
	Output     hexutil.Bytes
}

type callTracer struct {
	callstack   []callFrame
	config      callTracerConfig
	gasLimit    uint64
	depth       int
	interrupt   uint32 // Atomic flag to signal execution interruption
	reason      error  // Textual reason for the interruption
	logIndex    uint64
	logGaps     map[uint64]int
	precompiles []bool // keep track of whether scopes are for pre-compiles or not
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
			OnTxEnd:   t.OnTxEnd,
			OnEnter:   t.OnEnter,
			OnExit:    t.OnExit,
			OnLog:     t.OnLog,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}, nil
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *callTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.precompiles = append(t.precompiles, precompile)
	if precompile && !t.config.IncludePrecompiles {
		return
	}

	t.callstack[0] = callFrame{
		Type:  vm.CALL,
		From:  from,
		To:    to,
		Input: common.CopyBytes(input),
		Gas:   t.gasLimit, // gas has intrinsicGas already subtracted
	}
	if value != nil {
		t.callstack[0].Value = value.ToBig()
	}
	if create {
		t.callstack[0].Type = vm.CREATE
	}
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (t *callTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {

	if len(t.callstack) == 0 {
		// can happen if top-level is a call to precompile
		// and includePrecompiles is false
		return
	}

	t.callstack[0].processOutput(output, err)
}

// CaptureEnter is called when EVM enters a new scope (via call, create or selfdestruct).
func (t *callTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.depth = depth
	t.precompiles = append(t.precompiles, precompile)
	if t.config.OnlyTopCall && depth > 0 {
		return
	}
	if precompile && !t.config.IncludePrecompiles {
		return
	}
	// Skip if tracing was interrupted
	if atomic.LoadUint32(&t.interrupt) > 0 {
		return
	}

	call := callFrame{
		Type:  vm.OpCode(typ),
		From:  from,
		To:    to,
		Input: common.CopyBytes(input),
		Gas:   gas,
	}
	if value != nil {
		call.Value = value.ToBig()
	}

	if depth == 0 {
		call.Gas = t.gasLimit
	}
	t.callstack = append(t.callstack, call)
}

func (t *callTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if depth == 0 {
		t.captureEnd(output, gasUsed, err, reverted)
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

	call.GasUsed = gasUsed
	call.processOutput(output, err)
	t.callstack[size-1].Calls = append(t.callstack[size-1].Calls, call)
}

func (t *callTracer) captureEnd(output []byte, gasUsed uint64, err error, reverted bool) {
	if len(t.callstack) != 1 {
		return
	}
	t.callstack[0].processOutput(output, err)
}

func (t *callTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	t.gasLimit = tx.GetGasLimit()
	t.logIndex = 0
	t.logGaps = make(map[uint64]int)
}

func (t *callTracer) OnTxEnd(receipt *types.Receipt, err error) {
	// Error happened during tx validation.
	if err != nil {
		return
	}

	if len(t.callstack) == 0 {
		// can happen if top-level is a call to precompile
		// and includePrecompiles is false
		return
	}

	t.callstack[0].GasUsed = receipt.GasUsed
	if t.config.WithLog {
		// Logs are not emitted when the call fails
		clearFailedLogs(&t.callstack[0], false, t.logGaps)
		fixLogIndexGap(&t.callstack[0], addCumulativeGaps(t.logIndex, t.logGaps))
	}
	t.logIndex = 0
	t.logGaps = nil
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
	if atomic.LoadUint32(&t.interrupt) > 0 {
		return
	}
	t.callstack[len(t.callstack)-1].Logs = append(t.callstack[len(t.callstack)-1].Logs, callLog{Address: log.Address, Topics: log.Topics, Data: log.Data, Index: t.logIndex, Position: hexutil.Uint(len(t.callstack[len(t.callstack)-1].Calls))})
	t.logIndex++
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *callTracer) GetResult() (json.RawMessage, error) {
	if len(t.callstack) == 0 && !t.config.IncludePrecompiles {
		// can happen if top-level is a call to precompile
		// and includePrecompiles is false
		// do not return err, just empty result
		return nil, nil
	}
	if len(t.callstack) != 1 {
		return nil, errors.New("incorrect number of top-level calls")
	}
	res, err := json.Marshal(t.callstack[0])
	if err != nil {
		return nil, err
	}
	return res, t.reason
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *callTracer) Stop(err error) {
	t.reason = err
	atomic.StoreUint32(&t.interrupt, 1)
}

// clearFailedLogs clears the logs of a callframe and all its children
// in case of execution failure.
func clearFailedLogs(cf *callFrame, parentFailed bool, logGaps map[uint64]int) {
	failed := cf.failed() || parentFailed
	if failed {
		lastIdx := len(cf.Logs) - 1
		if lastIdx >= 0 && logGaps != nil {
			idx := cf.Logs[lastIdx].Index
			logGaps[idx] = len(cf.Logs)
		}
		// Clear own logs
		cf.Logs = nil
	}
	for i := range cf.Calls {
		clearFailedLogs(&cf.Calls[i], failed, logGaps)
	}
}

// Find the shift position of each potential logIndex
func addCumulativeGaps(h uint64, logGaps map[uint64]int) []uint64 {
	if len(logGaps) == 0 || logGaps == nil {
		return nil
	}
	cumulativeGaps := make([]uint64, h)
	for idx, gap := range logGaps {
		if idx+1 < h {
			cumulativeGaps[idx+1] = uint64(gap) // Next index of the last failed index
		}
	}
	for i := 1; i < int(h); i++ {
		cumulativeGaps[i] += cumulativeGaps[i-1]
	}
	return cumulativeGaps
}

// Recursively shift log indices of callframe - self and children
func fixLogIndexGap(cf *callFrame, cumulativeGaps []uint64) {
	if cumulativeGaps == nil {
		return
	}
	if len(cf.Logs) > 0 {
		for i := range cf.Logs {
			cf.Logs[i].Index -= cumulativeGaps[cf.Logs[i].Index]
		}
	}
	for i := range cf.Calls {
		fixLogIndexGap(&cf.Calls[i], cumulativeGaps)
	}
}
