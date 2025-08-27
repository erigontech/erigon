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
	"strconv"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/types"
)

func init() {
	register("4byteTracer", newFourByteTracer)
}

// fourByteTracer searches for 4byte-identifiers, and collects them for post-processing.
// It collects the methods identifiers along with the size of the supplied data, so
// a reversed signature can be matched against the size of the data.
//
// Example:
//
//	> debug.traceTransaction( "0x214e597e35da083692f5386141e69f47e973b2c56e7a8073b1ea08fd7571e9de", {tracer: "4byteTracer"})
//	{
//	  0x27dc297e-128: 1,
//	  0x38cc4831-0: 2,
//	  0x524f3889-96: 1,
//	  0xadf59f99-288: 1,
//	  0xc281d19e-0: 1
//	}
type fourByteTracer struct {
	ids               map[string]int   // ids aggregates the 4byte ids found
	interrupt         uint32           // Atomic flag to signal execution interruption
	reason            error            // Textual reason for the interruption
	activePrecompiles []common.Address // Updated on tx start based on given rules
}

// newFourByteTracer returns a native go tracer which collects
// 4 byte-identifiers of a tx, and implements vm.EVMLogger.
func newFourByteTracer(ctx *tracers.Context, _ json.RawMessage) (*tracers.Tracer, error) {
	t := &fourByteTracer{
		ids: make(map[string]int),
	}
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: t.OnTxStart,
			OnEnter:   t.OnEnter,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}, nil
}

// isPrecompiled returns whether the addr is a precompile. Logic borrowed from newJsTracer in eth/tracers/js/tracer.go
func (t *fourByteTracer) isPrecompiled(addr common.Address) bool {
	for _, p := range t.activePrecompiles {
		if p == addr {
			return true
		}
	}
	return false
}

// store saves the given identifier and datasize.
func (t *fourByteTracer) store(id []byte, size int) {
	key := bytesToHex(id) + "-" + strconv.Itoa(size)
	t.ids[key] += 1
}

func (t *fourByteTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	blockContext := evmtypes.BlockContext{
		BlockNumber: env.BlockNumber,
		Time:        env.Time,
	}
	rules := blockContext.Rules(env.ChainConfig)
	t.activePrecompiles = vm.ActivePrecompiles(rules)
}

func (t *fourByteTracer) OnEnter(depth int, opcode byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) { // Skip if tracing was interrupted
	// Skip if tracing was interrupted
	if atomic.LoadUint32(&t.interrupt) > 0 {
		return
	}
	if len(input) < 4 {
		return
	}
	op := vm.OpCode(opcode)
	// primarily we want to avoid CREATE/CREATE2/SELFDESTRUCT
	if op != vm.DELEGATECALL && op != vm.STATICCALL &&
		op != vm.CALL && op != vm.CALLCODE {
		return
	}
	// Skip any pre-compile invocations, those are just fancy opcodes
	if t.isPrecompiled(to) {
		return
	}
	t.store(input[0:4], len(input)-4)
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *fourByteTracer) GetResult() (json.RawMessage, error) {
	res, err := json.Marshal(t.ids)
	if err != nil {
		return nil, err
	}
	return res, t.reason
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *fourByteTracer) Stop(err error) {
	t.reason = err
	atomic.StoreUint32(&t.interrupt, 1)
}

func bytesToHex(s []byte) string {
	return "0x" + common.Bytes2Hex(s)
}
