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

package tracer

import (
	"encoding/json"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/core/state"
	"github.com/erigontech/erigon/v3/core/vm"
	"github.com/erigontech/erigon/v3/eth/tracers"
)

func NewBorStateSyncTxnTracer(
	tracer vm.EVMLogger,
	stateSyncEventsCount int,
	stateReceiverContractAddress libcommon.Address,
) tracers.Tracer {
	return &borStateSyncTxnTracer{
		EVMLogger:                    tracer,
		stateSyncEventsCount:         stateSyncEventsCount,
		stateReceiverContractAddress: stateReceiverContractAddress,
	}
}

// borStateSyncTxnTracer is a special tracer which is used only for tracing bor state sync transactions. Bor state sync
// transactions are synthetic transactions that are used to bridge assets from L1 (root chain) to L2 (child chain).
// At end of each sprint bor executes the state sync events (0, 1 or many) coming from Heimdall by calling the
// StateReceiverContract with event.Data as input call data.
//
// The borStateSyncTxnTracer wraps any other tracer that the users have requested to use for tracing and tricks them
// to think that they are running in the same transaction as sub-calls. This is needed since when bor executes the
// state sync events at end of each sprint these are synthetically executed as if they were sub-calls of the
// state sync events bor transaction.
type borStateSyncTxnTracer struct {
	vm.EVMLogger
	captureStartCalledOnce       bool
	stateSyncEventsCount         int
	stateReceiverContractAddress libcommon.Address
}

func (bsstt *borStateSyncTxnTracer) CaptureTxStart(_ uint64) {
	bsstt.EVMLogger.CaptureTxStart(0)
}

func (bsstt *borStateSyncTxnTracer) CaptureTxEnd(_ uint64) {
	bsstt.EVMLogger.CaptureTxEnd(0)
}

func (bsstt *borStateSyncTxnTracer) CaptureStart(
	env *vm.EVM,
	from libcommon.Address,
	to libcommon.Address,
	precompile bool,
	create bool,
	input []byte,
	gas uint64,
	value *uint256.Int,
	code []byte,
) {
	if !bsstt.captureStartCalledOnce {
		// first event execution started
		// perform a CaptureStart for the synthetic state sync transaction
		from := state.SystemAddress
		to := bsstt.stateReceiverContractAddress
		bsstt.EVMLogger.CaptureStart(env, from, to, false, false, nil, 0, uint256.NewInt(0), nil)
		bsstt.captureStartCalledOnce = true
	}

	// trick the tracer to think it is a CaptureEnter
	bsstt.EVMLogger.CaptureEnter(vm.CALL, from, to, precompile, create, input, gas, value, code)
}

func (bsstt *borStateSyncTxnTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	if bsstt.stateSyncEventsCount == 0 {
		// guard against unexpected use
		panic("unexpected extra call to borStateSyncTxnTracer.CaptureEnd")
	}

	// finished executing 1 event
	bsstt.stateSyncEventsCount--

	// trick tracer to think it is a CaptureExit
	bsstt.EVMLogger.CaptureExit(output, usedGas, err)

	if bsstt.stateSyncEventsCount == 0 {
		// reached last event
		// perform a CaptureEnd for the synthetic state sync transaction
		bsstt.EVMLogger.CaptureEnd(nil, 0, nil)
	}
}

func (bsstt *borStateSyncTxnTracer) CaptureState(
	pc uint64,
	op vm.OpCode,
	gas uint64,
	cost uint64,
	scope *vm.ScopeContext,
	rData []byte,
	depth int,
	err error,
) {
	// trick tracer to think it is 1 level deeper
	bsstt.EVMLogger.CaptureState(pc, op, gas, cost, scope, rData, depth+1, err)
}

func (bsstt *borStateSyncTxnTracer) CaptureFault(
	pc uint64,
	op vm.OpCode,
	gas uint64,
	cost uint64,
	scope *vm.ScopeContext,
	depth int,
	err error,
) {
	// trick tracer to think it is 1 level deeper
	bsstt.EVMLogger.CaptureFault(pc, op, gas, cost, scope, depth+1, err)
}

func (bsstt *borStateSyncTxnTracer) GetResult() (json.RawMessage, error) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		return tracer.GetResult()
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.GetResult called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) Stop(err error) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.Stop(err)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.Stop called on a wrapped tracer which does not support it")
	}
}
