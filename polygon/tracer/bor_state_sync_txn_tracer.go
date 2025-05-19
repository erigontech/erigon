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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
)

func NewBorStateSyncTxnTracer(
	tracer *tracers.Tracer,
	stateReceiverContractAddress common.Address,
) *tracers.Tracer {
	l := &borStateSyncTxnTracer{
		Tracer:                       tracer,
		stateReceiverContractAddress: stateReceiverContractAddress,
	}
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart:       l.OnTxStart,
			OnTxEnd:         l.OnTxEnd,
			OnEnter:         l.OnEnter,
			OnExit:          l.OnExit,
			OnOpcode:        l.OnOpcode,
			OnFault:         l.OnFault,
			OnGasChange:     l.OnGasChange,
			OnBalanceChange: l.OnBalanceChange,
			OnNonceChange:   l.OnNonceChange,
			OnCodeChange:    l.OnCodeChange,
			OnStorageChange: l.OnStorageChange,
			OnLog:           l.OnLog,
		},
		GetResult: l.GetResult,
		Stop:      l.Stop,
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
type borStateSyncTxnTracer struct { /// LOOKS WRONG
	Tracer                       *tracers.Tracer
	stateReceiverContractAddress common.Address
	createdTopLevel              bool
}

func (bsstt *borStateSyncTxnTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	if bsstt.Tracer.OnTxStart != nil {
		bsstt.Tracer.OnTxStart(env, tx, from)
	}
}

func (bsstt *borStateSyncTxnTracer) OnTxEnd(receipt *types.Receipt, err error) {
	// close top level call
	if bsstt.Tracer.OnExit != nil {
		bsstt.Tracer.OnExit(0, nil, 0, err, err != nil)
	}

	if bsstt.Tracer.OnTxEnd != nil {
		bsstt.Tracer.OnTxEnd(receipt, err)
	}
}

func (bsstt *borStateSyncTxnTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if bsstt.Tracer.OnExit != nil {
		bsstt.Tracer.OnExit(depth+1, output, gasUsed, err, reverted)
	}
}

func (bsstt *borStateSyncTxnTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if bsstt.Tracer.OnEnter != nil {
		if !bsstt.createdTopLevel {
			bsstt.Tracer.OnEnter(0, byte(vm.CALL), state.SystemAddress, bsstt.stateReceiverContractAddress, false, nil, 0, u256.N0, nil)
			bsstt.createdTopLevel = true
		}

		bsstt.Tracer.OnEnter(depth+1, typ, from, to, precompile, input, gas, value, code)
	}
}

func (bsstt *borStateSyncTxnTracer) GetResult() (json.RawMessage, error) {
	if bsstt.Tracer.GetResult != nil {
		return bsstt.Tracer.GetResult()
	}
	return json.RawMessage{}, nil
}

func (bsstt *borStateSyncTxnTracer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	if bsstt.Tracer.OnOpcode != nil {
		// trick tracer to think it is 1 level deeper
		bsstt.Tracer.OnOpcode(pc, op, gas, cost, scope, rData, depth+1, err)
	}
}

func (bsstt *borStateSyncTxnTracer) OnFault(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, depth int, err error) {
	if bsstt.Tracer.OnFault != nil {
		// trick tracer to think it is 1 level deeper
		bsstt.Tracer.OnFault(pc, op, gas, cost, scope, depth+1, err)
	}
}

func (bsstt *borStateSyncTxnTracer) Stop(err error) {
	if bsstt.Tracer.Stop != nil {
		bsstt.Tracer.Stop(err)
	}
}

// OnGasChange is called when gas is either consumed or refunded.
func (bsstt *borStateSyncTxnTracer) OnGasChange(old, new uint64, reason tracing.GasChangeReason) {
	if bsstt.Tracer.OnGasChange != nil {
		bsstt.Tracer.OnGasChange(old, new, reason)
	}
}

func (bsstt *borStateSyncTxnTracer) OnBlockStart(event tracing.BlockEvent) {
	if bsstt.Tracer.OnBlockStart != nil {
		bsstt.Tracer.OnBlockStart(event)
	}
}

func (bsstt *borStateSyncTxnTracer) OnBlockEnd(err error) {
	if bsstt.Tracer.OnBlockEnd != nil {
		bsstt.Tracer.OnBlockEnd(err)
	}
}

func (bsstt *borStateSyncTxnTracer) OnGenesisBlock(b *types.Block, alloc types.GenesisAlloc) {
	if bsstt.Tracer.OnGenesisBlock != nil {
		bsstt.Tracer.OnGenesisBlock(b, alloc)
	}
}

func (bsstt *borStateSyncTxnTracer) OnBalanceChange(a common.Address, prev, new *uint256.Int, reason tracing.BalanceChangeReason) {
	if bsstt.Tracer.OnBalanceChange != nil {
		bsstt.Tracer.OnBalanceChange(a, prev, new, reason)
	}
}

func (bsstt *borStateSyncTxnTracer) OnNonceChange(a common.Address, prev, new uint64) {
	if bsstt.Tracer.OnNonceChange != nil {
		bsstt.Tracer.OnNonceChange(a, prev, new)
	}
}

func (bsstt *borStateSyncTxnTracer) OnCodeChange(a common.Address, prevCodeHash common.Hash, prev []byte, codeHash common.Hash, code []byte) {
	if bsstt.Tracer.OnCodeChange != nil {
		bsstt.Tracer.OnCodeChange(a, prevCodeHash, prev, codeHash, code)
	}
}

func (bsstt *borStateSyncTxnTracer) OnStorageChange(a common.Address, k common.Hash, prev, new uint256.Int) {
	if bsstt.Tracer.OnStorageChange != nil {
		bsstt.Tracer.OnStorageChange(a, k, prev, new)
	}
}

func (bsstt *borStateSyncTxnTracer) OnLog(log *types.Log) {
	if bsstt.Tracer.OnLog != nil {
		bsstt.Tracer.OnLog(log)
	}
}
