// Copyright 2022 The go-ethereum Authors
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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func init() {
	register("muxTracer", newMuxTracer)
}

// muxTracer is a go implementation of the Tracer interface which
// runs multiple tracers in one go.
type muxTracer struct {
	names   []string
	tracers []*tracers.Tracer
}

// newMuxTracer returns a new mux tracer.
func newMuxTracer(ctx *tracers.Context, cfg json.RawMessage) (*tracers.Tracer, error) {
	var config map[string]json.RawMessage
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}
	}
	objects := make([]*tracers.Tracer, 0, len(config))
	names := make([]string, 0, len(config))
	for k, v := range config {
		t, err := tracers.New(k, ctx, v)
		if err != nil {
			return nil, err
		}
		objects = append(objects, t)
		names = append(names, k)
	}

	t := &muxTracer{names: names, tracers: objects}
	return t.tracer(), nil
}

// tracer wires up all muxTracer methods into a tracers.Tracer.
func (t *muxTracer) tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart:           t.OnTxStart,
			OnTxEnd:             t.OnTxEnd,
			OnEnter:             t.OnEnter,
			OnExit:              t.OnExit,
			OnOpcode:            t.OnOpcode,
			OnFault:             t.OnFault,
			OnGasChange:         t.OnGasChange,
			OnBalanceChange:     t.OnBalanceChange,
			OnNonceChangeV2:     t.OnNonceChangeV2,
			OnCodeChangeV2:      t.OnCodeChangeV2,
			OnStorageChange:     t.OnStorageChange,
			OnLog:               t.OnLog,
			OnSystemCallStartV2: t.OnSystemCallStartV2,
			OnSystemCallEnd:     t.OnSystemCallEnd,
		},
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}
}

func (t *muxTracer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnOpcode != nil {
			t.OnOpcode(pc, op, gas, cost, scope, rData, depth, err)
		}
	}
}

func (t *muxTracer) OnFault(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, depth int, err error) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnFault != nil {
			t.OnFault(pc, op, gas, cost, scope, depth, err)
		}
	}
}

func (t *muxTracer) OnGasChange(old, new uint64, reason tracing.GasChangeReason) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnGasChange != nil {
			t.OnGasChange(old, new, reason)
		}
	}
}

func (t *muxTracer) OnEnter(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnEnter != nil {
			t.OnEnter(depth, typ, from, to, precompile, input, gas, value, code)
		}
	}
}

func (t *muxTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnExit != nil {
			t.OnExit(depth, output, gasUsed, err, reverted)
		}
	}
}

func (t *muxTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnTxStart != nil {
			t.OnTxStart(env, tx, from)
		}
	}
}

func (t *muxTracer) OnTxEnd(receipt *types.Receipt, err error) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnTxEnd != nil {
			t.OnTxEnd(receipt, err)
		}
	}
}

func (t *muxTracer) OnBalanceChange(a accounts.Address, prev, new uint256.Int, reason tracing.BalanceChangeReason) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnBalanceChange != nil {
			t.OnBalanceChange(a, prev, new, reason)
		}
	}
}

func (t *muxTracer) OnNonceChangeV2(a accounts.Address, prev, new uint64, reason tracing.NonceChangeReason) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnNonceChangeV2 != nil {
			t.OnNonceChangeV2(a, prev, new, reason)
		} else if t.OnNonceChange != nil {
			t.OnNonceChange(a, prev, new)
		}
	}
}

func (t *muxTracer) OnCodeChangeV2(a accounts.Address, prevCodeHash accounts.CodeHash, prev []byte, codeHash accounts.CodeHash, code []byte, reason tracing.CodeChangeReason) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnCodeChangeV2 != nil {
			t.OnCodeChangeV2(a, prevCodeHash, prev, codeHash, code, reason)
		} else if t.OnCodeChange != nil {
			t.OnCodeChange(a, prevCodeHash, prev, codeHash, code)
		}
	}
}

func (t *muxTracer) OnStorageChange(addr accounts.Address, slot accounts.StorageKey, prev uint256.Int, new uint256.Int) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnStorageChange != nil {
			t.OnStorageChange(addr, slot, prev, new)
		}
	}
}

func (t *muxTracer) OnLog(log *types.Log) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnLog != nil {
			t.OnLog(log)
		}
	}
}

func (t *muxTracer) OnSystemCallStartV2(vm *tracing.VMContext) {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnSystemCallStartV2 != nil {
			t.OnSystemCallStartV2(vm)
		} else if t.OnSystemCallStart != nil {
			t.OnSystemCallStart()
		}
	}
}

// OnSystemCallEnd fans out to children. None of the data-extracting leaf
// tracers (StructLogger, JSONLogger, JsonStreamLogger, mdLogger, goja JS,
// prestate) wire this today — they let the next OnTxStart /
// OnSystemCallStartV2 overwrite their env reference. The debug tracer
// (tracers/debug) does register it, to record and forward the signal to
// any wrapped tracer. Kept symmetric with OnSystemCallStartV2 for parity
// with geth and to support a future WrapWithJournal follow-up that emits
// revert-style events on system-call rollback.
func (t *muxTracer) OnSystemCallEnd() {
	for _, t := range t.tracers {
		if t.Hooks == nil {
			continue
		}
		if t.OnSystemCallEnd != nil {
			t.OnSystemCallEnd()
		}
	}
}

// GetResult returns an empty json object.
func (t *muxTracer) GetResult() (json.RawMessage, error) {
	resObject := make(map[string]json.RawMessage)
	for i, tt := range t.tracers {
		r, err := tt.GetResult()
		if err != nil {
			return nil, err
		}
		resObject[t.names[i]] = r
	}
	res, err := json.Marshal(resObject)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *muxTracer) Stop(err error) {
	for _, t := range t.tracers {
		t.Stop(err)
	}
}
