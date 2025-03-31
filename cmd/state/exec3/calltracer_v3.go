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

package exec3

import (
	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/eth/tracers"
)

type CallTracer struct {
	hooks *tracing.Hooks
	froms map[libcommon.Address]struct{}
	tos   map[libcommon.Address]struct{}
}

func NewCallTracer(hooks *tracing.Hooks) *CallTracer {
	return &CallTracer{
		hooks: hooks,
		froms: make(map[libcommon.Address]struct{}),
		tos:   make(map[libcommon.Address]struct{}),
	}
}

func (ct *CallTracer) Tracer() *tracers.Tracer {
	var hooks tracing.Hooks

	if ct.hooks != nil {
		hooks = *ct.hooks

		if ct.hooks.OnEnter != nil {
			hooks.OnEnter = func(depth int, typ byte, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
				ct.OnEnter(depth, typ, from, to, precompile, input, gas, value, code)
				ct.hooks.OnEnter(depth, typ, from, to, precompile, input, gas, value, code)
			}
		}
	}

	if hooks.OnEnter == nil {
		hooks.OnEnter = ct.OnEnter
	}

	return &tracers.Tracer{
		Hooks: &hooks,
	}
}

func (ct *CallTracer) Reset() {
	ct.froms, ct.tos = map[libcommon.Address]struct{}{}, map[libcommon.Address]struct{}{}
}
func (ct *CallTracer) Froms() map[libcommon.Address]struct{} { return ct.froms }
func (ct *CallTracer) Tos() map[libcommon.Address]struct{}   { return ct.tos }

func (ct *CallTracer) OnEnter(depth int, typ byte, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.froms[from], ct.tos[to] = struct{}{}, struct{}{}
}
