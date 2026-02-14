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

package jsonrpc

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type TouchTracer struct {
	searchAddr accounts.Address
	Found      bool
	hooks      *tracing.Hooks
}

func NewTouchTracer(searchAddr accounts.Address) *TouchTracer {
	tracer := &TouchTracer{
		searchAddr: searchAddr,
	}
	tracer.hooks = &tracing.Hooks{
		OnEnter: tracer.OnEnter,
	}

	return tracer
}

func (t *TouchTracer) TracingHooks() *tracing.Hooks {
	return t.hooks
}

func (t *TouchTracer) OnEnter(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	if !t.Found && (t.searchAddr == from || t.searchAddr == to) {
		t.Found = true
	}
}
