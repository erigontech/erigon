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
	"bytes"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
)

type TouchTracer struct {
	searchAddr common.Address
	Found      bool
	hooks      *tracing.Hooks
}

func NewTouchTracer(searchAddr common.Address) *TouchTracer {
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

func (t *TouchTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if !t.Found && (bytes.Equal(t.searchAddr.Bytes(), from.Bytes()) || bytes.Equal(t.searchAddr.Bytes(), to.Bytes())) {
		t.Found = true
	}
}
