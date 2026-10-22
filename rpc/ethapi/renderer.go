// Copyright 2026 The Erigon Authors
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

package ethapi

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon/execution/types"
)

// Renderer adjusts the RPC rendering of a transaction after the fields every
// type shares are set.
type Renderer func(txn types.Transaction, result *RPCTransaction)

var (
	renderersMu sync.RWMutex
	renderers   = map[byte]Renderer{}

	renderingEnabled bool
)

// SetTxRendering turns the renderers of registered transaction types on or off, from
// httpcfg.HttpCfg.RenderRegisteredTxTypes. Call it before the RPC servers start.
func SetTxRendering(on bool) { renderingEnabled = on }

// RegisterTxRenderer makes render adjust the RPC rendering of transactions of
// txType, a type registered with types.RegisterTxType.
func RegisterTxRenderer(txType byte, render Renderer) {
	if !types.IsRegisteredTxType(txType) {
		panic(fmt.Sprintf("ethapi: renderer for unregistered transaction type %#x", txType))
	}
	renderersMu.Lock()
	defer renderersMu.Unlock()
	renderers[txType] = render
}

func renderer(txType byte) (Renderer, bool) {
	renderersMu.RLock()
	defer renderersMu.RUnlock()
	r, ok := renderers[txType]
	return r, ok
}
