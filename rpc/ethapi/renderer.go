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

	"github.com/erigontech/erigon/execution/types"
)

// TransactionRenderer adjusts the RPC rendering of a transaction after the
// fields every type shares are set.
type TransactionRenderer func(txn types.Transaction, result *RPCTransaction)

// txRenderers is written only before the RPC servers start and read only
// while they serve, so it needs no lock.
var txRenderers = map[byte]TransactionRenderer{}

// RegisterTxRenderer makes render adjust the RPC rendering of transactions of
// txType, a type registered with types.RegisterTxType. Call it before the RPC
// servers start; it is not safe for concurrent use.
func RegisterTxRenderer(txType byte, render TransactionRenderer) {
	if !types.IsRegisteredTxType(txType) {
		panic(fmt.Sprintf("ethapi: renderer for unregistered transaction type %#x", txType))
	}
	txRenderers[txType] = render
}
