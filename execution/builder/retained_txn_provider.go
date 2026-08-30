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

package builder

import (
	"context"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// RetainedTxnBatch exposes both selected transactions and the retained-order pass boundary.
type RetainedTxnBatch struct {
	Transactions types.Transactions
	// PassComplete reports that this batch reached the end of the provider's stable retained order.
	PassComplete bool
}

// RetainedTxnProvider exposes stable pass boundaries for a reusable transaction set.
type RetainedTxnProvider interface {
	txnprovider.TxnProvider
	ProvideRetainedTxns(context.Context, ...txnprovider.ProvideOption) (RetainedTxnBatch, error)
}
