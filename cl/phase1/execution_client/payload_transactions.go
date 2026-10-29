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

package execution_client

import (
	"errors"
	"slices"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/execution/types"
)

// DecodePayloadTransactions requires typed transactions without an RLP string wrapper.
// types.DecodeTransactions also accepts the wrapped form used in stored block bodies.
func DecodePayloadTransactions(payload *cltypes.Eth1Block) ([]types.Transaction, error) {
	transactions := payload.Transactions.UnderlyngReference()
	if slices.ContainsFunc(transactions, types.TypedTransactionMarshalledAsRlpString) {
		return nil, errors.New("typed txn marshalled as RLP string")
	}
	return types.DecodeTransactions(transactions)
}
