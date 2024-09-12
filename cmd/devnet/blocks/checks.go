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

package blocks

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/turbo/jsonrpc"
)

var CompletionChecker = BlockHandlerFunc(
	func(ctx context.Context, node devnet.Node, block *requests.Block, transaction *jsonrpc.RPCTransaction) error {
		traceResults, err := node.TraceTransaction(transaction.Hash)

		if err != nil {
			return fmt.Errorf("Failed to trace transaction: %s: %w", transaction.Hash, err)
		}

		for _, traceResult := range traceResults {
			if traceResult.TransactionHash == transaction.Hash {
				if len(traceResult.Error) != 0 {
					return fmt.Errorf("Transaction error: %s", traceResult.Error)
				}

				break
			}
		}

		return nil
	})
