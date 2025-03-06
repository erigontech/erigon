package blocks

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
)

var CompletionChecker = BlockHandlerFunc(
	func(ctx context.Context, node devnet.Node, block *requests.Block, transaction *ethapi.RPCTransaction) error {
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
