package blocks

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

var CompletionChecker = BlockHandlerFunc(
	func(ctx context.Context, node devnet.Node, block *requests.BlockResult, transaction *requests.Transaction) error {
		transactionHash := libcommon.HexToHash(transaction.Hash)
		traceResults, err := node.TraceTransaction(transactionHash)

		if err != nil {
			return fmt.Errorf("Failed to trace transaction: %s: %w", transaction.Hash, err)
		}

		for _, traceResult := range traceResults {
			if traceResult.TransactionHash == transactionHash {
				if len(traceResult.Error) != 0 {
					return fmt.Errorf("Transaction error: %s", traceResult.Error)
				}

				break
			}
		}

		return nil
	})
