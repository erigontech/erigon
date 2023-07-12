package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
)

func CheckTxPoolContent(ctx context.Context, expectedPendingSize, expectedQueuedSize, expectedBaseFeeSize int) {
	pendingSize, queuedSize, baseFeeSize, err := devnet.SelectNode(ctx).TxpoolContent()

	logger := devnet.Logger(ctx)

	if err != nil {
		logger.Error("FAILURE getting txpool content", "error", err)
		return
	}

	if expectedPendingSize >= 0 && pendingSize != expectedPendingSize {
		logger.Error("FAILURE mismatched pending subpool size", "expected", expectedPendingSize, "got", pendingSize)
		return
	}

	if expectedQueuedSize >= 0 && queuedSize != expectedQueuedSize {
		logger.Error("FAILURE mismatched queued subpool size", "expected", expectedQueuedSize, "got", queuedSize)
		return
	}

	if expectedBaseFeeSize >= 0 && baseFeeSize != expectedBaseFeeSize {
		logger.Error("FAILURE mismatched basefee subpool size", "expected", expectedBaseFeeSize, "got", baseFeeSize)
	}

	logger.Info("Subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
}
