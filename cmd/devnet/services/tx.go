package services

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

func CheckTxPoolContent(expectedPendingSize, expectedQueuedSize, expectedBaseFeeSize int, logger log.Logger) {
	pendingSize, queuedSize, baseFeeSize, err := requests.TxpoolContent(models.ReqId, logger)
	if err != nil {
		logger.Error("FAILURE getting txpool content", "error", err)
		return
	}

	if pendingSize != expectedPendingSize {
		logger.Error("FAILURE mismatched pending subpool size", "expected", expectedPendingSize, "got", pendingSize)
		return
	}

	if queuedSize != expectedQueuedSize {
		logger.Error("FAILURE mismatched queued subpool size", "expected", expectedQueuedSize, "got", queuedSize)
		return
	}

	if baseFeeSize != expectedBaseFeeSize {
		logger.Error("FAILURE mismatched basefee subpool size", "expected", expectedBaseFeeSize, "got", baseFeeSize)
	}

	logger.Info("SUCCESS => subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
}
