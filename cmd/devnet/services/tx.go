package services

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func CheckTxPoolContent(expectedPendingSize, expectedQueuedSize, expectedBaseFeeSize int) {
	pendingSize, queuedSize, baseFeeSize, err := requests.TxpoolContent(models.ReqId)
	if err != nil {
		fmt.Printf("FAILURE => error getting txpool content: %v\n", err)
		return
	}

	if pendingSize != expectedPendingSize {
		fmt.Printf("FAILURE => %v\n", fmt.Errorf("expected %d transaction(s) in pending pool, got %d", expectedPendingSize, pendingSize))
		return
	}

	if queuedSize != expectedQueuedSize {
		fmt.Printf("FAILURE => %v\n", fmt.Errorf("expected %d transaction(s) in queued pool, got %d", expectedQueuedSize, queuedSize))
		return
	}

	if baseFeeSize != expectedBaseFeeSize {
		fmt.Printf("FAILURE => %v\n", fmt.Errorf("expected %d transaction(s) in baseFee pool, got %d", expectedBaseFeeSize, baseFeeSize))
	}

	fmt.Printf("SUCCESS => %d transaction(s) in the pending pool, %d transaction(s) in the queued pool and %d transaction(s) in the baseFee pool\n", pendingSize, queuedSize, baseFeeSize)
}
