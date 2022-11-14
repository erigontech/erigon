package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func checkTxPoolContent(expectedPendingSize, expectedQueuedSize int) {
	pendingSize, queuedSize, err := requests.TxpoolContent(models.ReqId)
	if err != nil {
		fmt.Printf("FAILURE => error getting txpool content: %v\n", err)
	}

	if pendingSize != expectedPendingSize {
		fmt.Printf("FAILURE => %v\n", fmt.Errorf("expected %d transaction(s) in pending pool, got %d", expectedPendingSize, pendingSize))
	}

	if queuedSize != expectedQueuedSize {
		fmt.Printf("FAILURE => %v\n", fmt.Errorf("expected %d transaction(s) in queued pool, got %d", expectedQueuedSize, queuedSize))
	}

	fmt.Printf("SUCCESS => %d transaction(s) in the pending pool and %d transaction(s) in the queued pool\n", pendingSize, queuedSize)
}
