package commands

import (
	"context"
)
import "github.com/ledgerwatch/erigon/rpc"

// BscAPI is a collection of functions that are exposed in the
type BscAPI interface {
	// Receipt related (see ./eth_receipts.go)
	GetTransactionReceiptsByBlockNumber(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error)
}

type BscAPIImpl struct {
	ethApi *APIImpl
}

// NewBscAPI returns BscAPIImpl instance.
func NewBscAPI(eth *APIImpl) *BscAPIImpl {
	return &BscAPIImpl{
		ethApi: eth,
	}
}

func (api *BscAPIImpl) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]map[string]interface{}, error) {
	return api.ethApi.GetBlockReceipts(ctx, blockNr)
}
