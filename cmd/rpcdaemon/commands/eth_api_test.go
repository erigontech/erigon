package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/common"
)

func TestGetTransactionReceipt(t *testing.T) {
	db := createTestKV(t)
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for transaction which is not in the database
	if _, err := api.GetTransactionReceipt(context.Background(), common.Hash{}); err != nil {
		t.Errorf("calling GetTransactionReceipt with empty hash: %v", err)
	}
}

func TestGetTransactionReceiptUnprotected(t *testing.T) {
	db := createTestKV(t)
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for un-protected transaction
	if _, err := api.GetTransactionReceipt(context.Background(), common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")); err != nil {
		t.Errorf("calling GetTransactionReceipt for unprotected tx: %v", err)
	}
}
