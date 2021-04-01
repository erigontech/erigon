package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestGetTransactionReceipt(t *testing.T) {
	db, err := createTestKV()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()
	api := NewEthAPI(db, nil, 5000000, nil, nil)
	// Call GetTransactionReceipt for transaction which is not in the database
	if _, err := api.GetTransactionReceipt(context.Background(), common.Hash{}); err != nil {
		t.Errorf("calling GetTransactionReceipt with empty hash: %v", err)
	}
}
