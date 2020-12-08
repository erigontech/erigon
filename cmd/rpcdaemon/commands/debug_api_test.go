package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth"
)

func TestTraceTransaction(t *testing.T) {
	db, err := createTestDb()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	api := NewPrivateDebugAPI(db)
	api.TraceTransaction(context.Background(), common.HexToHash("0xfffg"), &eth.TraceConfig{})
}
