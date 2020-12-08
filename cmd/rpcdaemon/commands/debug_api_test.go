package commands

import (
	"context"
	"fmt"
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
	result, err1 := api.TraceTransaction(context.Background(), common.HexToHash("2e9f3fff37671c144fdd1745e2f2a6dbda67c68bd7c9b43c857a329ed93dab36"), &eth.TraceConfig{})
	if err1 != nil {
		t.Fatalf("traceTransaction: %v", err1)
	}
	fmt.Printf("%T\n", result)
}
