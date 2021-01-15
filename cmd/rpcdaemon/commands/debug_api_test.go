package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
)

var debugTraceTransactionTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, ""},
	{"2e9f3fff37671c144fdd1745e2f2a6dbda67c68bd7c9b43c857a329ed93dab36", 33689, false, "0000000000000000000000000000000000000000000000000000000000000001"},
	{"ab94617b30b363cc1665524643d98f29a0ec594811a0251899b6eb9c8e305477", 36899, false, ""},
}

var debugTraceTransactionNoRefundTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, ""},
	{"2e9f3fff37671c144fdd1745e2f2a6dbda67c68bd7c9b43c857a329ed93dab36", 33689, false, "0000000000000000000000000000000000000000000000000000000000000001"},
	{"ab94617b30b363cc1665524643d98f29a0ec594811a0251899b6eb9c8e305477", 60899, false, ""},
}

func TestTraceTransaction(t *testing.T) {
	db, err := createTestDb()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	api := NewPrivateDebugAPI(db, 0)
	for _, tt := range debugTraceTransactionTests {
		result, err1 := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &eth.TraceConfig{})
		if err1 != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err1)
		}
		er := result.(*ethapi.ExecutionResult)
		if er.Gas != tt.gas {
			t.Errorf("wrong gas for transaction %s, got %d, expected %d", tt.txHash, er.Gas, tt.gas)
		}
		if er.Failed != tt.failed {
			t.Errorf("wrong failed flag for transaction %s, got %t, expected %t", tt.txHash, er.Failed, tt.failed)
		}
		if er.ReturnValue != tt.returnValue {
			t.Errorf("wrong return value for transaction %s, got %s, expected %s", tt.txHash, er.ReturnValue, tt.returnValue)
		}
	}
}

func TestTraceTransactionNoRefund(t *testing.T) {
	db, err := createTestDb()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	api := NewPrivateDebugAPI(db, 0)
	for _, tt := range debugTraceTransactionNoRefundTests {
		var norefunds bool = true
		result, err1 := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &eth.TraceConfig{NoRefunds: &norefunds})
		if err1 != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err1)
		}
		er := result.(*ethapi.ExecutionResult)
		if er.Gas != tt.gas {
			t.Errorf("wrong gas for transaction %s, got %d, expected %d", tt.txHash, er.Gas, tt.gas)
		}
		if er.Failed != tt.failed {
			t.Errorf("wrong failed flag for transaction %s, got %t, expected %t", tt.txHash, er.Failed, tt.failed)
		}
		if er.ReturnValue != tt.returnValue {
			t.Errorf("wrong return value for transaction %s, got %s, expected %s", tt.txHash, er.ReturnValue, tt.returnValue)
		}
	}
}
