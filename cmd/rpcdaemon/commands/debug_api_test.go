package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
)

var debugTraceTransactionTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, ""},
	{"f588c6426861d9ad25d5ccc12324a8d213f35ef1ed4153193f0c13eb81ca7f4a", 49189, false, "0000000000000000000000000000000000000000000000000000000000000001"},
	{"b6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059", 38899, false, ""},
}

var debugTraceTransactionNoRefundTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, ""},
	{"f588c6426861d9ad25d5ccc12324a8d213f35ef1ed4153193f0c13eb81ca7f4a", 49189, false, "0000000000000000000000000000000000000000000000000000000000000001"},
	{"b6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059", 62899, false, ""},
}

func TestTraceTransaction(t *testing.T) {
	db, err := createTestKV()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()
	api := NewPrivateDebugAPI(db, 0, nil)
	for _, tt := range debugTraceTransactionTests {
		result, err1 := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &tracers.TraceConfig{})
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
	db, err := createTestKV()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()
	api := NewPrivateDebugAPI(db, 0, nil)
	for _, tt := range debugTraceTransactionNoRefundTests {
		var norefunds = true
		result, err1 := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &tracers.TraceConfig{NoRefunds: &norefunds})
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
