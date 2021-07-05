package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/internal/ethapi"
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
	db := rpcdaemontest.CreateTestKV(t)
	api := NewPrivateDebugAPI(NewBaseApi(nil), db, 0)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		err := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &tracers.TraceConfig{}, stream)
		if err != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err)
		}
		if err = stream.Flush(); err != nil {
			t.Fatalf("error flusing: %v", err)
		}
		var er ethapi.ExecutionResult
		if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
			t.Fatalf("parsing result: %v", err)
		}
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
	db := rpcdaemontest.CreateTestKV(t)
	api := NewPrivateDebugAPI(NewBaseApi(nil), db, 0)
	for _, tt := range debugTraceTransactionNoRefundTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		var norefunds = true
		err := api.TraceTransaction(context.Background(), common.HexToHash(tt.txHash), &tracers.TraceConfig{NoRefunds: &norefunds}, stream)
		if err != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err)
		}
		if err = stream.Flush(); err != nil {
			t.Fatalf("error flusing: %v", err)
		}
		var er ethapi.ExecutionResult
		if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
			t.Fatalf("parsing result: %v", err)
		}
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
