package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"

	// Force-load native and js packages, to trigger registration
	_ "github.com/ledgerwatch/erigon/eth/tracers/js"
	_ "github.com/ledgerwatch/erigon/eth/tracers/native"
)

/*
Testing tracing RPC API by generating patters of contracts invoking one another based on the input
*/

func TestGeneratedDebugApi(t *testing.T) {
	m := rpcdaemontest.CreateTestSentryForTraces(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine)
	api := NewPrivateDebugAPI(baseApi, m.DB, 0)
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	callTracer := "callTracer"
	err := api.TraceBlockByNumber(context.Background(), rpc.BlockNumber(1), &tracers.TraceConfig{Tracer: &callTracer}, stream)
	if err != nil {
		t.Errorf("debug_traceBlock %d: %v", 0, err)
	}
	if err = stream.Flush(); err != nil {
		t.Fatalf("error flusing: %v", err)
	}
	var result interface{}
	if err = json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
	expectedJSON := `
	[
		{
		  "result": {
			"calls": [
			  {
				"calls": [
				  {
					"from": "0x00000000000000000000000000000000000001ff",
					"gas": "0x595a",
					"gasUsed": "0x16",
					"input": "0x0100",
					"output": "0x0100",
					"to": "0x00000000000000000000000000000000000000ff",
					"type": "CALL",
					"value": "0x0"
				  }
				],
				"from": "0x00000000000000000000000000000000000002ff",
				"gas": "0x6525",
				"gasUsed": "0xa7b",
				"input": "0x000100",
				"output": "0x0100",
				"to": "0x00000000000000000000000000000000000001ff",
				"type": "CALL",
				"value": "0x0"
			  },
			  {
				"calls": [
				  {
					"from": "0x00000000000000000000000000000000000001ff",
					"gas": "0x584a",
					"gasUsed": "0x10",
					"input": "0x",
					"to": "0x00000000000000000000000000000000000000ff",
					"type": "CALL",
					"value": "0x0"
				  }
				],
				"from": "0x00000000000000000000000000000000000002ff",
				"gas": "0x5a4c",
				"gasUsed": "0xb1",
				"input": "0x00",
				"to": "0x00000000000000000000000000000000000001ff",
				"type": "CALL",
				"value": "0x0"
			  }
			],
			"from": "0x71562b71999873db5b286df957af199ec94617f7",
			"gas": "0x7120",
			"gasUsed": "0x684c",
			"input": "0x01000100",
			"to": "0x00000000000000000000000000000000000002ff",
			"type": "CALL",
			"value": "0x0"
		  }
		}
	]`
	var expected interface{}
	if err = json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("parsing expected: %v", err)
	}
	if !assert.Equal(t, expected, result) {
		t.Fatalf("not equal")
	}
}

func TestGeneratedTraceApi(t *testing.T) {
	m := rpcdaemontest.CreateTestSentryForTraces(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine)
	api := NewTraceAPI(baseApi, m.DB, &httpcfg.HttpCfg{})
	traces, err := api.Block(context.Background(), rpc.BlockNumber(1))
	if err != nil {
		t.Errorf("trace_block %d: %v", 0, err)
	}
	buf, err := json.Marshal(traces)
	if err != nil {
		t.Errorf("marshall result into JSON: %v", err)
	}
	var result interface{}
	if err = json.Unmarshal(buf, &result); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
	expectedJSON := `
	[
		{
		  "action": {
			"from": "0x71562b71999873db5b286df957af199ec94617f7",
			"callType": "call",
			"gas": "0x7120",
			"input": "0x01000100",
			"to": "0x00000000000000000000000000000000000002ff",
			"value": "0x0"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": {
			"gasUsed": "0x161c",
			"output": "0x"
		  },
		  "subtraces": 2,
		  "traceAddress": [],
		  "transactionHash": "0xb42edc1d46932ef34be0ba49402dc94e3d2319c066f02945f6828cd344fcfa7b",
		  "transactionPosition": 0,
		  "type": "call"
		},
		{
		  "action": {
			"from": "0x00000000000000000000000000000000000002ff",
			"callType": "call",
			"gas": "0x6525",
			"input": "0x000100",
			"to": "0x00000000000000000000000000000000000001ff",
			"value": "0x0"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": {
			"gasUsed": "0xa7b",
			"output": "0x0100"
		  },
		  "subtraces": 1,
		  "traceAddress": [
			0
		  ],
		  "transactionHash": "0xb42edc1d46932ef34be0ba49402dc94e3d2319c066f02945f6828cd344fcfa7b",
		  "transactionPosition": 0,
		  "type": "call"
		},
		{
		  "action": {
			"from": "0x00000000000000000000000000000000000001ff",
			"callType": "call",
			"gas": "0x595a",
			"input": "0x0100",
			"to": "0x00000000000000000000000000000000000000ff",
			"value": "0x0"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": {
			"gasUsed": "0x16",
			"output": "0x0100"
		  },
		  "subtraces": 0,
		  "traceAddress": [
			0,
			0
		  ],
		  "transactionHash": "0xb42edc1d46932ef34be0ba49402dc94e3d2319c066f02945f6828cd344fcfa7b",
		  "transactionPosition": 0,
		  "type": "call"
		},
		{
		  "action": {
			"from": "0x00000000000000000000000000000000000002ff",
			"callType": "call",
			"gas": "0x5a4c",
			"input": "0x00",
			"to": "0x00000000000000000000000000000000000001ff",
			"value": "0x0"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": {
			"gasUsed": "0xb1",
			"output": "0x"
		  },
		  "subtraces": 1,
		  "traceAddress": [
			1
		  ],
		  "transactionHash": "0xb42edc1d46932ef34be0ba49402dc94e3d2319c066f02945f6828cd344fcfa7b",
		  "transactionPosition": 0,
		  "type": "call"
		},
		{
		  "action": {
			"from": "0x00000000000000000000000000000000000001ff",
			"callType": "call",
			"gas": "0x584a",
			"input": "0x",
			"to": "0x00000000000000000000000000000000000000ff",
			"value": "0x0"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": {
			"gasUsed": "0x10",
			"output": "0x"
		  },
		  "subtraces": 0,
		  "traceAddress": [
			1,
			0
		  ],
		  "transactionHash": "0xb42edc1d46932ef34be0ba49402dc94e3d2319c066f02945f6828cd344fcfa7b",
		  "transactionPosition": 0,
		  "type": "call"
		},
		{
		  "action": {
			"author": "0x0100000000000000000000000000000000000000",
			"rewardType": "block",
			"value": "0x1bc16d674ec80000"
		  },
		  "blockHash": "0x2c7ee9236a9eb58cbaf6473f458ddb41716c6735f4a63eacf0f8b759685f1dbc",
		  "blockNumber": 1,
		  "result": null,
		  "subtraces": 0,
		  "traceAddress": [],
		  "type": "reward"
		}
	  ]`
	var expected interface{}
	if err = json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("parsing expected: %v", err)
	}
	if !assert.Equal(t, expected, result) {
		t.Fatalf("not equal")
	}
}

func TestGeneratedTraceApiCollision(t *testing.T) {
	m := rpcdaemontest.CreateTestSentryForTracesCollision(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine)
	api := NewTraceAPI(baseApi, m.DB, &httpcfg.HttpCfg{})
	traces, err := api.Transaction(context.Background(), common.HexToHash("0xb2b9fa4c999c1c8370ce1fbd1c4315a9ce7f8421fe2ebed8a9051ff2e4e7e3da"))
	if err != nil {
		t.Errorf("trace_block %d: %v", 0, err)
	}
	buf, err := json.Marshal(traces)
	if err != nil {
		t.Errorf("marshall result into JSON: %v", err)
	}
	var result interface{}
	if err = json.Unmarshal(buf, &result); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
	expectedJSON := `
[
    {
        "action": {
            "from": "0x71562b71999873db5b286df957af199ec94617f7",
            "callType": "call",
            "gas": "0x13498",
            "input": "0x",
            "to": "0x000000000000000000000000000000000000bbbb",
            "value": "0x0"
        },
        "blockHash": "0xc78e9674685b04e1300d62cafdae0708d030a9bc0ff7aa9eb9315da23de650dc",
        "blockNumber": 1,
        "result": {
            "gasUsed": "0x131bb",
            "output": "0x"
        },
        "subtraces": 1,
        "traceAddress": [],
        "transactionHash": "0xb2b9fa4c999c1c8370ce1fbd1c4315a9ce7f8421fe2ebed8a9051ff2e4e7e3da",
        "transactionPosition": 2,
        "type": "call"
    },
    {
        "action": {
            "from": "0x000000000000000000000000000000000000bbbb",
            "gas": "0xb49d",
            "init": "0x600360035560046004556158ff6000526002601ef3",
            "value": "0x0"
        },
        "blockHash": "0xc78e9674685b04e1300d62cafdae0708d030a9bc0ff7aa9eb9315da23de650dc",
        "blockNumber": 1,
        "error": "contract address collision",
        "result": null,
        "subtraces": 0,
        "traceAddress": [
            0
        ],
        "transactionHash": "0xb2b9fa4c999c1c8370ce1fbd1c4315a9ce7f8421fe2ebed8a9051ff2e4e7e3da",
        "transactionPosition": 2,
        "type": "create"
    }
]	
`
	var expected interface{}
	if err = json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Fatalf("parsing expected: %v", err)
	}
	if !assert.Equal(t, expected, result) {
		t.Fatalf("not equal")
	}
}
