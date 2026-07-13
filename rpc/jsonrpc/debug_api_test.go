// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var dumper = spew.ConfigState{Indent: "    "}

var debugTraceTransactionTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, "0x"},
	{"f588c6426861d9ad25d5ccc12324a8d213f35ef1ed4153193f0c13eb81ca7f4a", 49189, false, "0x0000000000000000000000000000000000000000000000000000000000000001"},
	{"b6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059", 38899, false, "0x"},
}

var debugTraceTransactionNoRefundTests = []struct {
	txHash      string
	gas         uint64
	failed      bool
	returnValue string
}{
	{"3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea", 21000, false, "0x"},
	{"f588c6426861d9ad25d5ccc12324a8d213f35ef1ed4153193f0c13eb81ca7f4a", 49189, false, "0x0000000000000000000000000000000000000000000000000000000000000001"},
	{"b6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059", 62899, false, "0x"},
}

func TestTraceBlockByNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0)
	ethApi := newEthApiForTest(baseApi, m.OverlayDB(), nil, nil)
	api := NewPrivateDebugAPI(baseApi, m.OverlayDB(), nil, 0, false)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
		tx, err := ethApi.GetTransactionByHash(m.Ctx, common.HexToHash(tt.txHash))
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		if tx == nil {
			t.Errorf("nil tx")
		}
		if tx.BlockHash == nil {
			t.Errorf("nil block hash")
		}
		txcount, err := ethApi.GetBlockTransactionCountByHash(m.Ctx, *tx.BlockHash)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		err = api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(tx.BlockNumber.ToInt().Uint64()), &tracersConfig.TraceConfig{}, s)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		if err = s.Flush(); err != nil {
			t.Fatalf("error flusing: %v", err)
		}
		var er []ethapi.ExecutionResult
		if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
			t.Fatalf("parsing result: %v", err)
		}
		if len(er) != int(*txcount) {
			t.Fatalf("incorrect length: %v", err)
		}
	}
	var buf bytes.Buffer
	s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	err := api.TraceBlockByNumber(m.Ctx, rpc.LatestBlockNumber, &tracersConfig.TraceConfig{}, s)
	if err != nil {
		t.Errorf("traceBlock %v: %v", rpc.LatestBlockNumber, err)
	}
	if err = s.Flush(); err != nil {
		t.Fatalf("error flusing: %v", err)
	}
	var er []ethapi.ExecutionResult
	if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
}

func TestTraceBlockByHash(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ethApi := newEthApiForTest(newBaseApiForTest(m), m.OverlayDB(), nil, nil)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
		tx, err := ethApi.GetTransactionByHash(m.Ctx, common.HexToHash(tt.txHash))
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		txcount, err := ethApi.GetBlockTransactionCountByHash(m.Ctx, *tx.BlockHash)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		err = api.TraceBlockByHash(m.Ctx, *tx.BlockHash, &tracersConfig.TraceConfig{}, s)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		if err = s.Flush(); err != nil {
			t.Fatalf("error flusing: %v", err)
		}
		var er []ethapi.ExecutionResult
		if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
			t.Fatalf("parsing result: %v", err)
		}
		if len(er) != int(*txcount) {
			t.Fatalf("incorrect length: %v", err)
		}
	}
}

func TestTraceTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
		err := api.TraceTransaction(m.Ctx, common.HexToHash(tt.txHash), &tracersConfig.TraceConfig{}, s)
		if err != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err)
		}
		if err = s.Flush(); err != nil {
			t.Fatalf("error flusing: %v", err)
		}
		var er ethapi.ExecutionResult
		if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
			t.Fatalf("parsing result: %v, %s", err, buf.String())
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

func TestTraceTransactionNotFound(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)

	var buf bytes.Buffer
	s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	err := api.TraceTransaction(m.Ctx, common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"), &tracersConfig.TraceConfig{}, s)
	require.ErrorContains(t, err, "transaction not found")
}

// TestTraceErrorPathsWriteNoStream verifies that streaming trace methods write nothing to the
// stream on early error paths. This is required for JSON-RPC 2.0 compliance: the handler omits
// the "result" field when the stream is untouched, producing {error:...} without result:null.
func TestTraceErrorPathsWriteNoStream(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)

	newStream := func() (*bytes.Buffer, jsonstream.Stream) {
		var buf bytes.Buffer
		return &buf, jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	}

	t.Run("TraceBlockByNumber_genesis", func(t *testing.T) {
		buf, s := newStream()
		err := api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(0), &tracersConfig.TraceConfig{}, s)
		require.ErrorContains(t, err, "genesis is not traceable")
		require.NoError(t, s.Flush())
		require.Empty(t, buf.Bytes(), "stream must be empty on early error")
	})

	t.Run("TraceBlockByHash_genesis", func(t *testing.T) {
		var genesisHash common.Hash
		require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
			genesisHash, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 0)
			return nil
		}))
		buf, s := newStream()
		err := api.TraceBlockByHash(m.Ctx, genesisHash, &tracersConfig.TraceConfig{}, s)
		require.ErrorContains(t, err, "genesis is not traceable")
		require.NoError(t, s.Flush())
		require.Empty(t, buf.Bytes(), "stream must be empty on early error")
	})

	t.Run("TraceTransaction_genesis", func(t *testing.T) {
		buf, s := newStream()
		err := api.TraceTransaction(m.Ctx, common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"), &tracersConfig.TraceConfig{}, s)
		require.ErrorContains(t, err, "transaction not found")
		require.NoError(t, s.Flush())
		require.Empty(t, buf.Bytes(), "stream must be empty on early error")
	})

	from := common.Address{0xFF}
	to := common.Address{0x01}
	gas := hexutil.Uint64(21000)
	gasPrice := hexutil.Big(*big.NewInt(1e9))
	traceCallArgs := ethapi.CallArgs{From: &from, To: &to, Gas: &gas, GasPrice: &gasPrice}
	for _, tc := range []struct {
		name   string
		tracer string
	}{
		{"streaming", ""},
		{"callTracer", "callTracer"},
	} {
		t.Run("TraceCall_execution_error_"+tc.name, func(t *testing.T) {
			var cfg *tracersConfig.TraceConfig
			if tc.tracer != "" {
				name := tc.tracer
				cfg = &tracersConfig.TraceConfig{Tracer: &name}
			}
			buf, s := newStream()
			err := api.TraceCall(m.Ctx, traceCallArgs, rpc.BlockNumberOrHashWithNumber(1), cfg, s)
			require.Error(t, err)
			require.NoError(t, s.Flush())
			require.Empty(t, buf.Bytes(), "stream must be empty on execution error so handler omits result field")
		})
	}

	tracer := "callTracer"
	timeout := "garbage"
	badTimeoutCfg := &tracersConfig.TraceConfig{Tracer: &tracer, Timeout: &timeout}

	t.Run("TraceTransaction_bad_timeout", func(t *testing.T) {
		buf, s := newStream()
		err := api.TraceTransaction(m.Ctx, common.HexToHash(debugTraceTransactionTests[0].txHash), badTimeoutCfg, s)
		require.Error(t, err)
		require.NoError(t, s.Flush())
		require.Empty(t, buf.Bytes(), "stream must be empty on AssembleTracer error so handler omits result field")
	})

	t.Run("TraceCall_bad_timeout", func(t *testing.T) {
		buf, s := newStream()
		err := api.TraceCall(m.Ctx, traceCallArgs, rpc.BlockNumberOrHashWithNumber(1), badTimeoutCfg, s)
		require.Error(t, err)
		require.NoError(t, s.Flush())
		require.Empty(t, buf.Bytes(), "stream must be empty on AssembleTracer error so handler omits result field")
	})
}

func (c *baseFeeTestChain) debugAPI() *DebugAPIImpl {
	return NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.OverlayDB(), nil, 0, false)
}

// callDebugTraceCall invokes debug_traceCall with the given args and
// BlockOverrides, and returns the call's return data.
func callDebugTraceCall(t *testing.T, api *DebugAPIImpl, args ethapi.CallArgs, overrides *ethapi.BlockOverrides) string {
	t.Helper()

	var buf bytes.Buffer
	s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	err := api.TraceCall(context.Background(), args, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), &tracersConfig.TraceConfig{
		BlockOverrides: overrides,
	}, s)
	require.NoError(t, err)
	require.NoError(t, s.Flush())

	var er ethapi.ExecutionResult
	require.NoError(t, json.Unmarshal(buf.Bytes(), &er))
	return er.ReturnValue
}

func TestDebugTraceCallBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	contractAddr := c.deployOpcodeContract(t, opGasprice)

	args := ethapi.CallArgs{
		From:                 &c.bankAddress,
		To:                   &contractAddr,
		Gas:                  newUint64(100_000),
		MaxFeePerGas:         (*hexutil.Big)(big.NewInt(100)),
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(2)),
	}
	returnValue := callDebugTraceCall(t, c.debugAPI(), args, &ethapi.BlockOverrides{
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(10)),
	})
	// effective gas price = BaseFeePerGas(10) + MaxPriorityFeePerGas(2) = 12 = 0xc
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", returnValue)
}

func TestDebugTraceCallBlockOverridesOtherFieldsAffectOpcodes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	for _, tc := range blockOverrideOpcodeCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
			contractAddr := c.deployOpcodeContract(t, tc.opcode)

			args := ethapi.CallArgs{From: &c.bankAddress, To: &contractAddr}
			returnValue := callDebugTraceCall(t, c.debugAPI(), args, tc.override)
			require.Equal(t, hexutil.Bytes(tc.expected).String(), returnValue)
		})
	}
}

// TestTxResultFieldStreamLazy verifies the lazy-write semantics of LazyFieldStream
// with prependSeparator=true (the per-tx result field case).
func TestTxResultFieldStreamLazy(t *testing.T) {
	newInner := func() (*bytes.Buffer, jsonstream.Stream) {
		var buf bytes.Buffer
		return &buf, jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	}

	t.Run("no_writes_when_unused", func(t *testing.T) {
		buf, inner := newInner()
		_ = jsonstream.NewLazyFieldStream(inner, "result", true)
		require.NoError(t, inner.Flush())
		require.Empty(t, buf.Bytes())
	})

	t.Run("writes_separator_and_field_on_first_value", func(t *testing.T) {
		buf, inner := newInner()
		lazy := jsonstream.NewLazyFieldStream(inner, "result", true)
		lazy.WriteNil()
		require.NoError(t, inner.Flush())
		require.Equal(t, `,"result":null`, buf.String())
	})

	t.Run("field_written_only_once", func(t *testing.T) {
		buf, inner := newInner()
		lazy := jsonstream.NewLazyFieldStream(inner, "result", true)
		lazy.WriteArrayStart()
		lazy.WriteString("a")
		lazy.WriteMore()
		lazy.WriteString("b")
		lazy.WriteArrayEnd()
		require.NoError(t, inner.Flush())
		require.Equal(t, `,"result":["a","b"]`, buf.String())
	})
}

// TestTraceBlockErrorBeforeWrite verifies traceBlock produces valid JSON when AssembleTracer fails
// before any write (inner.Written stays false): each tx object has "error" but no "result" field.
func TestTraceBlockErrorBeforeWrite(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	ethApi := newEthApiForTest(newBaseApiForTest(m), m.OverlayDB(), nil, nil)

	tx, err := ethApi.GetTransactionByHash(m.Ctx, common.HexToHash(debugTraceTransactionTests[0].txHash))
	require.NoError(t, err)
	require.NotNil(t, tx)
	blockNum := rpc.BlockNumber(tx.BlockNumber.ToInt().Uint64())

	// Invalid timeout makes AssembleTracer fail before any write to inner, so inner.Written stays
	// false and the error handler writes only the "error" field inside the tx object.
	tracer := "callTracer"
	timeout := "garbage"
	cfg := &tracersConfig.TraceConfig{Tracer: &tracer, Timeout: &timeout}

	var buf bytes.Buffer
	s := jsonstream.NewStackStream(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
	require.NoError(t, api.TraceBlockByNumber(m.Ctx, blockNum, cfg, s))
	require.NoError(t, s.Flush())

	var entries []json.RawMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entries), "traceBlock output is not valid JSON: %s", buf.String())
	require.NotEmpty(t, entries)
	for i, entry := range entries {
		var obj map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(entry, &obj), "tx entry %d is not a JSON object", i)
		require.Contains(t, obj, "txHash", "tx entry %d missing txHash", i)
		require.Contains(t, obj, "error", "tx entry %d: error must be inside the tx object, not at array level", i)
		require.NotContains(t, obj, "result", "tx entry %d: result must not be written when error occurs before any write", i)
	}
}

// TestTraceBlockErrorAfterWrite exercises the Written==true close path: when a tracer starts
// writing to the result field before an error occurs, CloseIfOpen must seal the partial JSON
// back to the tx-object level so the overall output remains valid.
func TestTraceBlockErrorAfterWrite(t *testing.T) {
	var buf bytes.Buffer
	s := jsonstream.New(&buf)
	inner := jsonstream.NewLazyFieldStream(s, "result", true)

	// Replicate the per-tx structure of the traceBlock loop.
	s.WriteArrayStart()
	s.WriteObjectStart()
	s.WriteObjectField("txHash")
	s.WriteString("0xdeadbeef")
	inner.ResetField()

	// Simulate TraceTx writing a partial result before returning an error:
	// the first write to inner triggers ensure() and sets Written=true.
	inner.WriteObjectStart()
	inner.WriteObjectField("from")
	inner.WriteString("0xabcd")
	// Replicate the traceBlock error handler.
	inner.CloseIfOpen()
	s.WriteMore()
	s.WriteObjectField("error")
	s.WriteString("partial write error")
	s.WriteObjectEnd()

	s.WriteArrayEnd()
	require.NoError(t, s.Flush())

	var entries []json.RawMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entries), "traceBlock output is not valid JSON: %s", buf.String())
	require.Len(t, entries, 1)
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0], &obj), "tx entry is not a JSON object")
	require.Contains(t, obj, "txHash")
	require.Contains(t, obj, "result", "result must be present when Written=true before error")
	require.Contains(t, obj, "error", "error must be inside the tx object, not at array level")
	var resultObj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(obj["result"], &resultObj), "result must be valid JSON after CloseIfOpen: %s", obj["result"])
}

func TestTraceTransactionNoRefund(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	for _, tt := range debugTraceTransactionNoRefundTests {
		var buf bytes.Buffer
		s := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))
		var norefunds = true
		err := api.TraceTransaction(m.Ctx, common.HexToHash(tt.txHash), &tracersConfig.TraceConfig{NoRefunds: &norefunds}, s)
		if err != nil {
			t.Errorf("traceTransaction %s: %v", tt.txHash, err)
		}
		if err = s.Flush(); err != nil {
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

func TestStorageRangeAt(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	t.Run("invalid addr", func(t *testing.T) {
		var block4 *types.Block
		var err error
		err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
			block4, err = m.BlockReader.BlockByNumber(m.Ctx, tx, 4)
			return err
		})
		require.NoError(t, err)
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf55")
		expect := StorageRangeResult{storageMap{}, nil}
		result, err := api.StorageRangeAt(m.Ctx, block4.Hash(), 0, addr, nil, 100)
		require.NoError(t, err)
		require.Equal(t, expect, result)
	})
	t.Run("block 4, addr 1", func(t *testing.T) {
		var block4 *types.Block
		err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
			block4, _ = m.BlockReader.BlockByNumber(m.Ctx, tx, 4)
			return nil
		})
		require.NoError(t, err)
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
		keys := []common.Hash{ // hashes of Keys of storage
			common.HexToHash("0x405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace"),
			common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
		}
		storage := storageMap{
			keys[0]: {Key: &keys[1], Value: common.HexToHash("0000000000000000000000000d3ab14bbad3d99f4203bd7a11acb94882050e7e")},
		}
		expect := StorageRangeResult{storageMap{keys[0]: storage[keys[0]]}, nil}

		result, err := api.StorageRangeAt(m.Ctx, block4.Hash(), 0, addr, nil, 100)
		require.NoError(t, err)
		require.Equal(t, expect, result)
	})
	t.Run("block latest, addr 1", func(t *testing.T) {
		var latestBlock *types.Block
		err := m.DB.View(m.Ctx, func(tx kv.Tx) (err error) {
			latestBlock, err = m.BlockReader.CurrentBlock(tx)
			return err
		})
		require.NoError(t, err)
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
		keys := []common.Hash{ // hashes of Keys of storage
			common.HexToHash("0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563"),
			common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),

			common.HexToHash("0x405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace"),
			common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),

			common.HexToHash("0xb077f7530a1364c54ee00cf94ba99175db81e7e002c97e344aa5d3c4908617c4"),
			common.HexToHash("0x9541d803110b392ecde8e03af7ae34d4457eb4934dac09903ccee819bec4a355"),

			common.HexToHash("0xb6b80924ee71b506e16a000e00b0f8f3a82f53791c6b87f5958fdf562f3d12c8"),
			common.HexToHash("0xf41f8421ae8c8d7bb78783a0bdadb801a5f895bea868c1d867ae007558809ef1"),
		}
		storage := storageMap{
			keys[0]: {Key: &keys[1], Value: common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000000a")},
			keys[2]: {Key: &keys[3], Value: common.HexToHash("0x0000000000000000000000000d3ab14bbad3d99f4203bd7a11acb94882050e7e")},
			keys[4]: {Key: &keys[5], Value: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")},
			keys[6]: {Key: &keys[7], Value: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000007")},
		}
		expect := StorageRangeResult{
			storageMap{keys[0]: storage[keys[0]], keys[2]: storage[keys[2]], keys[4]: storage[keys[4]], keys[6]: storage[keys[6]]},
			nil}

		result, err := api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, 100)
		require.NoError(t, err)
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// limited — default Erigon mode returns nextKey as raw key
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[0]: storage[keys[0]], keys[2]: storage[keys[2]]}, &keys[5]}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// start from something, limited
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, expect.NextKey[:], 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[4]: storage[keys[4]], keys[6]: storage[keys[6]]}, nil}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}
	})

}

func TestStorageRangeAtGethCompat(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, true) // gethCompatibility=true
	t.Run("block latest, addr 1", func(t *testing.T) {
		var latestBlock *types.Block
		err := m.DB.View(m.Ctx, func(tx kv.Tx) (err error) {
			latestBlock, err = m.BlockReader.CurrentBlock(tx)
			return err
		})
		require.NoError(t, err)
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
		keys := []common.Hash{ // pairs: (seckey, rawkey) — ordered by seckey (keccak256 hash)
			common.HexToHash("0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563"),
			common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),

			common.HexToHash("0x405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace"),
			common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),

			common.HexToHash("0xb077f7530a1364c54ee00cf94ba99175db81e7e002c97e344aa5d3c4908617c4"),
			common.HexToHash("0x9541d803110b392ecde8e03af7ae34d4457eb4934dac09903ccee819bec4a355"),

			common.HexToHash("0xb6b80924ee71b506e16a000e00b0f8f3a82f53791c6b87f5958fdf562f3d12c8"),
			common.HexToHash("0xf41f8421ae8c8d7bb78783a0bdadb801a5f895bea868c1d867ae007558809ef1"),
		}
		storage := storageMap{
			keys[0]: {Key: &keys[1], Value: common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000000a")},
			keys[2]: {Key: &keys[3], Value: common.HexToHash("0x0000000000000000000000000d3ab14bbad3d99f4203bd7a11acb94882050e7e")},
			keys[4]: {Key: &keys[5], Value: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003")},
			keys[6]: {Key: &keys[7], Value: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000007")},
		}

		// all entries
		expect := StorageRangeResult{
			storageMap{keys[0]: storage[keys[0]], keys[2]: storage[keys[2]], keys[4]: storage[keys[4]], keys[6]: storage[keys[6]]},
			nil}
		result, err := api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, 100)
		require.NoError(t, err)
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// limited — geth-compat mode returns nextKey as hashed key (seckey)
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[0]: storage[keys[0]], keys[2]: storage[keys[2]]}, &keys[4]}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// start from nextKey (hashed), limited — pagination
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, expect.NextKey[:], 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[4]: storage[keys[4]], keys[6]: storage[keys[6]]}, nil}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// negative maxResult should be handled safely and must not panic.
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, -1)
		require.NoError(t, err)
		require.Empty(t, result.Storage)
		require.NotNil(t, result.NextKey)
		require.Equal(t, keys[0], *result.NextKey)
	})
}

func TestAccountRange(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)

	t.Run("valid account", func(t *testing.T) {
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf55")
		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts, 2)

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts, 3)
	})
	t.Run("valid contract", func(t *testing.T) {
		addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts, 1)

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts, 2)

		n = rpc.BlockNumber(10)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts, 2)
	})
	t.Run("with storage", func(t *testing.T) {
		addr := common.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")

		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false, nil)
		require.NoError(t, err)
		require.Empty(t, result.Accounts)

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts[addr].Storage, 35)

		n = rpc.BlockNumber(10)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false, nil)
		require.NoError(t, err)
		require.Len(t, result.Accounts[addr].Storage, 35)
		require.Equal(t, 1, int(result.Accounts[addr].Nonce))
		for _, v := range result.Accounts {
			hashedCode, _ := common.HashData(v.Code)
			require.Equal(t, v.CodeHash.String(), hashedCode.String())
		}
	})
	t.Run("incompletes=true is accepted", func(t *testing.T) {
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf55")
		n := rpc.BlockNumber(1)
		without, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, nil)
		require.NoError(t, err)
		incompletes := true
		with, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true, &incompletes)
		require.NoError(t, err)
		require.Equal(t, without, with)
	})
	t.Run("prefix start right-pads to address boundary", func(t *testing.T) {
		n := rpc.BlockNumber(1)
		prefix := []byte{0x53, 0x7e}
		resultPrefix, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, prefix, 10, true, true, nil)
		require.NoError(t, err)
		var padded common.Address
		copy(padded[:], prefix)
		resultPadded, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, padded[:], 10, true, true, nil)
		require.NoError(t, err)
		require.Equal(t, resultPadded, resultPrefix)
	})
	t.Run("empty key returns error", func(t *testing.T) {
		n := rpc.BlockNumber(1)
		_, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []byte{}, 10, true, true, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty")
	})
	t.Run("32-byte key returns error", func(t *testing.T) {
		key32 := make([]byte, 32)
		n := rpc.BlockNumber(1)
		_, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, key32, 10, true, true, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "32-byte")
	})
	t.Run("oversized key returns error", func(t *testing.T) {
		key21 := make([]byte, 21)
		n := rpc.BlockNumber(1)
		_, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, key21, 10, true, true, nil)
		require.ErrorContains(t, err, "at most 20 bytes")
	})
}

func TestGetModifiedAccountsByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)

	t.Run("correct input", func(t *testing.T) {
		n, n2 := rpc.BlockNumber(1), rpc.BlockNumber(2)
		result, err := api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 3)
		// block 2 sends ETH to theAddr{1} — verify it appears in the result
		require.Contains(t, result, common.Address{1})

		n, n2 = rpc.BlockNumber(5), rpc.BlockNumber(8)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 37)

		n, n2 = rpc.BlockNumber(0), rpc.BlockNumber(10)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 40)

		// nil second param: returns accounts modified exactly in block startNum (Geth semantics)
		n = rpc.BlockNumber(0)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.NoError(t, err)
		require.Len(t, result, 3)

		n2 = rpc.BlockNumber(12)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, rpc.BlockNumber(11), &n2)
		require.NoError(t, err)
		require.NotEmpty(t, result)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, rpc.BlockNumber(11), nil)
		require.NoError(t, err)
	})
	t.Run("storage-only modified contracts", func(t *testing.T) {
		// Block 8 (i=7) executes a token.Transfer which writes to the ERC20 balances
		// mapping in storage, but does NOT change the contract's balance/nonce/code.
		// Without the StorageDomain pass the contract would be invisible to this API.
		// The token contract was deployed in block 7 and is known from the AccountRange test.
		tokenContract := common.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")
		n, n2 := rpc.BlockNumber(7), rpc.BlockNumber(8)
		result, err := api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Contains(t, result, tokenContract, "storage-only modified contract must be included")
	})
	t.Run("invalid input", func(t *testing.T) {
		n := rpc.BlockNumber(1_000_000)
		_, err := api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.Error(t, err)

		n = rpc.BlockNumber(11)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n)
		require.Error(t, err)

		// end block beyond latest is an error (Geth semantics)
		n2 := rpc.BlockNumber(77)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, rpc.BlockNumber(11), &n2)
		require.Error(t, err)
	})
}

func TestMapTxNum2BlockNum(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	if !m.HistoryV3 {
		t.Skip()
	}

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	checkIter := func(t *testing.T, expectTxNums stream.U64, txNumsIter *rawdbv3.MapTxNum2BlockNumIter) {
		for expectTxNums.HasNext() {
			require.True(t, txNumsIter.HasNext())
			expectTxNum, _ := expectTxNums.Next()
			txNum, _, _, _, _, _ := txNumsIter.Next()
			require.Equal(t, expectTxNum, txNum)
		}
	}
	t.Run("descend", func(t *testing.T) {
		tx, err := m.DB.BeginTemporalRo(m.Ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		txNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 1024, -1, order.Desc, kv.Unlim)
		require.NoError(t, err)
		txNumsIter := rawdbv3.TxNums2BlockNums(m.Ctx, tx, rawdbv3.TxNums, txNums, order.Desc)
		expectTxNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 1024, -1, order.Desc, kv.Unlim)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
	t.Run("ascend", func(t *testing.T) {
		tx, err := m.DB.BeginTemporalRo(m.Ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		txNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 0, 1024, order.Asc, kv.Unlim)
		require.NoError(t, err)
		txNumsIter := rawdbv3.TxNums2BlockNums(m.Ctx, tx, rawdbv3.TxNums, txNums, order.Desc)
		expectTxNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 0, 1024, order.Asc, kv.Unlim)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
	t.Run("ascend limit", func(t *testing.T) {
		tx, err := m.DB.BeginTemporalRo(m.Ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		txNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 0, 1024, order.Asc, 2)
		require.NoError(t, err)
		txNumsIter := rawdbv3.TxNums2BlockNums(m.Ctx, tx, rawdbv3.TxNums, txNums, order.Desc)
		expectTxNums, err := tx.IndexRange(kv.LogAddrIdx, addr[:], 0, 1024, order.Asc, 2)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
}

func TestAccountAt(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)

	var blockHash0, blockHash1, blockHash3, blockHash10, blockHashNonExistent common.Hash
	_ = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		blockHash0, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 0)
		blockHash1, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 1)
		blockHash3, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 3)
		blockHash10, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 10)
		blockHashNonExistent, _, _ = m.BlockReader.CanonicalHash(m.Ctx, tx, 20)
		_, _, _, _, _ = blockHash0, blockHash1, blockHash3, blockHash10, blockHashNonExistent
		return nil
	})

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	contract := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	t.Run("addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.AccountAt(m.Ctx, blockHash0, 0, addr)
		require.NoError(err)
		require.Equal(0, int(results.Nonce))

		results, err = api.AccountAt(m.Ctx, blockHash1, 0, addr)
		require.NoError(err)
		require.Equal(0, int(results.Nonce))

		results, err = api.AccountAt(m.Ctx, blockHash10, 0, addr)
		require.NoError(err)
		require.Equal(1, int(results.Nonce))

		// block 20 doesn't exist in the chain
		results, err = api.AccountAt(m.Ctx, blockHashNonExistent, 0, addr)
		require.NoError(err)
		require.Nil(results)
	})
	t.Run("contract", func(t *testing.T) {
		require := require.New(t)

		// check contract with more nonces
		results, err := api.AccountAt(m.Ctx, blockHash10, 0, contract)
		require.NoError(err)
		require.Equal(38, int(results.Nonce))

		// and in the middle of block
		results, err = api.AccountAt(m.Ctx, blockHash10, 1, contract)
		require.NoError(err)
		require.Equal(39, int(results.Nonce))
		require.Equal("0x", results.Code.String())

		// and too big txIndex
		results, err = api.AccountAt(m.Ctx, blockHash10, 1024, contract)
		require.NoError(err)
		require.Equal(42, int(results.Nonce))
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.AccountAt(m.Ctx, blockHash10, 0, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Equal(0, int(results.Nonce))
	})
}

func TestGetBadBlocks(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 5000000, false)
	ctx := context.Background()

	require := require.New(t)
	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
	defer tx.Rollback()

	putBlock := func(number uint64) common.Hash {
		// prepare db so it works with our test
		signer1 := types.MakeSigner(chainspec.Mainnet.Config, number, number-1)
		body := &types.Body{
			Transactions: []types.Transaction{
				mustSign(types.NewTransaction(number, testAddr, &u256.Num1, 1, &u256.Num1, nil), *signer1),
				mustSign(types.NewTransaction(number+1, testAddr, &u256.Num1, 2, &u256.Num1, nil), *signer1),
			},
			Uncles: []*types.Header{{Extra: []byte("test header")}},
		}

		header := &types.Header{Number: *uint256.NewInt(number)}
		require.NoError(rawdb.WriteCanonicalHash(tx, header.Hash(), number))
		require.NoError(rawdb.WriteHeader(tx, header))
		require.NoError(rawdb.WriteBody(tx, header.Hash(), number, body))

		return header.Hash()
	}

	number := *rawdb.ReadCurrentBlockNumber(tx)

	// put some blocks
	i := number
	for i <= number+6 {
		putBlock(i)
		i++
	}
	hash1 := putBlock(i)
	hash2 := putBlock(i + 1)
	hash3 := putBlock(i + 2)
	hash4 := putBlock(i + 3)
	require.NoError(rawdb.TruncateCanonicalHash(tx, i, true)) // trim since i

	tx.Commit()

	// Reset the global bad block cache so it reads only from this test's DB
	tx2, err := m.DB.BeginRo(ctx)
	require.NoError(err)
	defer tx2.Rollback()
	require.NoError(rawdb.ResetBadBlockCache(tx2, 100))
	tx2.Rollback()

	data, err := api.GetBadBlocks(ctx)
	require.NoError(err)

	require.Len(data, 4)
	require.Equal(data[0]["hash"], hash4)
	require.Equal(data[1]["hash"], hash3)
	require.Equal(data[2]["hash"], hash2)
	require.Equal(data[3]["hash"], hash1)
}

func TestGetRawTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 5000000, false)
	ctx := context.Background()

	require := require.New(t)
	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read transaction: %s", err)
	}
	defer tx.Rollback()
	number := *rawdb.ReadCurrentBlockNumber(tx)
	tx.Commit()

	if number < 1 {
		t.Error("TestSentry doesn't have enough blocks for this test")
	}
	var testedOnce = false
	for i := uint64(0); i < number; i++ {
		tx, err := m.DB.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		block, err := api._blockReader.BlockByNumber(ctx, tx, i)
		require.NoError(err)
		txns := block.Transactions()

		for _, txn := range txns {
			// Get the first txn
			txnBinary := bytes.Buffer{}
			err = txn.MarshalBinary(&txnBinary)
			require.NoError(err)
			data, err := api.GetRawTransaction(ctx, txn.Hash())
			require.NoError(err)
			require.NotEmpty(data)
			require.Equal([]byte(data), txnBinary.Bytes())
			testedOnce = true
		}
	}
	require.True(testedOnce, "Test flow didn't touch the target flow")
}

func TestGetRawReceipts(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 5000000, false)
	ctx := context.Background()

	require := require.New(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(err)
	defer tx.Rollback()
	number := *rawdb.ReadCurrentBlockNumber(tx)

	testedNonEmpty := false
	for i := uint64(0); i <= number; i++ {
		block, err := api.blockByNumberWithSenders(ctx, tx, i)
		require.NoError(err)
		receipts, err := api.getReceipts(ctx, tx, block)
		require.NoError(err)
		expected := make([]hexutil.Bytes, len(receipts))
		for j, receipt := range receipts {
			b, err := receipt.MarshalBinary()
			require.NoError(err)
			expected[j] = b
		}

		raw, err := api.GetRawReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(i)))
		require.NoError(err)
		require.Equal(expected, raw)
		if len(raw) > 0 {
			testedNonEmpty = true
		}
	}
	require.True(testedNonEmpty, "test chain has no receipts")
}

func TestExecutionWitness(t *testing.T) {
	// Enable historical commitment schema so the test aggregator maintains per-block history.
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), nil, 0, false)
	ctx := context.Background()

	// Write the DB flag so that debug_executionWitness accepts the request.
	err := m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	})
	require.NoError(t, err)

	// Get the latest block number
	var latestBlockNum uint64
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		latestBlockNum, _ = stages.GetStageProgress(tx, stages.Execution)
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, latestBlockNum, uint64(0), "test chain should have at least one block")
	t.Run("genesis block", func(t *testing.T) {
		// Note: commitment history starts from 1 in this test suite
		blockNum := rpc.BlockNumber(0)
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum}, nil)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
		require.NotNil(t, result.Codes, "Codes should not be nil")

		t.Logf("Genesis: %d state nodes, %d codes",
			len(result.State), len(result.Codes))
	})

	t.Run("by block number", func(t *testing.T) {
		// Test with block number 1
		blockNum := rpc.BlockNumber(1)
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum}, nil)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
		require.NotNil(t, result.Keys, "Keys must be populated with accessed address/slot preimages")
		for i, k := range result.Keys {
			require.Contains(t, []int{20, 32}, len(k), "key %d must be a 20B address or 32B slot preimage", i)
			if i > 0 {
				require.Negative(t, bytes.Compare(result.Keys[i-1], k), "keys must be sorted and deduplicated")
			}
		}
		if len(result.Headers) > 0 {
			require.Contains(t, result.headerByNumber, uint64(0), "parent header (block 0) must be present in lookup map when Headers is non-empty")
		}
	})

	t.Run("by block hash", func(t *testing.T) {
		var blockHash common.Hash
		err := m.DB.View(ctx, func(tx kv.Tx) error {
			blockHash, _, _ = m.BlockReader.CanonicalHash(ctx, tx, 1)
			return nil
		})
		require.NoError(t, err)

		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockHash: &blockHash}, nil)

		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("multiple blocks", func(t *testing.T) {
		for blockNum := uint64(1); blockNum <= latestBlockNum; blockNum++ {
			bn := rpc.BlockNumber(blockNum)
			result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)

			require.NoError(t, err, "ExecutionWitness failed for block %d", blockNum)
			require.NotNil(t, result, "Result should not be nil for block %d", blockNum)
			require.NotNil(t, result.State, "State should not be nil for block %d", blockNum)
		}
	})

	t.Run("latest block", func(t *testing.T) {
		blockNum := rpc.LatestBlockNumber
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum}, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
	})

	t.Run("non-existent block", func(t *testing.T) {
		// Very high block number that doesn't exist
		blockNum := rpc.BlockNumber(999999999)
		_, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum}, nil)
		require.Error(t, err, "should error for non-existent block")
	})
}

// mockEthBackend implements privateapi.EthBackend for testing
type mockEthBackend struct {
	setHeadCalled bool
	setHeadBlock  uint64
	setHeadErr    error
}

var _ privateapi.EthBackend = (*mockEthBackend)(nil) // compile-time interface check

func (m *mockEthBackend) Etherbase() (common.Address, error) {
	return common.Address{}, nil
}

func (m *mockEthBackend) NetVersion() (uint64, error) {
	return 1, nil
}

func (m *mockEthBackend) NetPeerCount() (uint64, error) {
	return 0, nil
}

func (m *mockEthBackend) NodesInfo(limit int) (*remoteproto.NodesInfoReply, error) {
	return &remoteproto.NodesInfoReply{}, nil
}

func (m *mockEthBackend) Peers(ctx context.Context) (*remoteproto.PeersReply, error) {
	return &remoteproto.PeersReply{}, nil
}

func (m *mockEthBackend) AddPeer(ctx context.Context, req *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	return &remoteproto.AddPeerReply{}, nil
}

func (m *mockEthBackend) RemovePeer(ctx context.Context, req *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	return &remoteproto.RemovePeerReply{}, nil
}

func (m *mockEthBackend) AddTrustedPeer(ctx context.Context, url *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	return nil, nil
}

func (m *mockEthBackend) RemoveTrustedPeer(ctx context.Context, url *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	return nil, nil
}

func (m *mockEthBackend) SetHead(ctx context.Context, targetBlock uint64) error {
	m.setHeadCalled = true
	m.setHeadBlock = targetBlock
	return m.setHeadErr
}

// TestSetHead tests debug_setHead RPC validation.
func TestSetHead(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := m.Ctx
	logger := log.New()

	// Determine the canonical head of the test chain.
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	head, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	roTx.Rollback()
	require.Greater(t, head, uint64(1), "test chain must have at least 2 blocks")

	// Helper to build a DebugAPIImpl wired to a mockEthBackend
	makeAPI := func(mock *mockEthBackend) *DebugAPIImpl {
		backendServer := privateapi.NewEthBackendServer(ctx, mock, m.DB, m.Notifications, m.BlockReader, nil, logger, builder.NewLatestBlockBuiltStore(), nil)
		backendClient := direct.NewEthBackendClientDirect(backendServer)
		backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
		return NewPrivateDebugAPI(newBaseApiForTest(m), m.OverlayDB(), backend, 0, false)
	}

	// Rewinding one block below the current head is the simplest valid rewind.
	t.Run("set head one below current", func(t *testing.T) {
		mock := &mockEthBackend{}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(head-1))
		require.NoError(t, err)
		require.True(t, mock.setHeadCalled)
		require.Equal(t, head-1, mock.setHeadBlock)
	})

	// We accept setting head to the current block as a harmless no-op; the backend receives
	// the call and can short-circuit internally.
	t.Run("set head to current block is allowed", func(t *testing.T) {
		mock := &mockEthBackend{}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(head))
		require.NoError(t, err)
		require.True(t, mock.setHeadCalled)
		require.Equal(t, head, mock.setHeadBlock)
	})

	// Setting head one beyond the current canonical head must be rejected before reaching the backend.
	t.Run("block exactly one past head is rejected", func(t *testing.T) {
		mock := &mockEthBackend{}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(head+1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "in the future")
		require.False(t, mock.setHeadCalled, "backend must not be called for a future block")
	})

	// A far-future block number must be rejected.
	t.Run("far future block is rejected", func(t *testing.T) {
		mock := &mockEthBackend{}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(99999))
		require.Error(t, err)
		require.Contains(t, err.Error(), "in the future")
		require.False(t, mock.setHeadCalled, "backend must not be called for a future block")
	})

	// Backend errors must propagate unmodified to the caller.
	t.Run("backend error propagates", func(t *testing.T) {
		mock := &mockEthBackend{setHeadErr: fmt.Errorf("execution module is busy")}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(5))
		require.Error(t, err)
		require.Contains(t, err.Error(), "execution module is busy")
	})

	// Pruned blocks should be rejected by checkPruneHistory.
	// On a default test sentry there is no pruning configured, so we verify the
	// wiring by confirming that a low block passes validation.
	t.Run("low block passes when not pruned", func(t *testing.T) {
		mock := &mockEthBackend{}
		api := makeAPI(mock)
		err := api.SetHead(ctx, hexutil.Uint64(1))
		require.NoError(t, err)
		require.True(t, mock.setHeadCalled)
		require.Equal(t, uint64(1), mock.setHeadBlock)
	})
}

// TestSetHeadCanonicalCleanup verifies that ExecModule.SetHead properly cleans
// up canonical chain metadata after unwinding. Specifically it checks that:
//   - Canonical hashes above the target block are removed
//   - HeadHeaderHash is updated to match the target block
func TestSetHeadCanonicalCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := m.Ctx

	// Snapshot canonical state before the unwind.
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	head, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Greater(t, head, uint64(2), "test chain must have at least 3 blocks")

	targetBlock := head - 2

	// Record canonical hashes that should be removed after unwind.
	staleHashes := make(map[uint64]common.Hash)
	for i := targetBlock + 1; i <= head; i++ {
		h, err := rawdb.ReadCanonicalHash(roTx, i)
		require.NoError(t, err)
		require.NotEqual(t, common.Hash{}, h, "block %d must have a canonical hash before unwind", i)
		staleHashes[i] = h
	}

	// Record the target block hash (should survive the unwind).
	targetHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, targetHash)
	roTx.Rollback()

	// --- Perform the unwind ---
	err = m.ExecModule.SetHead(ctx, targetBlock)
	require.NoError(t, err)

	// --- Verify DB state after unwind ---
	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// 1) Canonical hashes above targetBlock must be gone.
	for blockNum := range staleHashes {
		h, err := rawdb.ReadCanonicalHash(roTx, blockNum)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, h,
			"canonical hash at block %d should be removed after SetHead(%d)", blockNum, targetBlock)
	}

	// 2) The target block's canonical hash must still be intact.
	h, err := rawdb.ReadCanonicalHash(roTx, targetBlock)
	require.NoError(t, err)
	require.Equal(t, targetHash, h, "canonical hash at target block must survive the unwind")

	// 3) HeadHeaderHash must point to the target block.
	headHeaderHash := rawdb.ReadHeadHeaderHash(roTx)
	require.Equal(t, targetHash, headHeaderHash,
		"HeadHeaderHash should equal the target block hash after SetHead")

	// 4) rpchelper.GetLatestBlockNumber must return the target block, not the stale
	//    forkchoice head that pointed to the old (unwound) chain tip.
	latestBlock, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, targetBlock, latestBlock,
		"GetLatestBlockNumber should return targetBlock after SetHead, not the stale forkchoice head")

	// Record the original head hash (before unwind) for the FCU below.
	originalHeadHash := staleHashes[head]
	roTx.Rollback()

	// 5) A subsequent UpdateForkChoice back to the original head must succeed,
	//    proving that SetHead left the DB in a consistent state.
	receipt, err := m.ExecModule.UpdateForkChoice(ctx, originalHeadHash, originalHeadHash, originalHeadHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, receipt.Status,
		"UpdateForkChoice back to original head should succeed after SetHead")

	// Verify the chain is back at the original head.
	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	restoredHead, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, head, restoredHead, "chain head should be restored after UpdateForkChoice")
}
