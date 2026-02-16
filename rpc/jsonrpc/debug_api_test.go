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
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0)
	ethApi := newEthApiForTest(baseApi, m.DB, nil, nil)
	api := NewPrivateDebugAPI(baseApi, m.DB, 0)
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ethApi := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)
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

func TestTraceTransactionNoRefund(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)
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

		// limited
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, nil, 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[0]: storage[keys[0]], keys[2]: storage[keys[2]]}, &keys[5]}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}

		// start from something, limited
		result, err = api.StorageRangeAt(m.Ctx, latestBlock.Hash(), 0, addr, expect.NextKey.Bytes(), 2)
		require.NoError(t, err)
		expect = StorageRangeResult{storageMap{keys[4]: storage[keys[4]], keys[6]: storage[keys[6]]}, nil}
		if !reflect.DeepEqual(result, expect) {
			t.Fatalf("wrong result:\ngot %s\nwant %s", dumper.Sdump(result), dumper.Sdump(&expect))
		}
	})

}

func TestAccountRange(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)

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
}

func TestGetModifiedAccountsByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)

	t.Run("correct input", func(t *testing.T) {
		n, n2 := rpc.BlockNumber(1), rpc.BlockNumber(2)
		result, err := api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 3)

		n, n2 = rpc.BlockNumber(5), rpc.BlockNumber(8)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 37)

		n, n2 = rpc.BlockNumber(0), rpc.BlockNumber(10)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Len(t, result, 40)

		//nil value means: to = from + 1
		n = rpc.BlockNumber(0)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.NoError(t, err)
		require.Len(t, result, 3)

		// latest block is 13, should work both ways: [11,12) and [11,nil)
		n2 = rpc.BlockNumber(12)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, rpc.BlockNumber(11), &n2)
		require.NoError(t, err)
		require.Len(t, result, 3)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, rpc.BlockNumber(11), nil)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})
	t.Run("invalid input", func(t *testing.T) {
		n := rpc.BlockNumber(1_000_000)
		_, err := api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.Error(t, err)

		n = rpc.BlockNumber(11)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n)
		require.Error(t, err)
	})
}

func TestMapTxNum2BlockNum(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)

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
		require.Equal(40, int(results.Nonce))
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.AccountAt(m.Ctx, blockHash10, 0, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Equal(0, int(results.Nonce))
	})
}

func TestGetBadBlocks(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 5000000)
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

		header := &types.Header{Number: big.NewInt(int64(number))}
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

	data, err := api.GetBadBlocks(ctx)
	require.NoError(err)

	require.Len(data, 4)
	require.Equal(data[0]["hash"], hash4)
	require.Equal(data[1]["hash"], hash3)
	require.Equal(data[2]["hash"], hash2)
	require.Equal(data[3]["hash"], hash1)
}

func TestGetRawTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 5000000)
	ctx := context.Background()

	require := require.New(t)
	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read transaction: %s", err)
	}
	number := *rawdb.ReadCurrentBlockNumber(tx)
	tx.Commit()

	if number < 1 {
		t.Error("TestSentry doesn't have enough blocks for this test")
	}
	var testedOnce = false
	for i := uint64(0); i < number; i++ {
		tx, err := m.DB.BeginRo(ctx)
		require.NoError(err)
		block, err := api._blockReader.BlockByNumber(ctx, tx, i)
		require.NoError(err)
		tx.Rollback()
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

func TestExecutionWitness(t *testing.T) {
	// Enable historical commitment to allow witness generation for historical blocks
	statecfg.EnableHistoricalCommitment()

	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, 0)
	ctx := context.Background()

	// Get the latest block number
	var latestBlockNum uint64
	err := m.DB.View(ctx, func(tx kv.Tx) error {
		latestBlockNum, _ = stages.GetStageProgress(tx, stages.Execution)
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, latestBlockNum, uint64(0), "test chain should have at least one block")
	t.Run("genesis block", func(t *testing.T) {
		blockNum := rpc.BlockNumber(0)
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})

		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
		require.NotNil(t, result.Keys, "Keys should not be nil")
		require.NotNil(t, result.Codes, "Codes should not be nil")

		t.Logf("Genesis: %d state nodes, %d codes, %d keys",
			len(result.State), len(result.Codes), len(result.Keys))
	})

	t.Run("by block number", func(t *testing.T) {
		// Test with block number 1
		blockNum := rpc.BlockNumber(1)
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})

		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
		require.NotNil(t, result.Keys, "Keys should not be nil")

		t.Logf("Block 1: %d state nodes, %d codes, %d keys, %d headers",
			len(result.State), len(result.Codes), len(result.Keys), len(result.Headers))
	})

	t.Run("by block hash", func(t *testing.T) {
		var blockHash common.Hash
		err := m.DB.View(ctx, func(tx kv.Tx) error {
			blockHash, _, _ = m.BlockReader.CanonicalHash(ctx, tx, 1)
			return nil
		})
		require.NoError(t, err)

		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockHash: &blockHash})

		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Block hash %s: %d state nodes, %d codes, %d keys",
			blockHash.Hex()[:10], len(result.State), len(result.Codes), len(result.Keys))
	})

	t.Run("multiple blocks", func(t *testing.T) {
		for blockNum := uint64(13); blockNum <= latestBlockNum; blockNum++ {
			bn := rpc.BlockNumber(blockNum)
			result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn})

			require.NoError(t, err, "ExecutionWitness failed for block %d", blockNum)
			require.NotNil(t, result, "Result should not be nil for block %d", blockNum)
			require.NotNil(t, result.State, "State should not be nil for block %d", blockNum)

			t.Logf("Block %d: %d state nodes, %d codes, %d keys",
				blockNum, len(result.State), len(result.Codes), len(result.Keys))
		}
	})

	t.Run("latest block", func(t *testing.T) {
		blockNum := rpc.LatestBlockNumber
		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.State, "State should not be nil")
		require.NotNil(t, result.Keys, "Keys should not be nil")
		t.Logf("Latest block: %d state nodes, %d codes, %d keys",
			len(result.State), len(result.Codes), len(result.Keys))
	})

	t.Run("non-existent block", func(t *testing.T) {
		// Very high block number that doesn't exist
		blockNum := rpc.BlockNumber(999999999)
		_, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})
		require.Error(t, err, "should error for non-existent block")
	})
}
