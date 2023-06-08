package commands

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

var dumper = spew.ConfigState{Indent: "    "}

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

func TestTraceBlockByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br, _ := m.NewBlocksIO()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	ethApi := NewEthAPI(baseApi, m.DB, nil, nil, nil, 5000000, 100_000, log.New())
	api := NewPrivateDebugAPI(baseApi, m.DB, 0)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		tx, err := ethApi.GetTransactionByHash(m.Ctx, common.HexToHash(tt.txHash))
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		txcount, err := ethApi.GetBlockTransactionCountByHash(m.Ctx, *tx.BlockHash)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		err = api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(tx.BlockNumber.ToInt().Uint64()), &tracers.TraceConfig{}, stream)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		if err = stream.Flush(); err != nil {
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
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	err := api.TraceBlockByNumber(m.Ctx, rpc.LatestBlockNumber, &tracers.TraceConfig{}, stream)
	if err != nil {
		t.Errorf("traceBlock %v: %v", rpc.LatestBlockNumber, err)
	}
	if err = stream.Flush(); err != nil {
		t.Fatalf("error flusing: %v", err)
	}
	var er []ethapi.ExecutionResult
	if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
}

func TestTraceBlockByHash(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br, _ := m.NewBlocksIO()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	ethApi := NewEthAPI(baseApi, m.DB, nil, nil, nil, 5000000, 100_000, log.New())
	api := NewPrivateDebugAPI(baseApi, m.DB, 0)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		tx, err := ethApi.GetTransactionByHash(m.Ctx, common.HexToHash(tt.txHash))
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		txcount, err := ethApi.GetBlockTransactionCountByHash(m.Ctx, *tx.BlockHash)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		err = api.TraceBlockByHash(m.Ctx, *tx.BlockHash, &tracers.TraceConfig{}, stream)
		if err != nil {
			t.Errorf("traceBlock %s: %v", tt.txHash, err)
		}
		if err = stream.Flush(); err != nil {
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
	agg := m.HistoryV3Components()
	br, _ := m.NewBlocksIO()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	api := NewPrivateDebugAPI(base, m.DB, 0)
	for _, tt := range debugTraceTransactionTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		err := api.TraceTransaction(m.Ctx, common.HexToHash(tt.txHash), &tracers.TraceConfig{}, stream)
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br, _ := m.NewBlocksIO()
	agg := m.HistoryV3Components()
	api := NewPrivateDebugAPI(
		NewBaseApi(nil, kvcache.New(kvcache.DefaultCoherentConfig), br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs),
		m.DB, 0)
	for _, tt := range debugTraceTransactionNoRefundTests {
		var buf bytes.Buffer
		stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
		var norefunds = true
		err := api.TraceTransaction(m.Ctx, common.HexToHash(tt.txHash), &tracers.TraceConfig{NoRefunds: &norefunds}, stream)
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

func TestStorageRangeAt(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br, _ := m.NewBlocksIO()
	agg := m.HistoryV3Components()
	api := NewPrivateDebugAPI(
		NewBaseApi(nil, kvcache.New(kvcache.DefaultCoherentConfig), br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs),
		m.DB, 0)
	t.Run("invalid addr", func(t *testing.T) {
		var block4 *types.Block
		var err error
		err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
			block4, err = br.BlockByNumber(m.Ctx, tx, 4)
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
			block4, _ = br.BlockByNumber(m.Ctx, tx, 4)
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
			latestBlock, err = br.CurrentBlock(tx)
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
	br, _ := m.NewBlocksIO()
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	api := NewPrivateDebugAPI(base, m.DB, 0)

	t.Run("valid account", func(t *testing.T) {
		addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf55")
		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Accounts))

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true)
		require.NoError(t, err)
		require.Equal(t, 3, len(result.Accounts))
	})
	t.Run("valid contract", func(t *testing.T) {
		addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Accounts))

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Accounts))

		n = rpc.BlockNumber(10)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 10, true, true)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Accounts))
	})
	t.Run("with storage", func(t *testing.T) {
		addr := common.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")

		n := rpc.BlockNumber(1)
		result, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false)
		require.NoError(t, err)
		require.Equal(t, 0, len(result.Accounts))

		n = rpc.BlockNumber(7)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false)
		require.NoError(t, err)
		require.Equal(t, 0, len(result.Accounts[addr].Storage))

		n = rpc.BlockNumber(10)
		result, err = api.AccountRange(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, addr[:], 1, false, false)
		require.NoError(t, err)
		require.Equal(t, 35, len(result.Accounts[addr].Storage))
		require.Equal(t, 1, int(result.Accounts[addr].Nonce))
		for _, v := range result.Accounts {
			hashedCode, _ := common2.HashData(v.Code)
			require.Equal(t, v.CodeHash.String(), hashedCode.String())
		}
	})
}

func TestGetModifiedAccountsByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br, _ := m.NewBlocksIO()
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	api := NewPrivateDebugAPI(base, m.DB, 0)

	t.Run("correct input", func(t *testing.T) {
		n, n2 := rpc.BlockNumber(1), rpc.BlockNumber(2)
		result, err := api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Equal(t, 3, len(result))

		n, n2 = rpc.BlockNumber(5), rpc.BlockNumber(7)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Equal(t, 38, len(result))

		n, n2 = rpc.BlockNumber(0), rpc.BlockNumber(9)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.NoError(t, err)
		require.Equal(t, 40, len(result))

		//nil value means: to = from + 1
		n = rpc.BlockNumber(0)
		result, err = api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(result))
	})
	t.Run("invalid input", func(t *testing.T) {
		n, n2 := rpc.BlockNumber(0), rpc.BlockNumber(11)
		_, err := api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.Error(t, err)

		n, n2 = rpc.BlockNumber(0), rpc.BlockNumber(1_000_000)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, n, &n2)
		require.Error(t, err)

		n = rpc.BlockNumber(0)
		result, err := api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(result))

		n = rpc.BlockNumber(1_000_000)
		_, err = api.GetModifiedAccountsByNumber(m.Ctx, n, nil)
		require.Error(t, err)
	})
}

func TestMapTxNum2BlockNum(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	if !m.HistoryV3 {
		t.Skip()
	}

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	checkIter := func(t *testing.T, expectTxNums iter.U64, txNumsIter *MapTxNum2BlockNumIter) {
		for expectTxNums.HasNext() {
			require.True(t, txNumsIter.HasNext())
			expectTxNum, _ := expectTxNums.Next()
			txNum, _, _, _, _, _ := txNumsIter.Next()
			require.Equal(t, expectTxNum, txNum)
		}
	}
	t.Run("descend", func(t *testing.T) {
		dbtx, err := m.DB.BeginRo(m.Ctx)
		require.NoError(t, err)
		defer dbtx.Rollback()
		tx := dbtx.(kv.TemporalTx)

		txNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 1024, -1, order.Desc, kv.Unlim)
		require.NoError(t, err)
		txNumsIter := MapDescendTxNum2BlockNum(tx, txNums)
		expectTxNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 1024, -1, order.Desc, kv.Unlim)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
	t.Run("ascend", func(t *testing.T) {
		dbtx, err := m.DB.BeginRo(m.Ctx)
		require.NoError(t, err)
		defer dbtx.Rollback()
		tx := dbtx.(kv.TemporalTx)

		txNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 0, 1024, order.Asc, kv.Unlim)
		require.NoError(t, err)
		txNumsIter := MapDescendTxNum2BlockNum(tx, txNums)
		expectTxNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 0, 1024, order.Asc, kv.Unlim)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
	t.Run("ascend limit", func(t *testing.T) {
		dbtx, err := m.DB.BeginRo(m.Ctx)
		require.NoError(t, err)
		defer dbtx.Rollback()
		tx := dbtx.(kv.TemporalTx)

		txNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 0, 1024, order.Asc, 2)
		require.NoError(t, err)
		txNumsIter := MapDescendTxNum2BlockNum(tx, txNums)
		expectTxNums, err := tx.IndexRange(temporal.LogAddrIdx, addr[:], 0, 1024, order.Asc, 2)
		require.NoError(t, err)
		checkIter(t, expectTxNums, txNumsIter)
	})
}

func TestAccountAt(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br, _ := m.NewBlocksIO()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
	api := NewPrivateDebugAPI(base, m.DB, 0)

	var blockHash0, blockHash1, blockHash3, blockHash10, blockHash12 common.Hash
	_ = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		blockHash0, _ = br.CanonicalHash(m.Ctx, tx, 0)
		blockHash1, _ = br.CanonicalHash(m.Ctx, tx, 1)
		blockHash3, _ = br.CanonicalHash(m.Ctx, tx, 3)
		blockHash10, _ = br.CanonicalHash(m.Ctx, tx, 10)
		blockHash12, _ = br.CanonicalHash(m.Ctx, tx, 12)
		_, _, _, _, _ = blockHash0, blockHash1, blockHash3, blockHash10, blockHash12
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

		//only 11 blocks in chain
		results, err = api.AccountAt(m.Ctx, blockHash12, 0, addr)
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
		require.Equal(39, int(results.Nonce))
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.AccountAt(m.Ctx, blockHash10, 0, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Equal(0, int(results.Nonce))
	})
}
