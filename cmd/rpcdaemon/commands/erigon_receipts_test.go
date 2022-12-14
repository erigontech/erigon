package commands

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErigonGetLatestLogs(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	db := m.DB
	agg := m.HistoryV3Components()
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), db, nil)
	expectedLogs, _ := api.GetLogs(context.Background(), filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})

	expectedErigonLogs := make([]*types.ErigonLog, 0)
	for i := len(expectedLogs) - 1; i >= 0; i-- {
		expectedErigonLogs = append(expectedErigonLogs, &types.ErigonLog{
			Address:     expectedLogs[i].Address,
			Topics:      expectedLogs[i].Topics,
			Data:        expectedLogs[i].Data,
			BlockNumber: expectedLogs[i].BlockNumber,
			TxHash:      expectedLogs[i].TxHash,
			TxIndex:     expectedLogs[i].TxIndex,
			BlockHash:   expectedLogs[i].BlockHash,
			Index:       expectedLogs[i].Index,
			Removed:     expectedLogs[i].Removed,
			Timestamp:   expectedLogs[i].Timestamp,
		})
	}
	actual, err := api.GetLatestLogs(context.Background(), filters.FilterCriteria{}, filters.LogFilterOptions{
		LogCount: uint64((len(expectedLogs))),
	})
	if err != nil {
		t.Errorf("calling erigon_getLatestLogs: %v", err)
	}
	require.NotNil(t, actual)
	assert.EqualValues(expectedErigonLogs, actual)
}

func TestErigonGetLatestLogsIgnoreTopics(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	db := m.DB
	agg := m.HistoryV3Components()
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), db, nil)
	expectedLogs, _ := api.GetLogs(context.Background(), filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})

	expectedErigonLogs := make([]*types.ErigonLog, 0)
	for i := len(expectedLogs) - 1; i >= 0; i-- {
		expectedErigonLogs = append(expectedErigonLogs, &types.ErigonLog{
			Address:     expectedLogs[i].Address,
			Topics:      expectedLogs[i].Topics,
			Data:        expectedLogs[i].Data,
			BlockNumber: expectedLogs[i].BlockNumber,
			TxHash:      expectedLogs[i].TxHash,
			TxIndex:     expectedLogs[i].TxIndex,
			BlockHash:   expectedLogs[i].BlockHash,
			Index:       expectedLogs[i].Index,
			Removed:     expectedLogs[i].Removed,
			Timestamp:   expectedLogs[i].Timestamp,
		})
	}

	var lastBlock uint64
	var blockCount uint64
	containsTopics := make([][]common.Hash, 0)

	for i := range expectedLogs {
		if expectedLogs[i].BlockNumber != lastBlock {
			blockCount++
		}
		containsTopics = append(containsTopics, []common.Hash{
			expectedLogs[i].Topics[0],
		})
	}
	actual, err := api.GetLatestLogs(context.Background(), filters.FilterCriteria{Topics: containsTopics}, filters.LogFilterOptions{
		BlockCount: blockCount,
	})
	if err != nil {
		t.Errorf("calling erigon_getLatestLogs: %v", err)
	}
	require.NotNil(t, actual)
	assert.EqualValues(expectedErigonLogs, actual)
}

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

func TestGetBlockReceiptsByBlockHash(t *testing.T) {
	// Define three accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	signer := types.LatestSignerForChainID(nil)
	// Create a chain generator with some simple transactions (blatantly stolen from @fjl/chain_markets_test)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, the test bank sends account #1 some ether.
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, testKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, testKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, acc1Key)
			block.AddTx(tx1)
			block.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by account #2.
			block.SetCoinbase(acc2Addr)
			block.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := block.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			block.AddUncle(b2)
			b3 := block.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			block.AddUncle(b3)
		}
	}
	// Assemble the test environment
	m := mockWithGenerator(t, 4, generator)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, nil)

	err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		for i := uint64(0); i <= rawdb.ReadCurrentHeader(tx).Number.Uint64(); i++ {
			block, err := rawdb.ReadBlockByNumber(tx, i)
			if err != nil {
				return err
			}

			r, err := rawdb.ReadReceiptsByHash(tx, block.Hash())
			if err != nil {
				return err
			}

			marshaledReceipts := make([]map[string]interface{}, 0, len(r))
			for _, receipt := range r {
				txn := block.Transactions()[receipt.TransactionIndex]
				marshaledReceipts = append(marshaledReceipts, marshalReceipt(receipt, txn, m.ChainConfig, block, txn.Hash(), true))
			}

			receiptsFromBlock, err := api.GetBlockReceiptsByBlockHash(context.Background(), block.Hash())
			if err != nil {
				return err
			}

			assert.EqualValues(t, marshaledReceipts, receiptsFromBlock)
		}
		return nil
	})

	require.NoError(t, err)
}

// newTestBackend creates a chain with a number of explicitly defined blocks and
// wraps it into a mock backend.
func mockWithGenerator(t *testing.T, blocks int, generator func(int, *core.BlockGen)) *stages.MockSentry {
	m := stages.MockWithGenesis(t, &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}, testKey, false)
	if blocks > 0 {
		chain, _ := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, blocks, generator, true)
		err := m.InsertChain(chain)
		require.NoError(t, err)
	}
	return m
}
