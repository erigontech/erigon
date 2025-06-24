package sequencer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/memdb"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/zk/txpool"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

// Mock transaction for testing
func createMockTransaction(nonce uint64, to *common.Address, data []byte) types.Transaction {
	if to == nil {
		// Contract creation
		tx := types.NewContractCreation(nonce, uint256.NewInt(1000), 21000, uint256.NewInt(2000000000), data)
		return tx
	}
	// Regular transaction
	tx := types.NewTransaction(nonce, *to, uint256.NewInt(1000), 21000, uint256.NewInt(2000000000), data)
	return tx
}

// Mock transaction bytes for testing
func createMockTransactionBytes(tx types.Transaction) []byte {
	var buf bytes.Buffer
	tx.MarshalBinary(&buf)
	return buf.Bytes()
}

func TestNewPoolTransactionYielder(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{
		EffectiveGasPriceForEthTransfer:        10,
		EffectiveGasPriceForErc20Transfer:      20,
		EffectiveGasPriceForContractInvocation: 30,
		EffectiveGasPriceForContractDeployment: 40,
	}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	assert.NotNil(t, yielder)
	assert.Equal(t, ctx, yielder.ctx)
	assert.Equal(t, cfg, yielder.cfg)
	assert.Equal(t, pool, yielder.pool)
	assert.Equal(t, uint16(10), yielder.yieldSize)
	assert.Equal(t, mockDB, yielder.db)
	assert.Equal(t, cache, yielder.decodedTxCache)
	assert.False(t, yielder.startedYielding)
	assert.Empty(t, yielder.readyTransactions)
	assert.Empty(t, yielder.readyTransactionBytes)
	assert.Empty(t, yielder.toSkip)
}

func TestPoolTransactionYielder_YieldNextTransaction_EmptyPool(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Test yielding when no transactions are available
	tx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.Nil(t, tx)
	assert.Equal(t, uint8(0), effectiveGas)
	assert.False(t, hasMore)
}

func TestPoolTransactionYielder_YieldNextTransaction_WithValidTransactions(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{
		EffectiveGasPriceForEthTransfer:        10,
		EffectiveGasPriceForErc20Transfer:      20,
		EffectiveGasPriceForContractInvocation: 30,
		EffectiveGasPriceForContractDeployment: 40,
	}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test transactions
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx1 := createMockTransaction(1, &to, []byte{})                                   // ETH transfer
	tx2 := createMockTransaction(2, &to, []byte{169, 5, 156, 187, 169, 5, 156, 187}) // ERC20 transfer

	tx1Bytes := createMockTransactionBytes(tx1)
	tx2Bytes := createMockTransactionBytes(tx2)

	// Manually populate the ready transactions
	yielder.readyMtx.Lock()
	yielder.readyTransactions = []common.Hash{tx1.Hash(), tx2.Hash()}
	yielder.readyTransactionBytes[tx1.Hash()] = tx1Bytes
	yielder.readyTransactionBytes[tx2.Hash()] = tx2Bytes
	yielder.readyMtx.Unlock()

	// Test yielding first transaction
	tx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, uint8(10), effectiveGas)
	assert.True(t, hasMore)

	// pretend we mined this first transaction
	yielder.AddMined(tx.Hash())

	// Test yielding second transaction
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, uint8(20), effectiveGas) // ERC20 transfer effective gas
	assert.True(t, hasMore)

	// pretend we mined this second transaction
	yielder.AddMined(tx.Hash())

	// Test yielding when no more transactions
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.Nil(t, tx)
	assert.Equal(t, uint8(0), effectiveGas)
	assert.False(t, hasMore)
}

func TestPoolTransactionYielder_YieldNextTransaction_WithCachedTransactions(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{
		EffectiveGasPriceForEthTransfer: 10,
	}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test transaction
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx := createMockTransaction(1, &to, []byte{})
	txBytes := createMockTransactionBytes(tx)

	// Add transaction to cache
	cache.Add(tx.Hash(), &tx)

	// Manually populate the ready transactions
	yielder.readyMtx.Lock()
	yielder.readyTransactions = []common.Hash{tx.Hash()}
	yielder.readyTransactionBytes[tx.Hash()] = txBytes
	yielder.readyMtx.Unlock()

	// Test yielding cached transaction
	resultTx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.NotNil(t, resultTx)
	assert.Equal(t, tx.Hash(), resultTx.Hash())
	assert.Equal(t, uint8(10), effectiveGas)
	assert.True(t, hasMore)
}

func TestPoolTransactionYielder_YieldNextTransaction_WithSkippedTransactions(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{
		EffectiveGasPriceForEthTransfer: 10,
	}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test transactions
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx1 := createMockTransaction(1, &to, []byte{})
	tx2 := createMockTransaction(2, &to, []byte{})

	tx1Bytes := createMockTransactionBytes(tx1)
	tx2Bytes := createMockTransactionBytes(tx2)

	// Manually populate the ready transactions
	yielder.readyMtx.Lock()
	yielder.readyTransactions = []common.Hash{tx1.Hash(), tx2.Hash()}
	yielder.readyTransactionBytes[tx1.Hash()] = tx1Bytes
	yielder.readyTransactionBytes[tx2.Hash()] = tx2Bytes
	yielder.toSkip[tx1.Hash()] = struct{}{} // Mark first transaction as skipped
	yielder.readyMtx.Unlock()

	// Test yielding - should skip tx1 and return tx2
	tx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, tx2.Hash(), tx.Hash()) // Should return second transaction
	assert.Equal(t, uint8(10), effectiveGas)
	assert.True(t, hasMore)
}

func TestPoolTransactionYielder_AddMined(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test transaction
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx := createMockTransaction(1, &to, []byte{})
	txHash := tx.Hash()

	// Manually populate the ready transactions
	yielder.readyMtx.Lock()
	yielder.readyTransactions = []common.Hash{txHash, common.HexToHash("0x1234")}
	yielder.readyTransactionBytes[txHash] = createMockTransactionBytes(tx)
	yielder.readyMtx.Unlock()

	// Add transaction as mined
	yielder.AddMined(txHash)

	// Verify transaction was removed from ready transactions
	yielder.readyMtx.Lock()
	assert.Len(t, yielder.readyTransactions, 1)
	assert.Equal(t, common.HexToHash("0x1234"), yielder.readyTransactions[0])
	_, isSkipped := yielder.toSkip[txHash]
	yielder.readyMtx.Unlock()

	assert.True(t, isSkipped)
}

func TestPoolTransactionYielder_RemoveMinedTransactions(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test transaction
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx := createMockTransaction(1, &to, []byte{})
	txHash := tx.Hash()

	// Add transaction to cache
	cache.Add(txHash, &tx)

	// Add some transactions to skip map
	yielder.readyMtx.Lock()
	yielder.toSkip[common.HexToHash("0x1234")] = struct{}{}
	yielder.toSkip[common.HexToHash("0x5678")] = struct{}{}
	yielder.readyMtx.Unlock()

	// Remove mined transactions
	yielder.RemoveMinedTransactions([]common.Hash{txHash})

	// Verify transaction was removed from cache
	_, found := cache.Get(txHash)
	assert.False(t, found)

	// Verify skip map was cleared
	yielder.readyMtx.Lock()
	assert.Empty(t, yielder.toSkip)
	yielder.readyMtx.Unlock()
}

func TestPoolTransactionYielder_SetExecutionDetails(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Set execution details
	executionAt := uint64(1000)
	forkId := uint64(5)
	yielder.SetExecutionDetails(executionAt, forkId)

	assert.Equal(t, executionAt, yielder.executionAt)
	assert.Equal(t, forkId, yielder.forkId)
}

func TestPoolTransactionYielder_BeginYielding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Test BeginYielding when not started
	assert.False(t, yielder.startedYielding)

	yielder.BeginYielding()

	// Give some time for the goroutine to start
	time.Sleep(10 * time.Millisecond)

	assert.True(t, yielder.startedYielding)

	// Test BeginYielding when already started
	yielder.BeginYielding()
	assert.True(t, yielder.startedYielding)
}

func TestPoolTransactionYielder_ExtractTransactionsFromSlot(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create test slot
	slot := &types2.TxsRlp{
		TxIds: []common.Hash{
			common.HexToHash("0x1234"),
			common.HexToHash("0x5678"),
		},
		Txs: [][]byte{
			[]byte{0x01, 0x02, 0x03},
			[]byte{0x04, 0x05, 0x06},
		},
	}

	// Extract transactions
	ids, txBytes, err := yielder.extractTransactionsFromSlot(slot)

	assert.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Len(t, txBytes, 2)
	assert.Equal(t, common.HexToHash("0x1234"), ids[0])
	assert.Equal(t, common.HexToHash("0x5678"), ids[1])
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, txBytes[0])
	assert.Equal(t, []byte{0x04, 0x05, 0x06}, txBytes[1])
}

func TestPoolTransactionYielder_ExtractTransactionsFromSlot_Empty(t *testing.T) {
	ctx := context.Background()
	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Create empty slot
	slot := &types2.TxsRlp{
		TxIds: []common.Hash{},
		Txs:   [][]byte{},
	}

	// Extract transactions
	ids, txBytes, err := yielder.extractTransactionsFromSlot(slot)

	assert.NoError(t, err)
	assert.Empty(t, ids)
	assert.Empty(t, txBytes)
}

func TestLimboTransactionYielder(t *testing.T) {
	cfg := ethconfig.Zk{
		EffectiveGasPriceForEthTransfer: 10,
	}

	// Create test transactions
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx1 := createMockTransaction(1, &to, []byte{})
	tx2 := createMockTransaction(2, &to, []byte{})

	transactions := []types.Transaction{tx1, tx2}

	yielder := NewLimboTransactionYielder(transactions, cfg)

	// Test yielding first transaction
	tx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, tx1.Hash(), tx.Hash())
	assert.Equal(t, uint8(10), effectiveGas)
	assert.True(t, hasMore)

	// Test yielding second transaction
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, tx2.Hash(), tx.Hash())
	assert.Equal(t, uint8(10), effectiveGas)
	assert.True(t, hasMore)

	// Test yielding when no more transactions
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.Nil(t, tx)
	assert.Equal(t, uint8(0), effectiveGas)
	assert.False(t, hasMore)
}

func TestRecoveryTransactionYielder(t *testing.T) {
	// Create test transactions
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx1 := createMockTransaction(1, &to, []byte{})
	tx2 := createMockTransaction(2, &to, []byte{})

	transactions := []types.Transaction{tx1, tx2}
	effectivePercentages := []uint8{15, 25}

	yielder, err := NewRecoveryTransactionYielder(transactions, effectivePercentages)
	assert.NoError(t, err)

	// Test yielding first transaction
	tx, effectiveGas, hasMore := yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, tx1.Hash(), tx.Hash())
	assert.Equal(t, uint8(15), effectiveGas)
	assert.True(t, hasMore)

	// Test yielding second transaction
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.NotNil(t, tx)
	assert.Equal(t, tx2.Hash(), tx.Hash())
	assert.Equal(t, uint8(25), effectiveGas)
	assert.True(t, hasMore)

	// Test yielding when no more transactions
	tx, effectiveGas, hasMore = yielder.YieldNextTransaction()

	assert.Nil(t, tx)
	assert.Equal(t, uint8(0), effectiveGas)
	assert.False(t, hasMore)
}

func TestPoolTransactionYielder_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := ethconfig.Zk{}

	mockDB := memdb.NewTestDB(t)
	cache := expirable.NewLRU[common.Hash, *types.Transaction](100, nil, time.Hour)

	// Use nil TxPool for testing - we're not testing pool functionality
	var pool *txpool.TxPool = nil

	yielder := NewPoolTransactionYielder(ctx, cfg, pool, 10, mockDB, cache)

	// Start yielding
	yielder.BeginYielding()

	// Give some time for the goroutine to start
	time.Sleep(10 * time.Millisecond)

	// Cancel context
	cancel()

	// Give some time for the goroutine to stop
	time.Sleep(50 * time.Millisecond)

	// Verify the yielder stopped
	assert.False(t, yielder.startedYielding)
}
