package stages

import (
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	dsTypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
)

type mockChecker struct {
	data map[uint64]zktypes.L1InfoTreeUpdate
}

func (m *mockChecker) GetL1InfoTreeUpdate(idx uint64) (*zktypes.L1InfoTreeUpdate, error) {
	if val, ok := m.data[idx]; ok {
		return &val, nil
	}
	return nil, nil
}

func Test_CheckForBadBatch(t *testing.T) {
	tests := map[string]struct {
		data                map[uint64]zktypes.L1InfoTreeUpdate
		blocks              []zktx.DecodedBatchL2Data
		latestTimestamp     uint64
		timestampLimit      uint64
		highestAllowedIndex uint64
		expectedBadBatch    bool
	}{
		"no_issues": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 1000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    false,
		},
		"missing_l1_info_tree_update": {
			latestTimestamp: 500,
			data:            map[uint64]zktypes.L1InfoTreeUpdate{},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
		"l1_info_update_in_the_future": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
		"timestamp_exceeds_timestamp_limit": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      100,
			expectedBadBatch:    true,
		},
		"index_in_the_future": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  9000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 0,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &mockChecker{
				data: tt.data,
			}
			bad, err := checkForBadBatch(1, m, tt.latestTimestamp, tt.highestAllowedIndex, tt.timestampLimit, tt.blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if bad != tt.expectedBadBatch {
				t.Fatalf("expected bad batch: %v, got: %v", tt.expectedBadBatch, bad)
			}
		})
	}
}

type mockForkDb struct {
	allForks   []uint64
	allBatches []uint64
	batchForks map[uint64]uint64
}

func (m mockForkDb) GetAllForkHistory() ([]uint64, []uint64, error) {
	return m.allForks, m.allBatches, nil
}

func (m mockForkDb) GetLatestForkHistory() (uint64, uint64, error) {
	return m.allForks[len(m.allForks)-1], m.allBatches[len(m.allBatches)-1], nil

}

func (m mockForkDb) GetForkId(batch uint64) (uint64, error) {
	return m.batchForks[batch], nil
}

func (m mockForkDb) WriteForkIdBlockOnce(forkId, block uint64) error {
	return nil
}

func (m mockForkDb) WriteForkId(batch, forkId uint64) error {
	return nil
}

func Test_PrepareForkId_DuringRecovery(t *testing.T) {
	tests := map[string]struct {
		lastBatch  uint64
		allForks   []uint64
		allBatches []uint64
		batchForks map[uint64]uint64
		expected   uint64
	}{
		"simple case": {
			lastBatch:  2,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{1, 2, 3},
			batchForks: map[uint64]uint64{1: 1},
			expected:   2,
		},
		"with gaps": {
			lastBatch:  22,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{10, 20, 30},
			batchForks: map[uint64]uint64{1: 1},
			expected:   2,
		},
		"early in restore": {
			lastBatch:  2,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{1, 20, 30},
			batchForks: map[uint64]uint64{1: 1},
			expected:   1,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mock := mockForkDb{
				allForks:   test.allForks,
				allBatches: test.allBatches,
				batchForks: test.batchForks,
			}

			forkId, err := prepareForkId(test.lastBatch, 1, mock)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if forkId != test.expected {
				t.Errorf("expected: %v, got: %v", test.expected, forkId)
			}
		})
	}
}

// Mock implementation of zktx.DecodeTx for testing purposes
func mockDecodeTx(encoded []byte, effectiveGasPricePercentage byte, forkId uint64) (types.Transaction, uint8, error) {
	return types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), encoded), 0, nil
}

func TestResequenceBatchJob_HasMoreToProcess(t *testing.T) {
	tests := []struct {
		name     string
		job      ResequenceBatchJob
		expected bool
	}{
		{
			name: "Has more blocks to process",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{}, {}},
				StartBlockIndex: 1,
				StartTxIndex:    0,
			},
			expected: true,
		},
		{
			name: "Has more transactions to process",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{L2Txs: []dsTypes.L2TransactionProto{{}, {}}}},
				StartBlockIndex: 0,
				StartTxIndex:    0,
			},
			expected: true,
		},
		{
			name: "No more to process",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{}},
				StartBlockIndex: 1,
				StartTxIndex:    0,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.job.HasMoreBlockToProcess(); got != tt.expected {
				t.Errorf("ResequenceBatchJob.HasMoreBlockToProcess() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestResequenceBatchJob_CurrentBlock(t *testing.T) {
	tests := []struct {
		name     string
		job      ResequenceBatchJob
		expected *dsTypes.FullL2Block
	}{
		{
			name: "Has current block",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{L2BlockNumber: 1}, {L2BlockNumber: 2}},
				StartBlockIndex: 0,
				StartTxIndex:    0,
			},
			expected: &dsTypes.FullL2Block{L2BlockNumber: 1},
		},
		{
			name: "No current block",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{L2BlockNumber: 1}},
				StartBlockIndex: 1,
				StartTxIndex:    0,
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.job.CurrentBlock()
			if (got == nil && tt.expected != nil) || (got != nil && tt.expected == nil) {
				t.Errorf("ResequenceBatchJob.CurrentBlock() = %v, want %v", got, tt.expected)
			}
			if got != nil && tt.expected != nil && got.L2BlockNumber != tt.expected.L2BlockNumber {
				t.Errorf("ResequenceBatchJob.CurrentBlock().L2BlockNumber = %v, want %v", got.L2BlockNumber, tt.expected.L2BlockNumber)
			}
		})
	}
}

func TestResequenceBatchJob_YieldNextBlockTransactions(t *testing.T) {
	// Replace the actual zktx.DecodeTx with our mock function for testing

	tests := []struct {
		name            string
		job             ResequenceBatchJob
		expectedTxCount int
		expectedError   bool
	}{
		{
			name: "Yield transactions",
			job: ResequenceBatchJob{
				batchToProcess: []*dsTypes.FullL2Block{
					{
						L2Txs:  []dsTypes.L2TransactionProto{{}, {}},
						ForkId: 1,
					},
				},
				StartBlockIndex: 0,
				StartTxIndex:    0,
				txIndexMap:      make(map[common.Hash]txMatadata),
			},
			expectedTxCount: 2,
			expectedError:   false,
		},
		{
			name: "No transactions to yield",
			job: ResequenceBatchJob{
				batchToProcess:  []*dsTypes.FullL2Block{{}},
				StartBlockIndex: 1,
				StartTxIndex:    0,
				txIndexMap:      make(map[common.Hash]txMatadata),
			},
			expectedTxCount: 0,
			expectedError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txs, err := tt.job.YieldNextBlockTransactions(mockDecodeTx)
			if (err != nil) != tt.expectedError {
				t.Errorf("ResequenceBatchJob.YieldNextBlockTransactions() error = %v, expectedError %v", err, tt.expectedError)
				return
			}
			if len(txs) != tt.expectedTxCount {
				t.Errorf("ResequenceBatchJob.YieldNextBlockTransactions() returned %d transactions, expected %d", len(txs), tt.expectedTxCount)
			}
		})
	}
}

func TestResequenceBatchJob_YieldAndUpdate(t *testing.T) {
	// Setup the batch
	batch := []*dsTypes.FullL2Block{
		{L2Txs: []dsTypes.L2TransactionProto{{Encoded: []byte("1")}, {Encoded: []byte("2")}}, L2Blockhash: common.HexToHash("0")},
		{L2Txs: []dsTypes.L2TransactionProto{}, L2Blockhash: common.HexToHash("1")},
		{L2Txs: []dsTypes.L2TransactionProto{}, L2Blockhash: common.HexToHash("2")},
		{L2Txs: []dsTypes.L2TransactionProto{{Encoded: []byte("3")}, {Encoded: []byte("4")}}, L2Blockhash: common.HexToHash("3")},
	}

	job := ResequenceBatchJob{
		batchToProcess:  batch,
		StartBlockIndex: 0,
		StartTxIndex:    1, // Start at block 0, index 1
		txIndexMap:      make(map[common.Hash]txMatadata),
	}

	processTransactions := func(txs []types.Transaction) {
		for _, tx := range txs {
			job.UpdateLastProcessedTx(tx.Hash())
		}
	}

	// First call - should yield transaction 2 from block 0
	txs, err := job.YieldNextBlockTransactions(mockDecodeTx)
	if err != nil {
		t.Fatalf("First call: Unexpected error: %v", err)
	}
	if len(txs) != 1 || string(txs[0].GetData()) != "2" {
		t.Errorf("Expected 1 transaction with data '2', got %d transactions with data '%s'", len(txs), string(txs[0].GetData()))
	}
	processTransactions(txs)
	tx2 := txs[0]

	// Second call - should yield empty block (block 1)
	txs, err = job.YieldNextBlockTransactions(mockDecodeTx)
	if err != nil {
		t.Fatalf("Second call: Unexpected error: %v", err)
	}
	if len(txs) != 0 {
		t.Errorf("Expected 0 transactions, got %d", len(txs))
	}
	job.UpdateLastProcessedTx(job.CurrentBlock().L2Blockhash)

	// Third call - should yield empty block (block 2)
	txs, err = job.YieldNextBlockTransactions(mockDecodeTx)
	if err != nil {
		t.Fatalf("Third call: Unexpected error: %v", err)
	}
	if len(txs) != 0 {
		t.Errorf("Expected 0 transactions, got %d", len(txs))
	}
	job.UpdateLastProcessedTx(job.CurrentBlock().L2Blockhash)

	// Fourth call - should yield transactions 3 and 4, but we'll only process 3
	txs, err = job.YieldNextBlockTransactions(mockDecodeTx)
	if err != nil {
		t.Fatalf("Fourth call: Unexpected error: %v", err)
	}
	if len(txs) != 2 || string(txs[0].GetData()) != "3" || string(txs[1].GetData()) != "4" {
		t.Errorf("Expected 2 transactions with data '3' and '4', got %d transactions", len(txs))
	}
	processTransactions(txs[:1]) // Only process the first transaction (3)
	tx3 := txs[0]
	tx4 := txs[1]

	// Check final state
	if job.StartBlockIndex != 3 {
		t.Errorf("Expected StartBlockIndex to be 3, got %d", job.StartBlockIndex)
	}

	if job.StartTxIndex != 1 {
		t.Errorf("Expected StartTxIndex to be 1, got %d", job.StartTxIndex)
	}

	// Final call - should yield transaction 4
	txs, err = job.YieldNextBlockTransactions(mockDecodeTx)
	if err != nil {
		t.Fatalf("Final call: Unexpected error: %v", err)
	}
	if len(txs) != 1 || string(txs[0].GetData()) != "4" {
		t.Errorf("Expected 1 transaction with data '4', got %d transactions", len(txs))
	}

	processTransactions(txs)

	if job.HasMoreBlockToProcess() {
		t.Errorf("Expected no more blocks to process")
	}

	// Verify txIndexMap
	expectedTxIndexMap := map[common.Hash]txMatadata{
		common.HexToHash("0"): {0, 0},
		common.HexToHash("1"): {1, 0},
		common.HexToHash("2"): {2, 0},
		common.HexToHash("3"): {3, 0},
		tx2.Hash():            {0, 1}, // Transaction 2
		tx3.Hash():            {3, 0}, // Transaction 3
		tx4.Hash():            {3, 1}, // Transaction 4
	}

	for hash, index := range expectedTxIndexMap {
		if actualIndex, exists := job.txIndexMap[hash]; !exists {
			t.Errorf("Expected hash %s to exist in txIndexMap", hash.Hex())
		} else if !reflect.DeepEqual(actualIndex, index) {
			t.Errorf("For hash %s, expected index %v, got %v", hash.Hex(), index, actualIndex)
		}
	}
}
