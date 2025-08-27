package l1infotree

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/memdb"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock implementations
type MockSyncer struct {
	mock.Mock
}

func (m *MockSyncer) IsSyncStarted() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockSyncer) RunQueryBlocks(lastCheckedBlock uint64) {
	m.Called(lastCheckedBlock)
}

func (m *MockSyncer) GetLogsChan() <-chan []types.Log {
	args := m.Called()
	return args.Get(0).(<-chan []types.Log)
}

func (m *MockSyncer) GetProgressMessageChan() <-chan string {
	args := m.Called()
	return args.Get(0).(<-chan string)
}

func (m *MockSyncer) IsDownloading() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockSyncer) GetHeader(blockNumber uint64) (*types.Header, error) {
	args := m.Called(blockNumber)
	return args.Get(0).(*types.Header), args.Error(1)
}

func (m *MockSyncer) L1QueryHeaders(logs []types.Log) (map[uint64]*types.Header, error) {
	args := m.Called(logs)
	return args.Get(0).(map[uint64]*types.Header), args.Error(1)
}

func (m *MockSyncer) StopQueryBlocks() {
	m.Called()
}

func (m *MockSyncer) ConsumeQueryBlocks() {
	m.Called()
}

func (m *MockSyncer) WaitQueryBlocksToFinish() {
	m.Called()
}

func (m *MockSyncer) QueryForRootLog(to uint64) (*types.Log, error) {
	args := m.Called(to)
	return args.Get(0).(*types.Log), args.Error(1)
}

func (m *MockSyncer) ClearHeaderCache() {
	m.Called()
}

func (m *MockSyncer) GetDoneChan() <-chan uint64 {
	args := m.Called()
	return args.Get(0).(<-chan uint64)
}

func (m *MockSyncer) GetLastCheckedL1Block() uint64 {
	args := m.Called()
	return uint64(args.Int(0))
}

func (m *MockSyncer) CheckL1BlockFinalized(blockNo uint64) (finalized bool, finalizedBn uint64, err error) {
	args := m.Called(blockNo)
	return args.Bool(0), uint64(args.Int(1)), args.Error(2)
}

func TestUpdater_WarmUp(t *testing.T) {
	db := memdb.NewTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	mockSyncer := &MockSyncer{}
	cfg := &ethconfig.Zk{L1FirstBlock: 1000}
	updater := NewUpdater(context.Background(), cfg, mockSyncer, nil)

	// Set up mock expectations
	mockSyncer.On("IsSyncStarted").Return(false)
	mockSyncer.On("RunQueryBlocks", uint64(999)).Once()

	err = updater.WarmUp(tx)
	require.NoError(t, err)

	assert.Equal(t, uint64(999), updater.progress)
	mockSyncer.AssertExpectations(t)
}

func TestL1InfoWorkerPool_NewAndStart(t *testing.T) {
	ctx := context.Background()
	mockSyncer := &MockSyncer{}
	numWorkers := 3

	pool := NewL1InfoWorkerPool(ctx, numWorkers, mockSyncer)

	assert.NotNil(t, pool)
	assert.Len(t, pool.workers, numWorkers)
	assert.NotNil(t, pool.taskChan)
	assert.NotNil(t, pool.taskResChan)

	pool.Start()
	time.Sleep(10 * time.Millisecond) // Allow workers to start

	pool.Stop()
	pool.Wait()
}

func TestL1InfoTask_Run(t *testing.T) {
	mockSyncer := &MockSyncer{}
	log := types.Log{
		BlockNumber: 100,
		Topics:      []common.Hash{common.Hash{}, common.Hash{}, common.Hash{}},
	}
	header := &types.Header{
		Number: big.NewInt(100),
		Time:   uint64(time.Now().Unix()),
	}

	task := NewL1InfoTask(log, mockSyncer)

	// Test successful case
	mockSyncer.On("GetHeader", uint64(100)).Return(header, nil).Once()

	result := task.Run()
	assert.NoError(t, result.err)
	assert.NotNil(t, result.l1InfoTreeUpdate)

	// Test header not found
	mockSyncer.On("GetHeader", uint64(100)).Return((*types.Header)(nil), assert.AnError).Once()

	result = task.Run()
	assert.Error(t, result.err)
	assert.Nil(t, result.l1InfoTreeUpdate)

	mockSyncer.AssertExpectations(t)
}

func TestL1InfoTask_Run_WrongTopicsCount(t *testing.T) {
	mockSyncer := &MockSyncer{}
	log := types.Log{
		BlockNumber: 100,
		Topics:      []common.Hash{common.Hash{}}, // Only 1 topic instead of 3
	}
	header := &types.Header{
		Number: big.NewInt(100),
		Time:   uint64(time.Now().Unix()),
	}

	task := NewL1InfoTask(log, mockSyncer)

	mockSyncer.On("GetHeader", uint64(100)).Return(header, nil).Once()

	result := task.Run()
	assert.NoError(t, result.err)
	assert.Nil(t, result.l1InfoTreeUpdate) // Should be nil for wrong log
}

func TestCreateL1InfoTreeUpdate(t *testing.T) {
	timestamp := uint64(time.Now().Unix())
	parentHash := common.HexToHash("0x123")

	log := types.Log{
		BlockNumber: 100,
		Topics: []common.Hash{
			contracts.UpdateL1InfoTreeTopic,
			common.HexToHash("0x456"), // mainnetExitRoot
			common.HexToHash("0x789"), // rollupExitRoot
		},
	}

	header := &types.Header{
		Number:     big.NewInt(100),
		Time:       timestamp,
		ParentHash: parentHash,
	}

	update, err := createL1InfoTreeUpdate(log, header)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), update.BlockNumber)
	assert.Equal(t, timestamp, update.Timestamp)
	assert.Equal(t, parentHash, update.ParentHash)
	assert.Equal(t, log.Topics[1], update.MainnetExitRoot)
	assert.Equal(t, log.Topics[2], update.RollupExitRoot)
}

func TestCreateL1InfoTreeUpdate_InvalidTopics(t *testing.T) {
	log := types.Log{
		BlockNumber: 100,
		Topics:      []common.Hash{common.Hash{}}, // Wrong number of topics
	}

	header := &types.Header{Number: big.NewInt(100)}

	_, err := createL1InfoTreeUpdate(log, header)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "did not have 3 topics")
}

func TestCreateL1InfoTreeUpdate_BlockNumberMismatch(t *testing.T) {
	log := types.Log{
		BlockNumber: 100,
		Topics:      []common.Hash{common.Hash{}, common.Hash{}, common.Hash{}},
	}

	header := &types.Header{Number: big.NewInt(200)}

	_, err := createL1InfoTreeUpdate(log, header)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "did not match the block number")
}

func TestInitialiseL1InfoTree(t *testing.T) {
	db := memdb.NewTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDb(tx)

	tree, err := InitialiseL1InfoTree(hermezDb)
	require.NoError(t, err)
	assert.NotNil(t, tree)
}

func TestCheckForInfoTreeUpdates(t *testing.T) {
	// Set log level to Debug

	testCases := []struct {
		name              string
		logs              []types.Log
		expectedProcessed uint64
		expectedError     bool
		mockHeaders       map[uint64]*types.Header
	}{
		{
			name: "single valid log",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x111"), common.HexToHash("0x222")},
				},
			},
			expectedProcessed: 1,
			expectedError:     false,
			mockHeaders: map[uint64]*types.Header{
				10: {
					Number: big.NewInt(10),
					Time:   uint64(time.Now().Unix()),
				},
			},
		},
		{
			name: "multiple valid logs",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x111"), common.HexToHash("0x222")},
				},
				{
					BlockNumber: 11,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x333"), common.HexToHash("0x444")},
				},
			},
			expectedProcessed: 2,
			expectedError:     false,
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: uint64(time.Now().Unix())},
				11: {Number: big.NewInt(11), Time: uint64(time.Now().Unix())},
			},
		},
		{
			name: "log with wrong topic (should be skipped)",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{common.HexToHash("0xwrongtopic"), common.HexToHash("0x111"), common.HexToHash("0x222")},
				},
			},
			expectedProcessed: 0,
			expectedError:     false,
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: uint64(time.Now().Unix())},
			},
		},
		{
			name: "log with insufficient topics (should be skipped)",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x111")}, // Only 2 topics
				},
			},
			expectedProcessed: 0,
			expectedError:     false, // only warning, some weird case that must not happen
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: uint64(time.Now().Unix())},
			},
		},
		{
			name: "mixed valid and invalid logs",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{common.HexToHash("0xwrongtopic"), common.HexToHash("0x111"), common.HexToHash("0x222")},
				},
				{
					BlockNumber: 11,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x333"), common.HexToHash("0x444")},
				},
				{
					BlockNumber: 12,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x555")}, // Insufficient topics
				},
			},
			expectedProcessed: 1,
			expectedError:     false,
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: uint64(time.Now().Unix())},
				11: {Number: big.NewInt(11), Time: uint64(time.Now().Unix() + 1)},
				12: {Number: big.NewInt(12), Time: uint64(time.Now().Unix() + 2)},
			},
		},
		{
			name: "UpdateL1InfoTreeTopic followed by UpdateL1InfoTreeV2Topic",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x111"), common.HexToHash("0x222")},
					TxIndex:     0,
					Index:       0, // must be V2 topic leaf count (0x1) - 1
				},
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeV2Topic, common.HexToHash("0x1"), common.HexToHash("0x444")},
					Data:        common.FromHex("0x26896ed3a18c507006d6bcb9f30c8bd37ca1ec07148254600e51f1e73b6e9ad6"), // precalculated for v1 event
					TxIndex:     0,
					Index:       1,
				},
			},
			expectedProcessed: 2,
			expectedError:     false,
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: 1},
			},
		},
		{
			name: "UpdateL1InfoTreeV2Topic followed by UpdateL1InfoTreeTopic is invalid",
			logs: []types.Log{
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeV2Topic, common.HexToHash("0x333"), common.HexToHash("0x444")},
					Data:        make([]byte, 32), // Simulating data for V2
				},
				{
					BlockNumber: 10,
					Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, common.HexToHash("0x111"), common.HexToHash("0x222")},
				},
			},
			expectedProcessed: 0,
			expectedError:     true,
			mockHeaders: map[uint64]*types.Header{
				10: {Number: big.NewInt(10), Time: 1},
			},
		},
		{
			name:              "empty logs array",
			logs:              []types.Log{},
			expectedProcessed: 0,
			expectedError:     false,
			mockHeaders:       map[uint64]*types.Header{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := memdb.NewTestDB(t)
			defer db.Close()

			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			mockSyncer := &MockSyncer{}
			cfg := &ethconfig.Zk{L1FirstBlock: 1000}
			updater := NewUpdater(context.Background(), cfg, mockSyncer, nil)

			logsChan := make(chan []types.Log, 10)

			// Set up mock expectations
			mockSyncer.On("GetLogsChan").Return((<-chan []types.Log)(logsChan))
			mockSyncer.On("GetProgressMessageChan").Return((<-chan string)(make(chan string)))
			mockSyncer.On("ClearHeaderCache").Return()
			mockSyncer.On("StopQueryBlocks").Return().Maybe()
			mockSyncer.On("ConsumeQueryBlocks").Return().Maybe()
			mockSyncer.On("WaitQueryBlocksToFinish").Return().Maybe()
			mockSyncer.On("RunQueryBlocks", mock.AnythingOfType("uint64")).Return().Maybe()

			// Set up header mocks
			for blockNum, header := range tc.mockHeaders {
				mockSyncer.On("GetHeader", blockNum).Return(header, nil).Maybe() // any times
			}
			// Add a catch-all mock for any other GetHeader calls (like block 0)
			mockSyncer.On("GetHeader", mock.AnythingOfType("uint64")).Return((*types.Header)(nil), assert.AnError).Maybe()

			time.AfterFunc(1*time.Millisecond, func() {
				// Send logs to channel
				logsChan <- tc.logs
				close(logsChan)
			})

			processed, err := updater.CheckForInfoTreeUpdates("test", tx)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedProcessed, processed)
			}

			mockSyncer.AssertExpectations(t)
		})
	}
}
