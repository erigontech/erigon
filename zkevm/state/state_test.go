package state_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/db"
	"github.com/0xPolygonHermez/zkevm-node/event"
	"github.com/0xPolygonHermez/zkevm-node/event/nileventstorage"
	"github.com/0xPolygonHermez/zkevm-node/test/contracts/bin/Counter"
	"github.com/0xPolygonHermez/zkevm-node/test/dbutils"
	"github.com/0xPolygonHermez/zkevm-node/test/testutils"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/merkletree"
	mtDBclientpb "github.com/ledgerwatch/erigon/zkevm/merkletree/pb"
	"github.com/ledgerwatch/erigon/zkevm/state"
	"github.com/ledgerwatch/erigon/zkevm/state/metrics"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor"
	executorclientpb "github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
	"github.com/ledgerwatch/erigon/zkevm/test/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	ether155V = 27
)

var (
	testState  *state.State
	stateTree  *merkletree.StateTree
	stateDb    *pgxpool.Pool
	err        error
	stateDBCfg = dbutils.NewStateConfigFromEnv()
	ctx        = context.Background()
	stateCfg   = state.Config{
		MaxCumulativeGasUsed: 800000,
		ChainID:              1000,
		ForkIDIntervals: []state.ForkIDInterval{{
			FromBatchNumber: 0,
			ToBatchNumber:   math.MaxUint64,
			ForkId:          0,
			Version:         "",
		}},
	}
	forkID                             uint64 = 2
	executorClient                     executorclientpb.ExecutorServiceClient
	mtDBServiceClient                  mtDBclientpb.StateDBServiceClient
	executorClientConn, mtDBClientConn *grpc.ClientConn
)

func TestMain(m *testing.M) {
	initOrResetDB()

	stateDb, err = db.NewSQLDB(stateDBCfg)
	if err != nil {
		panic(err)
	}
	defer stateDb.Close()

	zkProverURI := testutils.GetEnv("ZKPROVER_URI", "localhost")

	executorServerConfig := executor.Config{URI: fmt.Sprintf("%s:50071", zkProverURI)}
	var executorCancel context.CancelFunc
	executorClient, executorClientConn, executorCancel = executor.NewExecutorClient(ctx, executorServerConfig)
	s := executorClientConn.GetState()
	log.Infof("executorClientConn state: %s", s.String())
	defer func() {
		executorCancel()
		executorClientConn.Close()
	}()

	mtDBServerConfig := merkletree.Config{URI: fmt.Sprintf("%s:50061", zkProverURI)}
	var mtDBCancel context.CancelFunc
	mtDBServiceClient, mtDBClientConn, mtDBCancel = merkletree.NewMTDBServiceClient(ctx, mtDBServerConfig)
	s = mtDBClientConn.GetState()
	log.Infof("stateDbClientConn state: %s", s.String())
	defer func() {
		mtDBCancel()
		mtDBClientConn.Close()
	}()

	stateTree = merkletree.NewStateTree(mtDBServiceClient)

	eventStorage, err := nileventstorage.NewNilEventStorage()
	if err != nil {
		panic(err)
	}
	eventLog := event.NewEventLog(event.Config{}, eventStorage)

	testState = state.NewState(stateCfg, state.NewPostgresStorage(stateDb), executorClient, stateTree, eventLog)

	result := m.Run()

	os.Exit(result)
}

func TestAddBlock(t *testing.T) {
	// Init database instance
	initOrResetDB()

	// ctx := context.Background()
	fmt.Println("db: ", stateDb)
	tx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	block := &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block, tx)
	assert.NoError(t, err)
	// Add the second block
	block.BlockNumber = 2
	err = testState.AddBlock(ctx, block, tx)
	assert.NoError(t, err)
	err = tx.Commit(ctx)
	require.NoError(t, err)
	// Get the last block
	lastBlock, err := testState.GetLastBlock(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), lastBlock.BlockNumber)
	assert.Equal(t, block.BlockHash, lastBlock.BlockHash)
	assert.Equal(t, block.ParentHash, lastBlock.ParentHash)
	// Get the previous block
	prevBlock, err := testState.GetPreviousBlock(ctx, 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), prevBlock.BlockNumber)
}

func TestProcessCloseBatch(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Set genesis batch
	_, err = testState.SetGenesis(ctx, state.Block{}, state.Genesis{}, dbTx)
	require.NoError(t, err)
	// Open batch #1
	// processingCtx1 := state.ProcessingContext{
	// 	BatchNumber:    1,
	// 	Coinbase:       common.HexToAddress("1"),
	// 	Timestamp:      time.Now().UTC(),
	// 	globalExitRoot: common.HexToHash("a"),
	// }
	// Txs for batch #1
	// rawTxs := "f84901843b9aca00827b0c945fbdb2315678afecb367f032d93f642f64180aa380a46057361d00000000000000000000000000000000000000000000000000000000000000048203e9808073efe1fa2d3e27f26f32208550ea9b0274d49050b816cadab05a771f4275d0242fd5d92b3fb89575c070e6c930587c520ee65a3aa8cfe382fcad20421bf51d621c"
	//TODO Finish and fix this test
	// err = testState.ProcessAndStoreClosedBatch(ctx, processingCtx1, common.Hex2Bytes(rawTxs), dbTx, state.SynchronizerCallerLabel)
	// require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))
}

func TestOpenCloseBatch(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Set genesis batch
	_, err = testState.SetGenesis(ctx, state.Block{}, state.Genesis{}, dbTx)
	require.NoError(t, err)
	// Open batch #1
	processingCtx1 := state.ProcessingContext{
		BatchNumber:    1,
		Coinbase:       common.HexToAddress("1"),
		Timestamp:      time.Now().UTC(),
		GlobalExitRoot: common.HexToHash("a"),
	}
	err = testState.OpenBatch(ctx, processingCtx1, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Fail opening batch #2 (#1 is still open)
	processingCtx2 := state.ProcessingContext{
		BatchNumber:    2,
		Coinbase:       common.HexToAddress("2"),
		Timestamp:      time.Now().UTC(),
		GlobalExitRoot: common.HexToHash("b"),
	}
	err = testState.OpenBatch(ctx, processingCtx2, dbTx)
	assert.Equal(t, state.ErrLastBatchShouldBeClosed, err)
	// Fail closing batch #1 (it has no txs yet)
	receipt1 := state.ProcessingReceipt{
		BatchNumber:   1,
		StateRoot:     common.HexToHash("1"),
		LocalExitRoot: common.HexToHash("1"),
	}
	err = testState.CloseBatch(ctx, receipt1, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Rollback(ctx))
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Add txs to batch #1
	tx1 := *types.NewTransaction(0, common.HexToAddress("0"), big.NewInt(0), 0, big.NewInt(0), []byte("aaa"))
	tx2 := *types.NewTransaction(1, common.HexToAddress("1"), big.NewInt(1), 0, big.NewInt(1), []byte("bbb"))
	txsBatch1 := []*state.ProcessTransactionResponse{
		{
			TxHash: tx1.Hash(),
			Tx:     tx1,
		},
		{
			TxHash: tx2.Hash(),
			Tx:     tx2,
		},
	}

	data, err := state.EncodeTransactions([]types.Transaction{tx1, tx2})
	require.NoError(t, err)
	receipt1.BatchL2Data = data

	err = testState.StoreTransactions(ctx, 1, txsBatch1, dbTx)
	require.NoError(t, err)
	// Close batch #1
	err = testState.CloseBatch(ctx, receipt1, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Fail opening batch #3 (should open batch #2)
	processingCtx3 := state.ProcessingContext{
		BatchNumber:    3,
		Coinbase:       common.HexToAddress("3"),
		Timestamp:      time.Now().UTC(),
		GlobalExitRoot: common.HexToHash("c"),
	}
	err = testState.OpenBatch(ctx, processingCtx3, dbTx)
	require.ErrorIs(t, err, state.ErrUnexpectedBatch)
	// Fail opening batch #2 (invalid timestamp)
	processingCtx2.Timestamp = processingCtx1.Timestamp.Add(-1 * time.Second)
	err = testState.OpenBatch(ctx, processingCtx2, dbTx)
	require.Equal(t, state.ErrTimestampGE, err)
	processingCtx2.Timestamp = time.Now()
	require.NoError(t, dbTx.Rollback(ctx))
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Open batch #2
	err = testState.OpenBatch(ctx, processingCtx2, dbTx)
	require.NoError(t, err)
	// Get batch #1 from DB and compare with on memory batch
	actualBatch, err := testState.GetBatchByNumber(ctx, 1, dbTx)
	require.NoError(t, err)
	batchL2Data, err := state.EncodeTransactions([]types.Transaction{tx1, tx2})
	require.NoError(t, err)
	assertBatch(t, state.Batch{
		BatchNumber:    1,
		Coinbase:       processingCtx1.Coinbase,
		BatchL2Data:    batchL2Data,
		StateRoot:      receipt1.StateRoot,
		LocalExitRoot:  receipt1.LocalExitRoot,
		Timestamp:      processingCtx1.Timestamp,
		GlobalExitRoot: processingCtx1.GlobalExitRoot,
	}, *actualBatch)
	require.NoError(t, dbTx.Commit(ctx))
}

func assertBatch(t *testing.T, expected, actual state.Batch) {
	assert.Equal(t, expected.Timestamp.Unix(), actual.Timestamp.Unix())
	actual.Timestamp = expected.Timestamp
	assert.Equal(t, expected, actual)
}

func TestAddForcedBatch(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	tx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	block := &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block, tx)
	assert.NoError(t, err)
	b := common.Hex2Bytes("0x617b3a3528F9")
	assert.NoError(t, err)
	forcedBatch := state.ForcedBatch{
		BlockNumber:       1,
		ForcedBatchNumber: 2,
		GlobalExitRoot:    common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		Sequencer:         common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
		RawTxsData:        b,
		ForcedAt:          time.Now(),
	}
	err = testState.AddForcedBatch(ctx, &forcedBatch, tx)
	require.NoError(t, err)
	fb, err := testState.GetForcedBatch(ctx, 2, tx)
	require.NoError(t, err)
	err = tx.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, forcedBatch.BlockNumber, fb.BlockNumber)
	assert.Equal(t, forcedBatch.ForcedBatchNumber, fb.ForcedBatchNumber)
	assert.NotEqual(t, time.Time{}, fb.ForcedAt)
	assert.Equal(t, forcedBatch.GlobalExitRoot, fb.GlobalExitRoot)
	assert.Equal(t, forcedBatch.RawTxsData, fb.RawTxsData)
	// Test GetNextForcedBatches
	tx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	forcedBatch = state.ForcedBatch{
		BlockNumber:       1,
		ForcedBatchNumber: 3,
		GlobalExitRoot:    common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		Sequencer:         common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
		RawTxsData:        b,
		ForcedAt:          time.Now(),
	}
	err = testState.AddForcedBatch(ctx, &forcedBatch, tx)
	require.NoError(t, err)

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num, forced_batch_num) VALUES (2, 2)")
	assert.NoError(t, err)
	virtualBatch := state.VirtualBatch{
		BlockNumber: 1,
		BatchNumber: 2,
		TxHash:      common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		Coinbase:    common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
	}
	err = testState.AddVirtualBatch(ctx, &virtualBatch, tx)
	require.NoError(t, err)

	batches, err := testState.GetNextForcedBatches(ctx, 1, tx)
	require.NoError(t, err)
	assert.Equal(t, forcedBatch.BlockNumber, batches[0].BlockNumber)
	assert.Equal(t, forcedBatch.ForcedBatchNumber, batches[0].ForcedBatchNumber)
	assert.NotEqual(t, time.Time{}, batches[0].ForcedAt)
	assert.Equal(t, forcedBatch.GlobalExitRoot, batches[0].GlobalExitRoot)
	assert.Equal(t, forcedBatch.RawTxsData, batches[0].RawTxsData)
	require.NoError(t, tx.Commit(ctx))
}

func TestAddVirtualBatch(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	tx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	block := &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block, tx)
	assert.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (1)")
	assert.NoError(t, err)
	virtualBatch := state.VirtualBatch{
		BlockNumber: 1,
		BatchNumber: 1,
		TxHash:      common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		Coinbase:    common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
	}
	err = testState.AddVirtualBatch(ctx, &virtualBatch, tx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

func TestGetTxsHashesToDelete(t *testing.T) {
	initOrResetDB()

	ctx := context.Background()
	tx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	block1 := &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block1, tx)
	assert.NoError(t, err)
	block2 := &state.Block{
		BlockNumber: 2,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block2, tx)
	assert.NoError(t, err)

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (1)")
	assert.NoError(t, err)
	require.NoError(t, err)
	virtualBatch1 := state.VirtualBatch{
		BlockNumber: 1,
		BatchNumber: 1,
		TxHash:      common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		Coinbase:    common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
	}

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (2)")
	assert.NoError(t, err)
	virtualBatch2 := state.VirtualBatch{
		BlockNumber: 1,
		BatchNumber: 2,
		TxHash:      common.HexToHash("0x132"),
		Coinbase:    common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"),
	}
	err = testState.AddVirtualBatch(ctx, &virtualBatch1, tx)
	require.NoError(t, err)
	err = testState.AddVirtualBatch(ctx, &virtualBatch2, tx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	_, err = testState.Exec(ctx, "INSERT INTO state.l2block (block_num, block_hash, received_at, batch_num, created_at) VALUES ($1, $2, $3, $4, $5)", 1, "0x423", time.Now(), 1, time.Now().UTC())
	require.NoError(t, err)
	l2Tx1 := types.NewTransaction(1, common.Address{}, big.NewInt(10), 21000, big.NewInt(1), []byte{})
	_, err = testState.Exec(ctx, "INSERT INTO state.transaction (l2_block_num, encoded, hash) VALUES ($1, $2, $3)",
		virtualBatch1.BatchNumber, fmt.Sprintf("encoded-%d", virtualBatch1.BatchNumber), l2Tx1.Hash().Hex())
	require.NoError(t, err)

	_, err = testState.Exec(ctx, "INSERT INTO state.l2block (block_num, block_hash, received_at, batch_num, created_at) VALUES ($1, $2, $3, $4, $5)", 2, "0x423", time.Now(), 2, time.Now().UTC())
	require.NoError(t, err)
	l2Tx2 := types.NewTransaction(2, common.Address{}, big.NewInt(10), 21000, big.NewInt(1), []byte{})
	_, err = testState.Exec(ctx, "INSERT INTO state.transaction (l2_block_num, encoded, hash) VALUES ($1, $2, $3)",
		virtualBatch2.BatchNumber, fmt.Sprintf("encoded-%d", virtualBatch2.BatchNumber), l2Tx2.Hash().Hex())
	require.NoError(t, err)
	txHashes, err := testState.GetTxsOlderThanNL1Blocks(ctx, 1, nil)
	require.NoError(t, err)
	require.Equal(t, l2Tx1.Hash().Hex(), txHashes[0].Hex())
}

func TestExecuteTransaction(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetInt64(400)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var sequencerBalance = 4000000
	scCounterByteCode, err := testutils.ReadBytecode("Counter/Counter.bin")
	require.NoError(t, err)

	// Deploy counter.sol
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scCounterByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx, err := auth.Signer(auth.From, tx)
	require.NoError(t, err)

	// Encode transaction
	v, r, s := signedTx.RawSignatureValues()
	sign := 1 - (v.Uint64() & 1)

	txCodedRlp, err := rlp.EncodeToBytes([]interface{}{
		signedTx.Nonce(),
		signedTx.GasPrice(),
		signedTx.Gas(),
		signedTx.To(),
		signedTx.Value(),
		signedTx.Data(),
		signedTx.ChainId(), uint(0), uint(0),
	})
	require.NoError(t, err)

	newV := new(big.Int).Add(big.NewInt(ether155V), big.NewInt(int64(sign)))
	newRPadded := fmt.Sprintf("%064s", r.Text(hex.Base))
	newSPadded := fmt.Sprintf("%064s", s.Text(hex.Base))
	newVPadded := fmt.Sprintf("%02s", newV.Text(hex.Base))
	batchL2Data, err := hex.DecodeString(hex.EncodeToString(txCodedRlp) + newRPadded + newSPadded + newVPadded)
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	log.Debugf("%v", processBatchRequest)

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	log.Debug(processBatchResponse)
	// TODO: assert processBatchResponse to make sure that the response makes sense
}

func TestCheckSupersetBatchTransactions(t *testing.T) {
	tcs := []struct {
		description      string
		existingTxHashes []common.Hash
		processedTxs     []*state.ProcessTransactionResponse
		expectedError    bool
		expectedErrorMsg string
	}{
		{
			description:      "empty existingTxHashes and processedTx is successful",
			existingTxHashes: []common.Hash{},
			processedTxs:     []*state.ProcessTransactionResponse{},
		},
		{
			description: "happy path",
			existingTxHashes: []common.Hash{
				common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c"),
				common.HexToHash("0x30c6a361ba88906ef2085d05a2aeac15e793caff2bdc1deaaae2f4910d83de52"),
				common.HexToHash("0x0d3453b6d17841b541d4f79f78d5fa22fff281551ed4012c7590b560b2969e7f"),
			},
			processedTxs: []*state.ProcessTransactionResponse{
				{TxHash: common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c")},
				{TxHash: common.HexToHash("0x30c6a361ba88906ef2085d05a2aeac15e793caff2bdc1deaaae2f4910d83de52")},
				{TxHash: common.HexToHash("0x0d3453b6d17841b541d4f79f78d5fa22fff281551ed4012c7590b560b2969e7f")},
			},
		},
		{
			description:      "existingTxHashes bigger than processedTx gives error",
			existingTxHashes: []common.Hash{common.HexToHash(""), common.HexToHash("")},
			processedTxs:     []*state.ProcessTransactionResponse{{}},
			expectedError:    true,
			expectedErrorMsg: state.ErrExistingTxGreaterThanProcessedTx.Error(),
		},
		{
			description: "processedTx not present in existingTxHashes gives error",
			existingTxHashes: []common.Hash{
				common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c"),
				common.HexToHash("0x30c6a361ba88906ef2085d05a2aeac15e793caff2bdc1deaaae2f4910d83de52"),
			},
			processedTxs: []*state.ProcessTransactionResponse{
				{TxHash: common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c")},
				{TxHash: common.HexToHash("0x0d3453b6d17841b541d4f79f78d5fa22fff281551ed4012c7590b560b2969e7f")},
			},
			expectedError:    true,
			expectedErrorMsg: state.ErrOutOfOrderProcessedTx.Error(),
		},
		{
			description: "out of order processedTx gives error",
			existingTxHashes: []common.Hash{
				common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c"),
				common.HexToHash("0x30c6a361ba88906ef2085d05a2aeac15e793caff2bdc1deaaae2f4910d83de52"),
				common.HexToHash("0x0d3453b6d17841b541d4f79f78d5fa22fff281551ed4012c7590b560b2969e7f"),
			},
			processedTxs: []*state.ProcessTransactionResponse{
				{TxHash: common.HexToHash("0x8a84686634729c57532b9ffa4e632e241b2de5c880c771c5c214d5e7ec465b1c")},
				{TxHash: common.HexToHash("0x0d3453b6d17841b541d4f79f78d5fa22fff281551ed4012c7590b560b2969e7f")},
				{TxHash: common.HexToHash("0x30c6a361ba88906ef2085d05a2aeac15e793caff2bdc1deaaae2f4910d83de52")},
			},
			expectedError:    true,
			expectedErrorMsg: state.ErrOutOfOrderProcessedTx.Error(),
		},
	}
	for _, tc := range tcs {
		// tc := tc
		t.Run(tc.description, func(t *testing.T) {
			require.NoError(t, testutils.CheckError(
				state.CheckSupersetBatchTransactions(tc.existingTxHashes, tc.processedTxs),
				tc.expectedError,
				tc.expectedErrorMsg,
			))
		})
	}
}

func TestGetTxsHashesByBatchNumber(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	// Set genesis batch
	_, err = testState.SetGenesis(ctx, state.Block{}, state.Genesis{}, dbTx)
	require.NoError(t, err)
	// Open batch #1
	processingCtx1 := state.ProcessingContext{
		BatchNumber:    1,
		Coinbase:       common.HexToAddress("1"),
		Timestamp:      time.Now().UTC(),
		GlobalExitRoot: common.HexToHash("a"),
	}
	err = testState.OpenBatch(ctx, processingCtx1, dbTx)
	require.NoError(t, err)

	// Add txs to batch #1
	tx1 := *types.NewTransaction(0, common.HexToAddress("0"), big.NewInt(0), 0, big.NewInt(0), []byte("aaa"))
	tx2 := *types.NewTransaction(1, common.HexToAddress("1"), big.NewInt(1), 0, big.NewInt(1), []byte("bbb"))
	txsBatch1 := []*state.ProcessTransactionResponse{
		{
			TxHash: tx1.Hash(),
			Tx:     tx1,
		},
		{
			TxHash: tx2.Hash(),
			Tx:     tx2,
		},
	}
	err = testState.StoreTransactions(ctx, 1, txsBatch1, dbTx)
	require.NoError(t, err)

	txs, err := testState.GetTxsHashesByBatchNumber(ctx, 1, dbTx)
	require.NoError(t, err)

	require.Equal(t, len(txsBatch1), len(txs))
	for i := range txsBatch1 {
		require.Equal(t, txsBatch1[i].TxHash, txs[i])
	}
	require.NoError(t, dbTx.Commit(ctx))
}

func TestDetermineProcessedTransactions(t *testing.T) {
	tcs := []struct {
		description               string
		input                     []*state.ProcessTransactionResponse
		expectedProcessedOutput   []*state.ProcessTransactionResponse
		expectedUnprocessedOutput map[string]*state.ProcessTransactionResponse
	}{
		{
			description:               "empty input returns empty",
			input:                     []*state.ProcessTransactionResponse{},
			expectedProcessedOutput:   []*state.ProcessTransactionResponse{},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{},
		},
		{
			description: "single processed transaction returns itself",
			input: []*state.ProcessTransactionResponse{
				{IsProcessed: true},
			},
			expectedProcessedOutput: []*state.ProcessTransactionResponse{
				{IsProcessed: true},
			},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{},
		},
		{
			description: "single unprocessed transaction returns empty",
			input: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: false,
				},
			},
			expectedProcessedOutput: []*state.ProcessTransactionResponse{},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{
				"0x000000000000000000000000000000000000000000000000000000000000000a": {
					TxHash:      common.HexToHash("a"),
					IsProcessed: false,
				},
			},
		},
		{
			description: "multiple processed transactions",
			input: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("b"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("c"),
					IsProcessed: true,
				},
			},
			expectedProcessedOutput: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("b"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("c"),
					IsProcessed: true,
				},
			},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{},
		},
		{
			description: "multiple unprocessed transactions",
			input: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: false,
				},
				{
					TxHash:      common.HexToHash("b"),
					IsProcessed: false,
				},
				{
					TxHash:      common.HexToHash("c"),
					IsProcessed: false,
				},
			},
			expectedProcessedOutput: []*state.ProcessTransactionResponse{},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{
				"0x000000000000000000000000000000000000000000000000000000000000000a": {
					TxHash:      common.HexToHash("a"),
					IsProcessed: false,
				},
				"0x000000000000000000000000000000000000000000000000000000000000000b": {
					TxHash:      common.HexToHash("b"),
					IsProcessed: false,
				},
				"0x000000000000000000000000000000000000000000000000000000000000000c": {
					TxHash:      common.HexToHash("c"),
					IsProcessed: false,
				},
			},
		},
		{
			description: "mixed processed and unprocessed transactions",
			input: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("b"),
					IsProcessed: false,
				},
				{
					TxHash:      common.HexToHash("c"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("d"),
					IsProcessed: false,
				},
			},
			expectedProcessedOutput: []*state.ProcessTransactionResponse{
				{
					TxHash:      common.HexToHash("a"),
					IsProcessed: true,
				},
				{
					TxHash:      common.HexToHash("c"),
					IsProcessed: true,
				},
			},
			expectedUnprocessedOutput: map[string]*state.ProcessTransactionResponse{
				"0x000000000000000000000000000000000000000000000000000000000000000b": {
					TxHash:      common.HexToHash("b"),
					IsProcessed: false,
				},
				"0x000000000000000000000000000000000000000000000000000000000000000d": {
					TxHash:      common.HexToHash("d"),
					IsProcessed: false,
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.description, func(t *testing.T) {
			actualProcessedTx, _, actualUnprocessedTxs, _ := state.DetermineProcessedTransactions(tc.input)
			require.Equal(t, tc.expectedProcessedOutput, actualProcessedTx)
			require.Equal(t, tc.expectedUnprocessedOutput, actualUnprocessedTxs)
		})
	}
}

func TestGenesis(t *testing.T) {
	block := state.Block{
		BlockNumber: 1,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	actions := []*state.GenesisAction{
		{
			Address: "0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FA",
			Type:    int(merkletree.LeafTypeBalance),
			Value:   "1000",
		},
		{
			Address: "0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FB",
			Type:    int(merkletree.LeafTypeBalance),
			Value:   "2000",
		},
		{
			Address: "0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FA",
			Type:    int(merkletree.LeafTypeNonce),
			Value:   "1",
		},
		{
			Address: "0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FB",
			Type:    int(merkletree.LeafTypeNonce),
			Value:   "1",
		},
		{
			Address:  "0xae4bb80be56b819606589de61d5ec3b522eeb032",
			Type:     int(merkletree.LeafTypeCode),
			Bytecode: "608060405234801561001057600080fd5b50600436106100675760003560e01c806333d6247d1161005057806333d6247d146100a85780633ed691ef146100bd578063a3c573eb146100d257600080fd5b806301fd90441461006c5780633381fe9014610088575b600080fd5b61007560015481565b6040519081526020015b60405180910390f35b6100756100963660046101c7565b60006020819052908152604090205481565b6100bb6100b63660046101c7565b610117565b005b43600090815260208190526040902054610075565b6002546100f29073ffffffffffffffffffffffffffffffffffffffff1681565b60405173ffffffffffffffffffffffffffffffffffffffff909116815260200161007f565b60025473ffffffffffffffffffffffffffffffffffffffff1633146101c2576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152603460248201527f476c6f62616c45786974526f6f744d616e616765724c323a3a7570646174654560448201527f786974526f6f743a204f4e4c595f425249444745000000000000000000000000606482015260840160405180910390fd5b600155565b6000602082840312156101d957600080fd5b503591905056fea2646970667358221220d6ed73b81f538d38669b0b750b93be08ca365978fae900eedc9ca93131c97ca664736f6c63430008090033",
		},
		{
			Address:         "0xae4bb80be56b819606589de61d5ec3b522eeb032",
			Type:            int(merkletree.LeafTypeStorage),
			StoragePosition: "0x0000000000000000000000000000000000000000000000000000000000000002",
			Value:           "0x9d98deabc42dd696deb9e40b4f1cab7ddbf55988",
		},
	}

	genesis := state.Genesis{
		Actions: actions,
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	for _, action := range actions {
		address := common.HexToAddress(action.Address)
		switch action.Type {
		case int(merkletree.LeafTypeBalance):
			balance, err := stateTree.GetBalance(ctx, address, stateRoot)
			require.NoError(t, err)
			require.Equal(t, action.Value, balance.String())
		case int(merkletree.LeafTypeNonce):
			nonce, err := stateTree.GetNonce(ctx, address, stateRoot)
			require.NoError(t, err)
			require.Equal(t, action.Value, nonce.String())
		case int(merkletree.LeafTypeCode):
			sc, err := stateTree.GetCode(ctx, address, stateRoot)
			require.NoError(t, err)
			require.Equal(t, common.Hex2Bytes(action.Bytecode), sc)
		case int(merkletree.LeafTypeStorage):
			st, err := stateTree.GetStorageAt(ctx, address, new(big.Int).SetBytes(common.Hex2Bytes(action.StoragePosition)), stateRoot)
			require.NoError(t, err)
			require.Equal(t, new(big.Int).SetBytes(common.Hex2Bytes(action.Value)), st)
		}
	}
}

func TestExecutor(t *testing.T) {
	var expectedNewRoot = "0xa2b0ad9cc19e2a4aa9a6d7e14b15e5e951e319ed17b619878bec201b4d064c3e"

	db := map[string]string{
		"2dc4db4293af236cb329700be43f08ace740a05088f8c7654736871709687e90": "00000000000000000000000000000000000000000000000000000000000000000d1f0da5a7b620c843fd1e18e59fd724d428d25da0cb1888e31f5542ac227c060000000000000000000000000000000000000000000000000000000000000000",
		"e31f5542ac227c06d428d25da0cb188843fd1e18e59fd7240d1f0da5a7b620c8": "ed22ec7734d89ff2b2e639153607b7c542b2bd6ec2788851b7819329410847833e63658ee0db910d0b3e34316e81aa10e0dc203d93f4e3e5e10053d0ebc646020000000000000000000000000000000000000000000000000000000000000000",
		"b78193294108478342b2bd6ec2788851b2e639153607b7c5ed22ec7734d89ff2": "16dde42596b907f049015d7e991a152894dd9dadd060910b60b4d5e9af514018b69b044f5e694795f57d81efba5d4445339438195426ad0a3efad1dd58c2259d0000000000000001000000000000000000000000000000000000000000000000",
		"3efad1dd58c2259d339438195426ad0af57d81efba5d4445b69b044f5e694795": "00000000dea000000000000035c9adc5000000000000003600000000000000000000000000000000000000000000000000000000000000000000000000000000",
		"e10053d0ebc64602e0dc203d93f4e3e50b3e34316e81aa103e63658ee0db910d": "66ee2be0687eea766926f8ca8796c78a4c2f3e938869b82d649e63bfe1247ba4b69b044f5e694795f57d81efba5d4445339438195426ad0a3efad1dd58c2259d0000000000000001000000000000000000000000000000000000000000000000",
	}

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D").String(),
		BatchL2Data:      common.Hex2Bytes("ee80843b9aca00830186a0944d5cf5032b2a844602278b01199ed191a86c93ff88016345785d8a0000808203e880801cee7e01dc62f69a12c3510c6d64de04ee6346d84b6a017f3e786c7d87f963e75d8cc91fa983cd6d9cf55fff80d73bd26cd333b0f098acc1e58edb1fd484ad731b"),
		OldStateRoot:     common.Hex2Bytes("2dc4db4293af236cb329700be43f08ace740a05088f8c7654736871709687e90"),
		GlobalExitRoot:   common.Hex2Bytes("090bcaf734c4f06c93954a827b45a6e8c67b8e0fd1e0a35a1c5982d6961828f9"),
		OldAccInputHash:  common.Hex2Bytes("17c04c3760510b48c6012742c540a81aba4bca2f78b9d14bfd2f123e2e53ea3e"),
		EthTimestamp:     uint64(1944498031),
		UpdateMerkleTree: 0,
		Db:               db,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)

	assert.Equal(t, common.HexToHash(expectedNewRoot), common.BytesToHash(processBatchResponse.NewStateRoot))
}

func TestExecutorRevert(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetInt64(1000)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	var sequencerBalance = 4000000
	scRevertByteCode, err := testutils.ReadBytecode("Revert2/Revert2.bin")
	require.NoError(t, err)

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: sequencerAddress.String(),
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "10000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)

	// Deploy revert.sol
	tx0 := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scRevertByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx0, err := auth.Signer(auth.From, tx0)
	require.NoError(t, err)

	// Call SC method
	tx1 := types.NewTransaction(1, scAddress, new(big.Int), 40000, new(big.Int).SetUint64(1), common.Hex2Bytes("4abbb40a"))
	signedTx1, err := auth.Signer(auth.From, tx1)
	require.NoError(t, err)

	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx0, *signedTx1})
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     stateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 0,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.Equal(t, runtime.ErrExecutionReverted, executor.RomErr(processBatchResponse.Responses[1].Error))

	// Unsigned
	receipt := &types.Receipt{
		Type:              uint8(signedTx0.Type()),
		PostState:         processBatchResponse.Responses[0].StateRoot,
		CumulativeGasUsed: processBatchResponse.Responses[0].GasUsed,
		BlockNumber:       big.NewInt(0),
		GasUsed:           processBatchResponse.Responses[0].GasUsed,
		TxHash:            signedTx0.Hash(),
		TransactionIndex:  0,
		Status:            types.ReceiptStatusSuccessful,
	}

	receipt1 := &types.Receipt{
		Type:              uint8(signedTx1.Type()),
		PostState:         processBatchResponse.Responses[1].StateRoot,
		CumulativeGasUsed: processBatchResponse.Responses[0].GasUsed + processBatchResponse.Responses[1].GasUsed,
		BlockNumber:       big.NewInt(0),
		GasUsed:           signedTx1.Gas(),
		TxHash:            signedTx1.Hash(),
		TransactionIndex:  1,
		Status:            types.ReceiptStatusSuccessful,
	}

	header := &types.Header{
		Number:     big.NewInt(1),
		ParentHash: state.ZeroHash,
		Coinbase:   state.ZeroAddress,
		Root:       common.BytesToHash(processBatchResponse.NewStateRoot),
		GasUsed:    receipt1.GasUsed,
		GasLimit:   receipt1.GasUsed,
		Time:       uint64(time.Now().Unix()),
	}

	receipts := []*types.Receipt{receipt, receipt1}

	transactions := []*types.Transaction{signedTx0, signedTx1}

	// Create block to be able to calculate its hash
	l2Block := types.NewBlock(header, transactions, []*types.Header{}, receipts, &trie.StackTrie{})
	l2Block.ReceivedAt = time.Now()

	receipt.BlockHash = l2Block.Hash()

	err = testState.AddL2Block(ctx, 0, l2Block, receipts, dbTx)
	require.NoError(t, err)
	l2Block, err = testState.GetL2BlockByHash(ctx, l2Block.Hash(), dbTx)
	require.NoError(t, err)

	require.NoError(t, dbTx.Commit(ctx))

	lastL2BlockNumber := l2Block.NumberU64()

	unsignedTx := types.NewTransaction(2, scAddress, new(big.Int), 40000, new(big.Int).SetUint64(1), common.Hex2Bytes("4abbb40a"))

	result, err := testState.ProcessUnsignedTransaction(ctx, unsignedTx, auth.From, &lastL2BlockNumber, false, nil)
	require.NoError(t, err)
	require.NotNil(t, result.Err)
	assert.Equal(t, fmt.Errorf("execution reverted: Today is not juernes").Error(), result.Err.Error())
}

func TestExecutorLogs(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetInt64(1000)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var sequencerBalance = 4000000
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	scLogsByteCode, err := testutils.ReadBytecode("EmitLog2/EmitLog2.bin")
	require.NoError(t, err)

	// Genesis DB
	genesisDB := map[string]string{
		"2dc4db4293af236cb329700be43f08ace740a05088f8c7654736871709687e90": "00000000000000000000000000000000000000000000000000000000000000000d1f0da5a7b620c843fd1e18e59fd724d428d25da0cb1888e31f5542ac227c060000000000000000000000000000000000000000000000000000000000000000",
		"e31f5542ac227c06d428d25da0cb188843fd1e18e59fd7240d1f0da5a7b620c8": "ed22ec7734d89ff2b2e639153607b7c542b2bd6ec2788851b7819329410847833e63658ee0db910d0b3e34316e81aa10e0dc203d93f4e3e5e10053d0ebc646020000000000000000000000000000000000000000000000000000000000000000",
		"b78193294108478342b2bd6ec2788851b2e639153607b7c5ed22ec7734d89ff2": "16dde42596b907f049015d7e991a152894dd9dadd060910b60b4d5e9af514018b69b044f5e694795f57d81efba5d4445339438195426ad0a3efad1dd58c2259d0000000000000001000000000000000000000000000000000000000000000000",
		"3efad1dd58c2259d339438195426ad0af57d81efba5d4445b69b044f5e694795": "00000000dea000000000000035c9adc5000000000000003600000000000000000000000000000000000000000000000000000000000000000000000000000000",
		"e10053d0ebc64602e0dc203d93f4e3e50b3e34316e81aa103e63658ee0db910d": "66ee2be0687eea766926f8ca8796c78a4c2f3e938869b82d649e63bfe1247ba4b69b044f5e694795f57d81efba5d4445339438195426ad0a3efad1dd58c2259d0000000000000001000000000000000000000000000000000000000000000000",
	}

	// Deploy Emitlog2.sol
	tx0 := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scLogsByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx0, err := auth.Signer(auth.From, tx0)
	require.NoError(t, err)

	// Call SC method
	tx1 := types.NewTransaction(1, scAddress, new(big.Int), 40000, new(big.Int).SetUint64(1), common.Hex2Bytes("7966b4f6"))
	signedTx1, err := auth.Signer(auth.From, tx1)
	require.NoError(t, err)

	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx0, *signedTx1})
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     common.Hex2Bytes("2dc4db4293af236cb329700be43f08ace740a05088f8c7654736871709687e90"),
		GlobalExitRoot:   common.Hex2Bytes("090bcaf734c4f06c93954a827b45a6e8c67b8e0fd1e0a35a1c5982d6961828f9"),
		OldAccInputHash:  common.Hex2Bytes("17c04c3760510b48c6012742c540a81aba4bca2f78b9d14bfd2f123e2e53ea3e"),
		EthTimestamp:     uint64(1944498031),
		UpdateMerkleTree: 0,
		Db:               genesisDB,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)

	assert.Equal(t, scAddress, common.HexToAddress(string(processBatchResponse.Responses[0].CreateAddress)))

	assert.Equal(t, 0, len(processBatchResponse.Responses[0].Logs))
	assert.Equal(t, 4, len(processBatchResponse.Responses[1].Logs))
	assert.Equal(t, 4, len(processBatchResponse.Responses[1].Logs[0].Topics))
	assert.Equal(t, 2, len(processBatchResponse.Responses[1].Logs[1].Topics))
	assert.Equal(t, 1, len(processBatchResponse.Responses[1].Logs[2].Topics))
	assert.Equal(t, 0, len(processBatchResponse.Responses[1].Logs[3].Topics))
}

func TestExecutorTransfer(t *testing.T) {
	var chainID = new(big.Int).SetInt64(1000)
	var senderAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var senderPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var receiverAddress = common.HexToAddress("0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FB")

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "10000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// Create transaction
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       &receiverAddress,
		Value:    new(big.Int).SetUint64(2),
		Gas:      uint64(30000),
		GasPrice: new(big.Int).SetUint64(1),
		Data:     nil,
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(senderPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err)

	signedTx, err := auth.Signer(auth.From, tx)
	require.NoError(t, err)

	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx})
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         receiverAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     stateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(0),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	// Read Sender Balance before execution
	balance, err := stateTree.GetBalance(ctx, senderAddress, processBatchRequest.OldStateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(10000000), balance.Uint64())

	// Read Receiver Balance before execution
	balance, err = stateTree.GetBalance(ctx, receiverAddress, processBatchRequest.OldStateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(0), balance.Uint64())

	// Process batch
	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)

	// Read Sender Balance
	balance, err = stateTree.GetBalance(ctx, senderAddress, processBatchResponse.Responses[0].StateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(9978998), balance.Uint64())

	// Read Receiver Balance
	balance, err = stateTree.GetBalance(ctx, receiverAddress, processBatchResponse.Responses[0].StateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(21002), balance.Uint64())

	// Read Modified Addresses directly from response
	readWriteAddresses := processBatchResponse.ReadWriteAddresses
	log.Debug(receiverAddress.String())
	data := readWriteAddresses[strings.ToLower(receiverAddress.String())]
	require.Equal(t, "21002", data.Balance)

	// Read Modified Addresses from converted response
	converted, err := testState.TestConvertToProcessBatchResponse([]types.Transaction{*signedTx}, processBatchResponse)
	require.NoError(t, err)
	convertedData := converted.ReadWriteAddresses[receiverAddress]
	require.Equal(t, uint64(21002), convertedData.Balance.Uint64())
	require.Equal(t, receiverAddress, convertedData.Address)
	require.Equal(t, (*uint64)(nil), convertedData.Nonce)
}

func TestExecutorTxHashAndRLP(t *testing.T) {
	// Test Case
	type TxHashTestCase struct {
		Nonce    string `json:"nonce"`
		GasPrice string `json:"gasPrice"`
		GasLimit string `json:"gasLimit"`
		To       string `json:"to"`
		Value    string `json:"value"`
		Data     string `json:"data"`
		ChainID  string `json:"chainId"`
		V        string `json:"v"`
		R        string `json:"r"`
		S        string `json:"s"`
		From     string `json:"from"`
		Hash     string `json:"hash"`
		Link     string `json:"link"`
	}

	var testCases, testCases2 []TxHashTestCase

	jsonFile, err := os.Open(filepath.Clean("test/vectors/src/tx-hash-ethereum/uniswap_formated.json"))
	require.NoError(t, err)
	defer func() { _ = jsonFile.Close() }()

	bytes, err := io.ReadAll(jsonFile)
	require.NoError(t, err)

	err = json.Unmarshal(bytes, &testCases)
	require.NoError(t, err)

	jsonFile2, err := os.Open(filepath.Clean("test/vectors/src/tx-hash-ethereum/rlp.json"))
	require.NoError(t, err)
	defer func() { _ = jsonFile2.Close() }()

	bytes2, err := io.ReadAll(jsonFile2)
	require.NoError(t, err)

	err = json.Unmarshal(bytes2, &testCases2)
	require.NoError(t, err)
	testCases = append(testCases, testCases2...)

	for x, testCase := range testCases {
		var stateRoot = state.ZeroHash
		var receiverAddress = common.HexToAddress(testCase.To)
		receiver := &receiverAddress
		if testCase.To == "0x" {
			receiver = nil
		}

		v, ok := new(big.Int).SetString(testCase.V, 0)
		require.Equal(t, true, ok)

		r, ok := new(big.Int).SetString(testCase.R, 0)
		require.Equal(t, true, ok)

		s, ok := new(big.Int).SetString(testCase.S, 0)
		require.Equal(t, true, ok)

		var value *big.Int

		if testCase.Value != "0x" {
			value, ok = new(big.Int).SetString(testCase.Value, 0)
			require.Equal(t, true, ok)
		}

		gasPrice, ok := new(big.Int).SetString(testCase.GasPrice, 0)
		require.Equal(t, true, ok)

		gasLimit, ok := new(big.Int).SetString(testCase.GasLimit, 0)
		require.Equal(t, true, ok)

		nonce, ok := new(big.Int).SetString(testCase.Nonce, 0)
		require.Equal(t, true, ok)

		// Create transaction
		tx := types.NewTx(&types.LegacyTx{
			Nonce:    nonce.Uint64(),
			To:       receiver,
			Value:    value,
			Gas:      gasLimit.Uint64(),
			GasPrice: gasPrice,
			Data:     common.Hex2Bytes(strings.TrimPrefix(testCase.Data, "0x")),
			V:        v,
			R:        r,
			S:        s,
		})
		t.Log("chainID: ", tx.ChainId())
		t.Log("txHash: ", tx.Hash())

		require.Equal(t, testCase.Hash, tx.Hash().String())

		batchL2Data, err := state.EncodeTransactions([]types.Transaction{*tx})
		require.NoError(t, err)

		// Create Batch
		processBatchRequest := &executorclientpb.ProcessBatchRequest{
			OldBatchNum:      uint64(x),
			Coinbase:         receiverAddress.String(),
			BatchL2Data:      batchL2Data,
			OldStateRoot:     stateRoot.Bytes(),
			GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
			OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
			EthTimestamp:     uint64(0),
			UpdateMerkleTree: 1,
			ChainId:          stateCfg.ChainID,
			ForkId:           forkID,
		}

		// Process batch
		processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
		require.NoError(t, err)

		// TX Hash
		log.Debugf("TX Hash=%v", tx.Hash().String())
		log.Debugf("Response TX Hash=%v", common.BytesToHash(processBatchResponse.Responses[0].TxHash).String())

		// RPL Encoding
		b, err := tx.MarshalBinary()
		require.NoError(t, err)
		log.Debugf("TX RLP=%v", hex.EncodeToHex(b))
		log.Debugf("Response TX RLP=%v", "0x"+common.Bytes2Hex(processBatchResponse.Responses[0].RlpTx))

		require.Equal(t, tx.Hash(), common.BytesToHash(processBatchResponse.Responses[0].TxHash))
		require.Equal(t, hex.EncodeToHex(b), "0x"+common.Bytes2Hex(processBatchResponse.Responses[0].RlpTx))
	}
}

func TestExecutorInvalidNonce(t *testing.T) {
	chainID := new(big.Int).SetInt64(1000)
	senderPvtKey := "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	receiverAddress := common.HexToAddress("0xb1D0Dc8E2Ce3a93EB2b32f4C7c3fD9dDAf1211FB")

	// authorization
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(senderPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err)
	senderAddress := auth.From

	type testCase struct {
		name         string
		currentNonce uint64
		txNonce      uint64
	}

	testCases := []testCase{
		{
			name:         "tx nonce is greater than expected",
			currentNonce: 1,
			txNonce:      2,
		},
		{
			name:         "tx nonce is less than expected",
			currentNonce: 5,
			txNonce:      4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			initOrResetDB()

			// Set Genesis
			block := state.Block{
				BlockNumber: 0,
				BlockHash:   state.ZeroHash,
				ParentHash:  state.ZeroHash,
				ReceivedAt:  time.Now(),
			}
			genesis := state.Genesis{
				Actions: []*state.GenesisAction{
					{
						Address: senderAddress.String(),
						Type:    int(merkletree.LeafTypeBalance),
						Value:   "10000000",
					},
					{
						Address: senderAddress.String(),
						Type:    int(merkletree.LeafTypeNonce),
						Value:   strconv.FormatUint(testCase.currentNonce, encoding.Base10),
					},
				},
			}
			dbTx, err := testState.BeginStateTransaction(ctx)
			require.NoError(t, err)
			stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
			require.NoError(t, err)
			require.NoError(t, dbTx.Commit(ctx))

			// Read Sender Balance
			currentNonce, err := stateTree.GetNonce(ctx, senderAddress, stateRoot)
			require.NoError(t, err)
			assert.Equal(t, testCase.currentNonce, currentNonce.Uint64())

			// Create transaction
			tx := types.NewTransaction(testCase.txNonce, receiverAddress, new(big.Int).SetUint64(2), uint64(30000), big.NewInt(1), nil)
			signedTx, err := auth.Signer(auth.From, tx)
			require.NoError(t, err)

			// encode txs
			batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx})
			require.NoError(t, err)

			// Create Batch
			processBatchRequest := &executorclientpb.ProcessBatchRequest{
				OldBatchNum:      0,
				Coinbase:         receiverAddress.String(),
				BatchL2Data:      batchL2Data,
				OldStateRoot:     stateRoot,
				GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
				OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
				EthTimestamp:     uint64(0),
				UpdateMerkleTree: 1,
				ChainId:          stateCfg.ChainID,
				ForkId:           forkID,
			}

			// Process batch
			processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
			require.NoError(t, err)

			transactionResponses := processBatchResponse.GetResponses()
			assert.Equal(t, true, executor.IsIntrinsicError(transactionResponses[0].Error), "invalid tx Error, it is expected to be INVALID TX")
		})
	}
}

func TestGenesisNewLeafType(t *testing.T) {
	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000",
			},
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeNonce),
				Value:   "0",
			},
			{
				Address: "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "200000000000000000000",
			},
			{
				Address: "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
				Type:    int(merkletree.LeafTypeNonce),
				Value:   "0",
			},
			{
				Address: "0x03e75d7dd38cce2e20ffee35ec914c57780a8e29",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "0",
			},
			{
				Address: "0x03e75d7dd38cce2e20ffee35ec914c57780a8e29",
				Type:    int(merkletree.LeafTypeNonce),
				Value:   "0",
			},
			{
				Address:  "0x03e75d7dd38cce2e20ffee35ec914c57780a8e29",
				Type:     int(merkletree.LeafTypeCode),
				Bytecode: "60606040525b600080fd00a165627a7a7230582012c9bd00152fa1c480f6827f81515bb19c3e63bf7ed9ffbb5fda0265983ac7980029",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	log.Debug(string(stateRoot))
	log.Debug(common.BytesToHash(stateRoot))
	log.Debug(common.BytesToHash(stateRoot).String())
	log.Debug(new(big.Int).SetBytes(stateRoot))
	log.Debug(common.Bytes2Hex(stateRoot))

	require.Equal(t, "49461512068930131501252998918674096186707801477301326632372959001738876161218", new(big.Int).SetBytes(stateRoot).String())
}

// TEST COMMENTED BECAUSE IT IS NOT STABLE WHEN RUNNING ON GITHUB ACTIONS
// WE NEED TO DOUBLE CHECK THE DEFER FUNC TO MAKE SURE IT WILL NOT
// DESTROY THE DB AND MAKE OTHER TESTS TO FAIL.
//
// func TestFromMock(t *testing.T) {
// 	executorClientBack := executorClient

// 	executorServerConfig := executor.Config{URI: "127.0.0.1:43071"}
// 	var executorCancel context.CancelFunc
// 	executorClient, executorClientConn, executorCancel = executor.NewExecutorClient(ctx, executorServerConfig)
// 	log.Infof("executorClientConn state: %s", executorClientConn.GetState().String())

// 	testState = state.NewState(stateCfg, state.NewPostgresStorage(stateDb), executorClient, stateTree)

// 	defer func() {
// 		executorCancel()
// 		executorClientConn.Close()
// 		executorClient = executorClientBack
// 		testState = state.NewState(stateCfg, state.NewPostgresStorage(stateDb), executorClient, stateTree)
// 	}()

// 	mtDBServiceClientBack := mtDBServiceClient
// 	mtDBServerConfig := merkletree.Config{URI: "127.0.0.1:43061"}
// 	var mtDBCancel context.CancelFunc
// 	mtDBServiceClient, mtDBClientConn, mtDBCancel = merkletree.NewMTDBServiceClient(ctx, mtDBServerConfig)
// 	log.Infof("stateDbClientConn state: %s", mtDBClientConn.GetState().String())

// 	stateTree = merkletree.NewStateTree(mtDBServiceClient)
// 	testState = state.NewState(stateCfg, state.NewPostgresStorage(stateDb), executorClient, stateTree)

// 	defer func() {
// 		mtDBCancel()
// 		mtDBClientConn.Close()
// 		mtDBServiceClient = mtDBServiceClientBack
// 		stateTree = merkletree.NewStateTree(mtDBServiceClient)
// 		testState = state.NewState(stateCfg, state.NewPostgresStorage(stateDb), executorClient, stateTree)
// 	}()

// 	tvContainer, err := testvector.NewContainer("../test/vectors/src", afero.NewOsFs())
// 	require.NoError(t, err)

// 	tv := tvContainer.E2E.Items[0]

// 	balances := map[common.Address]*big.Int{}
// 	nonces := map[common.Address]*big.Int{}
// 	smartContracts := map[common.Address][]byte{}
// 	storage := map[common.Address]map[*big.Int]*big.Int{}

// 	for _, item := range tv.GenesisRaw {
// 		address := common.HexToAddress(item.Address)
// 		switch item.Type {
// 		case int(merkletree.LeafTypeBalance):
// 			balance, ok := new(big.Int).SetString(item.Value, 10)
// 			require.True(t, ok)
// 			balances[address] = balance
// 		case int(merkletree.LeafTypeNonce):
// 			nonce, ok := new(big.Int).SetString(item.Value, 10)
// 			require.True(t, ok)
// 			nonces[address] = nonce
// 		case int(merkletree.LeafTypeCode):
// 			if strings.HasPrefix(item.Bytecode, "0x") { // nolint
// 				item.Bytecode = item.Bytecode[2:]
// 			}
// 			bytecodeSlice := common.Hex2Bytes(item.Bytecode)
// 			smartContracts[address] = bytecodeSlice
// 		case int(merkletree.LeafTypeStorage):
// 			if strings.HasPrefix(item.StoragePosition, "0x") { // nolint
// 				item.StoragePosition = item.StoragePosition[2:]
// 			}
// 			storageKey, ok := new(big.Int).SetString(item.StoragePosition, 16)
// 			require.True(t, ok)
// 			storageValue, ok := new(big.Int).SetString(item.Value, 10)
// 			require.True(t, ok)
// 			if storage[address] == nil {
// 				storage[address] = map[*big.Int]*big.Int{}
// 			}
// 			storage[address][storageKey] = storageValue

// 			// Currently the test vector includes storage values in base10 format,
// 			// our SetGenesis requires base16 values.
// 			item.Value = hex.EncodeBig(storageValue)
// 		}
// 	}

// 	block := state.Block{
// 		BlockNumber: 1,
// 		BlockHash:   state.ZeroHash,
// 		ParentHash:  state.ZeroHash,
// 		ReceivedAt:  time.Now(),
// 	}

// 	genesis := state.Genesis{
// 		Actions: tv.GenesisRaw,
// 	}

// 	require.NoError(t, dbutils.InitOrReset(cfg))

// 	dbTx, err := testState.BeginStateTransaction(ctx)
// 	require.NoError(t, err)
// 	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
// 	require.NoError(t, err)
// 	require.NoError(t, dbTx.Commit(ctx))

// 	expectedRoot := tv.GenesisRaw[len(tv.GenesisRaw)-1].Root
// 	require.Equal(t, expectedRoot, hex.EncodeToHex(stateRoot))

// 	// Check Balances
// 	for address, expectedBalance := range balances {
// 		actualBalance, err := stateTree.GetBalance(ctx, address, stateRoot)
// 		require.NoError(t, err)
// 		require.Equal(t, expectedBalance, actualBalance)
// 	}

// 	// Check Nonces
// 	for address, expectedNonce := range nonces {
// 		actualNonce, err := stateTree.GetNonce(ctx, address, stateRoot)
// 		require.NoError(t, err)
// 		require.Equal(t, expectedNonce, actualNonce)
// 	}

// 	// Check smart contracts
// 	for address, expectedBytecode := range smartContracts {
// 		actualBytecode, err := stateTree.GetCode(ctx, address, stateRoot)
// 		require.NoError(t, err)
// 		require.Equal(t, expectedBytecode, actualBytecode)
// 	}

// 	// Check Storage
// 	for address, storageMap := range storage {
// 		for expectedKey, expectedValue := range storageMap {
// 			actualValue, err := stateTree.GetStorageAt(ctx, address, expectedKey, stateRoot)
// 			require.NoError(t, err)

// 			require.Equal(t, expectedValue, actualValue)
// 		}
// 	}

// 	processCtx := state.ProcessingContext{
// 		BatchNumber:    tv.Traces.NumBatch,
// 		Coinbase:       common.HexToAddress(tv.Traces.SequencerAddr),
// 		Timestamp:      time.Unix(int64(tv.Traces.Timestamp), 0),
// 		globalExitRoot: common.HexToHash(tv.globalExitRoot),
// 	}

// 	if strings.HasPrefix(tv.BatchL2Data, "0x") { // nolint
// 		tv.BatchL2Data = tv.BatchL2Data[2:]
// 	}
// 	dbTx, err = testState.BeginStateTransaction(ctx)
// 	require.NoError(t, err)

// 	err = testState.ProcessAndStoreClosedBatch(ctx, processCtx, common.Hex2Bytes(tv.BatchL2Data), dbTx) // nolint:ineffassign,staticcheck
// 	// TODO: actually check for nil err in ProcessAndStoreClosedBatch return value,
// 	// currently blocked by the issue about the mismatched tx hashes described here
// 	// https://github.com/0xPolygonHermez/zkevm-node/issues/1033
// 	// require.NoError(t, err)

// 	// TODO: currently the db tx is marked as invalid after the first error, once
// 	// testState.ProcessAndStoreClosedBatch works properly we should make assertions
// 	// about the database contents: batches, blocksL2, logs, receipts, ....
// }

func TestExecutorUnsignedTransactions(t *testing.T) {
	// Init database instance
	initOrResetDB()

	var chainIDSequencer = new(big.Int).SetInt64(1000)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var gasLimit = uint64(4000000)
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	scByteCode, err := testutils.ReadBytecode("Counter/Counter.bin")
	require.NoError(t, err)

	// auth
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	// signed tx to deploy SC
	unsignedTxDeploy := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     common.Hex2Bytes(scByteCode),
	})
	signedTxDeploy, err := auth.Signer(auth.From, unsignedTxDeploy)
	require.NoError(t, err)

	incrementFnSignature := crypto.Keccak256Hash([]byte("increment()")).Bytes()[:4]
	retrieveFnSignature := crypto.Keccak256Hash([]byte("getCount()")).Bytes()[:4]

	// signed tx to call SC
	unsignedTxFirstIncrement := types.NewTx(&types.LegacyTx{
		Nonce:    1,
		To:       &scAddress,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     incrementFnSignature,
	})
	signedTxFirstIncrement, err := auth.Signer(auth.From, unsignedTxFirstIncrement)
	require.NoError(t, err)

	unsignedTxFirstRetrieve := types.NewTx(&types.LegacyTx{
		Nonce:    2,
		To:       &scAddress,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     retrieveFnSignature,
	})
	signedTxFirstRetrieve, err := auth.Signer(auth.From, unsignedTxFirstRetrieve)
	require.NoError(t, err)

	dbTx, err := testState.BeginStateTransaction(context.Background())
	require.NoError(t, err)
	// Set genesis
	genesis := state.Genesis{Actions: []*state.GenesisAction{
		{
			Address: sequencerAddress.Hex(),
			Type:    int(merkletree.LeafTypeBalance),
			Value:   "100000000000000000000000",
		},
	}}
	_, err = testState.SetGenesis(ctx, state.Block{}, genesis, dbTx)
	require.NoError(t, err)
	batchCtx := state.ProcessingContext{
		BatchNumber: 1,
		Coinbase:    sequencerAddress,
		Timestamp:   time.Now(),
	}
	err = testState.OpenBatch(context.Background(), batchCtx, dbTx)
	require.NoError(t, err)
	signedTxs := []types.Transaction{
		*signedTxDeploy,
		*signedTxFirstIncrement,
		*signedTxFirstRetrieve,
	}

	batchL2Data, err := state.EncodeTransactions(signedTxs)
	require.NoError(t, err)

	processBatchResponse, err := testState.ProcessSequencerBatch(context.Background(), 1, batchL2Data, metrics.SequencerCallerLabel, dbTx)
	require.NoError(t, err)
	// assert signed tx do deploy sc
	assert.Nil(t, processBatchResponse.Responses[0].RomError)
	assert.Equal(t, scAddress, processBatchResponse.Responses[0].CreateAddress)

	// assert signed tx to increment counter
	assert.Nil(t, processBatchResponse.Responses[1].RomError)

	// assert signed tx to increment counter
	assert.Nil(t, processBatchResponse.Responses[2].RomError)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001", hex.EncodeToString(processBatchResponse.Responses[2].ReturnValue))

	// Add txs to DB
	err = testState.StoreTransactions(context.Background(), 1, processBatchResponse.Responses, dbTx)
	require.NoError(t, err)
	// Close batch
	err = testState.CloseBatch(
		context.Background(),
		state.ProcessingReceipt{
			BatchNumber:   1,
			StateRoot:     processBatchResponse.NewStateRoot,
			LocalExitRoot: processBatchResponse.NewLocalExitRoot,
		}, dbTx,
	)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(context.Background()))

	unsignedTxSecondRetrieve := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       &scAddress,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     retrieveFnSignature,
	})
	l2BlockNumber := uint64(3)

	result, err := testState.ProcessUnsignedTransaction(context.Background(), unsignedTxSecondRetrieve, common.HexToAddress("0x1000000000000000000000000000000000000000"), &l2BlockNumber, true, nil)
	require.NoError(t, err)
	// assert unsigned tx
	assert.Nil(t, result.Err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001", hex.EncodeToString(result.ReturnValue))
}

func TestAddGetL2Block(t *testing.T) {
	// Init database instance
	initOrResetDB()

	ctx := context.Background()
	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	block := &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
	err = testState.AddBlock(ctx, block, dbTx)
	assert.NoError(t, err)

	batchNumber := uint64(1)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES ($1)", batchNumber)
	assert.NoError(t, err)

	time := time.Now()
	blockNumber := big.NewInt(1)

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      0,
		GasPrice: big.NewInt(0),
	})

	receipt := &types.Receipt{
		Type:              uint8(tx.Type()),
		PostState:         state.ZeroHash.Bytes(),
		CumulativeGasUsed: 0,
		BlockNumber:       blockNumber,
		GasUsed:           tx.Gas(),
		TxHash:            tx.Hash(),
		TransactionIndex:  0,
		Status:            types.ReceiptStatusSuccessful,
	}

	header := &types.Header{
		Number:     big.NewInt(1),
		ParentHash: state.ZeroHash,
		Coinbase:   state.ZeroAddress,
		Root:       state.ZeroHash,
		GasUsed:    1,
		GasLimit:   10,
		Time:       uint64(time.Unix()),
	}
	transactions := []*types.Transaction{tx}

	receipts := []*types.Receipt{receipt}

	// Create block to be able to calculate its hash
	l2Block := types.NewBlock(header, transactions, []*types.Header{}, receipts, &trie.StackTrie{})
	l2Block.ReceivedAt = time

	receipt.BlockHash = l2Block.Hash()

	err = testState.AddL2Block(ctx, batchNumber, l2Block, receipts, dbTx)
	require.NoError(t, err)
	result, err := testState.GetL2BlockByHash(ctx, l2Block.Hash(), dbTx)
	require.NoError(t, err)

	assert.Equal(t, l2Block.Hash(), result.Hash())

	result, err = testState.GetL2BlockByNumber(ctx, l2Block.NumberU64(), dbTx)
	require.NoError(t, err)

	assert.Equal(t, l2Block.Hash(), result.Hash())
	assert.Equal(t, l2Block.ReceivedAt.Unix(), result.ReceivedAt.Unix())
	assert.Equal(t, l2Block.Time(), result.Time())

	require.NoError(t, dbTx.Commit(ctx))
}

/*
func TestExecutorUniswapOutOfCounters(t *testing.T) {
	// Test Case
	type TxHashTestCase struct {
		Hash    string `json:"hash"`
		Encoded string `json:"encoded"`
	}

	var testCases []TxHashTestCase

	jsonFile, err := os.Open(filepath.Clean("test/vectors/src/tx-hash-ethereum/uniswap.json"))
	require.NoError(t, err)
	defer func() { _ = jsonFile.Close() }()

	bytes, err := ioutil.ReadAll(jsonFile)
	require.NoError(t, err)

	err = json.Unmarshal(bytes, &testCases)
	require.NoError(t, err)

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	transactions := make([]types.Transaction, len(testCases))

	for x, testCase := range testCases {
		log.Debugf("Hash:%v", testCase.Hash)
		tx, err := state.DecodeTx(strings.TrimLeft(testCase.Encoded, "0x"))
		require.NoError(t, err)
		transactions[x] = *tx
	}

	var numBatch uint64

	for len(transactions) != 0 {
		numBatch++
		log.Debugf("# of transactions to process= %d", len(transactions))

		batchL2Data, err := state.EncodeTransactions(transactions)
		require.NoError(t, err)

		// Create Batch
		processBatchRequest := &executorclientpb.ProcessBatchRequest{
			BatchNum:         numBatch,
			Coinbase:         common.Address{}.String(),
			BatchL2Data:      batchL2Data,
			OldStateRoot:     stateRoot,
			globalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
			OldLocalExitRoot: common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
			EthTimestamp:     uint64(0),
			UpdateMerkleTree: 1,
		}

		var testCases []TxHashTestCase

		jsonFile, err := os.Open(filepath.Clean("test/vectors/src/tx-hash-ethereum/uniswap.json"))
		require.NoError(t, err)
		defer func() { _ = jsonFile.Close() }()

		bytes, err := ioutil.ReadAll(jsonFile)
		require.NoError(t, err)

		err = json.Unmarshal(bytes, &testCases)
		require.NoError(t, err)

		// Set Genesis
		block := state.Block{
			BlockNumber: 0,
			BlockHash:   state.ZeroHash,
			ParentHash:  state.ZeroHash,
			ReceivedAt:  time.Now(),
		}

		genesis := state.Genesis{
			Actions: []*state.GenesisAction{
				{
					Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
					Type:    int(merkletree.LeafTypeBalance),
					Value:   "100000000000000000000000",
				},
				{
					Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
					Type:    int(merkletree.LeafTypeBalance),
					Value:   "100000000000000000000000",
				},
			},
		}

		initOrResetDB()

		dbTx, err := testState.BeginStateTransaction(ctx)
		require.NoError(t, err)
		stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
		require.NoError(t, err)
		require.NoError(t, dbTx.Commit(ctx))

		transactions := make([]types.Transaction, len(testCases))

		for x, testCase := range testCases {
			log.Debugf("Hash:%v", testCase.Hash)
			tx, err := state.DecodeTx(strings.TrimLeft(testCase.Encoded, "0x"))
			require.NoError(t, err)
			transactions[x] = *tx
		}

		var numBatch uint64

		for len(transactions) != 0 {
			numBatch++
			log.Debugf("# of transactions to process= %d", len(transactions))

			batchL2Data, err := state.EncodeTransactions(transactions)
			require.NoError(t, err)

			// Create Batch
			processBatchRequest := &executorclientpb.ProcessBatchRequest{
				BatchNum:         numBatch,
				Coinbase:         common.Address{}.String(),
				BatchL2Data:      batchL2Data,
				OldStateRoot:     stateRoot,
				globalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
				OldLocalExitRoot: common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
				EthTimestamp:     uint64(0),
				UpdateMerkleTree: 1,
				ChainId:          stateCfg.ChainID,
				ForkId: 		 forkID,
			}

			// Process batch
			processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
			require.NoError(t, err)

			processedTxs := len(processBatchResponse.Responses)

			if int32(processBatchResponse.Responses[processedTxs-1].Error) == executor.ERROR_OUT_OF_COUNTERS {
				newTransactions := transactions[0 : processedTxs-1]
				log.Debugf("# of transactions to reprocess= %d", len(newTransactions))

				batchL2Data, err := state.EncodeTransactions(newTransactions)
				require.NoError(t, err)

				// Create Batch
				processBatchRequest := &executorclientpb.ProcessBatchRequest{
					BatchNum:         numBatch,
					Coinbase:         common.Address{}.String(),
					BatchL2Data:      batchL2Data,
					OldStateRoot:     processBatchResponse.NewStateRoot,
					globalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
					OldLocalExitRoot: common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
					EthTimestamp:     uint64(0),
					UpdateMerkleTree: 1,
				}

				// Process batch
				processBatchResponse, err = executorClient.ProcessBatch(ctx, processBatchRequest)
				require.NoError(t, err)

				processedTxs = len(processBatchResponse.Responses)
			}

			for _, response := range processBatchResponse.Responses {
				require.Equal(t, executor.ERROR_NO_ERROR, int32(response.Error))
			}

			transactions = transactions[processedTxs:]
			stateRoot = processBatchResponse.NewStateRoot
		}
	}
}
*/

func initOrResetDB() {
	if err := dbutils.InitOrResetState(stateDBCfg); err != nil {
		panic(err)
	}
}

func TestExecutorEstimateGas(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetUint64(stateCfg.ChainID)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	var sequencerBalance = 4000000
	scRevertByteCode, err := testutils.ReadBytecode("Revert2/Revert2.bin")
	require.NoError(t, err)

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	nonce := uint64(0)

	// Deploy revert.sol
	tx0 := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scRevertByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx0, err := auth.Signer(auth.From, tx0)
	require.NoError(t, err)

	// Call SC method
	nonce++
	tx1 := types.NewTransaction(nonce, scAddress, new(big.Int), 40000, new(big.Int).SetUint64(1), common.Hex2Bytes("4abbb40a"))
	signedTx1, err := auth.Signer(auth.From, tx1)
	require.NoError(t, err)

	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx0, *signedTx1})
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     stateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 0,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.NotEqual(t, "", processBatchResponse.Responses[0].Error)

	convertedResponse, err := testState.TestConvertToProcessBatchResponse([]types.Transaction{*signedTx0, *signedTx1}, processBatchResponse)
	require.NoError(t, err)
	log.Debugf("%v", len(convertedResponse.Responses))

	// Store processed txs into the batch
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)

	processingContext := state.ProcessingContext{
		BatchNumber:    processBatchRequest.OldBatchNum + 1,
		Coinbase:       common.Address{},
		Timestamp:      time.Now(),
		GlobalExitRoot: common.BytesToHash(processBatchRequest.GlobalExitRoot),
	}

	err = testState.OpenBatch(ctx, processingContext, dbTx)
	require.NoError(t, err)

	err = testState.StoreTransactions(ctx, processBatchRequest.OldBatchNum+1, convertedResponse.Responses, dbTx)
	require.NoError(t, err)

	processingReceipt := state.ProcessingReceipt{
		BatchNumber:   processBatchRequest.OldBatchNum + 1,
		StateRoot:     convertedResponse.NewStateRoot,
		LocalExitRoot: convertedResponse.NewLocalExitRoot,
	}

	err = testState.CloseBatch(ctx, processingReceipt, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// l2BlockNumber := uint64(2)
	nonce++
	tx2 := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scRevertByteCode),
	})
	signedTx2, err := auth.Signer(auth.From, tx2)
	require.NoError(t, err)

	blockNumber, err := testState.GetLastL2BlockNumber(ctx, nil)
	require.NoError(t, err)

	estimatedGas, err := testState.EstimateGas(signedTx2, sequencerAddress, &blockNumber, nil)
	require.NoError(t, err)
	log.Debugf("Estimated gas = %v", estimatedGas)

	nonce++
	tx3 := types.NewTransaction(nonce, scAddress, new(big.Int), 40000, new(big.Int).SetUint64(1), common.Hex2Bytes("4abbb40a"))
	signedTx3, err := auth.Signer(auth.From, tx3)
	require.NoError(t, err)
	_, err = testState.EstimateGas(signedTx3, sequencerAddress, &blockNumber, nil)
	require.Error(t, err)
}

// TODO: Uncomment once the executor properly returns gas refund
/*
func TestExecutorGasRefund(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetInt64(1000)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	var sequencerBalance = 4000000
	scStorageByteCode, err := testutils.ReadBytecode("Storage/Storage.bin")
	require.NoError(t, err)

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// Deploy contract
	tx0 := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scStorageByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx0, err := auth.Signer(auth.From, tx0)
	require.NoError(t, err)

	// Call SC method to set value to 123456
	tx1 := types.NewTransaction(1, scAddress, new(big.Int), 80000, new(big.Int).SetUint64(0), common.Hex2Bytes("6057361d000000000000000000000000000000000000000000000000000000000001e240"))
	signedTx1, err := auth.Signer(auth.From, tx1)
	require.NoError(t, err)

	batchL2Data, err := state.EncodeTransactions([]types.Transaction{*signedTx0, *signedTx1})
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		BatchNum:         1,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     stateRoot,
		globalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldLocalExitRoot: common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId: 		 forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.Equal(t, pb.Error_ERROR_NO_ERROR, processBatchResponse.Responses[0].Error)
	assert.Equal(t, pb.Error_ERROR_NO_ERROR, processBatchResponse.Responses[1].Error)

	// Preparation to be able to estimate gas
	convertedResponse, err := state.TestConvertToProcessBatchResponse([]types.Transaction{*signedTx0, *signedTx1}, processBatchResponse)
	require.NoError(t, err)
	log.Debugf("%v", len(convertedResponse.Responses))

	// Store processed txs into the batch
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)

	processingContext := state.ProcessingContext{
		BatchNumber:    processBatchRequest.BatchNum,
		Coinbase:       common.Address{},
		Timestamp:      time.Now(),
		globalExitRoot: common.BytesToHash(processBatchRequest.globalExitRoot),
	}

	err = testState.OpenBatch(ctx, processingContext, dbTx)
	require.NoError(t, err)

	err = testState.StoreTransactions(ctx, processBatchRequest.BatchNum, convertedResponse.Responses, dbTx)
	require.NoError(t, err)

	processingReceipt := state.ProcessingReceipt{
		BatchNumber:   processBatchRequest.BatchNum,
		StateRoot:     convertedResponse.NewStateRoot,
		LocalExitRoot: convertedResponse.NewLocalExitRoot,
	}

	err = testState.CloseBatch(ctx, processingReceipt, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// Retrieve Value
	tx2 := types.NewTransaction(2, scAddress, new(big.Int), 80000, new(big.Int).SetUint64(0), common.Hex2Bytes("2e64cec1"))
	signedTx2, err := auth.Signer(auth.From, tx2)
	require.NoError(t, err)

	estimatedGas, err := testState.EstimateGas(signedTx2, sequencerAddress, nil, nil)
	require.NoError(t, err)
	log.Debugf("Estimated gas = %v", estimatedGas)

	tx2 = types.NewTransaction(2, scAddress, new(big.Int), estimatedGas, new(big.Int).SetUint64(0), common.Hex2Bytes("2e64cec1"))
	signedTx2, err = auth.Signer(auth.From, tx2)
	require.NoError(t, err)

	batchL2Data, err = state.EncodeTransactions([]types.Transaction{*signedTx2})
	require.NoError(t, err)

	processBatchRequest = &executorclientpb.ProcessBatchRequest{
		BatchNum:         2,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     processBatchResponse.NewStateRoot,
		globalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldLocalExitRoot: common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId: forkID,
	}

	processBatchResponse, err = executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.Equal(t, pb.Error_ERROR_NO_ERROR, processBatchResponse.Responses[0].Error)
	assert.LessOrEqual(t, processBatchResponse.Responses[0].GasUsed, estimatedGas)
	assert.NotEqual(t, uint64(0), processBatchResponse.Responses[0].GasRefunded)
	assert.Equal(t, new(big.Int).SetInt64(123456), new(big.Int).SetBytes(processBatchResponse.Responses[0].ReturnValue))
}
*/

func TestExecutorGasEstimationMultisig(t *testing.T) {
	var chainIDSequencer = new(big.Int).SetInt64(1000)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var erc20SCAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	var multisigSCAddress = common.HexToAddress("0x85e844b762a271022b692cf99ce5c59ba0650ac8")
	var multisigParameter = "00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000004000000000000000000000000617b3a3528F9cDd6630fd3301B9c8911F7Bf063D000000000000000000000000B2D0a21D2b14679331f67F3FAB36366ef2270312000000000000000000000000B2bF7Ef15AFfcd23d99A9FB41a310992a70Ed7720000000000000000000000005b6C62FF5dC5De57e9B1a36B64BE3ef4Ac9b08fb"
	var sequencerBalance = 4000000
	scERC20ByteCode, err := testutils.ReadBytecode("../compiled/ERC20Token/ERC20Token.bin")
	require.NoError(t, err)
	scMultiSigByteCode, err := testutils.ReadBytecode("../compiled/MultiSigWallet/MultiSigWallet.bin")
	require.NoError(t, err)

	// Set Genesis
	block := state.Block{
		BlockNumber: 0,
		BlockHash:   state.ZeroHash,
		ParentHash:  state.ZeroHash,
		ReceivedAt:  time.Now(),
	}

	genesis := state.Genesis{
		Actions: []*state.GenesisAction{
			{
				Address: "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
			{
				Address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
				Type:    int(merkletree.LeafTypeBalance),
				Value:   "100000000000000000000000",
			},
		},
	}

	initOrResetDB()

	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
	stateRoot, err := testState.SetGenesis(ctx, block, genesis, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// Deploy contract
	tx0 := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scERC20ByteCode),
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	signedTx0, err := auth.Signer(auth.From, tx0)
	require.NoError(t, err)

	// Deploy contract
	tx1 := types.NewTx(&types.LegacyTx{
		Nonce:    1,
		To:       nil,
		Value:    new(big.Int),
		Gas:      uint64(sequencerBalance),
		GasPrice: new(big.Int).SetUint64(0),
		Data:     common.Hex2Bytes(scMultiSigByteCode + multisigParameter),
	})

	signedTx1, err := auth.Signer(auth.From, tx1)
	require.NoError(t, err)

	// Transfer Ownership
	tx2 := types.NewTransaction(2, erc20SCAddress, new(big.Int), 80000, new(big.Int).SetUint64(0), common.Hex2Bytes("f2fde38b00000000000000000000000085e844b762a271022b692cf99ce5c59ba0650ac8"))
	signedTx2, err := auth.Signer(auth.From, tx2)
	require.NoError(t, err)

	// Transfer balance to multisig smart contract
	tx3 := types.NewTx(&types.LegacyTx{
		Nonce:    3,
		To:       &multisigSCAddress,
		Value:    new(big.Int).SetUint64(1000000000),
		Gas:      uint64(30000),
		GasPrice: new(big.Int).SetUint64(1),
		Data:     nil,
	})
	signedTx3, err := auth.Signer(auth.From, tx3)
	require.NoError(t, err)

	// Submit Transaction
	tx4 := types.NewTransaction(4, multisigSCAddress, new(big.Int), 150000, new(big.Int).SetUint64(0), common.Hex2Bytes("c64274740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000014352ca32838ab928d9e55bd7d1a39cb7fbd453ab1000000000000000000000000"))
	signedTx4, err := auth.Signer(auth.From, tx4)
	require.NoError(t, err)

	// Confirm transaction
	tx5 := types.NewTransaction(5, multisigSCAddress, new(big.Int), 150000, new(big.Int).SetUint64(0), common.Hex2Bytes("c01a8c840000000000000000000000000000000000000000000000000000000000000000"))
	signedTx5, err := auth.Signer(auth.From, tx5)
	require.NoError(t, err)

	transactions := []types.Transaction{*signedTx0, *signedTx1, *signedTx2, *signedTx3, *signedTx4, *signedTx5}

	batchL2Data, err := state.EncodeTransactions(transactions)
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     stateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[0].Error)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[1].Error)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[2].Error)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[3].Error)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[4].Error)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[5].Error)

	// Check SC code
	// Check Smart Contracts Code
	code, err := stateTree.GetCode(ctx, erc20SCAddress, processBatchResponse.NewStateRoot)
	require.NoError(t, err)
	require.NotEmpty(t, code)
	code, err = stateTree.GetCode(ctx, multisigSCAddress, processBatchResponse.NewStateRoot)
	require.NoError(t, err)
	require.NotEmpty(t, code)

	// Check Smart Contract Balance
	balance, err := stateTree.GetBalance(ctx, multisigSCAddress, processBatchResponse.NewStateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(1000000000), balance.Uint64())

	// Preparation to be able to estimate gas
	convertedResponse, err := testState.TestConvertToProcessBatchResponse(transactions, processBatchResponse)
	require.NoError(t, err)
	log.Debugf("%v", len(convertedResponse.Responses))

	// Store processed txs into the batch
	dbTx, err = testState.BeginStateTransaction(ctx)
	require.NoError(t, err)

	processingContext := state.ProcessingContext{
		BatchNumber:    processBatchRequest.OldBatchNum + 1,
		Coinbase:       common.Address{},
		Timestamp:      time.Now(),
		GlobalExitRoot: common.BytesToHash(processBatchRequest.GlobalExitRoot),
	}

	err = testState.OpenBatch(ctx, processingContext, dbTx)
	require.NoError(t, err)

	err = testState.StoreTransactions(ctx, processBatchRequest.OldBatchNum+1, convertedResponse.Responses, dbTx)
	require.NoError(t, err)

	processingReceipt := state.ProcessingReceipt{
		BatchNumber:   processBatchRequest.OldBatchNum + 1,
		StateRoot:     convertedResponse.NewStateRoot,
		LocalExitRoot: convertedResponse.NewLocalExitRoot,
	}

	err = testState.CloseBatch(ctx, processingReceipt, dbTx)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(ctx))

	// Revoke Confirmation
	tx6 := types.NewTransaction(6, multisigSCAddress, new(big.Int), 50000, new(big.Int).SetUint64(0), common.Hex2Bytes("20ea8d860000000000000000000000000000000000000000000000000000000000000000"))
	signedTx6, err := auth.Signer(auth.From, tx6)
	require.NoError(t, err)

	blockNumber, err := testState.GetLastL2BlockNumber(ctx, nil)
	require.NoError(t, err)

	estimatedGas, err := testState.EstimateGas(signedTx6, sequencerAddress, &blockNumber, nil)
	require.NoError(t, err)
	log.Debugf("Estimated gas = %v", estimatedGas)

	tx6 = types.NewTransaction(6, multisigSCAddress, new(big.Int), estimatedGas, new(big.Int).SetUint64(0), common.Hex2Bytes("20ea8d860000000000000000000000000000000000000000000000000000000000000000"))
	signedTx6, err = auth.Signer(auth.From, tx6)
	require.NoError(t, err)

	batchL2Data, err = state.EncodeTransactions([]types.Transaction{*signedTx6})
	require.NoError(t, err)

	processBatchRequest = &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      1,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     processBatchResponse.NewStateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 1,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err = executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)
	assert.Equal(t, executorclientpb.RomError_ROM_ERROR_NO_ERROR, processBatchResponse.Responses[0].Error)
	log.Debugf("Used gas = %v", processBatchResponse.Responses[0].GasUsed)
}

func TestExecuteWithoutUpdatingMT(t *testing.T) {
	// Init database instance
	initOrResetDB()

	var chainIDSequencer = new(big.Int).SetUint64(stateCfg.ChainID)
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var sequencerPvtKey = "0x28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e"
	var gasLimit = uint64(4000000)
	var scAddress = common.HexToAddress("0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98")
	scByteCode, err := testutils.ReadBytecode("Counter/Counter.bin")
	require.NoError(t, err)

	// auth
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(sequencerPvtKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainIDSequencer)
	require.NoError(t, err)

	// signed tx to deploy SC
	unsignedTxDeploy := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       nil,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     common.Hex2Bytes(scByteCode),
	})
	signedTxDeploy, err := auth.Signer(auth.From, unsignedTxDeploy)
	require.NoError(t, err)

	signedTxs := []types.Transaction{
		*signedTxDeploy,
	}

	batchL2Data, err := state.EncodeTransactions(signedTxs)
	require.NoError(t, err)

	// Create Batch
	processBatchRequest := &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      0,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 0,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err := executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)

	// assert signed tx do deploy sc
	assert.Equal(t, executorclientpb.RomError(1), processBatchResponse.Responses[0].Error)
	assert.Equal(t, scAddress, common.HexToAddress(processBatchResponse.Responses[0].CreateAddress))

	log.Debug(processBatchResponse)

	incrementFnSignature := crypto.Keccak256Hash([]byte("increment()")).Bytes()[:4]
	retrieveFnSignature := crypto.Keccak256Hash([]byte("getCount()")).Bytes()[:4]

	// signed tx to call SC
	unsignedTxFirstIncrement := types.NewTx(&types.LegacyTx{
		Nonce:    1,
		To:       &scAddress,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     incrementFnSignature,
	})

	signedTxFirstIncrement, err := auth.Signer(auth.From, unsignedTxFirstIncrement)
	require.NoError(t, err)

	unsignedTxFirstRetrieve := types.NewTx(&types.LegacyTx{
		Nonce:    2,
		To:       &scAddress,
		Value:    new(big.Int),
		Gas:      gasLimit,
		GasPrice: new(big.Int),
		Data:     retrieveFnSignature,
	})

	signedTxFirstRetrieve, err := auth.Signer(auth.From, unsignedTxFirstRetrieve)
	require.NoError(t, err)

	signedTxs2 := []types.Transaction{
		*signedTxFirstIncrement,
		*signedTxFirstRetrieve,
	}

	batchL2Data2, err := state.EncodeTransactions(signedTxs2)
	require.NoError(t, err)

	// Create Batch 2
	processBatchRequest = &executorclientpb.ProcessBatchRequest{
		OldBatchNum:      1,
		Coinbase:         sequencerAddress.String(),
		BatchL2Data:      batchL2Data2,
		OldStateRoot:     processBatchResponse.NewStateRoot,
		GlobalExitRoot:   common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		OldAccInputHash:  common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000"),
		EthTimestamp:     uint64(time.Now().Unix()),
		UpdateMerkleTree: 0,
		ChainId:          stateCfg.ChainID,
		ForkId:           forkID,
	}

	processBatchResponse, err = executorClient.ProcessBatch(ctx, processBatchRequest)
	require.NoError(t, err)

	log.Debug(processBatchResponse)

	// assert signed tx to increment counter
	assert.Equal(t, executorclientpb.RomError(1), processBatchResponse.Responses[0].Error)

	// assert signed tx to increment counter
	assert.Equal(t, executorclientpb.RomError(1), processBatchResponse.Responses[1].Error)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001", hex.EncodeToString(processBatchResponse.Responses[1].ReturnValue))
}

func TestExecutorUnsignedTransactionsWithCorrectL2BlockStateRoot(t *testing.T) {
	// Init database instance
	initOrResetDB()

	// auth
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(operations.DefaultSequencerPrivateKey, "0x"))
	require.NoError(t, err)
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, new(big.Int).SetUint64(stateCfg.ChainID))
	require.NoError(t, err)

	auth.Nonce = big.NewInt(0)
	auth.Value = nil
	auth.GasPrice = big.NewInt(0)
	auth.GasLimit = uint64(4000000)
	auth.NoSend = true

	_, scTx, sc, err := Counter.DeployCounter(auth, &ethclient.Client{})
	require.NoError(t, err)

	auth.Nonce = big.NewInt(1)
	tx1, err := sc.Increment(auth)
	require.NoError(t, err)

	auth.Nonce = big.NewInt(2)
	tx2, err := sc.Increment(auth)
	require.NoError(t, err)

	auth.Nonce = big.NewInt(3)
	tx3, err := sc.Increment(auth)
	require.NoError(t, err)

	dbTx, err := testState.BeginStateTransaction(context.Background())
	require.NoError(t, err)
	// Set genesis
	genesis := state.Genesis{Actions: []*state.GenesisAction{
		{
			Address: operations.DefaultSequencerAddress,
			Type:    int(merkletree.LeafTypeBalance),
			Value:   "100000000000000000000000",
		},
	}}
	_, err = testState.SetGenesis(ctx, state.Block{}, genesis, dbTx)
	require.NoError(t, err)
	batchCtx := state.ProcessingContext{
		BatchNumber: 1,
		Coinbase:    common.HexToAddress(operations.DefaultSequencerAddress),
		Timestamp:   time.Now(),
	}
	err = testState.OpenBatch(context.Background(), batchCtx, dbTx)
	require.NoError(t, err)
	signedTxs := []types.Transaction{
		*scTx,
		*tx1,
		*tx2,
		*tx3,
	}

	batchL2Data, err := state.EncodeTransactions(signedTxs)
	require.NoError(t, err)

	processBatchResponse, err := testState.ProcessSequencerBatch(context.Background(), 1, batchL2Data, metrics.SequencerCallerLabel, dbTx)
	require.NoError(t, err)
	// assert signed tx do deploy sc
	assert.Nil(t, processBatchResponse.Responses[0].RomError)
	assert.NotEqual(t, state.ZeroAddress, processBatchResponse.Responses[0].CreateAddress.Hex())
	assert.Equal(t, tx1.To().Hex(), processBatchResponse.Responses[0].CreateAddress.Hex())

	// assert signed tx to increment counter
	assert.Nil(t, processBatchResponse.Responses[1].RomError)
	assert.Nil(t, processBatchResponse.Responses[2].RomError)
	assert.Nil(t, processBatchResponse.Responses[3].RomError)

	// Add txs to DB
	err = testState.StoreTransactions(context.Background(), 1, processBatchResponse.Responses, dbTx)
	require.NoError(t, err)
	// Close batch
	err = testState.CloseBatch(
		context.Background(),
		state.ProcessingReceipt{
			BatchNumber:   1,
			StateRoot:     processBatchResponse.NewStateRoot,
			LocalExitRoot: processBatchResponse.NewLocalExitRoot,
		}, dbTx,
	)
	require.NoError(t, err)
	require.NoError(t, dbTx.Commit(context.Background()))

	getCountFnSignature := crypto.Keccak256Hash([]byte("getCount()")).Bytes()[:4]
	getCountUnsignedTx := types.NewTx(&types.LegacyTx{
		To:   &processBatchResponse.Responses[0].CreateAddress,
		Gas:  uint64(100000),
		Data: getCountFnSignature,
	})

	l2BlockNumber := uint64(1)
	result, err := testState.ProcessUnsignedTransaction(context.Background(), getCountUnsignedTx, auth.From, &l2BlockNumber, true, nil)
	require.NoError(t, err)
	// assert unsigned tx
	assert.Nil(t, result.Err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", hex.EncodeToString(result.ReturnValue))

	l2BlockNumber = uint64(2)
	result, err = testState.ProcessUnsignedTransaction(context.Background(), getCountUnsignedTx, auth.From, &l2BlockNumber, true, nil)
	require.NoError(t, err)
	// assert unsigned tx
	assert.Nil(t, result.Err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001", hex.EncodeToString(result.ReturnValue))

	l2BlockNumber = uint64(3)
	result, err = testState.ProcessUnsignedTransaction(context.Background(), getCountUnsignedTx, auth.From, &l2BlockNumber, true, nil)
	require.NoError(t, err)
	// assert unsigned tx
	assert.Nil(t, result.Err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000002", hex.EncodeToString(result.ReturnValue))

	l2BlockNumber = uint64(4)
	result, err = testState.ProcessUnsignedTransaction(context.Background(), getCountUnsignedTx, auth.From, &l2BlockNumber, true, nil)
	require.NoError(t, err)
	// assert unsigned tx
	assert.Nil(t, result.Err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000003", hex.EncodeToString(result.ReturnValue))
}
