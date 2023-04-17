package state_test

import (
	"context"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/hex"
	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	pgStateStorage *state.PostgresStorage
	block          = &state.Block{
		BlockNumber: 1,
		BlockHash:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ParentHash:  common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		ReceivedAt:  time.Now(),
	}
)

func setup() {
	pgStateStorage = state.NewPostgresStorage(stateDb)
}

func TestGetBatchByL2BlockNumber(t *testing.T) {
	setup()
	ctx := context.Background()
	dbTx, err := testState.BeginStateTransaction(ctx)
	require.NoError(t, err)
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
	receipt.BlockHash = l2Block.Hash()

	err = pgStateStorage.AddL2Block(ctx, batchNumber, l2Block, receipts, dbTx)
	require.NoError(t, err)
	result, err := pgStateStorage.BatchNumberByL2BlockNumber(ctx, l2Block.Number().Uint64(), dbTx)
	require.NoError(t, err)
	assert.Equal(t, batchNumber, result)
	require.NoError(t, dbTx.Commit(ctx))
}

func TestAddAndGetSequences(t *testing.T) {
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

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (0)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (1)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (2)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (3)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (4)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (5)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (6)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (7)")
	require.NoError(t, err)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (8)")
	require.NoError(t, err)

	sequence := state.Sequence{
		FromBatchNumber: 0,
		ToBatchNumber:   3,
	}
	err = testState.AddSequence(ctx, sequence, dbTx)
	require.NoError(t, err)

	sequence2 := state.Sequence{
		FromBatchNumber: 3,
		ToBatchNumber:   7,
	}
	err = testState.AddSequence(ctx, sequence2, dbTx)
	require.NoError(t, err)

	sequence3 := state.Sequence{
		FromBatchNumber: 7,
		ToBatchNumber:   8,
	}
	err = testState.AddSequence(ctx, sequence3, dbTx)
	require.NoError(t, err)

	sequences, err := testState.GetSequences(ctx, 0, dbTx)
	require.NoError(t, err)
	require.Equal(t, 3, len(sequences))
	require.Equal(t, uint64(0), sequences[0].FromBatchNumber)
	require.Equal(t, uint64(3), sequences[1].FromBatchNumber)
	require.Equal(t, uint64(7), sequences[2].FromBatchNumber)
	require.Equal(t, uint64(3), sequences[0].ToBatchNumber)
	require.Equal(t, uint64(7), sequences[1].ToBatchNumber)
	require.Equal(t, uint64(8), sequences[2].ToBatchNumber)

	sequences, err = testState.GetSequences(ctx, 3, dbTx)
	require.NoError(t, err)
	require.Equal(t, 2, len(sequences))
	require.Equal(t, uint64(3), sequences[0].FromBatchNumber)
	require.Equal(t, uint64(7), sequences[1].FromBatchNumber)
	require.Equal(t, uint64(7), sequences[0].ToBatchNumber)
	require.Equal(t, uint64(8), sequences[1].ToBatchNumber)

	require.NoError(t, dbTx.Commit(ctx))
}

func TestAddGlobalExitRoot(t *testing.T) {
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
	globalExitRoot := state.GlobalExitRoot{
		BlockNumber:     1,
		MainnetExitRoot: common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		RollupExitRoot:  common.HexToHash("0x30a885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9a0"),
		GlobalExitRoot:  common.HexToHash("0x40a885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9a0"),
	}
	err = testState.AddGlobalExitRoot(ctx, &globalExitRoot, tx)
	require.NoError(t, err)
	exit, _, err := testState.GetLatestGlobalExitRoot(ctx, math.MaxInt64, tx)
	require.NoError(t, err)
	err = tx.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, globalExitRoot.BlockNumber, exit.BlockNumber)
	assert.Equal(t, globalExitRoot.MainnetExitRoot, exit.MainnetExitRoot)
	assert.Equal(t, globalExitRoot.RollupExitRoot, exit.RollupExitRoot)
	assert.Equal(t, globalExitRoot.GlobalExitRoot, exit.GlobalExitRoot)
}

func TestVerifiedBatch(t *testing.T) {
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
	//require.NoError(t, tx.Commit(ctx))

	lastBlock, err := testState.GetLastBlock(ctx, dbTx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lastBlock.BlockNumber)

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (1)")

	require.NoError(t, err)
	virtualBatch := state.VirtualBatch{
		BlockNumber: 1,
		BatchNumber: 1,
		TxHash:      common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
	}
	err = testState.AddVirtualBatch(ctx, &virtualBatch, dbTx)
	require.NoError(t, err)
	expectedVerifiedBatch := state.VerifiedBatch{
		BlockNumber: 1,
		BatchNumber: 1,
		StateRoot:   common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f2"),
		Aggregator:  common.HexToAddress("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		TxHash:      common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
		IsTrusted:   true,
	}
	err = testState.AddVerifiedBatch(ctx, &expectedVerifiedBatch, dbTx)
	require.NoError(t, err)

	// Step to create done, retrieve it

	actualVerifiedBatch, err := testState.GetVerifiedBatch(ctx, 1, dbTx)
	require.NoError(t, err)
	require.Equal(t, expectedVerifiedBatch, *actualVerifiedBatch)

	require.NoError(t, dbTx.Commit(ctx))
}

func TestAddAccumulatedInputHash(t *testing.T) {
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

	_, err = testState.PostgresStorage.Exec(ctx, `INSERT INTO state.batch
	(batch_num, global_exit_root, local_exit_root, state_root, timestamp, coinbase, raw_txs_data)
	VALUES(1, '0x0000000000000000000000000000000000000000000000000000000000000000', '0x0000000000000000000000000000000000000000000000000000000000000000', '0xbf34f9a52a63229e90d1016011655bc12140bba5b771817b88cbf340d08dcbde', '2022-12-19 08:17:45.000', '0x0000000000000000000000000000000000000000', NULL);
	`)
	require.NoError(t, err)

	accInputHash := common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f2")
	batchNum := uint64(1)
	err = testState.AddAccumulatedInputHash(ctx, batchNum, accInputHash, dbTx)
	require.NoError(t, err)

	b, err := testState.GetBatchByNumber(ctx, batchNum, dbTx)
	require.NoError(t, err)
	assert.Equal(t, b.BatchNumber, batchNum)
	assert.Equal(t, b.AccInputHash, accInputHash)
	require.NoError(t, dbTx.Commit(ctx))
}

func TestForcedBatch(t *testing.T) {
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
	rtx := "29e885edaf8e4b51e1d2e05f9da28000000000000000000000000000000000000000000000000000000161d2fb4f6b1d53827d9b80a23cf2d7d9f1"
	raw, err := hex.DecodeString(rtx)
	assert.NoError(t, err)
	forcedBatch := state.ForcedBatch{
		BlockNumber:       1,
		ForcedBatchNumber: 1,
		Sequencer:         common.HexToAddress("0x2536C2745Ac4A584656A830f7bdCd329c94e8F30"),
		RawTxsData:        raw,
		ForcedAt:          time.Now(),
		GlobalExitRoot:    common.HexToHash("0x40a885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9a0"),
	}
	err = testState.AddForcedBatch(ctx, &forcedBatch, tx)
	require.NoError(t, err)
	fb, err := testState.GetForcedBatch(ctx, 1, tx)
	require.NoError(t, err)
	err = tx.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, forcedBatch.BlockNumber, fb.BlockNumber)
	assert.Equal(t, forcedBatch.ForcedBatchNumber, fb.ForcedBatchNumber)
	assert.Equal(t, forcedBatch.Sequencer, fb.Sequencer)
	assert.Equal(t, forcedBatch.RawTxsData, fb.RawTxsData)
	assert.Equal(t, rtx, common.Bytes2Hex(fb.RawTxsData))
	assert.Equal(t, forcedBatch.ForcedAt.Unix(), fb.ForcedAt.Unix())
	assert.Equal(t, forcedBatch.GlobalExitRoot, fb.GlobalExitRoot)
}
func TestCleanupLockedProofs(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	initOrResetDB()
	ctx := context.Background()
	batchNumber := uint64(42)
	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES ($1), ($2), ($3)", batchNumber, batchNumber+1, batchNumber+2)
	require.NoError(err)
	const addGeneratedProofSQL = "INSERT INTO state.proof (batch_num, batch_num_final, proof, proof_id, input_prover, prover, prover_id, generating_since, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	// proof with `generating_since` older than interval
	now := time.Now().Round(time.Microsecond)
	oneHourAgo := now.Add(-time.Hour).Round(time.Microsecond)
	olderProofID := "olderProofID"
	olderProof := state.Proof{
		ProofID:          &olderProofID,
		BatchNumber:      batchNumber,
		BatchNumberFinal: batchNumber,
		GeneratingSince:  &oneHourAgo,
	}
	_, err := testState.PostgresStorage.Exec(ctx, addGeneratedProofSQL, olderProof.BatchNumber, olderProof.BatchNumberFinal, olderProof.Proof, olderProof.ProofID, olderProof.InputProver, olderProof.Prover, olderProof.ProverID, olderProof.GeneratingSince, oneHourAgo, oneHourAgo)
	require.NoError(err)
	// proof with `generating_since` newer than interval
	newerProofID := "newerProofID"
	newerProof := state.Proof{
		ProofID:          &newerProofID,
		BatchNumber:      batchNumber + 1,
		BatchNumberFinal: batchNumber + 1,
		GeneratingSince:  &now,
		CreatedAt:        oneHourAgo,
		UpdatedAt:        now,
	}
	_, err = testState.PostgresStorage.Exec(ctx, addGeneratedProofSQL, newerProof.BatchNumber, newerProof.BatchNumberFinal, newerProof.Proof, newerProof.ProofID, newerProof.InputProver, newerProof.Prover, newerProof.ProverID, newerProof.GeneratingSince, oneHourAgo, now)
	require.NoError(err)
	// proof with `generating_since` nil (currently not generating)
	olderNotGenProofID := "olderNotGenProofID"
	olderNotGenProof := state.Proof{
		ProofID:          &olderNotGenProofID,
		BatchNumber:      batchNumber + 2,
		BatchNumberFinal: batchNumber + 2,
		CreatedAt:        oneHourAgo,
		UpdatedAt:        oneHourAgo,
	}
	_, err = testState.PostgresStorage.Exec(ctx, addGeneratedProofSQL, olderNotGenProof.BatchNumber, olderNotGenProof.BatchNumberFinal, olderNotGenProof.Proof, olderNotGenProof.ProofID, olderNotGenProof.InputProver, olderNotGenProof.Prover, olderNotGenProof.ProverID, olderNotGenProof.GeneratingSince, oneHourAgo, oneHourAgo)
	require.NoError(err)

	_, err = testState.CleanupLockedProofs(ctx, "1m", nil)

	require.NoError(err)
	rows, err := testState.PostgresStorage.Query(ctx, "SELECT batch_num, batch_num_final, proof, proof_id, input_prover, prover, prover_id, generating_since, created_at, updated_at FROM state.proof")
	require.NoError(err)
	proofs := make([]state.Proof, 0, len(rows.RawValues()))
	for rows.Next() {
		var proof state.Proof
		err := rows.Scan(
			&proof.BatchNumber,
			&proof.BatchNumberFinal,
			&proof.Proof,
			&proof.ProofID,
			&proof.InputProver,
			&proof.Prover,
			&proof.ProverID,
			&proof.GeneratingSince,
			&proof.CreatedAt,
			&proof.UpdatedAt,
		)
		require.NoError(err)
		proofs = append(proofs, proof)
	}
	assert.Len(proofs, 2)
	assert.Contains(proofs, olderNotGenProof)
	assert.Contains(proofs, newerProof)
}

func TestVirtualBatch(t *testing.T) {
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
	//require.NoError(t, tx.Commit(ctx))

	lastBlock, err := testState.GetLastBlock(ctx, dbTx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lastBlock.BlockNumber)

	_, err = testState.PostgresStorage.Exec(ctx, "INSERT INTO state.batch (batch_num) VALUES (1)")

	require.NoError(t, err)
	addr := common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	virtualBatch := state.VirtualBatch{
		BlockNumber:   1,
		BatchNumber:   1,
		Coinbase:      addr,
		SequencerAddr: addr,
		TxHash:        common.HexToHash("0x29e885edaf8e4b51e1d2e05f9da28161d2fb4f6b1d53827d9b80a23cf2d7d9f1"),
	}
	err = testState.AddVirtualBatch(ctx, &virtualBatch, dbTx)
	require.NoError(t, err)

	actualVirtualBatch, err := testState.GetVirtualBatch(ctx, 1, dbTx)
	require.NoError(t, err)
	require.Equal(t, virtualBatch, *actualVirtualBatch)
	require.NoError(t, dbTx.Commit(ctx))
}
