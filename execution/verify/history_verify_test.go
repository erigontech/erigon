package verify_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/verify"
)

// TestHistoryVerification_SimpleBlocks is an integration test that generates blocks
// with state changes, builds snapshot files, and verifies history via re-execution.
func TestHistoryVerification_SimpleBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping history verification integration test in short mode")
	}

	const stepSize = 10

	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	chainConfig.TerminalTotalDifficulty = common.Big0
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
	)
	ctx := context.Background()
	logger := log.New()

	// Generate blocks — coinbase rewards create account state changes.
	const numBlocks = 200
	parent := m.Genesis
	const batchSize = 100
	for batchStart := 0; batchStart < numBlocks; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > numBlocks {
			batchEnd = numBlocks
		}
		chainResult, err := blockgen.GenerateChain(m.ChainConfig, parent, m.Engine, m.DB, batchEnd-batchStart, func(i int, b *blockgen.BlockGen) {
			b.SetCoinbase(common.Address{1})
		})
		require.NoError(t, err)
		require.NoError(t, m.InsertChain(chainResult))
		parent = chainResult.TopBlock
	}
	t.Logf("Inserted %d blocks", numBlocks)

	agg := m.DB.(state.HasAgg).Agg().(*state.Aggregator)

	// Get last txNum.
	tx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	_, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	t.Logf("Last txNum: %d, stepSize: %d, steps: %d", lastTxNum, agg.StepSize(), lastTxNum/agg.StepSize())

	// Build files.
	err = agg.BuildFiles(lastTxNum)
	require.NoError(t, err)

	if agg.EndTxNumMinimax() == 0 {
		t.Skip("No files built (memory backend); skipping history verification integration test")
	}

	// Now run the history verifier on the built file ranges.
	historyVerifier := verify.NewHistoryVerifier(m.BlockReader, m.ChainConfig, m.Engine, 2, logger)

	// Get the step size and verify each non-base step.
	stepSz := agg.StepSize()
	maxStep := agg.EndTxNumMinimax() / stepSz
	for step := uint64(1); step < maxStep; step++ {
		fromTxNum := step * stepSz
		toTxNum := (step + 1) * stepSz
		t.Logf("Verifying history step %d [%d, %d)", step, fromTxNum, toTxNum)

		err := historyVerifier(ctx, m.DB, fromTxNum, toTxNum)
		require.NoError(t, err, "history verification failed for step %d [%d, %d)", step, fromTxNum, toTxNum)
	}

	t.Logf("History verification passed for %d steps", maxStep-1)
}

// TestHistoryVerification_WithUserTransactions exercises history verification with
// blocks containing ETH transfers, which create account balance/nonce changes.
func TestHistoryVerification_WithUserTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping history verification with user transactions test in short mode")
	}

	const stepSize = 7

	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	chainConfig.TerminalTotalDifficulty = common.Big0
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
	)
	ctx := context.Background()
	logger := log.New()
	signer := types.LatestSignerForChainID(chainConfig.ChainID)

	// Generate blocks with ETH transfers.
	const numBlocks = 50
	nonce := uint64(0)
	recipient := common.Address{0xDE, 0xAD}

	chainResult, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, numBlocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// Add 2 ETH transfers per block.
		for j := 0; j < 2; j++ {
			tx := types.NewTransaction(nonce, recipient, uint256.NewInt(1), 21000, uint256.NewInt(875000000), nil)
			signed, signErr := types.SignTx(tx, *signer, key)
			if signErr != nil {
				t.Fatal(signErr)
			}
			b.AddTx(signed)
			nonce++
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainResult))
	t.Logf("Inserted %d blocks with 2 user txs each", numBlocks)

	agg := m.DB.(state.HasAgg).Agg().(*state.Aggregator)

	// Get last txNum.
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	_, lastTxNum, err := rawdbv3.TxNums.Last(roTx)
	require.NoError(t, err)
	t.Logf("Last txNum: %d, stepSize: %d, steps: %d", lastTxNum, agg.StepSize(), lastTxNum/agg.StepSize())

	// Build files.
	err = agg.BuildFiles(lastTxNum)
	require.NoError(t, err)

	if agg.EndTxNumMinimax() == 0 {
		t.Skip("No files built (memory backend); skipping history verification integration test")
	}

	// Run the history verifier.
	historyVerifier := verify.NewHistoryVerifier(m.BlockReader, m.ChainConfig, m.Engine, 2, logger)

	stepSz := agg.StepSize()
	maxStep := agg.EndTxNumMinimax() / stepSz
	for step := uint64(1); step < maxStep; step++ {
		fromTxNum := step * stepSz
		toTxNum := (step + 1) * stepSz
		t.Logf("Verifying history step %d [%d, %d)", step, fromTxNum, toTxNum)

		err := historyVerifier(ctx, m.DB, fromTxNum, toTxNum)
		require.NoError(t, err, "history verification failed for step %d [%d, %d)", step, fromTxNum, toTxNum)
	}

	t.Logf("History verification passed for %d steps", maxStep-1)
}
