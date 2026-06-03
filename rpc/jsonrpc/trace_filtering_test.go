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
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	protocolrules "github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

// TestCallBlockParallelMatchesSequential verifies that doCallBlockParallel and
// doCallBlock produce identical trace output for the same block.  Block 6 in
// the test chain has 32 transactions, which is large enough to exercise the
// worker-pool path of doCallBlockParallel.
func TestCallBlockParallelMatchesSequential(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	ctx := context.Background()
	const blockNum = uint64(6) // block 6 has 32 txs (case i=5 in test chain generation)
	traceTypes := []string{TraceTypeTrace}

	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	block, err := m.BlockReader.BlockByNumber(ctx, tx, blockNum)
	require.NoError(t, err)
	require.NotNil(t, block, "block %d not found", blockNum)

	txs := block.Transactions()
	require.Greater(t, len(txs), 1, "block %d must have multiple txs to exercise parallel path", blockNum)

	cfg, err := api.chainConfig(ctx, tx)
	require.NoError(t, err)

	header := block.Header()
	engine := api.engine()
	signer := types.MakeSigner(cfg, blockNum, block.Time())

	// Derive chain rules from the block context (matches how callBlock does it).
	blockCtx := transactions.NewEVMBlockContext(engine, header, true /* requireCanonical */, tx, api._blockReader, cfg)
	rules := blockCtx.Rules(cfg)

	// Build msgs and callParams — identical inputs for both paths.
	msgs := make([]*types.Message, len(txs))
	callParams := make([]TraceCallParam, len(txs))
	for i, txn := range txs {
		txHash := txn.Hash()
		msg, msgErr := txn.AsMessage(*signer, header.BaseFee, rules)
		require.NoError(t, msgErr, "tx %d: AsMessage", i)
		msgs[i] = msg
		callParams[i] = TraceCallParam{txHash: &txHash, traceTypes: traceTypes}
	}

	pNo := blockNum - 1
	parentNo := rpc.BlockNumber(pNo)
	parentHash := block.ParentHash()
	parentNrOrHash := rpc.BlockNumberOrHash{
		BlockNumber:      &parentNo,
		BlockHash:        &parentHash,
		RequireCanonical: true,
	}

	// Set up the state reader at the parent block boundary (= pre-block-6 state).
	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, parentNrOrHash, 0,
		api.filters, api.stateCache, api._txNumReader)
	require.NoError(t, err)

	hsr, ok := stateReader.(state.HistoricalStateReader)
	require.True(t, ok, "expected HistoricalStateReader")
	baseTxNum := hsr.GetTxNum()

	sc := shards.NewStateCache(32, 0)
	cachedReader := state.NewCachedReader(stateReader, sc)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, sc)
	ibs := state.New(cachedReader)

	consensusHeaderReader := consensuschain.NewReader(cfg, tx, api._blockReader, nil)
	logger := log.New("trace_filtering_test")
	err = protocol.InitializeBlockExecution(engine.(protocolrules.Engine), consensusHeaderReader,
		block.HeaderNoCopy(), cfg, ibs, nil, logger, nil)
	require.NoError(t, err)
	err = ibs.CommitBlock(rules, cachedWriter)
	require.NoError(t, err)

	// Parallel path — creates its own MDBX read transactions per worker; does
	// not modify stateReader/ibs, so it is safe to call first.
	parallelResults, err := api.doCallBlockParallel(ctx, tx, baseTxNum, txs, msgs, callParams, header, false, nil)
	require.NoError(t, err)
	require.Len(t, parallelResults, len(txs))

	// Sequential path — uses the stateReader/ibs prepared above.
	sequentialResults, _, err := api.doCallBlock(ctx, tx, stateReader, sc, cachedWriter, ibs, txs, msgs,
		callParams, &parentNrOrHash, header, false, nil)
	require.NoError(t, err)
	require.Len(t, sequentialResults, len(txs))

	// Both paths must produce identical trace output for every transaction.
	for i := range parallelResults {
		parJSON, jsonErr := json.Marshal(parallelResults[i].Trace)
		require.NoError(t, jsonErr)
		seqJSON, jsonErr := json.Marshal(sequentialResults[i].Trace)
		require.NoError(t, jsonErr)
		require.Equal(t, string(seqJSON), string(parJSON),
			"tx %d: parallel and sequential traces differ", i)
	}
}

// chainWithWithdrawal builds a one-block PoS chain (Shanghai enabled) that
// includes a single beacon-chain withdrawal of withdrawalGwei to withdrawalAddr.
func chainWithWithdrawal(t *testing.T, withdrawalAddr common.Address, withdrawalGwei uint64) (*execmoduletester.ExecModuleTester, *types.Block) {
	t.Helper()
	bankFunds, _ := new(big.Int).SetString("100000000000000000000", 10)
	gspec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc:  types.GenesisAlloc{withdrawalAddr: {Balance: bankFunds}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
	generated, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(_ int, b *blockgen.BlockGen) {
		b.AddWithdrawal(&types.Withdrawal{
			Index:     0,
			Validator: 42,
			Address:   withdrawalAddr,
			Amount:    withdrawalGwei,
		})
	})
	require.NoError(t, err)
	err = m.InsertChain(generated)
	require.NoError(t, err)
	return m, generated.Blocks[0]
}

// TestBlockWithdrawalTraceEntries verifies that trace_block includes a
// "reward"/"withdrawal" entry for each beacon-chain withdrawal in the block.
func TestBlockWithdrawalTraceEntries(t *testing.T) {
	const withdrawalGwei = uint64(32_000_000) // 32 ETH in Gwei
	withdrawalAddr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	m, blk := chainWithWithdrawal(t, withdrawalAddr, withdrawalGwei)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	n := rpc.BlockNumber(blk.NumberU64())
	traces, err := api.Block(context.Background(), n, new(bool), nil)
	require.NoError(t, err)
	require.NotNil(t, traces)

	var wdTraces []ParityTrace
	for _, tr := range traces {
		action, ok := tr.Action.(*RewardTraceAction)
		if ok && action.RewardType == rewardTypeWithdrawal {
			wdTraces = append(wdTraces, tr)
		}
	}
	require.Len(t, wdTraces, 1, "expected one withdrawal trace entry")

	action := wdTraces[0].Action.(*RewardTraceAction)
	require.Equal(t, withdrawalAddr, action.Author)
	require.Equal(t, rewardTypeWithdrawal, action.RewardType)

	expectedWei := new(big.Int).Mul(new(big.Int).SetUint64(withdrawalGwei), big.NewInt(1e9))
	require.Equal(t, expectedWei, action.Value.ToInt())
	require.Equal(t, blk.NumberU64(), *wdTraces[0].BlockNumber)
	require.Equal(t, rewardTraceType, wdTraces[0].Type)

	// Withdrawal entries must not carry a transaction hash or position.
	require.Nil(t, wdTraces[0].TransactionHash)
	require.Nil(t, wdTraces[0].TransactionPosition)
}

// TestBlockWithdrawalNoEntriesPreShanghai verifies that blocks before Shanghai
// (no withdrawals) do not produce any withdrawal trace entries.
func TestBlockWithdrawalNoEntriesPreShanghai(t *testing.T) {
	// Default test chain uses TestChainBerlinConfig which has no ShanghaiTime.
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	// Block 1 is pre-Shanghai on the default test chain.
	n := rpc.BlockNumber(1)
	traces, err := api.Block(context.Background(), n, new(bool), nil)
	require.NoError(t, err)

	for _, tr := range traces {
		action, ok := tr.Action.(*RewardTraceAction)
		require.False(t, ok && action.RewardType == rewardTypeWithdrawal,
			"unexpected withdrawal trace entry in pre-Shanghai block")
	}
}

// TestReplayBlockTransactionsWithdrawalStateDiff verifies that
// trace_replayBlockTransactions returns a synthetic final entry whose
// stateDiff captures the balance increase from beacon-chain withdrawals.
func TestReplayBlockTransactionsWithdrawalStateDiff(t *testing.T) {
	const withdrawalGwei = uint64(1_000_000) // 1 ETH in Gwei
	withdrawalAddr := common.HexToAddress("0xcafecafecafecafecafecafecafecafecafecafe")

	m, blk := chainWithWithdrawal(t, withdrawalAddr, withdrawalGwei)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	n := rpc.BlockNumber(blk.NumberU64())
	bnOrHash := rpc.BlockNumberOrHash{BlockNumber: &n}
	results, err := api.ReplayBlockTransactions(context.Background(), bnOrHash, []string{"stateDiff"}, new(bool), nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// The last entry is the synthetic withdrawal entry (block has no real txs).
	last := results[len(results)-1]
	require.NotNil(t, last.StateDiff)

	internedAddr := internedAddress(withdrawalAddr.Hex())
	wdDiff, ok := last.StateDiff[internedAddr]
	require.True(t, ok, "withdrawal address not found in synthetic stateDiff entry")

	balDiff, ok := wdDiff.Balance.(*StateDiffBalance)
	require.True(t, ok, "balance diff has unexpected type: %T", wdDiff.Balance)

	expectedAmountWei := new(big.Int).Mul(new(big.Int).SetUint64(withdrawalGwei), big.NewInt(1e9))
	actualFrom := balDiff.From.ToInt()
	actualTo := balDiff.To.ToInt()
	delta := new(big.Int).Sub(actualTo, actualFrom)
	require.Equal(t, expectedAmountWei, delta,
		"withdrawal balance delta mismatch: from=%s to=%s expected delta=%s", actualFrom, actualTo, expectedAmountWei)

	// Code and nonce must be "=" (unchanged).
	require.Equal(t, "=", wdDiff.Code)
	require.Equal(t, "=", wdDiff.Nonce)
}

// TestReplayBlockTransactionsMultiWithdrawalSameAddr verifies that multiple
// withdrawals to the same address are collapsed into a single stateDiff entry.
func TestReplayBlockTransactionsMultiWithdrawalSameAddr(t *testing.T) {
	const (
		wd1Gwei = uint64(500_000)
		wd2Gwei = uint64(300_000)
	)
	withdrawalAddr := common.HexToAddress("0xbebebebebebebebebebebebebebebebebebebebe")
	bankFunds, _ := new(big.Int).SetString("100000000000000000000", 10)
	gspec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc:  types.GenesisAlloc{withdrawalAddr: {Balance: bankFunds}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
	generated, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(_ int, b *blockgen.BlockGen) {
		b.AddWithdrawal(&types.Withdrawal{Index: 0, Validator: 42, Address: withdrawalAddr, Amount: wd1Gwei})
		b.AddWithdrawal(&types.Withdrawal{Index: 1, Validator: 43, Address: withdrawalAddr, Amount: wd2Gwei})
	})
	require.NoError(t, err)
	err = m.InsertChain(generated)
	require.NoError(t, err)
	blk := generated.Blocks[0]

	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
	n := rpc.BlockNumber(blk.NumberU64())
	bnOrHash := rpc.BlockNumberOrHash{BlockNumber: &n}
	results, err := api.ReplayBlockTransactions(context.Background(), bnOrHash, []string{"stateDiff"}, new(bool), nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	last := results[len(results)-1]
	require.NotNil(t, last.StateDiff)

	internedAddr := internedAddress(withdrawalAddr.Hex())
	wdDiff, ok := last.StateDiff[internedAddr]
	require.True(t, ok, "withdrawal address not found in synthetic stateDiff entry")
	require.Len(t, last.StateDiff, 1, "expected one collapsed entry for the repeated address")

	balDiff, ok := wdDiff.Balance.(*StateDiffBalance)
	require.True(t, ok, "balance diff has unexpected type: %T", wdDiff.Balance)

	expectedDelta := new(big.Int).Mul(new(big.Int).SetUint64(wd1Gwei+wd2Gwei), big.NewInt(1e9))
	delta := new(big.Int).Sub(balDiff.To.ToInt(), balDiff.From.ToInt())
	require.Equal(t, expectedDelta, delta,
		"multi-withdrawal delta mismatch: from=%s to=%s expected delta=%s",
		balDiff.From.ToInt(), balDiff.To.ToInt(), expectedDelta)
	require.Equal(t, "=", wdDiff.Code)
	require.Equal(t, "=", wdDiff.Nonce)
}
