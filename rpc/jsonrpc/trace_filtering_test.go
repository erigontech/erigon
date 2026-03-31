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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/execution/protocol"
	protocolrules "github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
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
