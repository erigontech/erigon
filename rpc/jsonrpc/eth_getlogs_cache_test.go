// Copyright 2026 The Erigon Authors
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
	"math/big"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// TestGetLogsSingleBlockRepeatedQueryReusesResult pins that a repeated query on the head block is
// answered from the cached logs, already encoded.
func TestGetLogsSingleBlockRepeatedQueryReusesResult(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}

	first, err := api.GetLogs(m.Ctx, crit)
	require.NoError(t, err)
	require.Len(t, first.Logs, 3)
	second, err := api.GetLogs(m.Ctx, crit)
	require.NoError(t, err)
	require.Same(t, first.Logs[0], second.Logs[0])
	requireEncodesAsLogs(t, second)
}

// TestGetLogsHeadBlockFiltersCachedLogs pins that different filters on the head block share the
// cached logs of the block and each gets only its matches.
func TestGetLogsHeadBlockFiltersCachedLogs(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	withTopics := func(topics ...common.Hash) filters.FilterCriteria {
		crit := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}
		crit.Topics = [][]common.Hash{topics}
		return crit
	}

	middle, err := api.GetLogs(m.Ctx, withTopics(common.Hash{0x22}))
	require.NoError(t, err)
	require.Len(t, middle.Logs, 1)
	require.Equal(t, common.Hash{0x22}, middle.Logs[0].Topics[0])
	requireEncodesAsLogs(t, middle)

	outer, err := api.GetLogs(m.Ctx, withTopics(common.Hash{0x11}, common.Hash{0x33}))
	require.NoError(t, err)
	require.Len(t, outer.Logs, 2)
	require.Equal(t, common.Hash{0x11}, outer.Logs[0].Topics[0])
	require.Equal(t, common.Hash{0x33}, outer.Logs[1].Topics[0])
	require.NotNil(t, outer.encoded)
	requireEncodesAsLogs(t, outer)
}

// TestGetLogsHeadBlockMaxResultsCountsMatches pins that the result cap applies to the matching logs,
// not to all the logs of the cached block.
func TestGetLogsHeadBlockMaxResultsCountsMatches(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	base := newBaseApiForTest(m)
	base.getLogsMaxResults = 1
	api := newEthApiForTest(base, m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}

	one := crit
	one.Topics = [][]common.Hash{{{0x22}}}
	res, err := api.GetLogs(m.Ctx, one)
	require.NoError(t, err)
	require.Len(t, res.Logs, 1)
	marker := api.headLogs.Load()
	require.NotNil(t, marker, "a block with more logs than the cap must be marked once")
	require.True(t, marker.tooMany)
	require.Empty(t, marker.logs)

	_, err = api.GetLogs(m.Ctx, crit)
	require.ErrorContains(t, err, errExceedLogResults)
	require.Same(t, marker, api.headLogs.Load())
}

// TestGetLogsHeadBlockFillKeepsNewerHead pins that a query whose transaction still sees an older
// head does not replace the cached logs of a newer head.
func TestGetLogsHeadBlockFillKeepsNewerHead(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	newer := &headBlockLogs{hash: common.Hash{0xaa}, num: 2}
	api.headLogs.Store(newer)

	res, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)})
	require.NoError(t, err)
	require.Len(t, res.Logs, 3)
	require.Same(t, newer, api.headLogs.Load())
}

// TestGetLogsHeadBlockConcurrentQueriesShareOneFill pins that concurrent queries on a new head
// block wait for one fill of the cache instead of each reading the block.
func TestGetLogsHeadBlockConcurrentQueriesShareOneFill(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}

	const n = 16
	results := make([]LogsResult, n)
	var start, done sync.WaitGroup
	start.Add(1)
	for i := range n {
		done.Go(func() {
			start.Wait()
			res, err := api.GetLogs(m.Ctx, crit)
			assert.NoError(t, err)
			results[i] = res
		})
	}
	start.Done()
	done.Wait()

	for _, res := range results {
		require.Len(t, res.Logs, 3)
		require.Same(t, &results[0].encoded[0][0], &res.encoded[0][0])
	}
}

// TestGetLogsSingleBlockAfterReorg pins that a cached head result is not served for the block that
// a reorg replaced at the same height.
func TestGetLogsSingleBlockAfterReorg(t *testing.T) {
	t.Parallel()
	m := newLogsTestModule(t)
	orig, err := m.GenerateChain(2, logTopicAt(t, -1, common.Hash{0x11}))
	require.NoError(t, err)
	fork, err := m.GenerateChain(2, logTopicAt(t, -1, common.Hash{0x22}))
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(orig))

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(2)}
	before, err := api.GetLogs(m.Ctx, crit)
	require.NoError(t, err)
	require.Len(t, before.Logs, 1)
	require.Equal(t, common.Hash{0x11}, before.Logs[0].Topics[0])
	require.NotNil(t, before.encoded)

	require.NoError(t, m.InsertChain(fork))

	after, err := api.GetLogs(m.Ctx, crit)
	require.NoError(t, err)
	require.Len(t, after.Logs, 1)
	require.Equal(t, common.Hash{0x22}, after.Logs[0].Topics[0])
	require.Equal(t, fork.Blocks[1].Hash(), after.Logs[0].BlockHash)
	require.NotNil(t, after.encoded)
}

// TestGetLogsSingleBlockBelowHeadIsNotCached pins that only the latest executed block is cached.
func TestGetLogsSingleBlockBelowHeadIsNotCached(t *testing.T) {
	t.Parallel()
	m := newLogsTestModule(t)
	c, err := m.GenerateChain(2, logTopicAt(t, -1, common.Hash{0x11}))
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	below := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}
	first, err := api.GetLogs(m.Ctx, below)
	require.NoError(t, err)
	require.Len(t, first.Logs, 1)
	require.Nil(t, first.encoded)

	head := filters.FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(2)}
	first, err = api.GetLogs(m.Ctx, head)
	require.NoError(t, err)
	require.Len(t, first.Logs, 1)
	require.NotNil(t, first.encoded)
}

// TestGetLogsSingleBlockEmptyResultEncodesAsArray pins that a cached empty result encodes as [].
func TestGetLogsSingleBlockEmptyResultEncodesAsArray(t *testing.T) {
	t.Parallel()
	m := newLatestLogsTestChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1), Addresses: []common.Address{{0xff}}}

	for range 2 {
		res, err := api.GetLogs(m.Ctx, crit)
		require.NoError(t, err)
		encoded, err := jsonstream.Marshal(res)
		require.NoError(t, err)
		require.Equal(t, "[]", string(encoded))
	}
}

func newLogsTestModule(t testing.TB) *execmoduletester.ExecModuleTester {
	return execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1_000_000_000)}},
		}),
		execmoduletester.WithKey(testKey),
	)
}

// logTopicAt adds a transaction emitting one log with topic to block index i, or to every block when i is negative.
func logTopicAt(t testing.TB, i int, topic common.Hash) func(int, *blockgen.BlockGen) {
	signer := types.LatestSignerForChainID(nil)
	return func(j int, block *blockgen.BlockGen) {
		if i >= 0 && j != i {
			return
		}
		logOnCreate := append(append([]byte{0x7f}, topic[:]...), 0x60, 0x00, 0x60, 0x00, 0xa1, 0x00) // PUSH32 topic PUSH1 0 PUSH1 0 LOG1 STOP
		txn, err := types.SignTx(types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), logOnCreate), *signer, testKey)
		require.NoError(t, err)
		block.AddTx(txn)
	}
}

func requireEncodesAsLogs(t *testing.T, res LogsResult) {
	t.Helper()
	want, err := jsonstream.Marshal(res.Logs)
	require.NoError(t, err)
	got, err := jsonstream.Marshal(res)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
	std, err := res.MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(std))
}
