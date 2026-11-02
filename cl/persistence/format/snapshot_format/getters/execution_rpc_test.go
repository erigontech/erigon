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

package getters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/types"
)

type testExecutionEngineBodyFetcher struct {
	byHash map[common.Hash]*types.RawBody
}

type missingBodyBlockReader struct {
	dbservices.FullBlockReader
}

func (missingBodyBlockReader) BodyWithTransactions(context.Context, kv.Getter, common.Hash, uint64) (*types.Body, error) {
	return nil, nil
}

func (missingBodyBlockReader) Body(context.Context, kv.Getter, common.Hash, uint64) (*types.Body, uint32, error) {
	return nil, 0, nil
}

func (f *testExecutionEngineBodyFetcher) GetBodiesByHashes(_ context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	bodies := make([]*types.RawBody, len(hashes))
	for i, hash := range hashes {
		bodies[i] = f.byHash[hash]
	}
	return bodies, nil
}

func TestExecutionEngineReaderUsesBlockHash(t *testing.T) {
	canonicalHash := common.Hash{0x01}
	sideHash := common.Hash{0x02}
	canonicalBody := &types.RawBody{
		Transactions: [][]byte{{0x0a}},
		Withdrawals:  []*types.Withdrawal{{Index: 10}},
	}
	sideBody := &types.RawBody{
		Transactions: [][]byte{{0x0b}, {0x0c}},
		Withdrawals:  []*types.Withdrawal{{Index: 20}},
	}
	reader := NewExecutionEngineReader(t.Context(), &testExecutionEngineBodyFetcher{
		byHash: map[common.Hash]*types.RawBody{
			canonicalHash: canonicalBody,
			sideHash:      sideBody,
		},
	})

	transactions, err := reader.Transactions(42, sideHash)
	require.NoError(t, err)
	require.Equal(t, sideBody.Transactions, transactions.UnderlyngReference())

	withdrawals, err := reader.Withdrawals(42, sideHash)
	require.NoError(t, err)
	require.Equal(t, uint64(20), withdrawals.Get(0).Index)
}

func TestExecutionEngineReaderRejectsMissingBody(t *testing.T) {
	for _, engine := range []ExecutionEngineBodyFetcher{
		&testExecutionEngineBodyFetcher{byHash: map[common.Hash]*types.RawBody{}},
		nil,
	} {
		reader := NewExecutionEngineReader(t.Context(), engine)

		_, err := reader.Transactions(42, common.Hash{0x02})
		require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)

		_, err = reader.Withdrawals(42, common.Hash{0x02})
		require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)
	}
}

func TestExecutionEngineReaderCacheMatchesBlockHash(t *testing.T) {
	cachedHash := common.Hash{0x01}
	requestedHash := common.Hash{0x02}
	requestedBody := &types.RawBody{
		Transactions: [][]byte{{0x0b}},
		Withdrawals:  []*types.Withdrawal{{Index: 20}},
	}
	reader := NewExecutionEngineReader(t.Context(), &testExecutionEngineBodyFetcher{
		byHash: map[common.Hash]*types.RawBody{requestedHash: requestedBody},
	})
	reader.CacheBody(42, cachedHash, [][]byte{{0x0a}}, []*types.Withdrawal{{Index: 10}})
	cachedTransactions, err := reader.Transactions(42, cachedHash)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{0x0a}}, cachedTransactions.UnderlyngReference())
	cachedWithdrawals, err := reader.Withdrawals(42, cachedHash)
	require.NoError(t, err)
	require.Equal(t, uint64(10), cachedWithdrawals.Get(0).Index)

	transactions, err := reader.Transactions(42, requestedHash)
	require.NoError(t, err)
	require.Equal(t, requestedBody.Transactions, transactions.UnderlyngReference())

	withdrawals, err := reader.Withdrawals(42, requestedHash)
	require.NoError(t, err)
	require.Equal(t, uint64(20), withdrawals.Get(0).Index)
}

func TestExecutionEngineReaderCacheKeepsBodiesAtSameHeight(t *testing.T) {
	reader := NewExecutionEngineReader(t.Context(), nil)
	number := uint64(42)
	hashA := common.Hash{0xaa}
	hashB := common.Hash{0xbb}
	txsA := [][]byte{{0x0a}}
	txsB := [][]byte{{0x0b}}

	reader.CacheBody(number, hashA, txsA, nil)
	reader.CacheBody(number, hashB, txsB, nil)

	gotA, err := reader.Transactions(number, hashA)
	require.NoError(t, err)
	require.Equal(t, txsA, gotA.UnderlyngReference())
	gotB, err := reader.Transactions(number, hashB)
	require.NoError(t, err)
	require.Equal(t, txsB, gotB.UnderlyngReference())
}

func TestExecutionSnapshotReaderReportsMissingBody(t *testing.T) {
	reader := NewExecutionSnapshotReader(t.Context(), missingBodyBlockReader{}, mdbxtest.NewTestDB(t, dbcfg.ChainDB))
	reader.SetBeaconChainConfig(&clparams.MainnetBeaconConfig)

	_, err := reader.Transactions(42, common.Hash{0x42})
	require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)

	_, err = reader.Withdrawals(42, common.Hash{0x42})
	require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)
}
