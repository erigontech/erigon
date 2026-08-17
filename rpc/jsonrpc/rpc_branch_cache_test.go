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
	"bytes"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func poisonSharedBranchCache(t *testing.T, db kv.TemporalRoDB) func() {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	provider, ok := tx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	cache := provider.BranchCache()
	require.NotNil(t, cache)
	cache.Clear()
	t.Cleanup(cache.Clear)

	poison := []byte("invalid commitment branch")
	var poisonedKeys [][]byte
	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		key, value, err := it.Next()
		require.NoError(t, err)
		if bytes.Equal(key, commitment.KeyCommitmentState) || len(value) == 0 {
			continue
		}
		key = bytes.Clone(key)
		cache.Put(key, poison, 0, 0)
		poisonedKeys = append(poisonedKeys, key)
	}
	require.NotEmpty(t, poisonedKeys)

	return func() {
		for _, key := range poisonedKeys {
			cached, _, ok := cache.Get(key)
			require.True(t, ok)
			require.Equal(t, poison, cached)
		}
	}
}

func enableStateCacheForTest(t *testing.T) {
	t.Helper()
	previous := dbg.UseStateCache
	dbg.SetUseStateCache(true)
	t.Cleanup(func() { dbg.SetUseStateCache(previous) })
}

func TestGetProofIgnoresSharedBranchCache(t *testing.T) {
	enableStateCacheForTest(t)

	m, _, contractAddress, _ := chainWithDeployedContract(t)
	assertPoisoned := poisonSharedBranchCache(t, m.DB)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	proof, err := api.GetProof(t.Context(), contractAddress, nil, bnhPtr(rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)))
	require.NoError(t, err)
	require.NotNil(t, proof)
	assertPoisoned()
}

func TestSimulateV1IgnoresSharedBranchCache(t *testing.T) {
	enableStateCacheForTest(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	assertPoisoned := poisonSharedBranchCache(t, m.DB)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0000000000000000000000000000000000000001")
	value := (*hexutil.Big)(big.NewInt(100))
	gas := hexutil.Uint64(100_000)
	result, err := api.SimulateV1(t.Context(), SimulationRequest{
		BlockStateCalls: []SimulatedBlock{{Calls: []ethapi.CallArgs{{
			From:  &from,
			To:    &to,
			Value: value,
			Gas:   &gas,
		}}}},
	}, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertPoisoned()
}

func TestSnapshotCommitmentDomainsIgnoreSharedBranchCache(t *testing.T) {
	enableStateCacheForTest(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	assertPoisoned := poisonSharedBranchCache(t, m.DB)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer it.Close()
	var branchKey, branchValue []byte
	for it.HasNext() {
		key, value, err := it.Next()
		require.NoError(t, err)
		if !bytes.Equal(key, commitment.KeyCommitmentState) && len(value) > 0 {
			branchKey = bytes.Clone(key)
			branchValue = bytes.Clone(value)
			break
		}
	}
	require.NotEmpty(t, branchKey)

	domains, err := newSnapshotCommitmentDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	got, _, err := domains.GetLatest(kv.CommitmentDomain, tx, branchKey)
	require.NoError(t, err)
	require.Equal(t, branchValue, got)
	assertPoisoned()
}
