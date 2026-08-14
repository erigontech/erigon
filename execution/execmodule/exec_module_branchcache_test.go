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

package execmodule_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func sharedBranchCache(t *testing.T, tx kv.TemporalTx) *commitment.BranchCache {
	t.Helper()
	provider, ok := tx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	bc := provider.BranchCache()
	require.NotNil(t, bc)
	return bc
}

func someBranchKey(t *testing.T, tx kv.TemporalTx) []byte {
	t.Helper()
	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, 1<<20)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		if !bytes.Equal(k, commitment.KeyCommitmentState) && len(v) > 0 {
			return bytes.Clone(k)
		}
	}
	t.Fatal("no commitment branch row found")
	return nil
}

func TestWithoutSharedBranchCacheNeverTouchesSharedCache(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, gen *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{0xAA, byte(i)}, uint256.NewInt(10_000), params.TxGas+params.StateGasNewAccount, gasPrice, nil), *signer, m.Key)
		require.NoError(t, err)
		gen.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, chainPack.Blocks))

	roTx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	branchKey := someBranchKey(t, roTx)
	bc := sharedBranchCache(t, roTx)

	readBranch := func(opts ...execctx.SharedDomainOption) []byte {
		sd, err := execctx.NewSharedDomains(ctx, roTx, log.New(), opts...)
		require.NoError(t, err)
		defer sd.Close()
		v, _, err := sd.GetLatest(kv.CommitmentDomain, roTx, branchKey)
		require.NoError(t, err)
		require.NotEmpty(t, v)
		return v
	}

	authoritative, _, err := roTx.GetLatest(kv.CommitmentDomain, branchKey)
	require.NoError(t, err)
	cacheOnly := []byte("cache-only")
	bc.Clear()
	bc.Put(branchKey, cacheOnly, 0, 0)
	require.Equal(t, authoritative, readBranch(execctx.WithoutSharedBranchCache()))
	cached, _, ok := bc.Get(branchKey)
	require.True(t, ok)
	require.Equal(t, cacheOnly, cached)
	require.Equal(t, cacheOnly, readBranch())

	bc.Clear()
	readBranch(execctx.WithoutSharedBranchCache())
	_, _, ok = bc.Get(branchKey)
	require.False(t, ok, "detached SharedDomains read-filled the shared BranchCache")

	readBranch()
	_, _, ok = bc.Get(branchKey)
	require.True(t, ok, "default SharedDomains should read-fill the shared BranchCache")
}
