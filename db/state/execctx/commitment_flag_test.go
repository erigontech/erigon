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

package execctx_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxtest"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func withCommitmentFlag(t *testing.T, variant commitment.TrieVariant) {
	t.Helper()
	origPar := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment = origPar
	})
	statecfg.ExperimentalParallelCommitment = variant == commitment.VariantParallelHexPatricia
}

// Update set is identical across calls so the returned roots can be compared.
func runWriteCommitBatch(t *testing.T, sd *execctx.SharedDomains, rwTx kv.TemporalRwTx) []byte {
	t.Helper()

	ctx := t.Context()
	addr := make([]byte, length.Addr)
	addr[0] = 0x42
	addr[length.Addr-1] = 0x99

	acc := accounts.Account{
		Nonce:    7,
		Balance:  *uint256.NewInt(0xdeadbeef),
		CodeHash: accounts.EmptyCodeHash,
	}
	pv, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), 1, pv))

	rh, err := sd.ComputeCommitment(ctx, rwTx, false, 0, 1, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, rh)
	return rh
}

func TestSharedDomains_ParallelFlag_RootEquivalence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// No t.Parallel: mutates process-global statecfg flags.

	stepSize := uint64(16)

	runOnce := func(t *testing.T, parallel bool) []byte {
		t.Helper()
		variant := commitment.VariantHexPatriciaTrie
		if parallel {
			variant = commitment.VariantParallelHexPatricia
		}
		withCommitmentFlag(t, variant)

		db := execctxtest.NewTestDb(t, stepSize)

		ctx := t.Context()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		sd.EnableParaTrieDB(db)

		got := sd.GetCommitmentCtx().Trie().Variant()
		require.Equalf(t, variant, got, "trie variant for parallel=%v", parallel)

		return runWriteCommitBatch(t, sd, rwTx)
	}

	seqRoot := runOnce(t, false)
	parRoot := runOnce(t, true)

	require.Equalf(t, seqRoot, parRoot,
		"sequential and parallel commitment roots must match: sequential=%x parallel=%x",
		seqRoot, parRoot)
}
