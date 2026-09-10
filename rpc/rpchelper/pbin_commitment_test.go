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

package rpchelper

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
)

// Commitment replay recomputes roots with the hex trie over its own temporary
// aggregator, so it cannot serve a bin datadir.
func TestPBinCommitmentReplayRefusesBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	orig := statecfg.ExperimentalBinCommitment
	origParallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = orig
		statecfg.ExperimentalParallelCommitment = origParallel
	})
	statecfg.ExperimentalBinCommitment = true
	// erigondb.toml resolution refuses the combination: the bin trie is
	// sequential-only, regardless of a process-wide parallel default.
	statecfg.ExperimentalParallelCommitment = false

	// Fresh dirs: the replay resolves erigondb.toml itself, and a hex toml would
	// be refused there instead of at the SharedDomains this test pins.
	r := NewCommitmentReplay(datadir.New(t.TempDir()), rawdbv3.TxNums, log.New())
	_, err = r.ComputeCustomCommitmentFromStateHistory(t.Context(), tx, 0, nil)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}

func TestPBinDualCommitmentReplayGenesis(t *testing.T) {
	originalBin, originalDual := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment
	originalParallel, originalHash := statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash
	originalSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = originalBin, originalDual
		statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash = originalParallel, originalHash
		require.NoError(t, commitment.SetPBinHashSuite(originalSuite))
	})
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = true, true
	statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash = false, "blake3"
	require.NoError(t, commitment.SetPBinHashSuite("blake3"))
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	activation := uint64(30)
	config := chain.TestChainBerlinConfig.Copy()
	config.BinaryTrieTime = &activation
	genesis := &types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: types.GenesisAlloc{common.Address{1}: {Balance: big.NewInt(100)}}}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteGenesisIfNotExist(tx, genesis)
	}))
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		t.Run(domain.String(), func(t *testing.T) {
			replayDirs := datadir.New(t.TempDir())
			replay := NewCommitmentReplay(replayDirs, rawdbv3.TxNums, log.New())
			var target []kv.Domain
			if domain == kv.CommitmentBinDomain {
				target = append(target, domain)
				settings, err := dbstate.ResolveErigonDBSettings(replayDirs, log.New(), false)
				require.NoError(t, err)
				settings.FrozenAtTxNum = map[string]uint64{kv.CommitmentDomain.String(): 10}
				require.NoError(t, dbstate.WriteErigonDBSettings(replayDirs, settings))
			}
			called := false
			root, err := replay.ComputeCustomCommitmentFromStateHistory(t.Context(), tx, 0, func(_ context.Context, _ kv.TemporalTx, sd *execctx.SharedDomains) ([]byte, error) {
				called = true
				require.Len(t, sd.CommitmentDomains(), 2)
				require.Equal(t, domain, sd.GetCommitmentCtx().CommitmentDomain())
				return sd.GetCommitmentCtx().Trie().RootHash()
			}, target...)
			require.NoError(t, err)
			require.True(t, called)
			require.Len(t, root, 32)
			entries, err := os.ReadDir(replayDirs.Tmp)
			require.NoError(t, err)
			require.Empty(t, entries)
			if domain == kv.CommitmentBinDomain {
				settings, err := dbstate.ReadErigonDBSettings(replayDirs)
				require.NoError(t, err)
				frozenAt, frozen := settings.FrozenAt(kv.CommitmentDomain)
				require.True(t, frozen)
				require.Equal(t, uint64(10), frozenAt)
			}
		})
	}
}
