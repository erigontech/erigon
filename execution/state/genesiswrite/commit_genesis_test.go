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

package genesiswrite_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
)

// newTestOptions builds a minimal Options with a tiny custom genesis that still
// exercises the full CommitGenesisTx path (state root computation, bundle write,
// TxNums append) without pulling in mainnet's ~8k alloc entries.
func newTestOptions(t *testing.T) genesiswrite.Options {
	t.Helper()
	g := &types.Genesis{
		Config:     chain.AllProtocolChanges,
		Nonce:      66,
		Timestamp:  0,
		GasLimit:   5_000_000,
		Difficulty: uint256.NewInt(0x20000),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000042"),
	}
	return genesiswrite.Options{
		Genesis: g,
		Dirs:    datadir.New(t.TempDir()),
		Logger:  log.New(),
	}
}

// TestCommitGenesis_FreshDBKeyPresence is THE regression net for this refactor.
// On a fresh DB, every key that makes a chain DB valid at block 0 must be
// present after CommitGenesis. Each missing key fails in its own subtest so a
// regression pinpoints exactly which write was dropped.
func TestCommitGenesis_FreshDBKeyPresence(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: computes genesis state root")
	}
	t.Parallel()

	opts := newTestOptions(t)
	db := temporaltest.NewTestDB(t, opts.Dirs)

	cfg, block, err := genesiswrite.CommitGenesis(context.Background(), db, opts)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, block)
	hash := block.Hash()

	err = db.View(context.Background(), func(tx kv.Tx) error {
		checks := []struct {
			name  string
			table string
			key   []byte
		}{
			{"ConfigTable[GenesisKey]", kv.ConfigTable, kv.GenesisKey},
			{"ConfigTable[blockHash]", kv.ConfigTable, hash[:]},
			{"HeaderCanonical[0]", kv.HeaderCanonical, encodeU64(0)},
			{"HeaderNumber[hash]", kv.HeaderNumber, hash[:]},
			{"Headers[0|hash]", kv.Headers, headerKey(0, hash)},
			{"BlockBody[0|hash]", kv.BlockBody, headerKey(0, hash)},
			{"HeaderTD[0|hash]", kv.HeaderTD, headerKey(0, hash)},
			{"HeadBlockHash", kv.HeadBlockKey, []byte(kv.HeadBlockKey)},
			{"HeadHeaderHash", kv.HeadHeaderKey, []byte(kv.HeadHeaderKey)},
			{"MaxTxNum[0]", kv.MaxTxNum, encodeU64(0)},
		}
		for _, c := range checks {
			c := c
			t.Run(c.name, func(t *testing.T) {
				v, err := tx.GetOne(c.table, c.key)
				require.NoError(t, err)
				require.NotEmpty(t, v, "missing key: %s at %s", c.name, c.table)
			})
		}
		return nil
	})
	require.NoError(t, err)

	// Also confirm the structured reader returns a populated bundle.
	err = db.View(context.Background(), func(tx kv.Tx) error {
		bundle, err := rawdb.ReadGenesisBundle(tx)
		require.NoError(t, err)
		require.NotNil(t, bundle)
		require.NotNil(t, bundle.Genesis)
		require.NotNil(t, bundle.Block)
		require.NotNil(t, bundle.Config)
		require.NotNil(t, bundle.TD)
		require.Equal(t, hash, bundle.Block.Hash())
		return nil
	})
	require.NoError(t, err)
}

// TestCommitGenesis_RepairMissingChainConfig exercises the branch where a block
// is already persisted but ConfigTable[blockHash] is empty — the scenario that
// MustCommitGenesis used to create silently. After CommitGenesis runs again
// with a matching genesis, the chain config must be restored.
func TestCommitGenesis_RepairMissingChainConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: computes genesis state root")
	}
	t.Parallel()

	opts := newTestOptions(t)
	db := temporaltest.NewTestDB(t, opts.Dirs)

	// Seed the DB via the normal path, then delete only the chain config entry
	// (ConfigTable[blockHash]) while keeping the genesis JSON, block, TD,
	// canonical hash, and head pointers. This mirrors the stale-DB state the
	// old MustCommitGenesis code path could produce.
	_, block, err := genesiswrite.CommitGenesis(context.Background(), db, opts)
	require.NoError(t, err)
	hash := block.Hash()

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Delete(kv.ConfigTable, hash[:])
	})
	require.NoError(t, err)

	// Verify the precondition: chain config is gone.
	err = db.View(context.Background(), func(tx kv.Tx) error {
		cfg, err := rawdb.ReadChainConfig(tx, hash)
		require.NoError(t, err)
		require.Nil(t, cfg, "precondition: chain config must be absent before repair")
		return nil
	})
	require.NoError(t, err)

	// Second call must take the repair branch and rewrite the config without
	// wiping or double-writing the block.
	cfg, repaired, err := genesiswrite.CommitGenesis(context.Background(), db, opts)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, repaired)
	require.Equal(t, hash, repaired.Hash(), "repair path must return stored block")

	err = db.View(context.Background(), func(tx kv.Tx) error {
		got, err := rawdb.ReadChainConfig(tx, hash)
		require.NoError(t, err)
		require.NotNil(t, got, "chain config must be restored by repair branch")
		require.Equal(t, 0, cfg.ChainID.Cmp(got.ChainID))
		return nil
	})
	require.NoError(t, err)
}

// TestCommitGenesis_NilConfigNotPersisted asserts the ordering bug fix: if the
// caller passes Options.Genesis with nil Config, the function must return
// ErrGenesisNoConfig *before* writing anything. The old WriteGenesisBlock code
// path persisted the genesis JSON first, then validated, leaving a half-written
// DB on error.
func TestCommitGenesis_NilConfigNotPersisted(t *testing.T) {
	t.Parallel()

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	_, _, err := genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{
		Genesis: &types.Genesis{}, // Config == nil
		Dirs:    dirs,
		Logger:  log.New(),
	})
	require.ErrorIs(t, err, types.ErrGenesisNoConfig)

	// The DB must remain pristine: no genesis JSON, no canonical hash at 0.
	err = db.View(context.Background(), func(tx kv.Tx) error {
		g, err := rawdb.ReadGenesis(tx)
		require.NoError(t, err)
		require.Nil(t, g, "ErrGenesisNoConfig must leave ConfigTable[GenesisKey] untouched")

		h, err := rawdb.ReadCanonicalHash(tx, 0)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, h, "no canonical hash must be written on validation failure")
		return nil
	})
	require.NoError(t, err)
}

// TestCommitGenesis_Mismatch seeds the DB with one chain and then tries to
// commit a different one. CommitGenesis must return *GenesisMismatchError.
func TestCommitGenesis_Mismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: computes genesis state root twice")
	}
	t.Parallel()

	// Seed with customg (not a named chain).
	customg := &types.Genesis{
		Config:     &chain.Config{ChainID: big.NewInt(1), HomesteadBlock: common.NewUint64(3)},
		Difficulty: uint256.NewInt(0x20000),
		GasLimit:   5_000_000,
	}
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	_, storedBlock, err := genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{
		Genesis: customg,
		Dirs:    dirs,
		Logger:  log.New(),
	})
	require.NoError(t, err)
	require.NotNil(t, storedBlock)

	// Now try committing sepolia over it — hash must mismatch.
	_, _, err = genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{
		Genesis: chainspec.SepoliaGenesisBlock(),
		Dirs:    dirs,
		Logger:  log.New(),
	})
	require.Error(t, err)
	mismatch, ok := err.(*genesiswrite.GenesisMismatchError)
	require.True(t, ok, "expected *GenesisMismatchError, got %T: %v", err, err)
	require.Equal(t, storedBlock.Hash(), mismatch.Stored)
}

// encodeU64 writes a big-endian uint64 key, matching the layout used by
// rawdb.WriteCanonicalHash / rawdbv3.TxNums.Append (block_num_u64 keys).
func encodeU64(n uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(n >> 56)
	b[1] = byte(n >> 48)
	b[2] = byte(n >> 40)
	b[3] = byte(n >> 32)
	b[4] = byte(n >> 24)
	b[5] = byte(n >> 16)
	b[6] = byte(n >> 8)
	b[7] = byte(n)
	return b
}

// headerKey mirrors dbutils.HeaderKey: 8-byte big-endian block number followed
// by the 32-byte block hash. Used for Headers, HeaderTD, BlockBody table keys.
func headerKey(blockNum uint64, h common.Hash) []byte {
	k := make([]byte, 8+len(h))
	copy(k[:8], encodeU64(blockNum))
	copy(k[8:], h[:])
	return k
}
