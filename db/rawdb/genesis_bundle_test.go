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

package rawdb_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// newTestGenesisBundle builds a minimal GenesisBundle with a block at number 0.
// It does NOT call the genesiswrite layer (no state computation) — the bundle is
// purely a test fixture for the rawdb persistence layer.
func newTestGenesisBundle(t *testing.T, cfg *chain.Config) *rawdb.GenesisBundle {
	t.Helper()
	g := &types.Genesis{
		Config:     cfg,
		Nonce:      42,
		Timestamp:  1234,
		GasLimit:   1_000_000,
		Difficulty: uint256.NewInt(7),
		Coinbase:   common.HexToAddress("0x00000000000000000000000000000000deadbeef"),
	}
	head := &types.Header{
		Number:     *uint256.NewInt(0),
		Nonce:      types.EncodeNonce(g.Nonce),
		Time:       g.Timestamp,
		GasLimit:   g.GasLimit,
		Difficulty: *g.Difficulty,
		Coinbase:   g.Coinbase,
	}
	block := types.NewBlock(head, nil, nil, nil, nil)
	return &rawdb.GenesisBundle{
		Genesis: g,
		Config:  cfg,
		Block:   block,
		TD:      g.Difficulty.ToBig(),
	}
}

func TestGenesisBundle_WriteReadRoundTrip(t *testing.T) {
	t.Parallel()

	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	// Use AllProtocolChanges as a simple non-nil config.
	cfg := chain.AllProtocolChanges
	b := newTestGenesisBundle(t, cfg)

	err := rawdb.WriteGenesisBundle(tx, b, rawdb.WriteGenesisBundleOpts{FreshDB: true})
	require.NoError(t, err)

	got, err := rawdb.ReadGenesisBundle(tx)
	require.NoError(t, err)
	require.NotNil(t, got, "read bundle must not be nil after fresh write")

	// Genesis JSON round-trip: nonce/timestamp/gaslimit/coinbase should match.
	require.NotNil(t, got.Genesis, "Genesis must be persisted under ConfigTable[GenesisKey]")
	require.Equal(t, b.Genesis.Nonce, got.Genesis.Nonce)
	require.Equal(t, b.Genesis.Timestamp, got.Genesis.Timestamp)
	require.Equal(t, b.Genesis.GasLimit, got.Genesis.GasLimit)
	require.Equal(t, b.Genesis.Coinbase, got.Genesis.Coinbase)

	// Block: hash must match exactly.
	require.NotNil(t, got.Block)
	require.Equal(t, b.Block.Hash(), got.Block.Hash(), "round-tripped block hash must match")
	require.Equal(t, b.Block.NumberU64(), got.Block.NumberU64())

	// TD: must match exactly.
	require.NotNil(t, got.TD)
	require.Equal(t, 0, b.TD.Cmp(got.TD), "TD round-trip: want %s got %s", b.TD, got.TD)

	// Chain config: must match exactly (by ChainID as a proxy — DeepEqual fails
	// on unexported fields inside nested chain.Config types).
	require.NotNil(t, got.Config)
	require.Equal(t, 0, b.Config.ChainID.Cmp(got.Config.ChainID))
}

func TestGenesisBundle_WriteConfigOnly(t *testing.T) {
	t.Parallel()

	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	// Seed via fresh-DB write.
	b := newTestGenesisBundle(t, chain.AllProtocolChanges)
	err := rawdb.WriteGenesisBundle(tx, b, rawdb.WriteGenesisBundleOpts{FreshDB: true})
	require.NoError(t, err)

	// Build a new config with a distinct ChainID and rewrite in config-only mode.
	// chain.Config contains a sync.Once, so we cannot copy it by value; construct
	// a fresh Config with only the fields the test asserts against.
	newCfg := &chain.Config{ChainID: big.NewInt(999999)}

	err = rawdb.WriteGenesisBundle(tx, &rawdb.GenesisBundle{
		Block:  b.Block,
		Config: newCfg,
	}, rawdb.WriteGenesisBundleOpts{FreshDB: false})
	require.NoError(t, err)

	got, err := rawdb.ReadGenesisBundle(tx)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Config changed.
	require.NotNil(t, got.Config)
	require.Equal(t, int64(999999), got.Config.ChainID.Int64(), "config-only write must update ChainID")

	// Genesis JSON unchanged — still the one from the initial fresh write.
	require.NotNil(t, got.Genesis)
	require.Equal(t, b.Genesis.Nonce, got.Genesis.Nonce)

	// Block unchanged.
	require.Equal(t, b.Block.Hash(), got.Block.Hash())

	// TD unchanged — still the initial one.
	require.NotNil(t, got.TD)
	require.Equal(t, 0, b.TD.Cmp(got.TD))
}

func TestGenesisBundle_ReadEmptyDB(t *testing.T) {
	t.Parallel()

	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	got, err := rawdb.ReadGenesisBundle(tx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Nil(t, got.Genesis, "empty DB: no Genesis")
	require.Nil(t, got.Block, "empty DB: no Block")
	require.Nil(t, got.Config, "empty DB: no Config")
	require.Nil(t, got.TD, "empty DB: no TD")
}

func TestGenesisBundle_WriteNilRejects(t *testing.T) {
	t.Parallel()

	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	err := rawdb.WriteGenesisBundle(tx, nil, rawdb.WriteGenesisBundleOpts{FreshDB: true})
	require.Error(t, err)

	err = rawdb.WriteGenesisBundle(tx, &rawdb.GenesisBundle{}, rawdb.WriteGenesisBundleOpts{FreshDB: true})
	require.Error(t, err, "bundle without Block must be rejected")
}
