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

package app

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
)

// writeChainMetadata is a test helper that writes a genesis canonical hash and chain config
// to the database, mimicking what a real Erigon node writes during initialisation.
func writeChainMetadata(t *testing.T, ctx context.Context, db kv.RwDB, chainName string, chainID uint64) common.Hash {
	t.Helper()
	genesisHash := common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	chainCfg := &chain.Config{
		ChainName: chainName,
		ChainID:   uint256.NewInt(chainID),
	}
	err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, genesisHash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesisHash, chainCfg)
	})
	require.NoError(t, err)
	return genesisHash
}

// TestGetChainNameFromChainData_OldSchema verifies that chain-name detection works on a
// database that contains only the metadata tables (simulating an older Erigon version's
// schema). Without the WithTableCfg fix, opening such a DB in Accede+Readonly mode would
// fail because the MDBX wrapper tries to validate every table in ChaindataTablesCfg.
func TestGetChainNameFromChainData_OldSchema(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	// Create a database with ONLY the two tables needed for chain-name discovery.
	// This is the most extreme old-schema scenario: everything except the metadata
	// tables is missing. If getChainNameFromChainData depends on any other table,
	// this test catches it.
	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "mainnet", 1)
	db.Close()

	result, err := getChainNameFromChainData(ctx, nil, logger, chainDataDir)
	require.NoError(t, err, "should not fail on a DB with only metadata tables")
	require.True(t, result.Ok, "chain name should be detected")
	require.Equal(t, "mainnet", result.Value)
}

// TestGetChainNameFromChainData_CurrentSchema verifies the happy path: a fully migrated
// database with all current tables must still work correctly after the fix.
func TestGetChainNameFromChainData_CurrentSchema(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	// Create a database with the full current ChaindataTablesCfg — this is what a
	// fully migrated node would have.
	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "mainnet", 1)
	db.Close()

	result, err := getChainNameFromChainData(ctx, nil, logger, chainDataDir)
	require.NoError(t, err, "should not fail on a current-schema DB")
	require.True(t, result.Ok)
	require.Equal(t, "mainnet", result.Value)
}

// TestGetChainNameFromChainData_NonMainnet verifies that chain detection is not hardcoded to
// mainnet — it must correctly read whatever chain name is stored in the database.
func TestGetChainNameFromChainData_NonMainnet(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "sepolia", 11155111)
	db.Close()

	result, err := getChainNameFromChainData(ctx, nil, logger, chainDataDir)
	require.NoError(t, err)
	require.True(t, result.Ok)
	require.Equal(t, "sepolia", result.Value)
}

// TestGetChainNameFromChainData_EmptyDB verifies behaviour when the database exists but
// contains no chain metadata (no genesis hash, no config). This represents a freshly
// created but never-initialised datadir.
func TestGetChainNameFromChainData_EmptyDB(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)
	db.Close()

	// No chain metadata written — the function should return Ok=false with no error.
	result, err := getChainNameFromChainData(ctx, nil, logger, chainDataDir)
	require.NoError(t, err)
	require.False(t, result.Ok, "empty DB should not yield a chain name")
}

// TestGetChainNameFromChainData_NonExistentDir verifies graceful handling when the
// chaindata directory does not exist.
func TestGetChainNameFromChainData_NonExistentDir(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "nonexistent", "chaindata")

	result, err := getChainNameFromChainData(ctx, nil, logger, chainDataDir)
	require.Error(t, err)
	require.False(t, result.Ok)
}
