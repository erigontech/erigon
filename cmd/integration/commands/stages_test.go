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

package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
)

func TestRunMigrationsForDBRejectsMissingChaindataWithoutPendingMigration(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	missingPath := filepath.Join(dataDir, "missing-chaindata")
	markMigrationsAppliedExcept(t, dataDir, logger)

	err := runMigrationsForDB(dbcfg.ChainDB, missingPath, logger)
	require.ErrorIs(t, err, kv2.ErrDBDoesNotExists)
	require.NoDirExists(t, missingPath)
}

func TestRunMigrationsForDBRejectsNonChaindataWithoutPendingMigration(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	databasePath := filepath.Join(dataDir, "chaindata")
	inspectOpts := createSmallTxPoolDB(t, databasePath)
	before := tableNames(t, inspectOpts)
	beforeSize := databaseFileSize(t, databasePath)
	markMigrationsAppliedExcept(t, dataDir, logger)

	err := runMigrationsForDB(dbcfg.ChainDB, databasePath, logger)
	require.Error(t, err)
	require.Equal(t, beforeSize, databaseFileSize(t, databasePath))
	require.Equal(t, before, tableNames(t, inspectOpts))
}

func TestRunMigrationsForDBRejectsConsensusDirectoryWithoutDatabase(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	consensusPath := filepath.Join(dataDir, "aura")
	require.NoError(t, os.Mkdir(consensusPath, 0o755))

	err := runMigrationsForDB(dbcfg.ConsensusDB, consensusPath, logger)
	require.ErrorIs(t, err, kv2.ErrDBDoesNotExists)
	require.NoFileExists(t, filepath.Join(consensusPath, "mdbx.dat"))
}

func TestRunMigrationsForDBCreatesMissingConsensusTable(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	consensusPath := filepath.Join(dataDir, "aura")

	inspectOpts := createDBWithoutPtcTable(t, dbcfg.ConsensusDB, consensusPath, kv.ConsensusTablesCfg)
	require.NotContains(t, tableNames(t, inspectOpts), kv.PtcWindowEpochTable)

	require.NoError(t, runMigrationsForDB(dbcfg.ConsensusDB, consensusPath, logger))
	require.Contains(t, tableNames(t, inspectOpts), kv.PtcWindowEpochTable)
}
