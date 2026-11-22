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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/stagedsync"
)

func TestResolveExecUnwindTarget(t *testing.T) {
	db, _, _ := dbWithState(t, true)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, changeset.WriteDiffSet(tx, 1, common.Hash{}, &changeset.StateChangeSet{}))

	// This chain has one transaction per block. The block containing the last
	// transaction in the files must be retained even with older changesets in MDBX.
	filesEnd := tx.Debug().TxNumsInFiles(kv.AccountsDomain)
	require.Greater(t, filesEnd, uint64(2))
	floor := filesEnd - 1
	for _, tc := range []struct {
		name                           string
		progress, distance, wantTarget uint64
		wantErr                        error
	}{
		{"above file boundary", floor + 2, 1, floor + 1, nil},
		{"to file boundary", floor + 2, 2, floor, nil},
		{"part of the rewind is possible", floor + 2, 3, floor, nil},
		{"execution at file boundary", floor, 1, 0, stagedsync.ErrTooDeepUnwind},
		{"execution below file boundary", floor - 1, 1, 0, stagedsync.ErrTooDeepUnwind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, err := resolveExecUnwindTarget(tx, rawdbv3.TxNums, tc.progress, tc.distance)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTarget, target)
		})
	}
	_, err = resolveExecUnwindTarget(tx, rawdbv3.TxNums, floor, floor+1)
	require.ErrorContains(t, err, "cannot unwind past 0")
}

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
