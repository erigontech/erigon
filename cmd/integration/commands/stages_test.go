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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	chain2 "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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

func TestStageHeadersAndBodiesRequireUnwindOrReset(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	useTestBlockReader(t, dirs)
	require.ErrorContains(t, stageHeaders(db, t.Context(), log.New()), "only works with --unwind or --reset")
	require.ErrorContains(t, stageBodies(db, t.Context(), log.New()), "only works with --unwind or --reset")
}

func TestCheckSendersReturnsSendersCountMismatch(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	useTestBlockReader(t, dirs)
	br, _ := blocksIO(db, log.New())

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Difficulty: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 0))
	txn := types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil)
	require.NoError(t, rawdb.WriteBody(tx, header.Hash(), 0, &types.Body{Transactions: []types.Transaction{txn}}))

	require.ErrorContains(t, checkSenders(t.Context(), tx, br, chain2.AllProtocolChanges, 0, log.New()), "not equal amount of senders")
}

type failingHeaderReader struct {
	dbservices.FullBlockReader
	err error
}

func (r failingHeaderReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return nil, r.err
}

func TestCheckSendersReturnsHeaderReadError(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	useTestBlockReader(t, dirs)
	br, _ := blocksIO(db, log.New())

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	readErr := errors.New("header read failed")
	err = checkSenders(t.Context(), tx, failingHeaderReader{FullBlockReader: br, err: readErr}, chain2.AllProtocolChanges, 0, log.New())
	require.ErrorIs(t, err, readErr)
}

func TestCheckSendersFailsForWrongSender(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	useTestBlockReader(t, dirs)
	br, _ := blocksIO(db, log.New())

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Difficulty: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 0))
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := types.MakeSigner(chain2.AllProtocolChanges, 0, 0)
	txn := types.MustSignNewTx(key, *signer, types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil))
	require.NoError(t, rawdb.WriteBody(tx, header.Hash(), 0, &types.Body{Transactions: []types.Transaction{txn}}))
	require.NoError(t, rawdb.WriteSenders(tx, header.Hash(), 0, []common.Address{{2}}))

	require.ErrorContains(t, checkSenders(t.Context(), tx, br, chain2.AllProtocolChanges, 0, log.New()), "wrong sender")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = checkSenders(ctx, tx, cancelAfterSenderRead{br, cancel}, chain2.AllProtocolChanges, 0, log.New())
	require.ErrorContains(t, err, "wrong sender")
	require.ErrorIs(t, err, common.ErrStopped)
}

type cancelAfterSenderRead struct {
	dbservices.FullBlockReader
	cancel context.CancelFunc
}

func (r cancelAfterSenderRead) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, height uint64) (*types.Block, []common.Address, error) {
	block, senders, err := r.FullBlockReader.BlockWithSenders(ctx, tx, hash, height)
	r.cancel()
	return block, senders, err
}

func TestCheckSendersReturnsMissingBodyError(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	useTestBlockReader(t, dirs)
	br, _ := blocksIO(db, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Difficulty: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 0))

	require.ErrorContains(t, checkSenders(t.Context(), tx, br, chain2.AllProtocolChanges, 0, log.New()), "missing block body")
}

func TestMdbxCommandsReturnOpenErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	t.Run("f_to_mdbx", func(t *testing.T) {
		previous := file
		file = missing
		t.Cleanup(func() { file = previous })
		require.ErrorIs(t, fToMdbx(t.Context(), log.New(), filepath.Join(t.TempDir(), "output")), os.ErrNotExist)
	})
	t.Run("f_to_mdbx destination", func(t *testing.T) {
		previous := file
		file = filepath.Join(t.TempDir(), "input")
		t.Cleanup(func() { file = previous })
		require.NoError(t, os.WriteFile(file, nil, 0o600))
		notDir := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(notDir, nil, 0o600))
		require.Error(t, fToMdbx(t.Context(), log.New(), notDir))
	})
	t.Run("mdbx_top_dup", func(t *testing.T) {
		require.Error(t, mdbxTopDup(t.Context(), missing, kv.TxLookup, log.New()))
	})
}

func TestCompareStatesReturnsDBOpenError(t *testing.T) {
	notDir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(notDir, []byte("x"), 0o644))

	require.Error(t, compareStates(t.Context(), notDir, notDir))
	require.Error(t, compareBucketBetweenDatabases(t.Context(), notDir, notDir, kv.TxLookup))
}
