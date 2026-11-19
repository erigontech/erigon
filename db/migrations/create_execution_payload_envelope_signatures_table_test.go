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

package migrations

import (
	"maps"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func TestCreateExecutionPayloadEnvelopeSignaturesTableMigration(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	dataDir := t.TempDir()
	chaindata := filepath.Join(dataDir, "chaindata")
	oldTables := maps.Clone(kv.ChaindataTablesCfg)
	delete(oldTables, kv.ExecutionPayloadEnvelopeSignatures)

	oldDB, openErr := mdbx.New(dbcfg.ChainDB, logger).
		Path(chaindata).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return oldTables }).
		Open(ctx)
	require.NoError(t, openErr)
	oldDB.Close()

	migrationsDB, openErr := OpenMigrationsDB(filepath.Join(dataDir, "migrations"), logger)
	require.NoError(t, openErr)
	t.Cleanup(migrationsDB.Close)

	migrator := NewMigrator(dbcfg.ChainDB)
	require.NoError(t, migrationsDB.Update(ctx, func(tx kv.RwTx) error {
		for _, migration := range migrator.Migrations {
			if migration.Name == createExecutionPayloadEnvelopeSignaturesTable.Name {
				continue
			}
			if putErr := tx.Put(kv.Migrations, []byte(migration.Name), nil); putErr != nil {
				return putErr
			}
		}
		return nil
	}))

	hasPending, pendingErr := migrator.HasPendingMigrations(migrationsDB)
	require.NoError(t, pendingErr)
	require.True(t, hasPending)

	db, exclusiveOpenErr := mdbx.New(dbcfg.ChainDB, logger).Path(chaindata).Exclusive(true).Open(ctx)
	require.NoError(t, exclusiveOpenErr)
	t.Cleanup(db.Close)
	require.NoError(t, migrator.Apply(db, migrationsDB, dataDir, chaindata, logger))
	db.Close()

	db, accedeOpenErr := mdbx.New(dbcfg.ChainDB, logger).Path(chaindata).Accede(true).Open(ctx)
	require.NoError(t, accedeOpenErr)
	t.Cleanup(db.Close)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		_, err := tx.GetOne(kv.ExecutionPayloadEnvelopeSignatures, nil)
		return err
	}))
}
