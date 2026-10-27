// Copyright 2024 The Erigon Authors
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
	"maps"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/migrations"
)

func TestOpenRawDBCreatesTablesAddedByPendingMigration(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	dataDir := t.TempDir()
	chaindataPath := filepath.Join(dataDir, "chaindata")
	previousDatadir := datadirCli
	datadirCli = dataDir
	t.Cleanup(func() { datadirCli = previousDatadir })

	oldTables := maps.Clone(kv.ChaindataTablesCfg)
	delete(oldTables, kv.ExecutionPayloadEnvelopeSignatures)
	oldDB, err := kv2.New(dbcfg.ChainDB, logger).
		Path(chaindataPath).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return oldTables }).
		Open(ctx)
	require.NoError(t, err)
	oldDB.Close()

	migrationsDB, err := migrations.OpenMigrationsDB(filepath.Join(dataDir, "migrations"), logger)
	require.NoError(t, err)
	require.NoError(t, migrationsDB.Update(ctx, func(tx kv.RwTx) error {
		for _, migration := range migrations.NewMigrator(dbcfg.ChainDB).Migrations {
			if migration.Name == "create_execution_payload_envelope_signatures_table" {
				continue
			}
			if err := tx.Put(kv.Migrations, []byte(migration.Name), nil); err != nil {
				return err
			}
		}
		return nil
	}))
	migrationsDB.Close()

	var db kv.RwDB
	require.NotPanics(t, func() {
		db, err = openRawDB(dbCfg(dbcfg.ChainDB, chaindataPath), true, logger)
	})
	require.NoError(t, err)
	t.Cleanup(db.Close)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		_, err := tx.GetOne(kv.ExecutionPayloadEnvelopeSignatures, nil)
		return err
	}))
}
