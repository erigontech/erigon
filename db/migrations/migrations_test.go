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

package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// newTestMigrationsDB opens an in-memory migrations-tracking DB for use in tests.
func newTestMigrationsDB(t *testing.T) kv.RwDB {
	t.Helper()
	return mdbxtest.NewTestDB(t, dbcfg.MigrationsDB)
}

func TestApplyWithInit(t *testing.T) {
	require, db := require.New(t), mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	migrationsDB := newTestMigrationsDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
		{
			"two",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
	}

	migrator := NewMigrator(dbcfg.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))
	var applied map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		return err
	}))
	_, ok := applied[m[0].Name]
	require.True(ok)
	_, ok = applied[m[1].Name]
	require.True(ok)

	// apply again
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))
	var applied2 map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied2, err = AppliedMigrations(tx, false)
		return err
	}))
	require.Equal(applied, applied2)
}

func TestApplyWithoutInit(t *testing.T) {
	require, db := require.New(t), mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	migrationsDB := newTestMigrationsDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
		{
			"two",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
	}
	require.NoError(migrationsDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.Migrations, []byte(m[0].Name), []byte{1})
	}))

	migrator := NewMigrator(dbcfg.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))

	var applied map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		return err
	}))
	require.Len(applied, 2)
	_, ok := applied[m[1].Name]
	require.True(ok)
	_, ok = applied[m[0].Name]
	require.True(ok)

	// apply again
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))

	var applied2 map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied2, err = AppliedMigrations(tx, false)
		return err
	}))
	require.Equal(applied, applied2)

}

func TestWhenNonFirstMigrationAlreadyApplied(t *testing.T) {
	require, db := require.New(t), mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	migrationsDB := newTestMigrationsDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
		{
			"two",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
	}
	require.NoError(migrationsDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.Migrations, []byte(m[1].Name), []byte{1}) // apply non-first migration
	}))

	migrator := NewMigrator(dbcfg.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))

	var applied map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		return err
	}))
	require.Len(applied, 2)
	_, ok := applied[m[1].Name]
	require.True(ok)
	_, ok = applied[m[0].Name]
	require.True(ok)

	// apply again
	require.NoError(migrator.Apply(db, migrationsDB, "", "", logger))
	var applied2 map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied2, err = AppliedMigrations(tx, false)
		return err
	}))
	require.Equal(applied, applied2)
}

func TestValidation(t *testing.T) {
	require, db := require.New(t), mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	migrationsDB := newTestMigrationsDB(t)
	m := []Migration{
		{
			Name: "repeated_name",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
		{
			Name: "repeated_name",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(t.Context())
				if err != nil {
					return err
				}
				defer tx.Rollback()

				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			},
		},
	}
	migrator := NewMigrator(dbcfg.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, migrationsDB, "", "", logger)
	require.ErrorIs(err, ErrMigrationNonUniqueName)

	var applied map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Empty(applied)
		return nil
	}))
}

func TestCommitCallRequired(t *testing.T) {
	require, db := require.New(t), mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctx := t.Context()
	migrationsDB := newTestMigrationsDB(t)
	m := []Migration{
		{
			Name: "one",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				//don't call BeforeCommit
				return nil
			},
		},
	}
	migrator := NewMigrator(dbcfg.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, migrationsDB, "", "", logger)
	require.ErrorIs(err, ErrMigrationCommitNotCalled)

	var applied map[string][]byte
	require.NoError(migrationsDB.View(ctx, func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Empty(applied)
		return nil
	}))
}
