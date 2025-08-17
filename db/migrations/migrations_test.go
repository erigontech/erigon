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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestApplyWithInit(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t, kv.ChainDB)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(context.Background())
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
				tx, err := db.BeginRw(context.Background())
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

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, "", "", logger)
	require.NoError(err)
	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)

		_, ok := applied[m[0].Name]
		require.True(ok)
		_, ok = applied[m[1].Name]
		require.True(ok)
		return nil
	})
	require.NoError(err)

	// apply again
	err = migrator.Apply(db, "", "", logger)
	require.NoError(err)
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)
}

func TestApplyWithoutInit(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t, kv.ChainDB)
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
				tx, err := db.BeginRw(context.Background())
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
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Migrations, []byte(m[0].Name), []byte{1})
	})
	require.NoError(err)

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err = migrator.Apply(db, "", "", logger)
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)

		require.Len(applied, 2)
		_, ok := applied[m[1].Name]
		require.True(ok)
		_, ok = applied[m[0].Name]
		require.True(ok)
		return nil
	})
	require.NoError(err)

	// apply again
	err = migrator.Apply(db, "", "", logger)
	require.NoError(err)

	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)

}

func TestWhenNonFirstMigrationAlreadyApplied(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t, kv.ChainDB)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(context.Background())
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
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Migrations, []byte(m[1].Name), []byte{1}) // apply non-first migration
	})
	require.NoError(err)

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err = migrator.Apply(db, "", "", logger)
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)

		require.Len(applied, 2)
		_, ok := applied[m[1].Name]
		require.True(ok)
		_, ok = applied[m[0].Name]
		require.True(ok)
		return nil
	})
	require.NoError(err)

	// apply again
	err = migrator.Apply(db, "", "", logger)
	require.NoError(err)
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)
}

func TestValidation(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t, kv.ChainDB)
	m := []Migration{
		{
			Name: "repeated_name",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				tx, err := db.BeginRw(context.Background())
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
				tx, err := db.BeginRw(context.Background())
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
	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, "", "", logger)
	require.ErrorIs(err, ErrMigrationNonUniqueName)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Empty(applied)
		return nil
	})
	require.NoError(err)
}

func TestCommitCallRequired(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t, kv.ChainDB)
	m := []Migration{
		{
			Name: "one",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
				//don't call BeforeCommit
				return nil
			},
		},
	}
	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, "", "", logger)
	require.ErrorIs(err, ErrMigrationCommitNotCalled)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Empty(applied)
		return nil
	})
	require.NoError(err)
}
