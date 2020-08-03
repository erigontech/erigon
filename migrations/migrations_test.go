package migrations

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestApplyWithInit(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				OnLoadCommit(db, nil, true)
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				OnLoadCommit(db, nil, true)
				return nil
			},
		},
	}

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err := migrator.Apply(db, "")
	require.NoError(err)

	applied := map[string]bool{}
	err = db.Walk(dbutils.Migrations, nil, 0, func(k []byte, _ []byte) (bool, error) {
		applied[string(common.CopyBytes(k))] = true
		return true, nil
	})
	require.NoError(err)

	_, ok := applied[migrations[0].Name]
	require.True(ok)
	_, ok = applied[migrations[1].Name]
	require.True(ok)
}

func TestApplyWithoutInit(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				OnLoadCommit(db, nil, true)
				return nil
			},
		},
	}
	err := db.Put(dbutils.Migrations, []byte(migrations[0].Name), []byte{1})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err = migrator.Apply(db, "")
	require.NoError(err)

	i := 0
	applied := map[string]bool{}
	err = db.Walk(dbutils.Migrations, nil, 0, func(k []byte, _ []byte) (bool, error) {
		i++
		applied[string(common.CopyBytes(k))] = true
		return true, nil
	})
	require.NoError(err)

	require.Equal(2, i)
	_, ok := applied[migrations[1].Name]
	require.True(ok)
	_, ok = applied[migrations[0].Name]
	require.True(ok)
}

func TestWhenNonFirstMigrationAlreadyApplied(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				OnLoadCommit(db, nil, true)
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
	}
	err := db.Put(dbutils.Migrations, []byte(migrations[1].Name), []byte{1}) // apply non-first migration
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err = migrator.Apply(db, "")
	require.NoError(err)

	i := 0
	applied := map[string]bool{}
	err = db.Walk(dbutils.Migrations, nil, 0, func(k []byte, _ []byte) (bool, error) {
		i++
		applied[string(common.CopyBytes(k))] = true
		return true, nil
	})
	require.NoError(err)

	require.Equal(2, i)
	_, ok := applied[migrations[1].Name]
	require.True(ok)
	_, ok = applied[migrations[0].Name]
	require.True(ok)
}
