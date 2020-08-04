package migrations

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"testing"

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
				return OnLoadCommit(db, nil, true)
			},
		},
		{
			"two",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
	}

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err := migrator.Apply(db, "")
	require.NoError(err)

	applied, err := AppliedMigrations(db, false)
	require.NoError(err)

	_, ok := applied[migrations[0].Name]
	require.True(ok)
	_, ok = applied[migrations[1].Name]
	require.True(ok)

	// apply again
	err = migrator.Apply(db, "")
	require.NoError(err)

	applied2, err := AppliedMigrations(db, false)
	require.NoError(err)
	require.Equal(applied, applied2)
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
				return OnLoadCommit(db, nil, true)
			},
		},
	}
	err := db.Put(dbutils.Migrations, []byte(migrations[0].Name), []byte{1})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err = migrator.Apply(db, "")
	require.NoError(err)

	applied, err := AppliedMigrations(db, false)
	require.NoError(err)

	require.Equal(2, len(applied))
	_, ok := applied[migrations[1].Name]
	require.True(ok)
	_, ok = applied[migrations[0].Name]
	require.True(ok)

	// apply again
	err = migrator.Apply(db, "")
	require.NoError(err)

	applied2, err := AppliedMigrations(db, false)
	require.NoError(err)
	require.Equal(applied, applied2)
}

func TestWhenNonFirstMigrationAlreadyApplied(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
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

	applied, err := AppliedMigrations(db, false)
	require.NoError(err)

	require.Equal(2, len(applied))
	_, ok := applied[migrations[1].Name]
	require.True(ok)
	_, ok = applied[migrations[0].Name]
	require.True(ok)

	// apply again
	err = migrator.Apply(db, "")
	require.NoError(err)

	applied2, err := AppliedMigrations(db, false)
	require.NoError(err)
	require.Equal(applied, applied2)
}

func TestMarshalStages(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := stages.SaveStageProgress(db, stages.Execution, 42, []byte{})
	require.NoError(err)

	data, err := MarshalMigrationPayload(db)
	require.NoError(err)

	res, err := UnmarshalMigrationPayload(data)
	require.NoError(err)

	require.Equal(1, len(res))
	v, ok := res[string(stages.DBKeys[stages.Execution])]
	require.True(ok)
	require.NotNil(v)
}
