package migrations

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/kv"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/stretchr/testify/require"
)

func TestApplyWithInit(t *testing.T) {
	require, db := require.New(t), kv.NewTestKV(t)
	m := []Migration{
		{
			"one",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
		{
			"two",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
	}

	migrator := NewMigrator(ethdb.Chain)
	migrator.Migrations = m
	err := migrator.Apply(db, "")
	require.NoError(err)
	var applied map[string][]byte
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
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
	err = migrator.Apply(db, "")
	require.NoError(err)
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)
}

func TestApplyWithoutInit(t *testing.T) {
	require, db := require.New(t), kv.NewTestKV(t)
	m := []Migration{
		{
			"one",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
	}
	err := db.Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.Put(dbutils.Migrations, []byte(m[0].Name), []byte{1})
	})
	require.NoError(err)

	migrator := NewMigrator(ethdb.Chain)
	migrator.Migrations = m
	err = migrator.Apply(db, "")
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)

		require.Equal(2, len(applied))
		_, ok := applied[m[1].Name]
		require.True(ok)
		_, ok = applied[m[0].Name]
		require.True(ok)
		return nil
	})
	require.NoError(err)

	// apply again
	err = migrator.Apply(db, "")
	require.NoError(err)

	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)

}

func TestWhenNonFirstMigrationAlreadyApplied(t *testing.T) {
	require, db := require.New(t), kv.NewTestKV(t)
	m := []Migration{
		{
			"one",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
		{
			"two",
			func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
	}
	err := db.Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.Put(dbutils.Migrations, []byte(m[1].Name), []byte{1}) // apply non-first migration
	})
	require.NoError(err)

	migrator := NewMigrator(ethdb.Chain)
	migrator.Migrations = m
	err = migrator.Apply(db, "")
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)

		require.Equal(2, len(applied))
		_, ok := applied[m[1].Name]
		require.True(ok)
		_, ok = applied[m[0].Name]
		require.True(ok)
		return nil
	})
	require.NoError(err)

	// apply again
	err = migrator.Apply(db, "")
	require.NoError(err)
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)
}

func TestMarshalStages(t *testing.T) {
	require := require.New(t)
	_, tx := kv.NewTestTx(t)

	err := stages.SaveStageProgress(tx, stages.Execution, 42)
	require.NoError(err)

	data, err := MarshalMigrationPayload(tx)
	require.NoError(err)

	res, err := UnmarshalMigrationPayload(data)
	require.NoError(err)

	require.Equal(1, len(res))
	v, ok := res[string(stages.Execution)]
	require.True(ok)
	require.NotNil(v)
}

func TestValidation(t *testing.T) {
	require, db := require.New(t), kv.NewTestKV(t)
	m := []Migration{
		{
			Name: "repeated_name",
			Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
		{
			Name: "repeated_name",
			Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return OnLoadCommit(db, nil, true)
			},
		},
	}
	migrator := NewMigrator(ethdb.Chain)
	migrator.Migrations = m
	err := migrator.Apply(db, "")
	require.True(errors.Is(err, ErrMigrationNonUniqueName))

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(0, len(applied))
		return nil
	})
	require.NoError(err)
}

func TestCommitCallRequired(t *testing.T) {
	require, db := require.New(t), kv.NewTestKV(t)
	m := []Migration{
		{
			Name: "one",
			Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
				return nil // don't call OnLoadCommit
			},
		},
	}
	migrator := NewMigrator(ethdb.Chain)
	migrator.Migrations = m
	err := migrator.Apply(db, "")
	require.True(errors.Is(err, ErrMigrationCommitNotCalled))

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(0, len(applied))
		return nil
	})
	require.NoError(err)
}
