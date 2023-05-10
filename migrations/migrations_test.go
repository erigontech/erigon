package migrations

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestApplyWithInit(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
	err := migrator.Apply(db, "", logger)
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
	err = migrator.Apply(db, "", logger)
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
	require, db := require.New(t), memdb.NewTestDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
		{
			"two",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
	err = migrator.Apply(db, "", logger)
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
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
	err = migrator.Apply(db, "", logger)
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
	require, db := require.New(t), memdb.NewTestDB(t)
	m := []Migration{
		{
			"one",
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
			func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
	err = migrator.Apply(db, "", logger)
	require.NoError(err)

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
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
	err = migrator.Apply(db, "", logger)
	require.NoError(err)
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied2, err := AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(applied, applied2)
		return nil
	})
	require.NoError(err)
}

func TestMarshalStages(t *testing.T) {
	require := require.New(t)
	_, tx := memdb.NewTestTx(t)

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
	require, db := require.New(t), memdb.NewTestDB(t)
	m := []Migration{
		{
			Name: "repeated_name",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
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
	err := migrator.Apply(db, "", logger)
	require.True(errors.Is(err, ErrMigrationNonUniqueName))

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(0, len(applied))
		return nil
	})
	require.NoError(err)
}

func TestCommitCallRequired(t *testing.T) {
	require, db := require.New(t), memdb.NewTestDB(t)
	m := []Migration{
		{
			Name: "one",
			Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
				//don't call BeforeCommit
				return nil
			},
		},
	}
	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = m
	logger := log.New()
	err := migrator.Apply(db, "", logger)
	require.True(errors.Is(err, ErrMigrationCommitNotCalled))

	var applied map[string][]byte
	err = db.View(context.Background(), func(tx kv.Tx) error {
		applied, err = AppliedMigrations(tx, false)
		require.NoError(err)
		require.Equal(0, len(applied))
		return nil
	})
	require.NoError(err)
}
