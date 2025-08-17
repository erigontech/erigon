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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
)

// migrations apply sequentially in order of this array, skips applied migrations
// it allows - don't worry about merge conflicts and use switch branches
// see also dbutils.Migrations - it stores context in which each transaction was exectured - useful for bug-reports
//
// Idempotency is expected
// Best practices to achieve Idempotency:
//   - in dbutils/bucket.go add suffix for existing bucket variable, create new bucket with same variable name.
//     Example:
//   - SyncStageProgress = []byte("SSP1")
//   - SyncStageProgressOld1 = []byte("SSP1")
//   - SyncStageProgress = []byte("SSP2")
//   - in the beginning of migration: check that old bucket exists, clear new bucket
//   - in the end:drop old bucket (not in defer!).
//   - if you need migrate multiple buckets - create separate migration for each bucket
//   - write test - and check that it's safe to apply same migration twice
var migrations = map[kv.Label][]Migration{
	kv.ChainDB: {
		dbSchemaVersion5,
		ResetStageTxnLookup,
	},
	kv.TxPoolDB: {},
	kv.SentryDB: {},
}

type Callback func(tx kv.RwTx, progress []byte, isDone bool) error
type Migration struct {
	Name string
	Up   func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) error
}

var (
	ErrMigrationNonUniqueName   = errors.New("please provide unique migration name")
	ErrMigrationCommitNotCalled = errors.New("migration before-commit function was not called")
	ErrMigrationETLFilesDeleted = errors.New(
		"db migration progress was interrupted after extraction step and ETL files was deleted, please contact development team for help or re-sync from scratch",
	)
)

func NewMigrator(label kv.Label) *Migrator {
	return &Migrator{
		Migrations: migrations[label],
	}
}

type Migrator struct {
	Migrations []Migration
}

func AppliedMigrations(tx kv.Tx, withPayload bool) (map[string][]byte, error) {
	applied := map[string][]byte{}
	err := tx.ForEach(kv.Migrations, nil, func(k []byte, v []byte) error {
		if bytes.HasPrefix(k, []byte("_progress_")) {
			return nil
		}
		if withPayload {
			applied[string(common.CopyBytes(k))] = common.CopyBytes(v)
		} else {
			applied[string(common.CopyBytes(k))] = []byte{}
		}
		return nil
	})
	return applied, err
}

func (m *Migrator) HasPendingMigrations(db kv.RwDB) (bool, error) {
	var has bool
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		pending, err := m.PendingMigrations(tx)
		if err != nil {
			return err
		}
		has = len(pending) > 0
		return nil
	}); err != nil {
		return false, err
	}
	return has, nil
}

func (m *Migrator) PendingMigrations(tx kv.Tx) ([]Migration, error) {
	applied, err := AppliedMigrations(tx, false)
	if err != nil {
		return nil, err
	}

	counter := 0
	for i := range m.Migrations {
		v := m.Migrations[i]
		if _, ok := applied[v.Name]; ok {
			continue
		}
		counter++
	}

	pending := make([]Migration, 0, counter)
	for i := range m.Migrations {
		v := m.Migrations[i]
		if _, ok := applied[v.Name]; ok {
			continue
		}
		pending = append(pending, v)
	}
	return pending, nil
}

func (m *Migrator) VerifyVersion(db kv.RwDB, chaindata string) error {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		major, minor, _, ok, err := rawdb.ReadDBSchemaVersion(tx)
		if err != nil {
			return fmt.Errorf("reading DB schema version: %w", err)
		}
		if ok {
			if major > kv.DBSchemaVersion.Major {
				return fmt.Errorf("cannot downgrade major DB version from %d to %d", major, kv.DBSchemaVersion.Major)
			} else if major == kv.DBSchemaVersion.Major {
				if minor > kv.DBSchemaVersion.Minor {
					return fmt.Errorf("cannot downgrade minor DB version from %d.%d to %d.%d", major, minor, kv.DBSchemaVersion.Major, kv.DBSchemaVersion.Major)
				}
			} else {
				if kv.DBSchemaVersion.Major != major {
					return fmt.Errorf("cannot switch major DB version, db: %d, erigon: %d, try \"rm -rf %s\"", major, kv.DBSchemaVersion.Major, chaindata)
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("migrator.VerifyVersion: %w", err)
	}

	return nil
}

func (m *Migrator) Apply(db kv.RwDB, dataDir, chaindata string, logger log.Logger) error {
	if len(m.Migrations) == 0 {
		return nil
	}
	dirs := datadir.New(dataDir)

	var applied map[string][]byte
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		if err != nil {
			return fmt.Errorf("reading applied migrations: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := m.VerifyVersion(db, chaindata); err != nil {
		return fmt.Errorf("migrator.Apply: %w", err)
	}

	// migration names must be unique, protection against people's mistake
	uniqueNameCheck := map[string]bool{}
	for i := range m.Migrations {
		_, ok := uniqueNameCheck[m.Migrations[i].Name]
		if ok {
			return fmt.Errorf("%w, duplicate: %s", ErrMigrationNonUniqueName, m.Migrations[i].Name)
		}
		uniqueNameCheck[m.Migrations[i].Name] = true
	}

	for i := range m.Migrations {
		v := m.Migrations[i]
		if _, ok := applied[v.Name]; ok {
			continue
		}

		callbackCalled := false // commit function must be called if no error, protection against people's mistake

		logger.Info("Apply migration", "name", v.Name)
		var progress []byte
		if err := db.View(context.Background(), func(tx kv.Tx) (err error) {
			progress, err = tx.GetOne(kv.Migrations, []byte("_progress_"+v.Name))
			return err
		}); err != nil {
			return fmt.Errorf("migrator.Apply: %w", err)
		}

		dirs.Tmp = filepath.Join(dirs.DataDir, "migrations", v.Name)
		dir.MustExist(dirs.Tmp)
		if err := v.Up(db, dirs, progress, func(tx kv.RwTx, key []byte, isDone bool) error {
			if !isDone {
				if key != nil {
					if err := tx.Put(kv.Migrations, []byte("_progress_"+v.Name), key); err != nil {
						return err
					}
				}
				return nil
			}
			callbackCalled = true

			stagesProgress, err := json.Marshal(tx)
			if err != nil {
				return err
			}
			err = tx.Put(kv.Migrations, []byte(v.Name), stagesProgress)
			if err != nil {
				return err
			}

			err = tx.Delete(kv.Migrations, []byte("_progress_"+v.Name))
			if err != nil {
				return err
			}

			return nil
		}, logger); err != nil {
			return fmt.Errorf("migrator.Apply.Up: %s, %w", v.Name, err)
		}

		if !callbackCalled {
			return fmt.Errorf("%w: %s", ErrMigrationCommitNotCalled, v.Name)
		}
		logger.Info("Applied migration", "name", v.Name)
	}
	if err := db.Update(context.Background(), rawdb.WriteDBSchemaVersion); err != nil {
		return fmt.Errorf("migrator.Apply: %w", err)
	}
	logger.Info(
		"Updated DB schema to",
		"version",
		fmt.Sprintf(
			"%d.%d.%d",
			kv.DBSchemaVersion.Major,
			kv.DBSchemaVersion.Minor,
			kv.DBSchemaVersion.Patch,
		),
	)
	return nil
}
