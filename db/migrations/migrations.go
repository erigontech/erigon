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

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
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
	dbcfg.ChainDB: {
		domainLargeValuesLayout,
		dbSchemaVersion5,
		ResetStageTxnLookup,
		dbSchemaVersion6,
		dbSchemaVersion7,
		dropLegacyE2Tables,
	},
	dbcfg.TxPoolDB: {},
	dbcfg.SentryDB: {},
}

type Callback func(tx kv.RwTx, progress []byte, isDone bool) error
type Migration struct {
	Name string
	Up   func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) error

	// WipeDataIfMajorBelow, when non-zero, makes Apply wipe and reopen the DB
	// when the stored DBSchemaVersion.Major is below this value, ignoring Up.
	// Requires Migrator.ReopenDB to be set.
	WipeDataIfMajorBelow uint32
}

var (
	ErrMigrationNonUniqueName   = errors.New("please provide unique migration name")
	ErrMigrationCommitNotCalled = errors.New("migration before-commit function was not called")
	ErrMigrationETLFilesDeleted = errors.New(
		"db migration progress was interrupted after extraction step and ETL files was deleted, please contact development team for help or re-sync from scratch",
	)
)

// OpenMigrationsDB opens (or creates) the dedicated migrations-tracking database at the given
// directory. Only the kv.Migrations table is opened; all other tables are excluded. The DB
// survives deletion of any other sub-database (e.g. chaindata) so migration state persists
// across datadir clean-ups.
//
// MapSize is capped at 1 GB: the DB only tracks migration names (kilobytes
// even with thousands of migrations) and the default 2 TB MDBX reservation
// would otherwise consume process address space at a rate that limits how
// many engine-api testers can coexist in one process.
func OpenMigrationsDB(migrationsDir string, logger log.Logger) (kv.RwDB, error) {
	dir.MustExist(migrationsDir)
	return kv2.New(dbcfg.MigrationsDB, logger).
		Path(migrationsDir).
		MapSize(1 * datasize.GB).
		Open(context.Background())
}

func NewMigrator(label kv.Label) *Migrator {
	return &Migrator{
		Migrations: migrations[label],
	}
}

type Migrator struct {
	Migrations []Migration
	ReopenDB   func() (kv.RwDB, error)
}

// AppliedMigrations returns the set of migration names that have already been recorded as
// complete. tx must be a read transaction on the migrations-tracking DB (opened via
// OpenMigrationsDB), NOT on the target database.
func AppliedMigrations(tx kv.Tx, withPayload bool) (map[string][]byte, error) {
	applied := map[string][]byte{}
	err := tx.ForEach(kv.Migrations, nil, func(k []byte, v []byte) error {
		if bytes.HasPrefix(k, []byte("_progress_")) {
			return nil
		}
		if withPayload {
			applied[string(common.Copy(k))] = common.Copy(v)
		} else {
			applied[string(common.Copy(k))] = []byte{}
		}
		return nil
	})
	return applied, err
}

// HasPendingMigrations reports whether any registered migrations have not yet been applied.
// migrationsDB must be the database returned by OpenMigrationsDB.
func (m *Migrator) HasPendingMigrations(migrationsDB kv.RwDB) (bool, error) {
	var has bool
	if err := migrationsDB.View(context.Background(), func(tx kv.Tx) error {
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

// PendingMigrations returns the subset of registered migrations that have not yet been applied.
// tx must be a read transaction on the migrations-tracking DB.
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

// readMajorVersion reads the stored DBSchemaVersion.Major from db.
// Returns (0, false, nil) when no version record exists yet.
func readMajorVersion(db kv.RoDB) (major uint32, ok bool, err error) {
	err = db.View(context.Background(), func(tx kv.Tx) error {
		major, _, _, ok, err = rawdb.ReadDBSchemaVersion(tx)
		return err
	})
	return major, ok, err
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
					return fmt.Errorf("cannot downgrade minor DB version from %d.%d to %d.%d", major, minor, kv.DBSchemaVersion.Major, kv.DBSchemaVersion.Minor)
				}
			}
			// major < DBSchemaVersion.Major: upgrade handled by migrations
		}
		return nil
	}); err != nil {
		return fmt.Errorf("migrator.VerifyVersion: %w", err)
	}

	return nil
}

// Apply runs all pending migrations in order and returns the (possibly reopened)
// target DB handle. Callers must use the returned handle rather than the one passed
// in, because a wipe migration may have closed and replaced it.
//
// On error the returned db may be nil (if the old handle was closed for a wipe but
// reopening failed). Callers must close a non-nil returned handle even on error.
//
//   - db is the target database being migrated (e.g. chaindata).
//   - migrationsDB is the dedicated migrations-tracking database (opened via OpenMigrationsDB).
//     Applied-migration records are written here, so they survive deletion of the target DB.
//   - dataDir is the root data directory (used to set up per-migration temp dirs).
//   - chaindata is the path to the target DB directory (used only in error messages).
func (m *Migrator) Apply(db kv.RwDB, migrationsDB kv.RwDB, dataDir, chaindata string, logger log.Logger) (kv.RwDB, error) {
	if len(m.Migrations) == 0 {
		return db, nil
	}
	dirs := datadir.New(dataDir)

	var applied map[string][]byte
	if err := migrationsDB.View(context.Background(), func(tx kv.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		if err != nil {
			return fmt.Errorf("reading applied migrations: %w", err)
		}
		return nil
	}); err != nil {
		return db, err
	}
	if err := m.VerifyVersion(db, chaindata); err != nil {
		return db, fmt.Errorf("migrator.Apply: %w", err)
	}

	// migration names must be unique, protection against people's mistake
	uniqueNameCheck := map[string]bool{}
	for i := range m.Migrations {
		_, ok := uniqueNameCheck[m.Migrations[i].Name]
		if ok {
			return db, fmt.Errorf("%w, duplicate: %s", ErrMigrationNonUniqueName, m.Migrations[i].Name)
		}
		uniqueNameCheck[m.Migrations[i].Name] = true
	}

	for i := range m.Migrations {
		v := m.Migrations[i]
		if _, ok := applied[v.Name]; ok {
			continue
		}
		var err error
		db, err = m.applyOne(db, migrationsDB, v, dirs, logger)
		if err != nil {
			return db, err
		}
	}
	if err := db.Update(context.Background(), rawdb.WriteDBSchemaVersion); err != nil {
		return db, fmt.Errorf("migrator.Apply: %w", err)
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
	return db, nil
}

// applyOne runs a single migration v against db, recording completion in migrationsDB.
// It returns the (possibly replaced) db handle — a wipe migration closes the old handle
// and opens a fresh one.
func (m *Migrator) applyOne(db kv.RwDB, migrationsDB kv.RwDB, v Migration, dirs datadir.Dirs, logger log.Logger) (kv.RwDB, error) {
	logger.Info("Apply migration", "name", v.Name)

	if v.WipeDataIfMajorBelow > 0 {
		major, versionOK, err := readMajorVersion(db)
		if err != nil {
			return db, fmt.Errorf("migrator.applyOne: %w", err)
		}
		if versionOK && major < v.WipeDataIfMajorBelow {
			if m.ReopenDB == nil {
				return db, fmt.Errorf("migrator.applyOne: migration %s requires ReopenDB", v.Name)
			}
			dbPath := db.Path()
			if dbPath == "" || !filepath.IsAbs(dbPath) {
				return db, fmt.Errorf("migrator.applyOne: migration %s: refusing wipe on unsafe DB path %q", v.Name, dbPath)
			}
			logger.Warn("[migration] chaindata predates required schema — wiping; state will be re-synced from snapshots",
				"migration", v.Name, "db_major", major, "min_major", v.WipeDataIfMajorBelow, "path", dbPath)
			db.Close()
			if err := dir.RemoveAll(dbPath); err != nil {
				return nil, fmt.Errorf("migrator.applyOne: wipe %s: %w", dbPath, err)
			}
			newDB, err := m.ReopenDB()
			if err != nil {
				return nil, fmt.Errorf("migrator.applyOne: reopen after wipe: %w", err)
			}
			db = newDB
		}
		if err := migrationsDB.Update(context.Background(), func(migTx kv.RwTx) error {
			if err := migTx.Put(kv.Migrations, []byte(v.Name), nil); err != nil {
				return err
			}
			return migTx.Delete(kv.Migrations, []byte("_progress_"+v.Name))
		}); err != nil {
			return db, fmt.Errorf("migrator.applyOne: record migration %s: %w", v.Name, err)
		}
		logger.Info("Applied migration", "name", v.Name)
		return db, nil
	}

	callbackCalled := false // commit function must be called if no error, protection against people's mistake

	var progress []byte
	if err := migrationsDB.View(context.Background(), func(tx kv.Tx) (err error) {
		progress, err = tx.GetOne(kv.Migrations, []byte("_progress_"+v.Name))
		return err
	}); err != nil {
		return db, fmt.Errorf("migrator.applyOne: %w", err)
	}

	// Each migration gets its own sub-directory inside dirs.Migrations for ETL temp files.
	dirs.Tmp = filepath.Join(dirs.Migrations, v.Name)
	dir.MustExist(dirs.Tmp)
	if err := v.Up(db, dirs, progress, func(tx kv.RwTx, key []byte, isDone bool) error {
		if !isDone {
			if key != nil {
				// Persist resumable progress in the migrations DB.
				if err := migrationsDB.Update(context.Background(), func(migTx kv.RwTx) error {
					return migTx.Put(kv.Migrations, []byte("_progress_"+v.Name), key)
				}); err != nil {
					return err
				}
			}
			return nil
		}
		callbackCalled = true

		// Capture the target DB's stage state for bug-report context, then record
		// the migration as complete in the migrations-tracking DB.
		stagesProgress, err := json.Marshal(tx)
		if err != nil {
			return err
		}
		return migrationsDB.Update(context.Background(), func(migTx kv.RwTx) error {
			if err := migTx.Put(kv.Migrations, []byte(v.Name), stagesProgress); err != nil {
				return err
			}
			return migTx.Delete(kv.Migrations, []byte("_progress_"+v.Name))
		})
	}, logger); err != nil {
		return db, fmt.Errorf("migrator.applyOne.Up: %s, %w", v.Name, err)
	}

	if !callbackCalled {
		return db, fmt.Errorf("%w: %s", ErrMigrationCommitNotCalled, v.Name)
	}
	logger.Info("Applied migration", "name", v.Name)
	return db, nil
}
