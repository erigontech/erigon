package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ugorji/go/codec"
)

// migrations apply sequentially in order of this array, skips applied migrations
// it allows - don't worry about merge conflicts and use switch branches
// see also dbutils.Migrations - it stores context in which each transaction was exectured - useful for bug-reports
//
// Idempotency is expected
// Best practices to achieve Idempotency:
// - in dbutils/bucket.go add suffix for existing bucket variable, create new bucket with same variable name.
//	Example:
//		- SyncStageProgress = []byte("SSP1")
//		+ SyncStageProgressOld1 = []byte("SSP1")
//		+ SyncStageProgress = []byte("SSP2")
// - in the beginning of migration: check that old bucket exists, clear new bucket
// - in the end:drop old bucket (not in defer!).
//	Example:
//	Up: func(db ethdb.Database, tmpdir string, OnLoadCommit etl.LoadCommitHandler) error {
//		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.SyncStageProgressOld1); err != nil {
//			return err
//		} else if !exists {
//			return OnLoadCommit(db, nil, true)
//		}
//
//		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.SyncStageProgress); err != nil {
//			return err
//		}
//
//		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
//			... // migration logic
//		}
//		if err := etl.Transform(...); err != nil {
//			return err
//		}
//
//		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.SyncStageProgressOld1); err != nil {  // clear old bucket
//			return err
//		}
//	},
// - if you need migrate multiple buckets - create separate migration for each bucket
// - write test where apply migration twice
var migrations = map[ethdb.Label][]Migration{
	ethdb.Chain: {
		headerPrefixToSeparateBuckets,
		removeCliqueBucket,
		dbSchemaVersion,
		rebuilCallTraceIndex,
	},
	ethdb.TxPool: {},
	ethdb.Sentry: {},
}

type Migration struct {
	Name string
	Up   func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommitOnLoadCommit etl.LoadCommitHandler) error
}

var (
	ErrMigrationNonUniqueName   = fmt.Errorf("please provide unique migration name")
	ErrMigrationCommitNotCalled = fmt.Errorf("migration commit function was not called")
	ErrMigrationETLFilesDeleted = fmt.Errorf("db migration progress was interrupted after extraction step and ETL files was deleted, please contact development team for help or re-sync from scratch")
)

func NewMigrator(label ethdb.Label) *Migrator {
	return &Migrator{
		Migrations: migrations[label],
	}
}

type Migrator struct {
	Migrations []Migration
}

func AppliedMigrations(tx ethdb.Tx, withPayload bool) (map[string][]byte, error) {
	applied := map[string][]byte{}
	err := tx.ForEach(dbutils.Migrations, nil, func(k []byte, v []byte) error {
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

func (m *Migrator) HasPendingMigrations(db ethdb.RwKV) (bool, error) {
	var has bool
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
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

func (m *Migrator) PendingMigrations(tx ethdb.Tx) ([]Migration, error) {
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

func (m *Migrator) Apply(db ethdb.Database, datadir string) error {
	if len(m.Migrations) == 0 {
		return nil
	}

	var applied map[string][]byte
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		var err error
		applied, err = AppliedMigrations(tx, false)
		return err
	}); err != nil {
		return err
	}

	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()

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

		commitFuncCalled := false // commit function must be called if no error, protection against people's mistake

		log.Info("Apply migration", "name", v.Name)
		progress, err := tx.GetOne(dbutils.Migrations, []byte("_progress_"+v.Name))
		if err != nil {
			return err
		}

		if err = v.Up(tx, path.Join(datadir, "migrations", v.Name), progress, func(_ ethdb.Putter, key []byte, isDone bool) error {
			if !isDone {
				if key != nil {
					err = tx.Put(dbutils.Migrations, []byte("_progress_"+v.Name), key)
					if err != nil {
						return err
					}
				}
				// do commit, but don't save partial progress
				if err := tx.CommitAndBegin(context.Background()); err != nil {
					return err
				}
				return nil
			}
			commitFuncCalled = true

			stagesProgress, err := MarshalMigrationPayload(tx)
			if err != nil {
				return err
			}
			err = tx.Put(dbutils.Migrations, []byte(v.Name), stagesProgress)
			if err != nil {
				return err
			}

			err = tx.Delete(dbutils.Migrations, []byte("_progress_"+v.Name), nil)
			if err != nil {
				return err
			}

			if err := tx.CommitAndBegin(context.Background()); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		if !commitFuncCalled {
			return fmt.Errorf("%w: %s", ErrMigrationCommitNotCalled, v.Name)
		}
		log.Info("Applied migration", "name", v.Name)
	}
	// Write DB schema version
	var version [12]byte
	binary.BigEndian.PutUint32(version[:], dbutils.DBSchemaVersion.Major)
	binary.BigEndian.PutUint32(version[4:], dbutils.DBSchemaVersion.Minor)
	binary.BigEndian.PutUint32(version[8:], dbutils.DBSchemaVersion.Patch)
	if err := tx.Put(dbutils.DatabaseInfoBucket, dbutils.DBSchemaVersionKey, version[:]); err != nil {
		return fmt.Errorf("writing DB schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing DB version update: %w", err)
	}
	log.Info("Updated DB schema to", "version", fmt.Sprintf("%d.%d.%d", dbutils.DBSchemaVersion.Major, dbutils.DBSchemaVersion.Minor, dbutils.DBSchemaVersion.Patch))
	return nil
}

func MarshalMigrationPayload(db ethdb.Getter) ([]byte, error) {
	s := map[string][]byte{}

	buf := bytes.NewBuffer(nil)
	encoder := codec.NewEncoder(buf, &codec.CborHandle{})

	for _, stage := range stages.AllStages {
		v, err := db.GetOne(dbutils.SyncStageProgress, []byte(stage))
		if err != nil {
			return nil, err
		}
		if len(v) > 0 {
			s[string(stage)] = common.CopyBytes(v)
		}

		v, err = db.GetOne(dbutils.SyncStageUnwind, []byte(stage))
		if err != nil {
			return nil, err
		}
		if len(v) > 0 {
			s["unwind_"+string(stage)] = common.CopyBytes(v)
		}
	}

	if err := encoder.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalMigrationPayload(data []byte) (map[string][]byte, error) {
	s := map[string][]byte{}

	if err := codec.NewDecoder(bytes.NewReader(data), &codec.CborHandle{}).Decode(&s); err != nil {
		return nil, err
	}
	return s, nil
}
