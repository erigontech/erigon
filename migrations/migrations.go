package migrations

import (
	"bytes"
	"errors"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
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
//	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
//		if exists, err := db.(ethdb.NonTransactional).BucketExists(dbutils.SyncStageProgressOld1); err != nil {
//			return err
//		} else if !exists {
//			return nil
//		}
//
//		if err := db.(ethdb.NonTransactional).ClearBuckets(dbutils.SyncStageProgress); err != nil {
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
//		if err := db.(ethdb.NonTransactional).DropBuckets(dbutils.SyncStageProgressOld1); err != nil {  // clear old bucket
//			return err
//		}
//	},
// - if you need migrate multiple buckets - create separate migration for each bucket
// - write test where apply migration twice
var migrations = []Migration{
	stagesToUseNamedKeys,
	unwindStagesToUseNamedKeys,
}

type Migration struct {
	Name string
	Up   func(db ethdb.Database, dataDir string, OnLoadCommit etl.LoadCommitHandler) error
}

func NewMigrator() *Migrator {
	return &Migrator{
		Migrations: migrations,
	}
}

type Migrator struct {
	Migrations []Migration
}

func AppliedMigrations(db ethdb.Database, withPayload bool) (map[string][]byte, error) {
	applied := map[string][]byte{}
	err := db.Walk(dbutils.Migrations, nil, 0, func(k []byte, v []byte) (bool, error) {
		if withPayload {
			applied[string(common.CopyBytes(k))] = common.CopyBytes(v)
		} else {
			applied[string(common.CopyBytes(k))] = []byte{}
		}
		return true, nil
	})
	return applied, err
}

func (m *Migrator) Apply(db ethdb.Database, datadir string) error {
	if len(m.Migrations) == 0 {
		return nil
	}

	applied, err := AppliedMigrations(db, false)
	if err != nil {
		return err
	}

	for i := range m.Migrations {
		v := m.Migrations[i]
		if _, ok := applied[v.Name]; ok {
			continue
		}
		log.Info("Apply migration", "name", v.Name)
		if err := v.Up(db, datadir, func(putter ethdb.Putter, key []byte, isDone bool) error {
			if !isDone {
				return nil // don't save partial progress
			}
			stagesProgress, err := MarshalMigrationPayload(db)
			if err != nil {
				return err
			}
			err = db.Put(dbutils.Migrations, []byte(v.Name), stagesProgress)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		log.Info("Applied migration", "name", v.Name)
	}
	return nil
}

func MarshalMigrationPayload(db ethdb.Getter) ([]byte, error) {
	s := map[string][]byte{}

	buf := bytes.NewBuffer(nil)
	encoder := codec.NewEncoder(buf, &codec.CborHandle{})

	for i := range stages.DBKeys {
		v, err := db.Get(dbutils.SyncStageProgress, stages.DBKeys[i])
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil, err
		}
		if len(v) > 0 {
			s[string(stages.DBKeys[i])] = common.CopyBytes(v)
		}

		v, err = db.Get(dbutils.SyncStageUnwind, stages.DBKeys[i])
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil, err
		}
		if len(v) > 0 {
			s["unwind_"+string(stages.DBKeys[i])] = common.CopyBytes(v)
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
