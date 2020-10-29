package migrations

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var headersRemoveCanonicalMapping = Migration{
	Name: "headers_remove_canonical_mapping",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		const loadStep = "load"

		collector, err1 := etl.NewCollectorFromFiles(tmpdir)
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			if collector != nil { //  can't use files if progress field not set
				_ = os.RemoveAll(tmpdir)
				collector = nil
			}
		case loadStep:
			if collector == nil {
				return ErrMigrationETLFilesDeleted
			}
			goto LoadStep
		}

		collector = etl.NewCriticalCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		if err1 = db.Walk(dbutils.HeaderPrefix, nil, 0, func(k, v []byte) (bool, error) {
			if dbutils.IsHeaderTDKey(k) {
				if err := collector.Collect(k, v); err != nil {
					return false, fmt.Errorf("collecting key %x: %w", k, err)
				}
			} else if len(k) == 40 {
				encoded := common.CopyBytes(k[:8])
				hashBytes := common.CopyBytes(k[8:])

				expected, err := db.Get(dbutils.HeaderPrefix, append(encoded, dbutils.HeaderHashSuffix...))
				if err != nil {
					return false, err
				}
				if !bytes.Equal(expected, hashBytes) {
					if err := collector.Collect(k, v); err != nil {
						return false, fmt.Errorf("collecting key %x: %w", k, err)
					}
					return true, nil
				}
				if err := collector.Collect(encoded, append(hashBytes, v...)); err != nil {
					return false, fmt.Errorf("collecting key %x: %w", k, err)
				}
			}
			return true, nil
		}); err1 != nil {
			return err1
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderPrefix); err != nil {
			return err
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return err
		}

	LoadStep:
		// Commit again
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return err
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err := collector.Load("headers_remove_canonical_mapping", db, dbutils.HeaderPrefix, etl.IdentityLoadFunc, etl.TransformArgs{OnLoadCommit: CommitProgress}); err != nil {
			return err
		}
		return nil
	},
}
