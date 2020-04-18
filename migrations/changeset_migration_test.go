package migrations

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestChangeSetMigrationSuccess(t *testing.T) {
	f := func(t *testing.T, n, batchSize int) {
		db := ethdb.NewMemDatabase()
		accCS, storageCS := generateChangeSets(t, n)
		for i, v := range accCS {
			enc, err := changeset.EncodeChangeSet(v)
			if err != nil {
				t.Error(err)
			}
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.AccountsHistoryBucket), common.CopyBytes(enc))
			if err != nil {
				t.Error(err)
			}
		}
		for i, v := range storageCS {
			enc, err := changeset.EncodeChangeSet(v)
			if err != nil {
				t.Error(err)
			}
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.StorageHistoryBucket), common.CopyBytes(enc))
			if err != nil {
				t.Error(err)
			}
		}
		migrator := NewMigrator()
		migrator.Migrations = []Migration{splitChangeSetMigration(batchSize)}
		err := migrator.Apply(db, false, false, false, false, false)
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			ts, bucket := dbutils.DecodeTimestamp(k)
			return false, fmt.Errorf("changeset bucket is not empty block %v bucket %v", ts, string(bucket))
		})
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			cs, innerErr := changeset.DecodeChangeSet(v)
			if innerErr != nil {
				return false, fmt.Errorf("decode fails block - %v err: %v", blockNum, innerErr)
			}
			if !cs.Equals(accCS[blockNum]) {
				spew.Dump(cs)
				spew.Dump(accCS[blockNum])
				return false, fmt.Errorf("not equal (%v)", blockNum)
			}
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			cs, innerErr := changeset.DecodeChangeSet(v)
			if innerErr != nil {
				return false, fmt.Errorf("decode fails block - %v err: %v", blockNum, innerErr)
			}
			if !cs.Equals(storageCS[blockNum]) {
				spew.Dump(cs)
				spew.Dump(storageCS[blockNum])
				return false, fmt.Errorf("not equal (%v)", blockNum)
			}
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("less batch", func(t *testing.T) {
		f(t, 50, 60)
	})
	t.Run("more batch", func(t *testing.T) {
		f(t, 100, 60)
	})
	t.Run("two batches", func(t *testing.T) {
		f(t, 100, 50)
	})
}

func TestChangeSetMigrationThinHistorySuccess(t *testing.T) {
	f := func(t *testing.T, n, batchSize int) {
		db := ethdb.NewMemDatabase()
		accCS, storageCS := generateChangeSets(t, n)

		for i, v := range accCS {
			enc, err := changeset.EncodeChangeSet(v)
			if err != nil {
				t.Error(err)
			}
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.AccountsHistoryBucket), common.CopyBytes(enc))
			if err != nil {
				t.Error(err)
			}
		}

		for i, v := range storageCS {
			enc, err := changeset.EncodeChangeSet(v)
			if err != nil {
				t.Error(err)
			}
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.StorageHistoryBucket), common.CopyBytes(enc))
			if err != nil {
				t.Error(err)
			}
		}

		migrator := NewMigrator()
		migrator.Migrations = []Migration{splitChangeSetMigration(batchSize)}
		err := migrator.Apply(db, false, false, false, false, true)
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			ts, bucket := dbutils.DecodeTimestamp(k)
			return false, fmt.Errorf("changeset bucket is not empty block %v bucket %v", ts, string(bucket))
		})
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			cs, innerErr := changeset.DecodeAccounts(v)
			if innerErr != nil {
				return false, innerErr
			}

			if !cs.Equals(accCS[blockNum]) {
				spew.Dump(cs)
				spew.Dump(accCS[blockNum])
				return false, fmt.Errorf("not equal %v", blockNum)
			}
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}

		err = db.Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			cs, innerErr := changeset.DecodeStorage(v)
			if innerErr != nil {
				t.Error(innerErr, blockNum)
			}
			if !cs.Equals(storageCS[blockNum]) {
				spew.Dump(cs)
				spew.Dump(storageCS[blockNum])
				return false, fmt.Errorf("not equal (%v)", blockNum)
			}
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("less batch", func(t *testing.T) {
		f(t, 50, 60)
	})
	t.Run("more batch", func(t *testing.T) {
		f(t, 100, 60)
	})
	t.Run("two batches", func(t *testing.T) {
		f(t, 100, 50)
	})

}

func TestChangeSetMigrationFail(t *testing.T) {
	db := ethdb.NewMemDatabase()
	accCS, storageCS := generateChangeSets(t, 50)

	for i, v := range accCS {
		enc, err := changeset.EncodeChangeSet(v)
		if err != nil {
			t.Error(err)
		}
		if i == 25 {
			//incorrect value
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.AccountsHistoryBucket), common.CopyBytes(enc[:5]))
			if err != nil {
				t.Error(err)
			}
		} else {
			err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.AccountsHistoryBucket), common.CopyBytes(enc))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for i, v := range storageCS {
		enc, err := changeset.EncodeChangeSet(v)
		if err != nil {
			t.Error(err)
		}
		err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(i)), dbutils.StorageHistoryBucket), common.CopyBytes(enc))
		if err != nil {
			t.Error(err)
		}
	}

	migrator := NewMigrator()
	migrator.Migrations = []Migration{splitChangeSetMigration(20)}
	err := migrator.Apply(db, false, false, false, false, true)
	if err == nil {
		t.Error("should fail")
	}

	_, err = db.Get(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration)
	if err == nil {
		t.Error("should fail")
	}

	//fix incorrect changeset
	enc, err := changeset.EncodeChangeSet(accCS[25])
	if err != nil {
		t.Error(err)
	}
	err = db.Put(ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(uint64(25)), dbutils.AccountsHistoryBucket), common.CopyBytes(enc))
	if err != nil {
		t.Error(err)
	}

	err = migrator.Apply(db, false, false, false, false, true)
	if err != nil {
		t.Error(err)
	}

	err = db.Walk(ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		return false, fmt.Errorf("changeset bucket is not empty block %v bucket %v", ts, string(bucket))
	})
	if err != nil {
		t.Error(err)
	}

	err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		cs, innerErr := changeset.DecodeAccounts(v)
		if innerErr != nil {
			return false, innerErr
		}

		if !cs.Equals(accCS[blockNum]) {
			spew.Dump(cs)
			spew.Dump(accCS[blockNum])
			return false, fmt.Errorf("not equal %v", blockNum)
		}
		return true, nil
	})
	if err != nil {
		t.Error(err)
	}

	err = db.Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		cs, innerErr := changeset.DecodeStorage(v)
		if innerErr != nil {
			t.Error(innerErr, blockNum)
		}
		if !cs.Equals(storageCS[blockNum]) {
			spew.Dump(cs)
			spew.Dump(storageCS[blockNum])
			return false, fmt.Errorf("not equal (%v)", blockNum)
		}
		return true, nil
	})
	if err != nil {
		t.Error(err)
	}
}

func generateChangeSets(t *testing.T, n int) ([]*changeset.ChangeSet, []*changeset.ChangeSet) {
	t.Helper()
	accChangeset := make([]*changeset.ChangeSet, n)
	storageChangeset := make([]*changeset.ChangeSet, n)
	for i := 0; i < n; i++ {
		csAcc := changeset.NewChangeSet()
		hash := common.Hash{uint8(i)}
		err := csAcc.Add(hash.Bytes(), hash.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		accChangeset[i] = csAcc

		csStorage := changeset.NewChangeSet()
		err = csStorage.Add(
			dbutils.GenerateCompositeStorageKey(
				hash,
				^uint64(1),
				hash,
			),
			hash.Bytes(),
		)
		if err != nil {
			t.Fatal(err)
		}
		storageChangeset[i] = csStorage
	}
	return accChangeset, storageChangeset
}
