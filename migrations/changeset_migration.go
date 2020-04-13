package migrations

import (
	"bytes"
	"errors"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	//ChangeSetBucket -  key - encoded timestamp(block number) + history bucket(hAT/hST)
	// value - encoded ChangeSet{k - addrHash|compositeKey(for storage) v - account(encoded) | originalValue(common.Hash)}
	ChangeSetBucket = []byte("ChangeSet")
	//LastBatchKey - last inserted key
	LastBatchKey = []byte("lastBatchKeyForSplitChangesetMigration")
)

const splitChangesetBatchSize = 5000

func splitChangeSetMigration(batchSize int) Migration {
	return Migration{
		Name: "split_changeset",
		Up: func(db ethdb.Database, history, receipts, txIndex, preImages, thinHistory bool) error {
			boltDB, ok := db.(*ethdb.BoltDatabase)
			if !ok {
				return errors.New("only boltdb migration")
			}

			var rowNum int
			changesetsToRemove := make([][]byte, 0)
			accChangesets := make([][]byte, 0)
			storageChangesets := make([][]byte, 0)
			var (
				currentKey, currentValue []byte
				done                     bool
			)

			currentKey, err := db.Get(dbutils.DatabaseInfoBucket, LastBatchKey)
			if err != nil && err != ethdb.ErrKeyNotFound {
				return err
			}

			startTime := time.Now()
			for !done {
				err := boltDB.KV().Update(func(tx *bolt.Tx) error {
					changesetBucket := tx.Bucket(ChangeSetBucket)
					dbInfoBucket, err := tx.CreateBucketIfNotExists(dbutils.DatabaseInfoBucket, false)
					if err != nil {
						return err
					}
					if changesetBucket == nil {
						done = true
						return nil
					}
					changesetCursor := changesetBucket.Cursor()

					if currentKey == nil {
						currentKey, currentValue = changesetCursor.First()
					} else {
						currentKey, currentValue = changesetCursor.Seek(currentKey)
					}

					for currentKey != nil {
						changesetsToRemove = append(changesetsToRemove, common.CopyBytes(currentKey))
						ts, bucket := dbutils.DecodeTimestamp(currentKey)
						encTS := dbutils.EncodeTimestamp(ts)

						switch {
						case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
							if thinHistory {
								cs, innerErr := changeset.DecodeChangeSet(currentValue)
								if innerErr != nil {
									return innerErr
								}
								v, innerErr := changeset.EncodeAccounts(cs)
								if innerErr != nil {
									return innerErr
								}

								accChangesets = append(accChangesets, encTS, common.CopyBytes(v))
							} else {
								accChangesets = append(accChangesets, encTS, common.CopyBytes(currentValue))
							}

						case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
							if thinHistory {
								cs, innerErr := changeset.DecodeChangeSet(currentValue)
								if innerErr != nil {
									return innerErr
								}

								v, innerErr := changeset.EncodeStorage(cs)
								if innerErr != nil {
									log.Error("Error on encode storage changeset", "err", innerErr)
									return innerErr
								}
								storageChangesets = append(storageChangesets, encTS, common.CopyBytes(v))

							} else {
								storageChangesets = append(storageChangesets, encTS, common.CopyBytes(currentValue))
							}
						}

						currentKey, currentValue = changesetCursor.Next()
						if rowNum >= batchSize || currentKey == nil {
							commTime := time.Now()

							if len(storageChangesets) > 0 {
								storageCSBucket, innerErr := tx.CreateBucketIfNotExists(dbutils.StorageChangeSetBucket, false)
								if innerErr != nil {
									return innerErr
								}

								innerErr = storageCSBucket.MultiPut(storageChangesets...)
								if innerErr != nil {
									return innerErr
								}
							}

							if len(accChangesets) > 0 {
								accCSBucket, innerErr := tx.CreateBucketIfNotExists(dbutils.AccountChangeSetBucket, false)
								if innerErr != nil {
									return innerErr
								}
								innerErr = accCSBucket.MultiPut(accChangesets...)
								if innerErr != nil {
									return innerErr
								}
							}

							if len(changesetsToRemove) > 0 {
								for _, v := range changesetsToRemove {
									innerErr := changesetBucket.Delete(v)
									if innerErr != nil {
										return innerErr
									}
								}
							}

							log.Warn("Commit", "block", ts, "commit time", time.Since(commTime), "migration time", time.Since(startTime))
							accChangesets = make([][]byte, 0)
							storageChangesets = make([][]byte, 0)
							changesetsToRemove = make([][]byte, 0)
							rowNum = 0
							break
						} else {
							rowNum++
						}
					}

					if currentKey == nil {
						done = true
						err = dbInfoBucket.Delete(LastBatchKey)
						if err != nil {
							return err
						}
					} else {
						currentKey = common.CopyBytes(currentKey)
						err = dbInfoBucket.Put(LastBatchKey, currentKey)
						if err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}
