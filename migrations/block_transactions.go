package migrations

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rlp"
	"path/filepath"
)

type BodyForStorageDeprecated struct {
	BaseTxId uint64
	TxAmount uint32
	Uncles   []*types.Header
}

var splitCanonicalAndNonCanonicalTransactionsBuckets = Migration{
	Name: "split_canonical_and_noncanonical_txs",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		bodiesPath := filepath.Join(tmpdir, "bodies")
		ethtxPath := filepath.Join(tmpdir, "ethtx")
		nonCanonicalPath := filepath.Join(tmpdir, "noncanonicaltx")

		var ethTXIndex, nonCanonicalIndex uint64
		logPrefix := "split_canonical_and_noncanonical_txs"
		const loadStep = "load"

		bodiesCollector, err := etl.NewCollectorFromFiles(bodiesPath)
		if err != nil {
			return err
		}
		ethTXCollector, err := etl.NewCollectorFromFiles(ethtxPath)
		if err != nil {
			return err
		}
		nonCanonicalCollector, err := etl.NewCollectorFromFiles(nonCanonicalPath)
		if err != nil {
			return err
		}

		ethTXWriteCursor, err := db.(ethdb.HasTx).Tx().(ethdb.RwTx).Cursor(dbutils.EthTx)
		if err != nil {
			return err
		}
		defer ethTXWriteCursor.Close()

		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if bodiesCollector != nil {
				bodiesCollector.Close(logPrefix)
				bodiesCollector = nil
			}

			if ethTXCollector != nil {
				ethTXCollector.Close(logPrefix)
				ethTXCollector = nil
			}

			if nonCanonicalCollector != nil {
				nonCanonicalCollector.Close(logPrefix)
				nonCanonicalCollector = nil
			}
		case loadStep:
			if nonCanonicalCollector == nil || ethTXCollector == nil || bodiesCollector == nil {
				return ErrMigrationETLFilesDeleted
			}
			defer func() {
				// don't clean if error or panic happened
				if err != nil {
					return
				}
				if rec := recover(); rec != nil {
					panic(rec)
				}
				bodiesCollector.Close(logPrefix)
				ethTXCollector.Close(logPrefix)
				nonCanonicalCollector.Close(logPrefix)
			}()

			goto LoadStep
		}

		bodiesCollector = etl.NewCriticalCollector(bodiesPath, etl.NewSortableBuffer(etl.BufferOptimalSize))
		ethTXCollector = etl.NewCriticalCollector(ethtxPath, etl.NewSortableBuffer(etl.BufferOptimalSize))
		nonCanonicalCollector = etl.NewCriticalCollector(nonCanonicalPath, etl.NewSortableBuffer(etl.BufferOptimalSize))

		err = db.ForEach(dbutils.BlockBodyPrefix, []byte{}, func(k, v []byte) error {
			blockNum := binary.BigEndian.Uint64(k[:8])
			blockHash := common.BytesToHash(k[8:])
			bfsOld := &BodyForStorageDeprecated{}
			err = rlp.DecodeBytes(v, bfsOld)
			if err != nil {
				return err
			}
			bfsNew := &types.BodyForStorage{
				TxAmount: bfsOld.TxAmount,
				Uncles:   bfsOld.Uncles,
			}
			canonicalHash, err := rawdb.ReadCanonicalHash(db, blockNum)
			if err != nil {
				return err
			}
			if blockHash == canonicalHash {
				bfsNew.Canonical = true
				if bfsNew.TxAmount > 0 {
					txIdKey := make([]byte, 8)
					binary.BigEndian.PutUint64(txIdKey, bfsOld.BaseTxId)
					i := uint32(0)

					for k, v, err := ethTXWriteCursor.SeekExact(txIdKey); k != nil; k, v, err = ethTXWriteCursor.Next() {
						if err != nil {
							return err
						}

						err = ethTXCollector.Collect(dbutils.EncodeBlockNumber(ethTXIndex+uint64(i)), common.CopyBytes(v))
						if err != nil {
							return err
						}

						i++
						if i >= bfsOld.TxAmount {
							break
						}
					}
				}
				bfsNew.BaseTxId = ethTXIndex
				ethTXIndex += uint64(bfsNew.TxAmount)
			} else {
				bfsNew.Canonical = false
				if bfsNew.TxAmount > 0 {
					txIdKey := make([]byte, 8)
					binary.BigEndian.PutUint64(txIdKey, bfsOld.BaseTxId)
					i := uint32(0)

					for k, v, err := ethTXWriteCursor.SeekExact(txIdKey); k != nil; k, v, err = ethTXWriteCursor.Next() {
						if err != nil {
							return err
						}

						err = nonCanonicalCollector.Collect(dbutils.EncodeBlockNumber(nonCanonicalIndex+uint64(i)), common.CopyBytes(v))
						if err != nil {
							return err
						}

						i++
						if i >= bfsNew.TxAmount {
							break
						}
					}
				}
				bfsNew.BaseTxId = nonCanonicalIndex
				nonCanonicalIndex += uint64(bfsNew.TxAmount)
			}
			bodyBytes, err := rlp.EncodeToBytes(bfsNew)
			if err != nil {
				return err
			}

			err = bodiesCollector.Collect(common.CopyBytes(k), bodyBytes)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		err = bodiesCollector.Flush()
		if err!=nil {
			return err
		}

		err = ethTXCollector.Flush()
		if err!=nil {
			return err
		}

		err = nonCanonicalCollector.Flush()
		if err!=nil {
			return err
		}


		err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.EthTx)
		if err != nil {
			return err
		}

		ethTXWriteCursor.Close()
		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of table: %w", err)
		}

	LoadStep:
		err = bodiesCollector.Load("bodies", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.BlockBodyPrefix, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}

		err = ethTXCollector.Load("ethtx", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.EthTx, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}

		err = nonCanonicalCollector.Load("noncanonicaltx", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.NonCanonicalTXBucket, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}

		ethTXLast, _, err := db.Last(dbutils.EthTx)
		if err != nil {
			return err
		}
		//if you run from scratch
		if len(ethTXLast) == 8 {
			err = db.Put(dbutils.Sequence, []byte(dbutils.EthTx), dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(ethTXLast)+1))
			if err != nil {
				return err
			}
		}

		nonCanonicalTXLast, _, err := db.Last(dbutils.NonCanonicalTXBucket)
		if err != nil {
			return err
		}
		//if you run from scratch
		if len(ethTXLast) == 8 {
			err = db.Put(dbutils.Sequence, []byte(dbutils.NonCanonicalTXBucket), dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(nonCanonicalTXLast)+1))
			if err != nil {
				return err
			}
		}

		return CommitProgress(db, nil, true)
	},
}
