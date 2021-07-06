package migrations

import (
	"encoding/binary"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rlp"
	"path/filepath"
)

var splitCanonicalAndNonCanonicalTransactionsBuckets = Migration{
	Name: "split_canonical_and_noncanonical_txs",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		bodiesPath:=filepath.Join(tmpdir, "bodies")
		ethtxPath:=filepath.Join(tmpdir, "ethtx")
		nonCanonicalPath:=filepath.Join(tmpdir, "noncanonicaltx")
		bodiesCollector := etl.NewCollector(bodiesPath, etl.NewSortableBuffer(etl.BufferOptimalSize))
		ethTXCollector := etl.NewCollector(ethtxPath, etl.NewSortableBuffer(etl.BufferOptimalSize))
		nonCanonicalCollector := etl.NewCollector(nonCanonicalPath, etl.NewSortableBuffer(etl.BufferOptimalSize))

		ethTXWriteCursor,err:=db.(ethdb.HasTx).Tx().(ethdb.RwTx).Cursor(dbutils.EthTx)
		if err!=nil {
		    return err
		}
		bfs:=&types.BodyForStorage{}
		var ethTXIndex, nonCanonicalIndex uint64

		err = db.ForEach(dbutils.BlockBodyPrefix, []byte{}, func(k, v []byte) error {
			blockNum:=binary.BigEndian.Uint64(k[:8])
			blockHash:=common.BytesToHash(k[8:])
			err = rlp.DecodeBytes(v, bfs)
			if err!=nil {
			    return err
			}
			canonicalHash,err:=rawdb.ReadCanonicalHash(db,blockNum)
			if err!=nil {
			    return err
			}
			if blockHash==canonicalHash {
				bfs.Canonical = true
				if bfs.TxAmount > 0 {
					txIdKey := make([]byte, 8)
					binary.BigEndian.PutUint64(txIdKey, bfs.BaseTxId)
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
						if i >= bfs.TxAmount {
							break
						}
					}
				}
				bfs.BaseTxId = ethTXIndex
				ethTXIndex+=uint64(bfs.TxAmount)
			} else {
				bfs.Canonical = false
				if bfs.TxAmount > 0 {
					txIdKey := make([]byte, 8)
					binary.BigEndian.PutUint64(txIdKey, bfs.BaseTxId)
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
						if i >= bfs.TxAmount {
							break
						}
					}
				}
				bfs.BaseTxId = nonCanonicalIndex
				nonCanonicalIndex+=uint64(bfs.TxAmount)
			}
			bodyBytes, err := rlp.EncodeToBytes(bfs)
			if err != nil {
				return err
			}

			err = bodiesCollector.Collect(common.CopyBytes(k), bodyBytes)
			if err!=nil {
			    return err
			}
			return  nil
		})
		if err!=nil {
		    return err
		}

		err = db.(ethdb.BucketMigrator).ClearBucket(dbutils.EthTx)
		if err!=nil {
		    return err
		}

		err = bodiesCollector.Load("bodies", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.BlockBodyPrefix, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}

		err = ethTXCollector.Load("ethtx", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.EthTx, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}

		err = ethTXCollector.Load("noncanonicaltx", db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.NonCanonicalTXBucket, etl.IdentityLoadFunc, etl.TransformArgs{})
		if err != nil {
			return err
		}


		ethTXLast, _, err:=db.Last(dbutils.EthTx)
		if err!=nil {
		    return err
		}
		err = db.Put(dbutils.Sequence, []byte(dbutils.EthTx), dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(ethTXLast)+1))
		if err!=nil {
		    return err
		}

		nonCanonicalTXLast, _, err:=db.Last(dbutils.NonCanonicalTXBucket)
		if err!=nil {
		    return err
		}
		err = db.Put(dbutils.Sequence, []byte(dbutils.NonCanonicalTXBucket), dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(nonCanonicalTXLast)+1))
		if err!=nil {
			return err
		}

		return nil
	},
}
