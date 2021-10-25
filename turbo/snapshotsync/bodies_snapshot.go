package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

func GenerateBodiesSnapshot(ctx context.Context, readTX kv.Tx, writeTX kv.RwTx, toBlock uint64) error {
	readBodyCursor, err := readTX.Cursor(kv.BlockBody)
	if err != nil {
		return err
	}

	writeBodyCursor, err := writeTX.RwCursor(kv.BlockBody)
	if err != nil {
		return err
	}
	writeEthTXCursor, err := writeTX.RwCursor(kv.EthTx)
	if err != nil {
		return err
	}
	readEthTXCursor, err := readTX.Cursor(kv.EthTx)
	if err != nil {
		return err
	}

	var expectedBaseTxId uint64
	err = ethdb.Walk(readBodyCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		if binary.BigEndian.Uint64(k) > toBlock {
			return false, nil
		}

		canonocalHash, err := readTX.GetOne(kv.HeaderCanonical, dbutils.EncodeBlockNumber(binary.BigEndian.Uint64(k)))
		if err != nil {
			return false, err
		}
		if !bytes.Equal(canonocalHash, k[8:]) {
			return true, nil
		}
		bd := &types.BodyForStorage{}
		err = rlp.DecodeBytes(v, bd)
		if err != nil {
			return false, fmt.Errorf("block %s decode err %w", common.Bytes2Hex(k), err)
		}
		baseTxId := bd.BaseTxId
		amount := bd.TxAmount

		bd.BaseTxId = expectedBaseTxId
		newV, err := rlp.EncodeToBytes(bd)
		if err != nil {
			return false, err
		}
		err = writeBodyCursor.Append(common.CopyBytes(k), newV)
		if err != nil {
			return false, err
		}

		newExpectedTx := expectedBaseTxId
		err = ethdb.Walk(readEthTXCursor, dbutils.EncodeBlockNumber(baseTxId), 0, func(k, v []byte) (bool, error) {
			if newExpectedTx >= expectedBaseTxId+uint64(amount) {
				return false, nil
			}
			err = writeEthTXCursor.Append(dbutils.EncodeBlockNumber(newExpectedTx), common.CopyBytes(v))
			if err != nil {
				return false, err
			}
			newExpectedTx++
			return true, nil
		})
		if err != nil {
			return false, err
		}
		if newExpectedTx > expectedBaseTxId+uint64(amount) {
			fmt.Println("newExpectedTx > expectedBaseTxId+amount", newExpectedTx, expectedBaseTxId, amount, "block", common.Bytes2Hex(k))
			return false, errors.New("newExpectedTx > expectedBaseTxId+amount")
		}
		expectedBaseTxId += uint64(amount)
		return true, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func CreateBodySnapshot(readTx kv.Tx, logger log.Logger, lastBlock uint64, snapshotPath string) error {
	db, err := mdbx.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.BlockBody: kv.ChaindataTablesCfg[kv.BlockBody],
			kv.EthTx:     kv.ChaindataTablesCfg[kv.EthTx],
		}
	}).Path(snapshotPath).Open()
	if err != nil {
		return err
	}

	defer db.Close()
	writeTX, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer writeTX.Rollback()
	err = GenerateBodiesSnapshot(context.TODO(), readTx, writeTX, lastBlock)
	if err != nil {
		return err
	}
	return writeTX.Commit()
}

func OpenBodiesSnapshot(logger log.Logger, dbPath string) (kv.RoDB, error) {
	return mdbx.NewMDBX(logger).Path(dbPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.BlockBody: kv.ChaindataTablesCfg[kv.BlockBody],
			kv.EthTx:     kv.ChaindataTablesCfg[kv.EthTx],
		}
	}).Readonly().Open()
}

func RemoveBlocksData(db kv.RoDB, tx kv.RwTx, newSnapshot uint64) (err error) {
	log.Info("Remove blocks data", "to", newSnapshot)
	if _, ok := db.(snapshotdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	bodiesSnapshot := db.(snapshotdb.SnapshotUpdater).BodiesSnapshot()
	if bodiesSnapshot == nil {
		log.Info("bodiesSnapshot is empty")
		return nil
	}
	blockBodySnapshotReadTX, err := bodiesSnapshot.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer blockBodySnapshotReadTX.Rollback()
	ethtxSnapshotReadTX, err := blockBodySnapshotReadTX.Cursor(kv.EthTx)
	if err != nil {
		return err
	}
	lastEthTXSnapshotKey, _, err := ethtxSnapshotReadTX.Last()
	if err != nil {
		return err
	}
	rewriteId := binary.BigEndian.Uint64(lastEthTXSnapshotKey) + 1

	writeTX := tx.(snapshotdb.DBTX).DBTX()
	blockBodyWriteCursor, err := writeTX.RwCursor(kv.BlockBody)
	if err != nil {
		return fmt.Errorf("get bodies cursor %w", err)
	}
	ethTXWriteCursor, err := writeTX.RwCursor(kv.EthTx)
	if err != nil {
		return fmt.Errorf("get ethtx cursor %w", err)
	}

	logPrefix := "RemoveBlocksData"
	bodiesCollector := etl.NewCollector(logPrefix, os.TempDir(), etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer bodiesCollector.Close()
	ethTXCollector := etl.NewCollector(logPrefix, os.TempDir(), etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer ethTXCollector.Close()
	err = ethdb.Walk(blockBodyWriteCursor, dbutils.BlockBodyKey(0, common.Hash{}), 0, func(k, v []byte) (bool, error) {
		if binary.BigEndian.Uint64(k) > newSnapshot {
			return false, nil
		}
		has, err := blockBodySnapshotReadTX.Has(kv.BlockBody, k)
		if err != nil {
			return false, err
		}
		bd := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bd)
		if err != nil {
			return false, err
		}

		if has {
			innerErr := blockBodyWriteCursor.Delete(k, nil)
			if innerErr != nil {
				return false, fmt.Errorf("remove %v err:%w", common.Bytes2Hex(k), innerErr)
			}
			for i := bd.BaseTxId; i < bd.BaseTxId+uint64(bd.TxAmount); i++ {
				err = ethTXWriteCursor.Delete(dbutils.EncodeBlockNumber(i), nil)
				if err != nil {
					return false, err
				}
			}
		} else {
			collectKey := common.CopyBytes(k)
			oldBaseTxId := bd.BaseTxId
			bd.BaseTxId = rewriteId
			bodyBytes, err := rlp.EncodeToBytes(bd)
			if err != nil {
				return false, err
			}

			if bd.TxAmount > 0 {
				txIdKey := make([]byte, 8)
				binary.BigEndian.PutUint64(txIdKey, oldBaseTxId)
				i := uint32(0)

				for k, v, err := ethTXWriteCursor.SeekExact(txIdKey); k != nil; k, v, err = ethTXWriteCursor.Next() {
					if err != nil {
						return false, err
					}

					err = ethTXCollector.Collect(dbutils.EncodeBlockNumber(rewriteId+uint64(i)), common.CopyBytes(v))
					if err != nil {
						return false, err
					}

					i++
					if i >= bd.TxAmount {
						break
					}
				}
			}
			//we need to remove it to use snapshot data instead
			for i := oldBaseTxId; i < oldBaseTxId+uint64(bd.TxAmount); i++ {
				err = ethTXWriteCursor.Delete(dbutils.EncodeBlockNumber(i), nil)
				if err != nil {
					return false, err
				}
			}

			rewriteId += uint64(bd.TxAmount)

			err = bodiesCollector.Collect(collectKey, bodyBytes)
			if err != nil {
				return false, err
			}
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	err = bodiesCollector.Load(writeTX, kv.BlockBody, etl.IdentityLoadFunc, etl.TransformArgs{})
	if err != nil {
		return err
	}

	err = ethTXCollector.Load(writeTX, kv.EthTx, etl.IdentityLoadFunc, etl.TransformArgs{})
	if err != nil {
		return err
	}
	return nil
}
