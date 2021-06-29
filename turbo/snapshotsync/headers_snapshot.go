package snapshotsync

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	"os"
	"time"
)

func CreateHeadersSnapshot(ctx context.Context, readTX ethdb.Tx, toBlock uint64, snapshotPath string) error {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}

	snKV, err := kv.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Path(snapshotPath).Open()
	if err != nil {
		return err
	}
	sntx, err := snKV.BeginRw(context.Background())
	if err != nil {
		return fmt.Errorf("begin err: %w", err)
	}
	defer sntx.Rollback()

	err = GenerateHeadersSnapshot(ctx, readTX, sntx, toBlock)
	if err != nil {
		return fmt.Errorf("generate err: %w", err)
	}
	err = sntx.Commit()
	if err != nil {
		return fmt.Errorf("commit err: %w", err)
	}
	snKV.Close()

	return nil
}

func GenerateHeadersSnapshot(ctx context.Context, db ethdb.Tx, sntx ethdb.RwTx, toBlock uint64) error {
	headerCursor, err := sntx.RwCursor(dbutils.HeadersBucket)
	if err != nil {
		return err
	}
	var hash common.Hash
	var header []byte
	t := time.NewTicker(time.Second * 30)
	defer t.Stop()
	tt := time.Now()
	for i := uint64(0); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}
		select {
		case <-t.C:
			log.Info("Headers snapshot generation", "t", time.Since(tt), "block", i)
		default:
		}
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return err
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) < 2 {
			return fmt.Errorf("header %d is empty, %v", i, header)
		}

		err = headerCursor.Append(dbutils.HeaderKey(i, hash), header)
		if err != nil {
			return err
		}
	}
	return nil
}

func OpenHeadersSnapshot(dbPath string) (ethdb.RoKV, error) {
	return kv.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Readonly().Path(dbPath).Open()
}

func RemoveHeadersData(db ethdb.RoKV, tx ethdb.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	if _, ok := db.(kv.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot := db.(kv.SnapshotUpdater).HeadersSnapshot()
	if headerSnapshot == nil {
		return errors.New("empty headers snapshot")
	}
	writeTX := tx.(kv.DBTX).DBTX()
	c, err := writeTX.RwCursor(dbutils.HeadersBucket)
	if err != nil {
		return fmt.Errorf("get headers cursor %w", err)
	}

	return headerSnapshot.View(context.Background(), func(tx ethdb.Tx) error {
		c2, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer c2.Close()
		defer c2.Close()
		return ethdb.Walk(c2, dbutils.EncodeBlockNumber(currentSnapshot), 0, func(k, v []byte) (bool, error) {
			innerErr := c.Delete(k, nil)
			if innerErr != nil {
				return false, fmt.Errorf("remove %v err:%w", common.Bytes2Hex(k), innerErr)
			}
			return true, nil
		})
	})
}
