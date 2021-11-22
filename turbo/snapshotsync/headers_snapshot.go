package snapshotsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/log/v3"
)

func CreateHeadersSnapshot(ctx context.Context, readTX kv.Tx, toBlock uint64, snapshotPath string) error {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}

	snKV, err := mdbx.NewMDBX(log.New()).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.ChaindataTablesCfg[kv.Headers],
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

func GenerateHeadersSnapshot(ctx context.Context, db kv.Tx, sntx kv.RwTx, toBlock uint64) error {
	headerCursor, err := sntx.RwCursor(kv.Headers)
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
			return libcommon.ErrStopped
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

func OpenHeadersSnapshot(dbPath string) (kv.RoDB, error) {
	return mdbx.NewMDBX(log.New()).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.ChaindataTablesCfg[kv.Headers],
		}
	}).Readonly().Path(dbPath).Open()
}

func RemoveHeadersData(db kv.RoDB, tx kv.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	if _, ok := db.(snapshotdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot := db.(snapshotdb.SnapshotUpdater).HeadersSnapshot()
	if headerSnapshot == nil {
		return errors.New("empty headers snapshot")
	}
	writeTX := tx.(snapshotdb.DBTX).DBTX()
	c, err := writeTX.RwCursor(kv.Headers)
	if err != nil {
		return fmt.Errorf("get headers cursor %w", err)
	}

	return headerSnapshot.View(context.Background(), func(tx kv.Tx) error {
		c2, err := tx.Cursor(kv.Headers)
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
