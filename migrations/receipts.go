package migrations

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/cbor"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var receiptsCborEncode = Migration{
	Name: "receipts_cbor_encode",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		buf := make([]byte, 0, 100_000)
		if err := db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
			blockNum := binary.BigEndian.Uint64(k[:8])
			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("Migration progress", "blockNum", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			}

			// Convert the receipts from their storage form to their internal representation
			storageReceipts := []*types.ReceiptForStorage{}
			if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
				return false, fmt.Errorf("invalid receipt array RLP: %w, k=%x", err, k)
			}

			buf = buf[:0]
			if err := cbor.Marshal(&buf, storageReceipts); err != nil {
				return false, err
			}
			return true, db.Put(dbutils.BlockReceiptsPrefix, common.CopyBytes(k), common.CopyBytes(buf))
		}); err != nil {
			return err
		}

		return OnLoadCommit(db, nil, true)
	},
}

var receiptsCborDECODE = Migration{
	Name: "receipts_cbor_DECODE",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		if err := db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
			blockNum := binary.BigEndian.Uint64(k[:8])
			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("Migration progress", "blockNum", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			}

			// Convert the receipts from their storage form to their internal representation
			storageReceipts := []*types.ReceiptForStorage{}
			if err := cbor.Unmarshal(&storageReceipts, v); err != nil {
				return false, fmt.Errorf("unmarshalling receipt from CBOR: %w, k=%x", err, k)
			}
			buf, err := rlp.EncodeToBytes(storageReceipts)
			if err != nil {
				return false, fmt.Errorf("encoding receipt to RLP: %w, k=%x", err, k)
			}
			if err = collector.Collect(k, buf); err != nil {
				return false, fmt.Errorf("collecting key %x: %w", k, err)
			}
			return true, nil
		}); err != nil {
			return err
		}
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsPrefix); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}
		// Commit clearing of the bucket - freelist should now be written to the database
		if err := OnLoadCommit(db, nil, false); err != nil {
			return fmt.Errorf("committing the ")
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err := collector.Load(db, dbutils.BlockReceiptsPrefix, etl.IdentityLoadFunc, etl.TransformArgs{OnLoadCommit: OnLoadCommit}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}
