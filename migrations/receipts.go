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
	"github.com/valyala/gozstd"
)

var receiptsCborEncode = Migration{
	Name: "receipts_cbor_encode",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		collector, err1 := etl.NewCollectorFromFiles(datadir)
		if err1 != nil {
			return err1
		}
		if collector == nil {
			collector = etl.NewCriticalCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
			buf := make([]byte, 0, 100_000)
			if err1 = db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
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
				if err := collector.Collect(k, buf); err != nil {
					return false, fmt.Errorf("collecting key %x: %w", k, err)
				}
				return true, nil
			}); err1 != nil {
				return err1
			}
		}
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsPrefix); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}
		// Commit clearing of the bucket - freelist should now be written to the database
		if err := OnLoadCommit(db, nil, false); err != nil {
			return fmt.Errorf("committing the removal of receipt table")
		}
		// Commit again
		if err := OnLoadCommit(db, nil, false); err != nil {
			return fmt.Errorf("committing again to create a stable view the removal of receipt table")
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err := collector.Load(db, dbutils.BlockReceiptsPrefix, etl.IdentityLoadFunc, etl.TransformArgs{OnLoadCommit: OnLoadCommit}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}

//nolint:deadcode,unused,varcheck
var receiptsZstdCompression = Migration{
	Name: "receipts_zstd_compression",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		if err := db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
			blockNum := binary.BigEndian.Uint64(k[:8])
			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("Migration progress", "blockNum", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			}

			newV := gozstd.CompressDict(nil, v, dbutils.CompressionDicts.CReceipts)
			return true, db.Put(dbutils.BlockReceiptsPrefix, common.CopyBytes(k), newV)
		}); err != nil {
			return err
		}

		return OnLoadCommit(db, nil, true)
	},
}
