package migrations

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
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
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) error {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		buf := bytes.NewBuffer(make([]byte, 0, 100_000))
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

			buf.Reset()
			if err := cbor.Marshal(buf, storageReceipts); err != nil {
				return false, err
			}
			if err := collector.Collect(k, buf.Bytes()); err != nil {
				return false, fmt.Errorf("collecting key %x: %w", k, err)
			}
			return true, nil
		}); err1 != nil {
			return err1
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsPrefix); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table")
		}

	LoadStep:
		// Commit again
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing again to create a stable view the removal of receipt table")
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err := collector.Load("receipts_cbor_encode", db, dbutils.BlockReceiptsPrefix, etl.IdentityLoadFunc, etl.TransformArgs{OnLoadCommit: CommitProgress}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}

var receiptsOnePerTx = Migration{
	Name: "receipts_one_per_transaction3",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "receipts_one_per_transaction"

		// Recently was introduced receipts serialization problem
		// Code was not generated well for types.Log type
		// So, to fix this problem - need deserialize by reflection (LegacyReceipt doesn't have generated code)
		// then serialize by generated code - types.Receipts and types.Log have generated code now
		type LegacyReceipt struct {
			PostState         []byte       `codec:"1"`
			Status            uint64       `codec:"2"`
			CumulativeGasUsed uint64       `codec:"3"`
			Logs              []*types.Log `codec:"4"`
		}

		buf := bytes.NewBuffer(make([]byte, 0, 100_000))
		reader := bytes.NewReader(nil)

		const loadStep = "load"

		collectorR, err1 := etl.NewCollectorFromFiles(tmpdir + "1")
		if err1 != nil {
			return err1
		}
		collectorL, err1 := etl.NewCollectorFromFiles(tmpdir + "2")
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorR != nil {
				collectorR.Close(logPrefix)
				collectorR = nil
			}

			if collectorL != nil {
				collectorL.Close(logPrefix)
				collectorL = nil
			}
		case loadStep:
			if collectorR == nil || collectorL == nil {
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
				collectorR.Close(logPrefix)
				collectorL.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorR = etl.NewCriticalCollector(tmpdir+"1", etl.NewSortableBuffer(etl.BufferOptimalSize*2))
		collectorL = etl.NewCriticalCollector(tmpdir+"2", etl.NewSortableBuffer(etl.BufferOptimalSize*2))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorR.Close(logPrefix)
			collectorL.Close(logPrefix)
		}()
		if err := db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
			blockNum := binary.BigEndian.Uint64(k[:8])
			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "blockNum", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			}

			// Convert the receipts from their storage form to their internal representation
			var legacyReceipts []*LegacyReceipt

			reader.Reset(v)
			if err := cbor.Unmarshal(&legacyReceipts, reader); err != nil {
				return false, err
			}

			// Convert the receipts from their storage form to their internal representation
			receipts := make(types.Receipts, len(legacyReceipts))
			for i := range legacyReceipts {
				receipts[i] = &types.Receipt{}
				receipts[i].PostState = legacyReceipts[i].PostState
				receipts[i].Status = legacyReceipts[i].Status
				receipts[i].CumulativeGasUsed = legacyReceipts[i].CumulativeGasUsed
				receipts[i].Logs = legacyReceipts[i].Logs
			}

			for txId, r := range receipts {
				if len(r.Logs) == 0 {
					continue
				}

				newK := make([]byte, 8+4)
				copy(newK, k[:8])
				binary.BigEndian.PutUint32(newK[8:], uint32(txId))

				buf.Reset()
				if err := cbor.Marshal(buf, r.Logs); err != nil {
					return false, err
				}
				if err := collectorL.Collect(newK, buf.Bytes()); err != nil {
					return false, fmt.Errorf("collecting key %x: %w", k, err)
				}
			}

			buf.Reset()
			if err := cbor.Marshal(buf, receipts); err != nil {
				return false, err
			}

			if err := collectorR.Collect(common.CopyBytes(k[:8]), buf.Bytes()); err != nil {
				return false, fmt.Errorf("collecting key %x: %w", k, err)
			}

			return true, nil
		}); err != nil {
			return err
		}

	LoadStep:
		//if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsPrefix); err != nil {
		//	return fmt.Errorf("clearing the receipt bucket: %w", err)
		//}
		// Commit clearing of the bucket - freelist should now be written to the database
		if err := OnLoadCommit(db, nil, false); err != nil {
			return fmt.Errorf("committing the removal of receipt table")
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err := collectorR.Load(logPrefix, db, dbutils.BlockReceiptsPrefix2, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		if err := collectorL.Load(logPrefix, db, dbutils.Log, etl.IdentityLoadFunc, etl.TransformArgs{OnLoadCommit: OnLoadCommit}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}
