package migrations

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var transactionsTable = Migration{
	Name: "tx_table_4",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "tx_table"

		const loadStep = "load"
		reader := bytes.NewReader(nil)
		buf := bytes.NewBuffer(make([]byte, 4096))
		body := new(types.Body)
		newK := make([]byte, 8)

		collectorB, err1 := etl.NewCollectorFromFiles(tmpdir + "1") // B - stands for blocks
		if err1 != nil {
			return err1
		}
		collectorT, err1 := etl.NewCollectorFromFiles(tmpdir + "2") // T - stands for transactions
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorB != nil {
				collectorB.Close(logPrefix)
				collectorB = nil
			}

			if collectorT != nil {
				collectorT.Close(logPrefix)
				collectorT = nil
			}
		case loadStep:
			if collectorB == nil || collectorT == nil {
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
				collectorB.Close(logPrefix)
				collectorT.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorB = etl.NewCriticalCollector(tmpdir+"1", etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectorT = etl.NewCriticalCollector(tmpdir+"2", etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorB.Close(logPrefix)
			collectorT.Close(logPrefix)
		}()

		if err = db.Walk(dbutils.BlockBodyPrefix, nil, 0, func(k, v []byte) (bool, error) {
			select {
			default:
			case <-logEvery.C:
				blockNum := binary.BigEndian.Uint64(k[:8])
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "blockNum", blockNum)
			}
			// don't need canonical check

			reader.Reset(v)
			if err = rlp.Decode(reader, body); err != nil {
				return false, fmt.Errorf("[%s]: invalid block body RLP: %w", logPrefix, err)
			}

			txIds := make([]uint64, len(body.Transactions))
			var baseTxId uint64
			baseTxId, err = db.IncrementSequence(dbutils.EthTx, uint64(len(body.Transactions)))
			if err != nil {
				return false, nil
			}

			txId := baseTxId
			for i, txn := range body.Transactions {
				binary.BigEndian.PutUint64(newK, txId)
				txIds[i] = txId
				txId++

				buf.Reset()
				if err = rlp.Encode(buf, txn); err != nil {
					return false, err
				}

				if err = collectorT.Collect(newK, buf.Bytes()); err != nil {
					return false, err
				}
			}

			buf.Reset()
			if err = rlp.Encode(buf, types.BodyForStorage{
				BaseTxId: baseTxId,
				TxAmount: uint32(len(body.Transactions)),
				Uncles:   body.Uncles,
			}); err != nil {
				return false, err
			}
			if err = collectorB.Collect(k, buf.Bytes()); err != nil {
				return false, err
			}
			return true, nil
		}); err != nil {
			return err
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.EthTx, dbutils.BlockBodyPrefix); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorT.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.EthTx, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the eth_tx table: %w", err)
		}
		if err = collectorB.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.BlockBodyPrefix, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the bodies table: %w", err)
		}
		return nil
	},
}
