package migrations

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"time"
)

var receiptsTopicNormalForm = Migration{
	Name: "receipt_topic_normal_form",
	Up: func(tx ethdb.DbWithPendingMutations, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := tx.(ethdb.BucketsMigrator).BucketExists(dbutils.BlockReceiptsPrefixOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(tx, nil, true)
		}

		if err := tx.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceipts, dbutils.LogTopic2Id, dbutils.LogId2Topic); err != nil {
			return err
		}

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		ids, err := ethdb.Ids(tx)
		if err != nil {
			return err
		}
		ids.Topic = 0 // Important! to reset topicID counter

		iBytes := make([]byte, 4)
		if err := tx.Walk(dbutils.BlockReceiptsPrefixOld1, nil, 0, func(k, v []byte) (bool, error) {
			blockHashBytes := k[len(k)-32:]
			blockNum64Bytes := k[:len(k)-32]
			blockNum := binary.BigEndian.Uint64(blockNum64Bytes)
			canonicalHash := rawdb.ReadCanonicalHash(tx, blockNum)
			if !bytes.Equal(blockHashBytes, canonicalHash[:]) {
				return true, nil
			}

			// Decode the receipts by legacy data type
			storageReceiptsLegacy := []*types.DeprecatedReceiptForStorage1{}
			if err := rlp.DecodeBytes(v, &storageReceiptsLegacy); err != nil {
				return false, fmt.Errorf("invalid receipt array RLP: %w, blockNum=%d", err, blockNum)
			}

			// Encode by new data type
			storageReceipts := make([]*types.ReceiptForStorage, len(storageReceiptsLegacy))
			for i, r := range storageReceiptsLegacy {
				storageReceipts[i] = (*types.ReceiptForStorage)(r)
			}

			for ri, r := range storageReceiptsLegacy {
				for li, l := range r.Logs {
					storageReceipts[ri].Logs[li].TopicIds = make([]uint32, len(storageReceipts[ri].Logs[li].Topics))
					for ti, topic := range l.Topics {
						id, err := tx.Get(dbutils.LogTopic2Id, topic[:])
						if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
							return false, err
						}

						// create topic if not exists with topicID++
						if err != nil && errors.Is(err, ethdb.ErrKeyNotFound) {
							ids.Topic++
							binary.BigEndian.PutUint32(iBytes, ids.Topic)
							storageReceipts[ri].Logs[li].TopicIds[ti] = ids.Topic

							err = tx.Put(dbutils.LogTopic2Id, topic[:], common.CopyBytes(iBytes))
							if err != nil {
								return false, err
							}
							err = tx.Append(dbutils.LogId2Topic, common.CopyBytes(iBytes), topic[:])
							if err != nil {
								return false, err
							}
							continue
						}

						storageReceipts[ri].Logs[li].TopicIds[ti] = binary.BigEndian.Uint32(id)
					}
				}
			}

			newV, err := rlp.EncodeToBytes(storageReceipts)
			if err != nil {
				log.Crit("Failed to encode block receipts", "err", err)
			}

			if err := tx.Append(dbutils.BlockReceipts, blockNum64Bytes, newV); err != nil {
				return false, err
			}

			select {
			default:
			case <-logEvery.C:
				sz, _ := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.BlockReceipts)
				sz1, _ := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.LogTopic2Id)
				sz2, _ := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.LogId2Topic)
				log.Info("Progress", "blockNum", blockNum,
					dbutils.BlockReceipts, common.StorageSize(sz),
					dbutils.LogTopic2Id, common.StorageSize(sz1),
					dbutils.LogId2Topic, common.StorageSize(sz2),
				)
			}
			return true, nil
		}); err != nil {
			return err
		}

		//if err := tx.(ethdb.BucketsMigrator).DropBuckets(dbutils.BlockReceiptsPrefixOld1); err != nil {
		//	return err
		//}
		return OnLoadCommit(tx, nil, true)
	},
}

var topicIndexID = Migration{
	Name: "topic_index_id",
	Up: func(tx ethdb.DbWithPendingMutations, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 32+2 {
				return next(k, k, v)
			}

			topic := k[:32]
			shardN := k[32:]

			if err := next(k, k, nil); err != nil { // delete old type of keys
				return err
			}

			id, err := tx.Get(dbutils.LogTopic2Id, topic[:])
			if err != nil {
				return err
			}

			newK := make([]byte, 4+2)
			copy(newK[:4], id)
			copy(newK[4:], shardN)

			if err := next(k, newK, v); err != nil { // create new type of keys
				return err
			}

			return nil
		}

		return etl.Transform(
			tx,
			dbutils.LogTopicIndex,
			dbutils.LogTopicIndex,
			datadir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		)
	},
}
