package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	SnapshotBlock = 11_500_000
)

var (
	HeadersPostProcessingStage = stages.SyncStage("post processing")
	Snapshot11kkTD             = []byte{138, 3, 199, 118, 5, 203, 95, 162, 81, 64, 161}
)

func PostProcessing(db ethdb.Database, downloadedSnapshots map[SnapshotType]*SnapshotsInfo) error {
	if _, ok := downloadedSnapshots[SnapshotType_headers]; ok {
		err := GenerateHeaderIndexes(context.Background(), db)
		if err != nil {
			return err
		}
	}
	if _, ok := downloadedSnapshots[SnapshotType_state]; ok {
		err := PostProcessState(db, downloadedSnapshots[SnapshotType_state])
		if err != nil {
			return err
		}
	}

	if _, ok := downloadedSnapshots[SnapshotType_bodies]; ok {
		err := PostProcessBodies(db)
		if err != nil {
			return err
		}
	}

	return nil
}

func PostProcessBodies(db ethdb.Database) error {
	v, err := stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}
	err = db.(*kv.ObjectDatabase).ClearBuckets(dbutils.TxLookupPrefix)
	if err != nil {
		return err
	}

	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	k, _, err := tx.Last(dbutils.EthTx)
	if err != nil {
		return err
	}
	if len(k) != 8 {
		return errors.New("incorrect transaction id in body snapshot")
	}
	secKey := make([]byte, 8)
	//2 additional tx at the end
	binary.BigEndian.PutUint64(secKey, binary.BigEndian.Uint64(k)+3)
	err = tx.Put(dbutils.Sequence, []byte(dbutils.EthTx), secKey)
	if err != nil {
		return err
	}

	k, body, err := tx.Last(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}

	if body == nil {
		return fmt.Errorf("empty body for key %s", common.Bytes2Hex(k))
	}

	number := binary.BigEndian.Uint64(k[:8])
	err = stages.SaveStageProgress(tx, stages.Bodies, number)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func PostProcessState(db ethdb.GetterPutter, info *SnapshotsInfo) error {
	v, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}
	// clear genesis state
	err = db.(*kv.ObjectDatabase).ClearBuckets(dbutils.PlainStateBucket, dbutils.EthTx)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Execution, info.SnapshotBlock)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Senders, info.SnapshotBlock)
	if err != nil {
		return err
	}
	return nil
}

//It'll be enabled later
func PostProcessNoBlocksSync(db ethdb.Database, blockNum uint64, blockHash common.Hash, blockHeaderBytes, blockBodyBytes []byte) error {
	v, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}
	log.Info("PostProcessNoBlocksSync", "blocknum", blockNum, "hash", blockHash.String())

	tx, err := db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//add header
	err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(SnapshotBlock, blockHash), blockHeaderBytes)
	if err != nil {
		return err
	}
	//add canonical
	err = tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(SnapshotBlock), blockHash.Bytes())
	if err != nil {
		return err
	}
	body := new(types.Body)
	err = rlp.DecodeBytes(blockBodyBytes, body)
	if err != nil {
		return err
	}
	err = rawdb.WriteBody(tx, blockHash, SnapshotBlock, body)
	if err != nil {
		return err
	}

	err = tx.Put(dbutils.HeaderNumberBucket, blockHash.Bytes(), dbutils.EncodeBlockNumber(SnapshotBlock))
	if err != nil {
		return err
	}
	b, err := rlp.EncodeToBytes(big.NewInt(0).SetBytes(Snapshot11kkTD))
	if err != nil {
		return err
	}
	err = tx.Put(dbutils.HeaderTDBucket, dbutils.HeaderKey(SnapshotBlock, blockHash), b)
	if err != nil {
		return err
	}

	err = tx.Put(dbutils.HeadHeaderKey, []byte(dbutils.HeadHeaderKey), blockHash.Bytes())
	if err != nil {
		return err
	}

	err = tx.Put(dbutils.HeadBlockKey, []byte(dbutils.HeadBlockKey), blockHash.Bytes())
	if err != nil {
		return err
	}

	err = stages.SaveStageProgress(tx, stages.Headers, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(tx, stages.Bodies, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(tx, stages.BlockHashes, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(tx, stages.Senders, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(tx, stages.Execution, blockNum)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func generateHeaderHashToNumberIndex(ctx context.Context, tx ethdb.DbWithPendingMutations) error {
	log.Info("Generate headers hash to number index")
	lastHeader, _, innerErr := tx.Last(dbutils.HeadersBucket)
	if innerErr != nil {
		return innerErr
	}
	headNumberBytes := lastHeader[:8]
	headHashBytes := lastHeader[8:]

	headNumber := big.NewInt(0).SetBytes(headNumberBytes).Uint64()
	headHash := common.BytesToHash(headHashBytes)

	return etl.Transform("Torrent post-processing 1", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderNumberBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:          ctx.Done(),
		ExtractEndKey: dbutils.HeaderKey(headNumber, headHash),
	})
}

func generateHeaderTDAndCanonicalIndexes(ctx context.Context, tx ethdb.DbWithPendingMutations) error {
	var hash common.Hash
	var number uint64
	var err error

	h := rawdb.ReadHeaderByNumber(tx, 0)
	td := h.Difficulty

	log.Info("Generate TD index & canonical")
	err = etl.Transform("Torrent post-processing 2", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderTDBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		header := &types.Header{}
		innerErr := rlp.DecodeBytes(v, header)
		if innerErr != nil {
			return innerErr
		}
		number = header.Number.Uint64()
		hash = header.Hash()
		td = td.Add(td, header.Difficulty)
		tdBytes, innerErr := rlp.EncodeToBytes(td)
		if innerErr != nil {
			return innerErr
		}

		return next(k, dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), tdBytes)
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit: ctx.Done(),
	})
	if err != nil {
		return err
	}
	log.Info("Generate TD index & canonical")
	err = etl.Transform("Torrent post-processing 2", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		return next(k, common.CopyBytes(k[:8]), common.CopyBytes(k[8:]))
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit: ctx.Done(),
	})
	if err != nil {
		return err
	}
	rawdb.WriteHeadHeaderHash(tx, hash)
	rawdb.WriteHeaderNumber(tx, hash, number)
	err = stages.SaveStageProgress(tx, stages.Headers, number)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(tx, stages.BlockHashes, number)
	if err != nil {
		return err
	}
	rawdb.WriteHeadBlockHash(tx, hash)
	log.Info("Last processed block", "num", number, "hash", hash.String())
	return nil
}

func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	v, err1 := stages.GetStageProgress(db, HeadersPostProcessingStage)
	if err1 != nil {
		return err1
	}

	if v == 0 {
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err = generateHeaderHashToNumberIndex(ctx, tx); err != nil {
			return err
		}
		if err = generateHeaderTDAndCanonicalIndexes(ctx, tx); err != nil {
			return err
		}
		err = stages.SaveStageProgress(tx, HeadersPostProcessingStage, 1)
		if err != nil {
			return err1
		}

		return tx.Commit()
	}
	return nil
}
