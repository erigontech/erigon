package stagedsync

import (
	"context"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"path"
)

const Epoch = 500_000

//Вытеснение readers
//Переключение базы и закрытие
//Асинхронная передивка с триггером

/*
Если идет начальный синк - используем стейдж
Если нода засинкалась, и идет синк в рамках одной транзакции - создаем триггер в finish стейдже и после коммита подменяем?

Как понять, какая база:
if hasTx, ok := tx.(ethdb.HasTx); !ok || hasTx.Tx() != nil {



 */

func SpawnHeadersSnapshotGenerationStage(s *StageState, db ethdb.Database, snapshotDir string, torrentClient *bittorrent.Client, quit <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RO)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	to, err := stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return fmt.Errorf("%w",  err)
	}

	if to<Epoch {
		s.Done()
		return nil
	}
	if s.BlockNumber > to {
		return fmt.Errorf("headers snapshot is higher canonical. snapshot %d headers %d", s.BlockNumber, to)
	}

	toBlock:=to-to%Epoch

	dbPath:=path.Join(snapshotDir, "headers")
	if s.BlockNumber == toBlock {
	//	// we already did snapshot creation for this block
	//	s.Done()
	//	return nil

		os.RemoveAll(dbPath)
	}
	err = CreateHeadersSnapshot(db, toBlock, snapshotDir)
	if err!=nil {
		return err
	}

	return s.DoneAndUpdate(db, toBlock)
}

func CreateHeadersSnapshot(chainDB ethdb.Database, toBlock uint64, snapshotDir string)  error {
	dbPath:=path.Join(snapshotDir, "headers")

	if err := os.MkdirAll(snapshotDir, 0700); err != nil {
		return err
	}

	snKV,err := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket:              dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Path(dbPath).Open()
	if err!=nil {
		return err
	}

	sntx,err:=snKV.BeginRw(context.Background())
	if err!=nil {
		return err
	}
	defer sntx.Rollback()
	err = GenerateHeadersSnapshot(chainDB, sntx, toBlock)
	if err!=nil {
		return err
	}
	err=sntx.Commit(context.Background())
	if err!=nil {
		return err
	}
	snKV.Close()

	info,err:=bittorrent.BuildInfoBytesForSnapshot(dbPath, bittorrent.LmdbFilename)
	if err!=nil {
		log.Error("BuildInfoBytesForSnapshot", "err", err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		log.Error("bencode.Marshal", "err", err)
		return err
	}
	log.Info("Created snapshot", "hash", metainfo.HashBytes(infoBytes))
	return nil
}


func GenerateHeadersSnapshot(db ethdb.Database, sntx ethdb.RwTx, toBlock uint64) error {
	headerCursor:=sntx.RwCursor(dbutils.HeadersBucket)
	var hash common.Hash
	var err error
	var header []byte
	for i := uint64(0); i <= toBlock; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return err
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			return err
		}

		err = headerCursor.Append(dbutils.HeaderKey(i, hash), header)
		if err != nil {
			return err
		}
	}
	return nil
}