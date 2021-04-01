package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"path"
	"time"
)

const Epoch = 500_000


//Переключение базы и закрытие
//Асинхронная передивка с триггером
//Вытеснение readers
/*
Если идет начальный синк - используем стейдж
Если нода засинкалась, и идет синк в рамках одной транзакции - создаем триггер в finish стейдже и после коммита подменяем?

Как понять, какая база:
if hasTx, ok := tx.(ethdb.HasTx); !ok || hasTx.Tx() != nil {



 */

func SpawnHeadersSnapshotGenerationStage(s *StageState, db ethdb.Database, snapshotDir string, torrentClient *bittorrent.Client, quit <-chan struct{}) error {
	tx, err := db.Begin(context.Background(), ethdb.RO)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	to, err := stages.GetStageProgress(tx, stages.Headers)
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
	os.RemoveAll(dbPath)

	if s.BlockNumber == toBlock {
	//	// we already did snapshot creation for this block
	//	s.Done()
	//	return nil

	}
	log.Info("Snapshot dir", "dbpath",dbPath, "snapshotDir", snapshotDir)
	if err := os.MkdirAll(dbPath, 0700); err != nil {
		return fmt.Errorf("creation %s, return %w", dbPath, err)
	}

	err = snapshotsync.CreateHeadersSnapshot(tx,toBlock, dbPath)
	if err!=nil {
		return err
	}

	info,err:=bittorrent.BuildInfoBytesForSnapshot(dbPath, bittorrent.LmdbFilename)
	if err!=nil {
		log.Error("BuildInfoBytesForSnapshot", "err", err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		log.Error("bencode.Marshal", "err", err)
		return err
	}

	log.Info("Created headers snapshot", "hash", metainfo.HashBytes(infoBytes))

	snapshotKV,err:=ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Path(dbPath).Open()
	tx.Rollback()
	if err!=nil {
		return err
	}
	log.Info("Headers snapshot db opened")
	done:=make(chan struct{})
	if _, ok:=db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater); ok {
		db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, done)
		select {
		case <-time.After(time.Minute*10):
			return errors.New("timout on closing snapshot database")
		case <-done:
			log.Info("Headers snapshot db switched")
			rmTX,err:=db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).WriteDB().BeginRw(context.Background())
			if err!=nil {
				return err
			}
			rmCursor:=rmTX.RwCursor(dbutils.HeadersBucket)
			err = ethdb.NewObjectDatabase(snapshotKV).Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
				innerErr:=rmCursor.Delete(k, nil)
				if innerErr!=nil {
					return false, innerErr
				}
				return true,  nil
			})
			if err!=nil {
				return err
			}

			return s.DoneAndUpdate(db, toBlock)
		}
	} else {
		return errors.New("db don't implement snapshot updater interface")
	}
}




