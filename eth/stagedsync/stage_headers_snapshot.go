package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/migrator"
	"os"
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

func SpawnHeadersSnapshotGenerationStage(s *StageState, db ethdb.Database, sm *migrator.SnapshotMigrator2, snapshotDir string, torrentClient *bittorrent.Client, quit <-chan struct{}) error {
	fmt.Println("SpawnHeadersSnapshotGenerationStage")
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		//we use this stage only for inital sync to save space
		s.Done()
		return nil
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


	if s.BlockNumber == toBlock {
		// we already did snapshot creation for this block
		s.Done()
		return nil
	}

	dbPath:=migrator.SnapshotName(snapshotDir, "headers", toBlock)
	//remove files on this path(in case of failed generation)
	err = os.RemoveAll(dbPath)
	if err!=nil {
		return err
	}

	log.Info("Snapshot dir", "dbpath",dbPath, "snapshotDir", snapshotDir)
	//create a dir if it's not exsist yet.
	if err := os.MkdirAll(snapshotDir, 0700); err != nil {
		return fmt.Errorf("creation %s, return %w", dbPath, err)
	}

	tt:=time.Now()
	snapshotPath:=migrator.SnapshotName(snapshotDir, "headers", toBlock)
	err=migrator.CreateHeadersSnapshot(context.Background(), db, toBlock, snapshotPath)
	if err!=nil {
		fmt.Println("-----------------------Create Error!", err)
		return err
	}
	log.Info("Snapshot create", "t", time.Since(tt))
	err = sm.ReplaceHeadersSnapshot(db, snapshotPath)
	if err!=nil {
		fmt.Println("-----------------------Replace Error!", err)
		return err
	}
	infohash,err:=db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
	if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		fmt.Println("-------get infohash err", err)
		return err
	}
	prevSnapshotBlockBytes,err:=db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		fmt.Println("-------get snapshot block err", err)
		return err
	}

	//save new snapshot block
	err = db.Put(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock, dbutils.EncodeBlockNumber(toBlock))
	if err!=nil {
		fmt.Println("Put error", err)
		return err
	}

	if len(infohash)==20 {
		hash:=metainfo.HashBytes(infohash)
		log.Info("Stop seeding previous snapshot", "type", "headers", "infohash", hash.String())

		err = torrentClient.StopSeeding(hash)
		if err!=nil {
			fmt.Println("-----------------------stop seeding!", err)
			return err
		}
	}

	seedingInfoHash, err := torrentClient.SeedSnapshot("headers", snapshotPath)
	if err!=nil {
		fmt.Println("-------seed snaopshot err", err)
		return err
	}

	//save new snapshot
	err = db.Put(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash, seedingInfoHash.Bytes())
	if err!=nil {
		fmt.Println("Put error", err)
		return err
	}

	log.Info("Seeding new snapshot started", "type", "headers", "infohash", seedingInfoHash.String())

	var prevSnapshotBlock uint64
	if len(prevSnapshotBlockBytes)==8 {
		prevSnapshotBlock = binary.BigEndian.Uint64(prevSnapshotBlockBytes)
	}

	if prevSnapshotBlock>0 {
		oldSnapshotPath:= migrator.SnapshotName(snapshotDir,"headers", prevSnapshotBlock)
		log.Info("Remove previous snapshot","type", "headers", "block", prevSnapshotBlock, "path", oldSnapshotPath)
		err = os.RemoveAll(oldSnapshotPath)
		if err!=nil {
			fmt.Println("snapshot hasn't removed")
		}
	}

	log.Info("Start pruning", "from", prevSnapshotBlock, "to", toBlock)
	tt=time.Now()
	mainDBRWTx,err:=db.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).WriteDB().BeginRw(context.Background())
	if err!=nil {
		fmt.Println("Begin rw error", err)
		return err
	}
	defer mainDBRWTx.Rollback()
	err = migrator.RemoveHeadersData(db, mainDBRWTx, prevSnapshotBlock, toBlock)
	if err!=nil {
		fmt.Println("Remove  error", err)
		return err
	}
	err = mainDBRWTx.Commit()
	if err!=nil {
		fmt.Println("Commit error", err)
		return err
	}
	log.Info("Pruning successful", "t", time.Since(tt))

	return s.DoneAndUpdate(db, toBlock)
}




