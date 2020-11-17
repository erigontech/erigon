package commands

import (
"context"
"fmt"
"github.com/ledgerwatch/turbo-geth/common/dbutils"
"github.com/ledgerwatch/turbo-geth/core/rawdb"
"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
"github.com/ledgerwatch/turbo-geth/ethdb"
"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
"io/ioutil"
"os"
"os/signal"
)
func VerifyStateSnapshot(dbPath, snapshotPath string, block uint64) error {
	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()
	db:=ethdb.MustOpen(dbPath)
	snapshotDir:= "/media/b00ris/nvme/snapshotsync/tg/snapshots"
	snapshotMode:="hb"
	kv:=db.KV()
	var err error
	if snapshotDir != "" {
		var mode snapshotsync.SnapshotMode
		mode, err = snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}
		kv, err = snapshotsync.WrapBySnapshots(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}
	db.SetKV(kv)
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket:       dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).ReadOnly().MustOpen()
	tmpPath,err:=ioutil.TempDir(os.TempDir(),"vrf*")
	if err!=nil {
		return err
	}
	tmpDB:=ethdb.NewLMDB().Path(tmpPath).MustOpen()
	defer os.RemoveAll(tmpPath)
	defer tmpDB.Close()
	snkv=ethdb.NewSnapshotKV().SnapshotDB(snkv).DB(tmpDB).For(dbutils.PlainStateBucket).MustOpen()
	sndb:=ethdb.NewObjectDatabase(snkv)
	tx,err:=sndb.Begin(context.Background(), ethdb.RO)
	if err!=nil {
		return err
	}
	defer tx.Rollback()
	hash, err := rawdb.ReadCanonicalHash(db, block)
	if err != nil {
		return err
	}
	syncHeadHeader := rawdb.ReadHeader(db, hash, block)
	if syncHeadHeader==nil {
		return fmt.Errorf("empty header")
	}
	expectedRootHash := syncHeadHeader.Root
	err = stagedsync.PromoteHashedStateCleanly("",tx, os.TempDir(), quitCh)
	if err!=nil {
		return err
	}
	err = stagedsync.RegenerateIntermediateHashes("", tx, os.TempDir(),expectedRootHash, quitCh)
	if err!=nil {
		fmt.Println("RegenerateIntermediateHashes", err)
		return err
	}
	return nil
}