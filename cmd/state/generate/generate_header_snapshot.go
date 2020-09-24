package generate

import (
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"
)


func GenerateHeaderSnapshot(dbPath, snapshotPath string, toBlock uint64) error {
	if snapshotPath=="" {
		return errors.New("empty snapshot path")
	}
	err:=os.RemoveAll(snapshotPath)
	if err!=nil {
		return err
	}
	kv:=ethdb.NewLMDB().Path(dbPath).MustOpen()
	snkv:=ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
			dbutils.HeadBlockKey: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	sndb:=ethdb.NewObjectDatabase(snkv)

	ch:=make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	t:=time.Now()
	chunkFile:=30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3)
	var hash common.Hash
	var header []byte
	for i:=uint64(1); i<=toBlock; i++ {
		select {
		case <-ch:
			return errors.New("interrupted")
		default:

		}
		hash=rawdb.ReadCanonicalHash(db, i)
		header=rawdb.ReadHeaderRLP(db,hash, i)
		tuples=append(tuples, []byte(dbutils.HeaderPrefix), dbutils.HeaderKey(i, hash), header)
		if len(tuples) >= chunkFile {
			log.Info("Commited","block", i)
			_, err:=sndb.MultiPut(tuples...)
			if err!=nil {
				log.Crit("Multiput error", "err", err)
				return err
			}
			tuples=tuples[:0]
		}
	}

	if len(tuples) > 0 {
		_, err:=sndb.MultiPut(tuples...)
		if err!=nil {
			log.Crit("Multiput error", "err", err)
			return err
		}
	}
	rawdb.WriteHeadBlockHash(sndb, hash)
	err=sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err!=nil {
		log.Crit("SnapshotHeadersHeadNumber error", "err", err)
		return err
	}
	err=sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash), hash.Bytes())
	if err!=nil {
		log.Crit("SnapshotHeadersHeadHash error", "err", err)
		return err
	}


	log.Info("Finished", "duration", time.Since(t))
	sndb.Close()
	err = os.Remove(snapshotPath+"/lock.mdb")
	if err!=nil {
		log.Warn("Remove lock", "err", err)
		return err
	}
	mi := metainfo.MetaInfo{
		CreatedBy: "turbogeth",
		CreationDate: time.Now().Unix(),
		Comment: "Snapshot of headers",
	}

	info,err:=torrent.BuildInfoBytesForLMDBSnapshot(snapshotPath)
	mi.InfoBytes, err = bencode.Marshal(info)
	if err!=nil {
		log.Warn("bencode.Marshal", "err", err)
		return err
	}
	magnet := mi.Magnet("headers", mi.HashInfoBytes()).String()
	fmt.Println(magnet)

	return nil
}