package generate

import (
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
	"time"
)

//"/Users/boris/tg/tg/chaindata"
//""/Users/boris/snapshot/"
//10400000

func GenerateHeaderSnapshot(dbPath, snapshotPath string, toBlock uint64) error {
	kv:=ethdb.NewLMDB().Path(dbPath).MustOpen()
	snkv:=ethdb.NewLMDB().Path(snapshotPath).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	sndb:=ethdb.NewObjectDatabase(snkv)

	t:=time.Now()
	chunkFile:=30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3)
	var hash common.Hash
	var header []byte
	for i:=uint64(2); i<=toBlock; i++ {
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
	err:=sndb.Put(dbutils.DatabaseInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err!=nil {
		log.Crit("SnapshotHeadersHeadNumber error", "err", err)
		return err
	}
	err=sndb.Put(dbutils.DatabaseInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash), hash.Bytes())
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

	//mi.AnnounceList=builtinAnnounceList
	info := metainfo.Info{PieceLength: torrent.DefaultChunkSize}
	err = info.BuildFromFilePath(snapshotPath)
	if err!=nil {
		log.Warn("BuildFromFilePath", "err", err)
		return err
	}

	mi.InfoBytes, err = bencode.Marshal(info)
	if err!=nil {
		log.Warn("bencode.Marshal", "err", err)
		return err
	}
	magnet := mi.Magnet("headers", mi.HashInfoBytes()).String()
	fmt.Println(magnet)

	return nil
}