package debug

import (
	"context"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/cmd/snapshots/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	trnt "github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"testing"
)

/*
24eeae876413056319e0e602623b4cc252dc7b17
 */
func TestDbSwitch(t *testing.T) {
	t.Skip()
	snapshotPath:="/media/b00ris/nvme/tmp/canonicalswitch"
	dbPath:="/media/b00ris/nvme/fresh_sync/tg/chaindata/"
	toBlock:=uint64(5000)
	epoch:=1000
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	kvMain:=ethdb.NewLMDB().InMem().Path(dbPath).MustOpen()
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()

	snKV := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket:              dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	snKV=ethdb.NewSnapshotKV().DB(kvMain).SnapshotDB([]string{dbutils.HeadersBucket}, snKV).Open()

	db := ethdb.NewObjectDatabase(kv)
	snDB := ethdb.NewObjectDatabase(snKV)
	tx,err:=snDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var hash common.Hash
	var header []byte
	for i := uint64(1); i <= toBlock; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			t.Fatal(err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			t.Fatal(err)
		}

		err = tx.Append(dbutils.HeadersBucket, dbutils.HeaderKey(i, hash), header)
		if err != nil {
			t.Fatal(err)
		}
		if i%uint64(epoch) ==0 {
			err=tx.Commit()
			if err!=nil {
				t.Fatal(err)
			}

		}
		//if i-100%uint64(epoch) == 0 {
		//	err=snDB.KV().(*ethdb.SnapshotKV).Migrate(dbutils.HeaderPrefix)
		//	if err!=nil {
		//		t.Fatal(err)
		//	}
		//
		//
		//}
	}
	tx.Rollback()
	snDB.Close()
	err = utils.RmTmpFiles(utils.TypeLMDB, snapshotPath)
	if err != nil {
		log.Warn("Remove lock", "err", err)
		t.Fatal(err)
	}

	info, err := trnt.BuildInfoBytesForSnapshot(snapshotPath,trnt.LmdbFilename)
	if err != nil {
		t.Fatal(err)
	}
	infoBytes1, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(metainfo.HashBytes(infoBytes1))
}