package generate

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"time"
)

func GenerateBittorrentBodySnapshot(dbPath, snapshotPath string, toBlock uint64) error {
	kv:=ethdb.NewLMDB().Path(dbPath).MustOpen()
	snkv:=ethdb.NewLMDB().Path(snapshotPath).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	sndb:=ethdb.NewObjectDatabase(snkv)

	t:=time.Now()
	chunkFile:=30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3+100)
	for i:=uint64(0); i<=toBlock; i++ {
		hash:=rawdb.ReadCanonicalHash(db, i)
		body:=rawdb.ReadBodyRLP(db, hash, i)
		tuples=append(tuples, []byte(dbutils.BlockBodyPrefix), dbutils.BlockBodyKey(i, hash), body)
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

	log.Info("Finished", "duration", time.Since(t))
	return nil
}