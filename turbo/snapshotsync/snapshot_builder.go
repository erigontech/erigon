package snapshotsync

import (
	"context"
	"errors"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"time"
)

type SnapshotMigrator struct {
	HeadersSnapshotBlock uint64
	HeadersSnapshotDir string
	HeadersSnapshotGeneration bool
	HeadersSnapshotReady bool
	Replacing bool
	HeadersSnapshotReplaced bool
}

func (sb *SnapshotMigrator) CreateHeadersSnapshot(chainDB ethdb.Database, toBlock uint64, dbPath string)  {
	if  sb.HeadersSnapshotGeneration {
		return
	}
	sb.HeadersSnapshotGeneration = true
	go func() {
		defer func() {
			sb.HeadersSnapshotGeneration = false
		}()
		err:=CreateHeadersSnapshot(chainDB, toBlock, dbPath)
		if err!=nil {
			log.Error("Create headers snapshot failed", "err", err)
			return
		}
		sb.HeadersSnapshotReady = true
	}()
}

func (sb *SnapshotMigrator) ReplaceHeadersSnapshot(chainDB ethdb.Database, snapshotPath string) error {
	if !sb.HeadersSnapshotReady || sb.HeadersSnapshotReplaced || sb.Replacing{
		return nil
	}
	if _, ok := chainDB.(ethdb.HasKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := chainDB.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	snapshotKV,err:=OpenHeadersSnapshot(snapshotPath)
	if err!=nil {
		return err
	}


	go func() {
		defer func() {
			sb.Replacing = false
		}()
		done := make(chan struct{})
		chainDB.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, done)
		select {
		case <-time.After(time.Minute * 30):
			log.Error("timout on closing headers snapshot database", )
		case <-done:
			sb.HeadersSnapshotReplaced = true
		}
	}()
	return  nil
}
func (sb *SnapshotMigrator) RemoveHeadersData(db ethdb.Database) error {
	log.Info("Headers snapshot db switched")
	if _, ok := db.(ethdb.HasKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot:=db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).SnapshotKV(dbutils.HeadersBucket)
	snapshotDB:=ethdb.NewObjectDatabase(headerSnapshot)
	rmTX, err := db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).WriteDB().BeginRw(context.Background())
	if err != nil {
		return err
	}
	rmCursor := rmTX.RwCursor(dbutils.HeadersBucket)
	err = snapshotDB.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		innerErr := rmCursor.Delete(k, nil)
		if innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	return rmTX.Commit(context.Background())
}

func OpenHeadersSnapshot(dbPath string) (ethdb.KV, error) {
	return ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).Path(dbPath).Open()

}
func CreateHeadersSnapshot(chainDB ethdb.Database, toBlock uint64, dbPath string)  error {
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