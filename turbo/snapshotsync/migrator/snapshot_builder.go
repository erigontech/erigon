package migrator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"
)

type SnapshotMigrator struct {
	MigrateToHeadersSnapshotBlock uint64
	CurrentHeadersSnapshotBlock uint64
	HeadersSnapshotDir string
	HeadersSnapshotGeneration bool
	HeadersSnapshotReady bool
	Replacing bool
	HeadersSnapshotReplaced bool
	toClean ToClean
	CleanedTo uint64
	SnapshotDir string
	MigrateToSnapshotPath string
	CurrentSnapshotPath string
	toRemove map[string]struct{}
}

type ToClean struct {
	From uint64
	To uint64
}

type Bittorrent interface {
	SeedSnapshot(db ethdb.Database, networkID uint64, path string) (metainfo.Hash, error)
	StopSeeding(hash metainfo.Hash) error
}

func (sb *SnapshotMigrator) IsFinished(blockNum uint64) bool  {
	return atomic.LoadUint64(&sb.CurrentHeadersSnapshotBlock)>=blockNum
}
func (sb *SnapshotMigrator) Cleaned(blockNum uint64) bool  {
	return atomic.LoadUint64(&sb.CleanedTo)>=blockNum && len(sb.toRemove)==0
}
func snapshotName(baseDir, name string, blockNum uint64) string  {
	return path.Join(baseDir, name)+strconv.FormatUint(blockNum, 10)
}
func (sb *SnapshotMigrator) CreateHeadersSnapshot(chainDB ethdb.Database, toBlock uint64) error {
	if sb.CurrentHeadersSnapshotBlock >= toBlock {
		return nil
	}

	if sb.HeadersSnapshotGeneration  {
		return nil
	}
	if sb.MigrateToHeadersSnapshotBlock >= toBlock {
		return nil
	}

	sb.HeadersSnapshotGeneration = true

	snapshotPath:=snapshotName(sb.SnapshotDir, "headers", toBlock)
	if err:= os.RemoveAll(snapshotPath); err!=nil {
		return err
	}

	go func() {
		var err error
		defer func() {
			sb.HeadersSnapshotGeneration = false
			if err1 := recover(); err1!=nil {
				log.Error("Snapshot generation panic", "err", err1)
				return
			}
			if err!=nil {
				log.Error("Snapshot han't generated", "err", err)
				return
			}

			fmt.Println("snapshot generated ", sb.MigrateToSnapshotPath)
		}()
		fmt.Println("Snapshot generation")
		err=CreateHeadersSnapshot(chainDB, toBlock, snapshotPath)
		if err!=nil {
			log.Error("Create headers snapshot failed", "err", err)
			fmt.Print("Snapshot generation failed", err)
			return
		}
		sb.MigrateToSnapshotPath = snapshotPath
		sb.MigrateToHeadersSnapshotBlock = toBlock
	}()
	return nil
}

func (sb *SnapshotMigrator) ReplaceHeadersSnapshot(chainDB ethdb.Database) error {
	if  sb.Replacing {
		return nil
	}
	if atomic.LoadUint64(&sb.CurrentHeadersSnapshotBlock) == atomic.LoadUint64(&sb.MigrateToHeadersSnapshotBlock) {
		return nil
	}
	if sb.MigrateToSnapshotPath == "" {
		log.Error("snapshot path is empty")
		return errors.New("snapshot path is empty")
	}
	if _, ok := chainDB.(ethdb.HasKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := chainDB.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	sb.Replacing = true
	snapshotKV,err:=OpenHeadersSnapshot(sb.MigrateToSnapshotPath)
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
			log.Error("timout on closing headers snapshot database" )
		case <-done:
			if len(sb.CurrentSnapshotPath) >0 {
				sb.toRemove[sb.CurrentSnapshotPath] = struct{}{}
			}
			from:=sb.CurrentHeadersSnapshotBlock
			if sb.toClean.From < sb.CurrentHeadersSnapshotBlock {
				from = sb.toClean.From
			}
			sb.toClean = ToClean{
				From: from,
				To: sb.MigrateToHeadersSnapshotBlock,
			}
			sb.CurrentSnapshotPath = sb.MigrateToSnapshotPath
			sb.CurrentHeadersSnapshotBlock = sb.MigrateToHeadersSnapshotBlock
			fmt.Println("snapshot replaced to ", sb.CurrentSnapshotPath)
		}
	}()
	return  nil
}
func (sb *SnapshotMigrator) RemoveHeadersData(db ethdb.Database) (err error) {
	if sb.toClean.To == 0 {
		return nil
	}
	if sb.CleanedTo >= sb.toClean.To {
		return nil
	}
	from:=sb.toClean.From
	if sb.CleanedTo> sb.toClean.From {
		from=sb.CleanedTo
	}

	log.Info("Remove data", "from", from, "to", sb.toClean.To)
	fmt.Println("Remove data", "from", from, "to", sb.toClean.To)
	if _, ok := db.(ethdb.HasKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot:=db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).SnapshotKV(dbutils.HeadersBucket)
	if headerSnapshot == nil {
		return  nil
	}
	snapshotDB:=ethdb.NewObjectDatabase(headerSnapshot)
	wdb:=db.(ethdb.HasKV).KV().(ethdb.SnapshotUpdater).WriteDB()
	if wdb == nil {
		return nil
	}
	rmTX, err := wdb.BeginRw(context.Background())
	if err != nil {
		return err
	}
	rmCursor := rmTX.RwCursor(dbutils.HeadersBucket)
	var lastCleaned uint64
	defer func() {
		if err == nil {
			atomic.StoreUint64(&sb.CleanedTo, lastCleaned)
			fmt.Println("removed to", lastCleaned)
		}
	}()
	err = snapshotDB.Walk(dbutils.HeadersBucket, dbutils.EncodeBlockNumber(from), 0, func(k, v []byte) (bool, error) {
		innerErr := rmCursor.Delete(k, nil)
		if innerErr != nil {
			return false, innerErr
		}
		lastCleaned = binary.BigEndian.Uint64(k[:8])
		return true, nil
	})
	if err != nil {
		return err
	}

	return rmTX.Commit(context.Background())
}

func (sb *SnapshotMigrator) RemovePreviousVersion() error {
	if len(sb.toRemove) == 0 {
		return nil
	}
	go func() {
		for v:=range sb.toRemove {
			err := os.RemoveAll(v)
			if err!=nil {
				fmt.Println("remove failed", err)
				log.Error("Remove failed")
				continue
			}
			delete(sb.toRemove, v)
		}

	}()
	return nil
}
func (sb *SnapshotMigrator) StopSeeding() error {
	if len(sb.toRemove) == 0 {
		return nil
	}
	go func() {
		for v:=range sb.toRemove {
			err := os.RemoveAll(v)
			if err!=nil {
				fmt.Println("remove failed", err)
				log.Error("Remove failed")
				continue
			}
			delete(sb.toRemove, v)
		}

	}()
	return nil
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
func CreateHeadersSnapshot(chainDB ethdb.Database, toBlock uint64, snapshotPath string)  error {
	snKV,err := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket:              dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Path(snapshotPath).Open()
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
			return fmt.Errorf("header %d is empty", i)
		}

		err = headerCursor.Append(dbutils.HeaderKey(i, hash), header)
		if err != nil {
			return err
		}
	}
	return nil
}