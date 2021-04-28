package snapshotsync

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
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//What number should we use?
const maxReorgDepth = 90000

func CalculateEpoch(block, epochSize uint64) uint64 {
	return 	block - (block + maxReorgDepth)%epochSize//Epoch

}

func SnapshotName(baseDir, name string, blockNum uint64) string  {
	return path.Join(baseDir, name)+strconv.FormatUint(blockNum, 10)
}

func GetSnapshotInfo(db ethdb.Database) (uint64, []byte, error) {
	v, err := db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil, err
	}

	var snapshotBlock uint64
	if len(v) == 8 {
		snapshotBlock = binary.BigEndian.Uint64(v)
	}

	infohash, err := db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil, err
	}
	return snapshotBlock, infohash, nil
}

func OpenHeadersSnapshot(dbPath string) (ethdb.RwKV, error) {
	return ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).Path(dbPath).Open()

}
func CreateHeadersSnapshot(ctx context.Context, chainDB ethdb.Database, toBlock uint64, snapshotPath string)  error {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err:= os.RemoveAll(snapshotPath)
	if err!=nil {
		return err
	}
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
	err = GenerateHeadersSnapshot(ctx, chainDB, sntx, toBlock)
	if err!=nil {
		return err
	}
	err=sntx.Commit()
	if err!=nil {
		return err
	}
	snKV.Close()


	return nil
}

func GenerateHeadersSnapshot(ctx context.Context, db ethdb.Database, sntx ethdb.RwTx, toBlock uint64) error {
	headerCursor,err :=sntx.RwCursor(dbutils.HeadersBucket)
	if err!=nil {
		return err
	}
	var hash common.Hash
	var header []byte
	t:=time.NewTicker(time.Second*30)
	defer t.Stop()
	tt:=time.Now()
	for i := uint64(0); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}
		select {
		case <-t.C:
			log.Info("Headers snapshot generation", "t", time.Since(tt), "block", i)
		default:
		}
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

func NewMigrator(snapshotDir string, currentSnapshotBlock uint64, currentSnapshotInfohash []byte) *SnapshotMigrator {
	return &SnapshotMigrator{
		snapshotsDir: snapshotDir,
		HeadersCurrentSnapshot: currentSnapshotBlock,
		HeadersNewSnapshotInfohash: currentSnapshotInfohash,
	}
}

type SnapshotMigrator struct {
	snapshotsDir string
	HeadersCurrentSnapshot uint64
	HeadersNewSnapshot uint64
	HeadersNewSnapshotInfohash []byte

	Stage uint64
	mtx sync.RWMutex

	cancel func()
}
func (sm *SnapshotMigrator) Close() {
	sm.cancel()
}

func (sm *SnapshotMigrator) RemoveNonCurrentSnapshots(db ethdb.Database) error {
	files, err:=ioutil.ReadDir(sm.snapshotsDir)
	if err!=nil {
		return err
	}

	for i:=range files {
		snapshotName:=files[i].Name()
		if files[i].IsDir() && strings.HasPrefix(snapshotName, "headers") {
			snapshotBlock,err:=strconv.ParseUint(strings.TrimPrefix(snapshotName,"headers"), 10, 64)
			if err!=nil {
				log.Warn("unknown snapshot", "name", snapshotName, "err", err)
				continue
			}
			if snapshotBlock!=sm.HeadersCurrentSnapshot {
				path:=path.Join(sm.snapshotsDir, snapshotName)
				err = os.RemoveAll(path)
				if err!=nil {
					log.Warn("useless snapshot has't removed", "path",path, "err", err)
				}
				log.Info("removed useless snapshot", "path", path)
			}

		}
	}
	return nil
}

func (sm *SnapshotMigrator) Finished(block uint64) bool {
	return atomic.LoadUint64(&sm.HeadersNewSnapshot)==atomic.LoadUint64(&sm.HeadersCurrentSnapshot) && atomic.LoadUint64(&sm.HeadersCurrentSnapshot)>0 && sm.Stage==StageStart && atomic.LoadUint64(&sm.HeadersCurrentSnapshot)== block
}

const (
	StageStart  = 0
	StageGenerate  = 1
	StageReplace  = 2
	StageStopSeeding  = 3
	StageStartSeedingNew  = 4
	StagePruneDB  = 5
	StageFinish  = 6
)
func (sm *SnapshotMigrator) GetStage() string {
	st:=atomic.LoadUint64(&sm.Stage)
	switch st {
	case StageStart:
		return "start"
	case StageGenerate:
		return "generate snapshot"
	case StageReplace:
		return "snapshot replace"
	case StageStopSeeding:
		return "stop seeding"
	case StageStartSeedingNew:
		return "start seeding"
	case StagePruneDB:
		return "prune db data"
	case StageFinish:
		return "finish"
	default:
		return "unknown stage"

	}
}
func (sm *SnapshotMigrator) Migrate(db ethdb.Database, tx ethdb.Database, toBlock uint64,  bittorrent *Client) error  {
	switch atomic.LoadUint64(&sm.Stage) {
	case StageStart:
		log.Info("Snapshot generation block", "skip",atomic.LoadUint64(&sm.HeadersNewSnapshot)>= toBlock)
		sm.mtx.Lock()
		if atomic.LoadUint64(&sm.HeadersNewSnapshot)>= toBlock {
			sm.mtx.Unlock()
			return nil
		}

		atomic.StoreUint64(&sm.HeadersNewSnapshot, toBlock)
		atomic.StoreUint64(&sm.Stage, StageGenerate)
		ctx, cancel:=context.WithCancel(context.Background())
		sm.cancel = cancel
		sm.mtx.Unlock()
		go func() {
			var err error
			defer func() {
				sm.mtx.Lock()
				//we need to put all errors to err var just to handle error case and return to start
				if err!=nil {
					atomic.StoreUint64(&sm.Stage, StageStart)
				}
				sm.cancel = nil
				sm.mtx.Unlock()
			}()
			snapshotPath:=SnapshotName(sm.snapshotsDir, "headers", toBlock)
			tt:=time.Now()
			log.Info("Create snapshot", "type", "headers")
			err=CreateHeadersSnapshot(ctx, db, toBlock, snapshotPath)
			if err!=nil {
				log.Error("Create snapshot", "err", err, "block", toBlock)
				return
			}
			log.Info("Snapshot created","t", time.Since(tt))

			atomic.StoreUint64(&sm.Stage, StageReplace)
			log.Info("Replace snapshot", "type", "headers")
			tt = time.Now()
			err = sm.ReplaceHeadersSnapshot(db, snapshotPath)
			if err!=nil {
				log.Error("Replace snapshot", "err", err, "block", toBlock, "path", snapshotPath)
				return
			}
			log.Info("Replaced snapshot", "type", "headers", "t", time.Since(tt))


			atomic.StoreUint64(&sm.Stage, StageStopSeeding)
			//todo headers infohash
			infohash,err:=db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
			if err!=nil {
				if !errors.Is(err, ethdb.ErrKeyNotFound) {
					log.Error("Get infohash", "err", err, "block", toBlock)
					return
				}
			}

			if len(infohash)==20 {
				var hash metainfo.Hash
				copy(hash[:], infohash)
				log.Info("Stop seeding snapshot", "type", "headers", "infohash", hash.String())
				tt = time.Now()
				err = bittorrent.StopSeeding(hash)
				if err!=nil {
					log.Error("Stop seeding", "err", err, "block", toBlock)
					return
				}
				log.Info("Stopped seeding snapshot", "type", "headers", "infohash", hash.String(), "t", time.Since(tt))
				atomic.StoreUint64(&sm.Stage, StageStartSeedingNew)
			} else {
				log.Warn("Hasn't stopped snapshot", "infohash",common.Bytes2Hex(infohash))
			}

			log.Info("Start seeding snapshot", "type", "headers")
			tt = time.Now()
			seedingInfoHash, err := bittorrent.SeedSnapshot("headers", snapshotPath)
			if err!=nil {
				log.Error("Seeding", "err",err)
				return
			}
			sm.HeadersNewSnapshotInfohash = seedingInfoHash[:]
			log.Info("Started seeding snapshot", "type", "headers", "t", time.Since(tt), "infohash", seedingInfoHash.String())
			atomic.StoreUint64(&sm.Stage, StagePruneDB)
		}()

	case StagePruneDB:
		var wtx ethdb.RwTx
		var useExternalTx bool
		var err error
		tt:=time.Now()
		log.Info("Prune db", "current", sm.HeadersCurrentSnapshot, "new", sm.HeadersNewSnapshot)
		if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
			wtx = tx.(ethdb.HasTx).Tx().(ethdb.DBTX).DBTX()
			useExternalTx = true
		} else {
			wtx, err = tx.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).WriteDB().BeginRw(context.Background())
			if err!=nil {
				return err
			}
		}

		err = sm.RemoveHeadersData(db, wtx)
		if err!=nil {
			log.Error("Remove headers data", "err", err)
			return err
		}
		c,err:=wtx.RwCursor(dbutils.BittorrentInfoBucket)
		if err!=nil {
			return err
		}
		if len(sm.HeadersNewSnapshotInfohash)==20 {
			err = c.Put(dbutils.CurrentHeadersSnapshotHash, sm.HeadersNewSnapshotInfohash)
			if err!=nil {
				return err
			}
		}
		err = c.Put(dbutils.CurrentHeadersSnapshotBlock, dbutils.EncodeBlockNumber(sm.HeadersNewSnapshot))
		if err!=nil {
			return err
		}

		if !useExternalTx {
			err = wtx.Commit()
			if err!=nil {
				return err
			}
		}
		log.Info("Prune db success", "t", time.Since(tt))
		atomic.StoreUint64(&sm.Stage, StageFinish)

	case StageFinish:
		tt:=time.Now()
		v, err := db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		if err!=nil {
			return err
		}

		sm.mtx.RLock()
		sm.mtx.RUnlock()
		if sm.HeadersCurrentSnapshot < sm.HeadersNewSnapshot && sm.HeadersCurrentSnapshot!=0 {
			oldSnapshotPath:= SnapshotName(sm.snapshotsDir,"headers", sm.HeadersCurrentSnapshot)
			log.Info("Removing old snapshot",  "path", oldSnapshotPath)
			tt=time.Now()
			err = os.RemoveAll(oldSnapshotPath)
			if err!=nil {
				log.Error("Remove snapshot", "err", err)
				return err
			}
			log.Info("Removed old snapshot",  "path", oldSnapshotPath,"t", time.Since(tt))
		}

		if len(v)!=8 {
			log.Error("Incorrect length", "ln", len(v))
			return nil
		}

		if binary.BigEndian.Uint64(v) == sm.HeadersNewSnapshot {
			atomic.StoreUint64(&sm.Stage, StageStart)
			atomic.StoreUint64(&sm.HeadersCurrentSnapshot,sm.HeadersNewSnapshot)
		}
		log.Info("Finish success", "t", time.Since(tt))

	default:
		return nil
	}
	return nil
}



func (sm *SnapshotMigrator) ReplaceHeadersSnapshot(chainDB ethdb.Database, snapshotPath string) error {
	if snapshotPath == "" {
		log.Error("snapshot path is empty")
		return errors.New("snapshot path is empty")
	}
	if _, ok := chainDB.(ethdb.HasRwKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := chainDB.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	snapshotKV,err:=OpenHeadersSnapshot(snapshotPath)
	if err!=nil {
		return err
	}

	done := make(chan struct{})
	chainDB.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, done)
	select {
	case <-time.After(time.Minute*10):
		log.Error("timeout on closing headers snapshot database")
		panic("timeout")
	case <-done:
	}

	return  nil
}


func (sb *SnapshotMigrator) RemoveHeadersData(db ethdb.Database, tx ethdb.RwTx) (err error) {
	return RemoveHeadersData(db, tx, sb.HeadersCurrentSnapshot, sb.HeadersNewSnapshot)
}


func RemoveHeadersData(db ethdb.Database, tx ethdb.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	if _, ok := db.(ethdb.HasRwKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := db.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot:=db.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).SnapshotKV(dbutils.HeadersBucket)
	if headerSnapshot == nil {
		return  nil
	}

	snapshotDB:=ethdb.NewObjectDatabase(headerSnapshot.(ethdb.RwKV))
	c,err:=tx.RwCursor(dbutils.HeadersBucket)
	if err!=nil {
		return err
	}
	err = snapshotDB.Walk(dbutils.HeadersBucket, dbutils.EncodeBlockNumber(currentSnapshot), 0, func(k, v []byte) (bool, error) {
		innerErr := c.Delete(k, nil)
		if innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	v:=make([]byte, 8)
	binary.BigEndian.PutUint64(v, newSnapshot)
	c2, err:=tx.RwCursor(dbutils.BittorrentInfoBucket)
	if err!=nil {
		return err
	}
	err = c2.Put(dbutils.CurrentHeadersSnapshotBlock, v)
	if err!=nil {
		return err
	}
	return nil
}