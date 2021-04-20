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
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)


func SnapshotName(baseDir, name string, blockNum uint64) string  {
	return path.Join(baseDir, name)+strconv.FormatUint(blockNum, 10)
}



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

	snapshotPath:=SnapshotName(sb.SnapshotDir, "headers", toBlock)
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
		err=CreateHeadersSnapshot(context.Background(), chainDB, toBlock, snapshotPath)
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
	fmt.Println("ReplaceHeadersSnapshot")
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
	if _, ok := chainDB.(ethdb.HasRwKV); !ok {
		return errors.New("db don't implement hasKV interface")
	}

	if _, ok := chainDB.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	fmt.Println("sb.Replacing = true")
	sb.Replacing = true
	snapshotKV,err:=OpenHeadersSnapshot(sb.MigrateToSnapshotPath)
	if err!=nil {
		return err
	}

	go func() {
		fmt.Println("Update snapshots")
		defer func() {
			fmt.Println("sb.Replacing = false")
			sb.Replacing = false
		}()
		done := make(chan struct{})
		chainDB.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, done)
		fmt.Println("-Update snapshots")
		select {
		case <-time.After(time.Minute):
			panic("timeout")
			log.Error("timout on closing headers snapshot database" )
		case <-done:
			fmt.Println("<-done")
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
	wdb:=db.(ethdb.HasRwKV).RwKV().(ethdb.SnapshotUpdater).WriteDB()
	if wdb == nil {
		return nil
	}
	rmTX, err := wdb.BeginRw(context.Background())
	if err != nil {
		return err
	}
	rmCursor,err:= rmTX.RwCursor(dbutils.HeadersBucket)
	if err!=nil {
		return err
	}
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

	return rmTX.Commit()
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



/*
Создать снепшот
Подменить снепшот в базе
Остановить раздачи старого снепшота
Удалить раздачу старого снепшота
Удалить старый снепшот
Удалить данные из основной бд


Находится в стейдже Final
Сохранение промежуточного прогресса
Проход для каждого снепшота последовательный
*/

func New(snapshotDir string) *SnapshotMigrator2 {
	return &SnapshotMigrator2{
		snapshotsDir: snapshotDir,
	}
}

type SnapshotMigrator2 struct {
	snapshotsDir string
	HeadersCurrentSnapshot uint64
	HeadersNewSnapshot uint64
	HeadersNewSnapshotInfohash []byte

	Stage uint64
	mtx sync.RWMutex

	cancel func()
}
func (sm *SnapshotMigrator2) Close() {
	sm.cancel()
}

func (sm *SnapshotMigrator2) Finished(block uint64) bool {
	return atomic.LoadUint64(&sm.HeadersNewSnapshot)==atomic.LoadUint64(&sm.HeadersCurrentSnapshot) && atomic.LoadUint64(&sm.HeadersCurrentSnapshot)>0 && sm.Stage==StageStart && atomic.LoadUint64(&sm.HeadersCurrentSnapshot)== block
}

const (
	StageStart  = 0
	StageGenerate  = 1
	StageReplace  = 2
	StageStopSeeding  = 3
	StageStartSeedingNew  = 4
	StageRemoveOldSnapshot  = 5
	StagePruneDB  = 6
	StageFinish  = 7
)
/*
todo save snapshot block before delete old snapshot
 */
func (sm *SnapshotMigrator2) Migrate(db ethdb.Database, tx ethdb.Database, toBlock uint64,  bittorrent *bittorrent.Client) error  {
	fmt.Println("Migrate",sm.Stage, sm.HeadersNewSnapshot, sm.HeadersCurrentSnapshot, toBlock)
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
				fmt.Println("-----------------------Create Error!", err)
				return
			}
			log.Info("Snapshot created","t", time.Since(tt))

			atomic.StoreUint64(&sm.Stage, StageReplace)
			log.Info("Replace snapshot", "type", "headers")
			tt = time.Now()
			err = sm.ReplaceHeadersSnapshot(db, snapshotPath)
			if err!=nil {
				fmt.Println("-----------------------Replace Error!", err)
				return
			}
			log.Info("Replaced snapshot", "type", "headers", "t", time.Since(tt))


			atomic.StoreUint64(&sm.Stage, StageStopSeeding)
			//todo headers infohash
			infohash,err:=db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
			if err!=nil {
				fmt.Println("-------get infohash err", err)
			}
			fmt.Println("stop seeding", common.Bytes2Hex(infohash))
			if len(infohash)==20 {
				var hash metainfo.Hash
				copy(hash[:], infohash)
				log.Info("Stop seeding snapshot", "type", "headers", "infohash", hash.String())
				tt = time.Now()
				err = bittorrent.StopSeeding(hash)
				if err!=nil {
					fmt.Println("-----------------------stop seeding!", err)
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
				log.Error("Seed error", "err",err)
				fmt.Println("-------seed snaopshot err", err)
			}
			sm.HeadersNewSnapshotInfohash = seedingInfoHash[:]
			log.Info("Started seeding snapshot", "type", "headers", "t", time.Since(tt), "infohash", seedingInfoHash.String())
			atomic.StoreUint64(&sm.Stage, StageRemoveOldSnapshot)
			sm.mtx.RLock()
			defer sm.mtx.RUnlock()
			if sm.HeadersCurrentSnapshot < sm.HeadersNewSnapshot && sm.HeadersCurrentSnapshot!=0 {
				oldSnapshotPath:= SnapshotName(sm.snapshotsDir,"headers", sm.HeadersCurrentSnapshot)
				log.Info("Removing old snapshot",  "path", oldSnapshotPath)
				tt=time.Now()
				err = os.RemoveAll(oldSnapshotPath)
				if err!=nil {
					fmt.Println("snapshot hasn't removed")
					log.Error("Remove snapshot", "err", err)
				}
				log.Info("Removed old snapshot",  "path", oldSnapshotPath,"t", time.Since(tt))
			}
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
			fmt.Println("RemoveHeadersData err", err)
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
		log.Info("Prune db succes", "t", time.Since(tt))
		atomic.StoreUint64(&sm.Stage, StageFinish)

	case StageFinish:
		fmt.Println("+Finish")
		tt:=time.Now()
		log.Info("Finish",)
		v, err := db.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		fmt.Println("+Finish")
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		if err!=nil {
			fmt.Println("err ", err)
			return err
		}
		fmt.Println("passed",binary.BigEndian.Uint64(v), sm.HeadersNewSnapshot, v)

		if len(v)!=8 {
			fmt.Println("incorrect length", len(v), v)
			log.Error("Incorrect length", "ln", len(v))
			return nil
		}
		if binary.BigEndian.Uint64(v) == sm.HeadersNewSnapshot {
			sm.mtx.Lock()
			atomic.StoreUint64(&sm.Stage, StageStart)
			atomic.StoreUint64(&sm.HeadersCurrentSnapshot,sm.HeadersNewSnapshot)
			sm.mtx.Unlock()
		}
		log.Info("Finish success", "t", time.Since(tt))

	default:
		return nil
	}
	return nil
}



func (sm *SnapshotMigrator2) ReplaceHeadersSnapshot(chainDB ethdb.Database, snapshotPath string) error {
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
	case <-time.After(time.Minute):
		panic("timeout")
		log.Error("timout on closing headers snapshot database")
	case <-done:
	}

	return  nil
}


func (sb *SnapshotMigrator2) RemoveHeadersData(db ethdb.Database, tx ethdb.RwTx) (err error) {
	return RemoveHeadersData(db, tx, sb.HeadersCurrentSnapshot, sb.HeadersNewSnapshot)
}


func RemoveHeadersData(db ethdb.Database, tx ethdb.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	fmt.Println("Remove data", "from", currentSnapshot, "to", newSnapshot)
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

	/*
			if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
	*/

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