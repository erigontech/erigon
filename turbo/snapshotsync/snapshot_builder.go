package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

//What number should we use?
const maxReorgDepth = 90000

func CalculateEpoch(block, epochSize uint64) uint64 {
	return block - (block+maxReorgDepth)%epochSize //Epoch

}

func SnapshotName(baseDir, name string, blockNum uint64) string {
	return path.Join(baseDir, name) + strconv.FormatUint(blockNum, 10)
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

func OpenHeadersSnapshot(dbPath string, useMdbx bool) (ethdb.RwKV, error) {
	if useMdbx {
		return ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
			}
		}).Readonly().Path(dbPath).Open()
	} else {
		return ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
			}
		}).Readonly().Path(dbPath).Open()
	}
}
func CreateHeadersSnapshot(ctx context.Context, chainDB ethdb.RwKV, toBlock uint64, snapshotPath string, useMdbx bool) error {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	var snKV ethdb.RwKV
	if useMdbx {
		snKV, err = ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
			}
		}).Path(snapshotPath).Open()
		if err != nil {
			return err
		}
	} else {
		snKV, err = ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
			}
		}).Path(snapshotPath).Open()
		if err != nil {
			return err
		}
	}

	sntx, err := snKV.BeginRw(context.Background())
	if err != nil {
		return fmt.Errorf("begin err: %w", err)
	}
	defer sntx.Rollback()

	tx, err := chainDB.BeginRo(context.Background())
	if err != nil {
		return fmt.Errorf("begin err: %w", err)
	}
	defer tx.Rollback()
	err = GenerateHeadersSnapshot(ctx, tx, sntx, toBlock)
	if err != nil {
		return fmt.Errorf("generate err: %w", err)
	}
	err = sntx.Commit()
	if err != nil {
		return fmt.Errorf("commit err: %w", err)
	}
	snKV.Close()

	return nil
}

func GenerateHeadersSnapshot(ctx context.Context, db ethdb.Tx, sntx ethdb.RwTx, toBlock uint64) error {
	headerCursor, err := sntx.RwCursor(dbutils.HeadersBucket)
	if err != nil {
		return err
	}
	var hash common.Hash
	var header []byte
	t := time.NewTicker(time.Second * 30)
	defer t.Stop()
	tt := time.Now()
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
		if len(header) < 2 {
			return fmt.Errorf("header %d is empty, %v", i, header)
		}

		err = headerCursor.Append(dbutils.HeaderKey(i, hash), header)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewMigrator(snapshotDir string, currentSnapshotBlock uint64, currentSnapshotInfohash []byte, useMdbx bool) *SnapshotMigrator {
	return &SnapshotMigrator{
		snapshotsDir:               snapshotDir,
		HeadersCurrentSnapshot:     currentSnapshotBlock,
		HeadersNewSnapshotInfohash: currentSnapshotInfohash,
		useMdbx:                    useMdbx,
	}
}

type SnapshotMigrator struct {
	snapshotsDir               string
	HeadersCurrentSnapshot     uint64
	HeadersNewSnapshot         uint64
	HeadersNewSnapshotInfohash []byte
	useMdbx                    bool

	Stage uint64
	mtx   sync.RWMutex

	cancel func()
}

func (sm *SnapshotMigrator) Close() {
	sm.cancel()
}

func (sm *SnapshotMigrator) RemoveNonCurrentSnapshots() error {
	files, err := ioutil.ReadDir(sm.snapshotsDir)
	if err != nil {
		return err
	}

	for i := range files {
		snapshotName := files[i].Name()
		if files[i].IsDir() && strings.HasPrefix(snapshotName, "headers") {
			snapshotBlock, innerErr := strconv.ParseUint(strings.TrimPrefix(snapshotName, "headers"), 10, 64)
			if innerErr != nil {
				log.Warn("unknown snapshot", "name", snapshotName, "err", innerErr)
				continue
			}
			if snapshotBlock != sm.HeadersCurrentSnapshot {
				snapshotPath := path.Join(sm.snapshotsDir, snapshotName)
				innerErr = os.RemoveAll(snapshotPath)
				if innerErr != nil {
					log.Warn("useless snapshot has't removed", "path", snapshotPath, "err", innerErr)
				}
				log.Info("removed useless snapshot", "path", snapshotPath)
			}
		}
	}
	return nil
}

func (sm *SnapshotMigrator) Finished(block uint64) bool {
	return atomic.LoadUint64(&sm.HeadersNewSnapshot) == atomic.LoadUint64(&sm.HeadersCurrentSnapshot) && atomic.LoadUint64(&sm.HeadersCurrentSnapshot) > 0 && atomic.LoadUint64(&sm.Stage) == StageStart && atomic.LoadUint64(&sm.HeadersCurrentSnapshot) == block
}

const (
	StageStart           = 0
	StageGenerate        = 1
	StageReplace         = 2
	StageStopSeeding     = 3
	StageStartSeedingNew = 4
	StagePruneDB         = 5
	StageFinish          = 6
)

func (sm *SnapshotMigrator) GetStage() string {
	st := atomic.LoadUint64(&sm.Stage)
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
func (sm *SnapshotMigrator) Migrate(db ethdb.RwKV, tx ethdb.RwTx, toBlock uint64, bittorrent *Client) error {
	switch atomic.LoadUint64(&sm.Stage) {
	case StageStart:
		log.Info("Snapshot generation block", "skip", atomic.LoadUint64(&sm.HeadersNewSnapshot) >= toBlock)
		sm.mtx.Lock()
		if atomic.LoadUint64(&sm.HeadersNewSnapshot) >= toBlock {
			sm.mtx.Unlock()
			return nil
		}

		atomic.StoreUint64(&sm.HeadersNewSnapshot, toBlock)
		atomic.StoreUint64(&sm.Stage, StageGenerate)
		ctx, cancel := context.WithCancel(context.Background())
		sm.cancel = cancel
		sm.mtx.Unlock()
		go func() {
			var err error
			defer func() {
				sm.mtx.Lock()
				//we need to put all errors to err var just to handle error case and return to start
				if err != nil {
					log.Warn("Rollback to stage start")
					atomic.StoreUint64(&sm.Stage, StageStart)
					atomic.StoreUint64(&sm.HeadersNewSnapshot, atomic.LoadUint64(&sm.HeadersCurrentSnapshot))
				}
				sm.cancel = nil
				sm.mtx.Unlock()
			}()
			snapshotPath := SnapshotName(sm.snapshotsDir, "headers", toBlock)
			tt := time.Now()
			log.Info("Create snapshot", "type", "headers")
			err = CreateHeadersSnapshot(ctx, db, toBlock, snapshotPath, sm.useMdbx)
			if err != nil {
				log.Error("Create snapshot", "err", err, "block", toBlock)
				return
			}
			log.Info("Snapshot created", "t", time.Since(tt))

			atomic.StoreUint64(&sm.Stage, StageReplace)
			log.Info("Replace snapshot", "type", "headers")
			tt = time.Now()
			err = sm.ReplaceHeadersSnapshot(db, snapshotPath)
			if err != nil {
				log.Error("Replace snapshot", "err", err, "block", toBlock, "path", snapshotPath)
				return
			}
			log.Info("Replaced snapshot", "type", "headers", "t", time.Since(tt))

			atomic.StoreUint64(&sm.Stage, StageStopSeeding)
			//todo headers infohash
			var infohash []byte
			err = db.View(ctx, func(tx ethdb.Tx) error {
				infohash, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
				return err
			})
			if err != nil {
				if !errors.Is(err, ethdb.ErrKeyNotFound) {
					log.Error("Get infohash", "err", err, "block", toBlock)
					return
				}
			}

			if len(infohash) == 20 {
				var hash metainfo.Hash
				copy(hash[:], infohash)
				log.Info("Stop seeding snapshot", "type", "headers", "infohash", hash.String())
				tt = time.Now()
				err = bittorrent.StopSeeding(hash)
				if err != nil {
					log.Error("Stop seeding", "err", err, "block", toBlock)
					return
				}
				log.Info("Stopped seeding snapshot", "type", "headers", "infohash", hash.String(), "t", time.Since(tt))
				atomic.StoreUint64(&sm.Stage, StageStartSeedingNew)
			} else {
				log.Warn("Hasn't stopped snapshot", "infohash", common.Bytes2Hex(infohash))
			}

			log.Info("Start seeding snapshot", "type", "headers")
			tt = time.Now()
			seedingInfoHash, err := bittorrent.SeedSnapshot("headers", snapshotPath)
			if err != nil {
				log.Error("Seeding", "err", err)
				return
			}
			sm.HeadersNewSnapshotInfohash = seedingInfoHash[:]
			log.Info("Started seeding snapshot", "type", "headers", "t", time.Since(tt), "infohash", seedingInfoHash.String())
			atomic.StoreUint64(&sm.Stage, StagePruneDB)
		}()

	case StagePruneDB:
		var wtx ethdb.RwTx
		var err error
		tt := time.Now()
		log.Info("Prune db", "current", sm.HeadersCurrentSnapshot, "new", sm.HeadersNewSnapshot)
		if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
			wtx = tx.(ethdb.HasTx).Tx().(ethdb.DBTX).DBTX()
		} else if wtx1, ok := tx.(ethdb.RwTx); ok {
			wtx = wtx1
		} else {
			log.Error("Incorrect db type", "type", tx)
			return nil
		}

		err = sm.RemoveHeadersData(db, wtx)
		if err != nil {
			log.Error("Remove headers data", "err", err)
			return err
		}
		c, err := wtx.RwCursor(dbutils.BittorrentInfoBucket)
		if err != nil {
			return err
		}
		if len(sm.HeadersNewSnapshotInfohash) == 20 {
			err = c.Put(dbutils.CurrentHeadersSnapshotHash, sm.HeadersNewSnapshotInfohash)
			if err != nil {
				return err
			}
		}
		err = c.Put(dbutils.CurrentHeadersSnapshotBlock, dbutils.EncodeBlockNumber(sm.HeadersNewSnapshot))
		if err != nil {
			return err
		}

		log.Info("Prune db success", "t", time.Since(tt))
		atomic.StoreUint64(&sm.Stage, StageFinish)

	case StageFinish:
		tt := time.Now()
		//todo check commited
		v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		sm.mtx.RLock()
		sm.mtx.RUnlock()
		if sm.HeadersCurrentSnapshot < sm.HeadersNewSnapshot && sm.HeadersCurrentSnapshot != 0 {
			oldSnapshotPath := SnapshotName(sm.snapshotsDir, "headers", sm.HeadersCurrentSnapshot)
			log.Info("Removing old snapshot", "path", oldSnapshotPath)
			tt = time.Now()
			err = os.RemoveAll(oldSnapshotPath)
			if err != nil {
				log.Error("Remove snapshot", "err", err)
				return err
			}
			log.Info("Removed old snapshot", "path", oldSnapshotPath, "t", time.Since(tt))
		}

		if len(v) != 8 {
			log.Error("Incorrect length", "ln", len(v))
			return nil
		}

		if binary.BigEndian.Uint64(v) == sm.HeadersNewSnapshot {
			atomic.StoreUint64(&sm.Stage, StageStart)
			atomic.StoreUint64(&sm.HeadersCurrentSnapshot, sm.HeadersNewSnapshot)
		}
		log.Info("Finish success", "t", time.Since(tt))

	default:
		return nil
	}
	return nil
}

func (sm *SnapshotMigrator) ReplaceHeadersSnapshot(chainDB ethdb.RwKV, snapshotPath string) error {
	if snapshotPath == "" {
		log.Error("snapshot path is empty")
		return errors.New("snapshot path is empty")
	}
	if _, ok := chainDB.(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	snapshotKV, err := OpenHeadersSnapshot(snapshotPath, sm.useMdbx)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	chainDB.(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, done)
	select {
	case <-time.After(time.Minute * 10):
		log.Error("timeout on closing headers snapshot database")
		panic("timeout")
	case <-done:
	}

	return nil
}

func (sb *SnapshotMigrator) RemoveHeadersData(db ethdb.RwKV, tx ethdb.RwTx) (err error) {
	return RemoveHeadersData(db, tx, sb.HeadersCurrentSnapshot, sb.HeadersNewSnapshot)
}

func RemoveHeadersData(db ethdb.RwKV, tx ethdb.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	if _, ok := db.(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot := db.(ethdb.SnapshotUpdater).SnapshotKV(dbutils.HeadersBucket)
	if headerSnapshot == nil {
		return nil
	}
	writeTX := tx.(ethdb.DBTX).DBTX()
	c, err := writeTX.RwCursor(dbutils.HeadersBucket)
	if err != nil {
		return fmt.Errorf("get headers cursor %w", err)
	}

	return headerSnapshot.View(context.Background(), func(tx ethdb.Tx) error {
		c2, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		defer c2.Close()
		defer c2.Close()
		return ethdb.Walk(c2, dbutils.EncodeBlockNumber(currentSnapshot), 0, func(k, v []byte) (bool, error) {
			innerErr := c.Delete(k, nil)
			if innerErr != nil {
				return false, fmt.Errorf("remove %v err:%w", common.Bytes2Hex(k), innerErr)
			}
			return true, nil
		})
	})
}
