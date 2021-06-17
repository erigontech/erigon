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
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/params"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

func NewMigrator(snapshotDir string, currentSnapshotBlock uint64, currentSnapshotInfohash []byte) *SnapshotMigrator {
	return &SnapshotMigrator{
		snapshotsDir:               snapshotDir,
		HeadersCurrentSnapshot:     currentSnapshotBlock,
		HeadersNewSnapshotInfohash: currentSnapshotInfohash,
		replaceChan:                make(chan struct{}),
	}
}

type SnapshotMigrator struct {
	snapshotsDir               string
	HeadersCurrentSnapshot     uint64
	HeadersNewSnapshot         uint64
	HeadersNewSnapshotInfohash []byte
	started                    uint64
	replaceChan                chan struct{}
	replaced                   uint64
}

func (sm *SnapshotMigrator) AsyncStages(migrateToBlock uint64, dbi ethdb.RwKV, rwTX ethdb.Tx, bittorrent *Client, async bool) error {
	if sm.HeadersCurrentSnapshot >= migrateToBlock || atomic.LoadUint64(&sm.HeadersNewSnapshot) >= migrateToBlock || atomic.LoadUint64(&sm.started) > 0 {
		return nil
	}
	atomic.StoreUint64(&sm.started, 1)
	snapshotPath := SnapshotName(sm.snapshotsDir, "headers", migrateToBlock)
	sm.HeadersNewSnapshot = migrateToBlock
	atomic.StoreUint64(&sm.replaced, 0)

	stages := []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error{
		func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
			return CreateHeadersSnapshot(context.Background(), tx, toBlock, snapshotPath)
		},
		func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
			//replace snapshot
			if _, ok := db.(ethdb.SnapshotUpdater); !ok {
				return errors.New("db don't implement snapshotUpdater interface")
			}
			snapshotKV, err := OpenHeadersSnapshot(snapshotPath)
			if err != nil {
				return err
			}

			db.(ethdb.SnapshotUpdater).UpdateSnapshots([]string{dbutils.HeadersBucket}, snapshotKV, sm.replaceChan)
			return nil
		},
		func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
			//todo headers infohash
			var infohash []byte
			var err error
			infohash, err = tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				log.Error("Get infohash", "err", err, "block", toBlock)
				return err
			}

			if len(infohash) == 20 {
				var hash metainfo.Hash
				copy(hash[:], infohash)
				log.Info("Stop seeding snapshot", "type", "headers", "infohash", hash.String())
				err = bittorrent.StopSeeding(hash)
				if err != nil {
					log.Error("Stop seeding", "err", err, "block", toBlock)
					return err
				}
				log.Info("Stopped seeding snapshot", "type", "headers", "infohash", hash.String())
				//atomic.StoreUint64(&sm.Stage, StageStartSeedingNew)
			} else {
				log.Warn("Hasn't stopped snapshot", "infohash", common.Bytes2Hex(infohash))
			}
			return nil
		},
		func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
			log.Info("Start seeding snapshot", "type", "headers")
			seedingInfoHash, err := bittorrent.SeedSnapshot("headers", snapshotPath)
			if err != nil {
				log.Error("Seeding", "err", err)
				return err
			}
			sm.HeadersNewSnapshotInfohash = seedingInfoHash[:]
			log.Info("Started seeding snapshot", "type", "headers", "infohash", seedingInfoHash.String())
			atomic.StoreUint64(&sm.started, 2)
			return nil
		},
	}

	startStages := func(tx ethdb.Tx) (innerErr error) {
		defer func() {
			if innerErr != nil {

				atomic.StoreUint64(&sm.started, 0)
				atomic.StoreUint64(&sm.HeadersNewSnapshot, 0)
				log.Error("Error on stage. Rollback", "err", innerErr)
			}
		}()
		for i := range stages {
			innerErr = stages[i](dbi, tx, migrateToBlock)
			if innerErr != nil {
				return innerErr
			}
		}
		return nil
	}
	if async {
		go func() {
			//@todo think about possibility that write tx has uncommited data that we don't have in readTXs
			defer func() { debug.LogPanic(nil, true, recover()) }()
			readTX, err := dbi.BeginRo(context.Background())
			if err != nil {
				//return fmt.Errorf("begin err: %w", err)
				return
			}
			defer readTX.Rollback()

			innerErr := startStages(readTX)
			if innerErr != nil {
				log.Error("Error ", "err", innerErr)
			}
		}()
	} else {
		return startStages(rwTX)
	}
	return nil
}

func (sm *SnapshotMigrator) Replaced() bool {
	select {
	case <-sm.replaceChan:
		log.Info("Snapshot replaced")
		atomic.StoreUint64(&sm.replaced, 1)
	default:
	}

	return atomic.LoadUint64(&sm.replaced) == 1
}

func (sm *SnapshotMigrator) SyncStages(migrateToBlock uint64, dbi ethdb.RwKV, rwTX ethdb.RwTx) error {
	log.Info("SyncStages", "started", atomic.LoadUint64(&sm.started))

	if atomic.LoadUint64(&sm.started) == 2 && sm.Replaced() {
		syncStages := []func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error{
			func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
				log.Info("Prune db", "current", sm.HeadersCurrentSnapshot, "new", atomic.LoadUint64(&sm.HeadersNewSnapshot))
				return RemoveHeadersData(db, tx, sm.HeadersCurrentSnapshot, atomic.LoadUint64(&sm.HeadersNewSnapshot))
			},
			func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
				log.Info("Save CurrentHeadersSnapshotHash", "new", common.Bytes2Hex(sm.HeadersNewSnapshotInfohash), "new", atomic.LoadUint64(&sm.HeadersNewSnapshot))
				c, err := tx.RwCursor(dbutils.BittorrentInfoBucket)
				if err != nil {
					return err
				}
				if len(sm.HeadersNewSnapshotInfohash) == 20 {
					err = c.Put(dbutils.CurrentHeadersSnapshotHash, sm.HeadersNewSnapshotInfohash)
					if err != nil {
						return err
					}
				}
				return c.Put(dbutils.CurrentHeadersSnapshotBlock, dbutils.EncodeBlockNumber(atomic.LoadUint64(&sm.HeadersNewSnapshot)))
			},
		}
		for i := range syncStages {
			innerErr := syncStages[i](dbi, rwTX, migrateToBlock)
			if innerErr != nil {
				return innerErr
			}
		}
		atomic.StoreUint64(&sm.started, 3)

	}
	return nil
}

func (sm *SnapshotMigrator) Final(tx ethdb.Tx) error {
	if atomic.LoadUint64(&sm.started) < 3 {
		return nil
	}

	v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if len(v) != 8 {
		log.Error("Incorrect length", "ln", len(v))
		return nil
	}

	if sm.HeadersCurrentSnapshot < atomic.LoadUint64(&sm.HeadersNewSnapshot) && sm.HeadersCurrentSnapshot != 0 {
		oldSnapshotPath := SnapshotName(sm.snapshotsDir, "headers", sm.HeadersCurrentSnapshot)
		log.Info("Removing old snapshot", "path", oldSnapshotPath)
		tt := time.Now()
		err = os.RemoveAll(oldSnapshotPath)
		if err != nil {
			log.Error("Remove snapshot", "err", err)
			return err
		}
		log.Info("Removed old snapshot", "path", oldSnapshotPath, "t", time.Since(tt))
	}

	if binary.BigEndian.Uint64(v) == atomic.LoadUint64(&sm.HeadersNewSnapshot) {
		atomic.StoreUint64(&sm.HeadersCurrentSnapshot, sm.HeadersNewSnapshot)
		atomic.StoreUint64(&sm.started, 0)
		atomic.StoreUint64(&sm.replaced, 0)
		log.Info("CurrentHeadersSnapshotBlock commited", "block", binary.BigEndian.Uint64(v))
		return nil
	}
	return nil
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

//CalculateEpoch - returns latest available snapshot block that possible to create.
func CalculateEpoch(block, epochSize uint64) uint64 {
	return block - (block+params.FullImmutabilityThreshold)%epochSize
}

func SnapshotName(baseDir, name string, blockNum uint64) string {
	return path.Join(baseDir, name) + strconv.FormatUint(blockNum, 10)
}

func GetSnapshotInfo(db ethdb.RwKV) (uint64, []byte, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	v, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err != nil {
		return 0, nil, err
	}
	if v == nil {
		return 0, nil, err
	}
	var snapshotBlock uint64
	if len(v) == 8 {
		snapshotBlock = binary.BigEndian.Uint64(v)
	}

	infohash, err := tx.GetOne(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotHash)
	if err != nil {
		return 0, nil, err
	}
	if infohash == nil {
		return 0, nil, err
	}
	return snapshotBlock, infohash, nil
}

func OpenHeadersSnapshot(dbPath string) (ethdb.RwKV, error) {
	return ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Readonly().Path(dbPath).Open()
}

func CreateHeadersSnapshot(ctx context.Context, readTX ethdb.Tx, toBlock uint64, snapshotPath string) error {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	var snKV ethdb.RwKV
	snKV, err = ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketsConfigs[dbutils.HeadersBucket],
		}
	}).Path(snapshotPath).Open()
	if err != nil {
		return err
	}

	sntx, err := snKV.BeginRw(context.Background())
	if err != nil {
		return fmt.Errorf("begin err: %w", err)
	}
	defer sntx.Rollback()

	err = GenerateHeadersSnapshot(ctx, readTX, sntx, toBlock)
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

func RemoveHeadersData(db ethdb.RoKV, tx ethdb.RwTx, currentSnapshot, newSnapshot uint64) (err error) {
	log.Info("Remove data", "from", currentSnapshot, "to", newSnapshot)
	if _, ok := db.(ethdb.SnapshotUpdater); !ok {
		return errors.New("db don't implement snapshotUpdater interface")
	}
	headerSnapshot := db.(ethdb.SnapshotUpdater).SnapshotKV(dbutils.HeadersBucket)
	if headerSnapshot == nil {
		log.Info("headerSnapshot is empty")
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
