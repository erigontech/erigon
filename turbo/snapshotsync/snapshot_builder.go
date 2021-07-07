package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
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
	BodiesCurrentSnapshot      uint64
	BodiesNewSnapshot          uint64
	HeadersNewSnapshotInfohash []byte
	BodiesNewSnapshotInfohash  []byte
	snapshotType               string
	started                    uint64
	replaceChan                chan struct{}
	replaced                   uint64
}

func (sm *SnapshotMigrator) AsyncStages(migrateToBlock uint64, dbi ethdb.RwKV, rwTX ethdb.Tx, bittorrent *Client, async bool) error {
	if atomic.LoadUint64(&sm.started) > 0 {
		return nil
	}

	var snapshotName string
	var snapshotHashKey []byte
	if sm.HeadersCurrentSnapshot < migrateToBlock && atomic.LoadUint64(&sm.HeadersNewSnapshot) < migrateToBlock {
		snapshotName = "headers"
		snapshotHashKey = dbutils.CurrentHeadersSnapshotHash
	} else if sm.BodiesCurrentSnapshot < migrateToBlock && atomic.LoadUint64(&sm.BodiesNewSnapshot) < migrateToBlock {
		snapshotName = "bodies"
		snapshotHashKey = dbutils.CurrentBodiesSnapshotHash
	} else {
		return nil
	}
	atomic.StoreUint64(&sm.started, 1)
	sm.snapshotType = snapshotName
	snapshotPath := SnapshotName(sm.snapshotsDir, sm.snapshotType, migrateToBlock)
	switch sm.snapshotType {
	case "headers":
		sm.HeadersNewSnapshot = migrateToBlock
	case "bodies":
		sm.BodiesNewSnapshot = migrateToBlock
	}
	atomic.StoreUint64(&sm.replaced, 0)

	var initialStages []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error
	switch sm.snapshotType {
	case "headers":
		initialStages = []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error{
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				return CreateHeadersSnapshot(context.Background(), tx, toBlock, snapshotPath)
			},
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				//replace snapshot
				if _, ok := db.(kv.SnapshotUpdater); !ok {
					return errors.New("db don't implement snapshotUpdater interface")
				}
				snapshotKV, err := OpenHeadersSnapshot(snapshotPath)
				if err != nil {
					return err
				}

				db.(kv.SnapshotUpdater).UpdateSnapshots("headers", snapshotKV, sm.replaceChan)
				return nil
			},
		}
	case "bodies":
		initialStages = []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error{
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				return CreateBodySnapshot(tx, toBlock, snapshotPath)
			},
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				//replace snapshot
				if _, ok := db.(kv.SnapshotUpdater); !ok {
					return errors.New("db don't implement snapshotUpdater interface")
				}
				snapshotKV, err := OpenBodiesSnapshot(snapshotPath)
				if err != nil {
					return err
				}

				db.(kv.SnapshotUpdater).UpdateSnapshots("bodies", snapshotKV, sm.replaceChan)
				return nil
			},
		}
	}

	btStages := func(shapshotHashKey []byte) []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
		return []func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error{
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				//todo headers infohash
				var infohash []byte
				var err error
				infohash, err = tx.GetOne(dbutils.BittorrentInfoBucket, shapshotHashKey)
				if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
					log.Error("Get infohash", "err", err, "block", toBlock)
					return err
				}

				if len(infohash) == 20 {
					var hash metainfo.Hash
					copy(hash[:], infohash)
					log.Info("Stop seeding snapshot", "type", snapshotName, "infohash", hash.String())
					err = bittorrent.StopSeeding(hash)
					if err != nil {
						log.Error("Stop seeding", "err", err, "block", toBlock)
						return err
					}
					log.Info("Stopped seeding snapshot", "type", snapshotName, "infohash", hash.String())
				} else {
					log.Warn("Hasn't stopped snapshot", "infohash", common.Bytes2Hex(infohash))
				}
				return nil
			},
			func(db ethdb.RoKV, tx ethdb.Tx, toBlock uint64) error {
				log.Info("Start seeding snapshot", "type", snapshotName)
				seedingInfoHash, err := bittorrent.SeedSnapshot(snapshotName, snapshotPath)
				if err != nil {
					log.Error("Seeding", "err", err)
					return err
				}

				switch snapshotName {
				case "bodies":
					sm.BodiesNewSnapshotInfohash = seedingInfoHash[:]
				case "headers":
					sm.HeadersNewSnapshotInfohash = seedingInfoHash[:]
				}

				log.Info("Started seeding snapshot", "type", snapshotName, "infohash", seedingInfoHash.String())
				atomic.StoreUint64(&sm.started, 2)
				return nil
			},
		}
	}

	stages := append(initialStages, btStages(snapshotHashKey)...)

	startStages := func(tx ethdb.Tx) (innerErr error) {
		defer func() {
			if innerErr != nil {
				atomic.StoreUint64(&sm.started, 0)
				switch snapshotName {
				case "headers":
					atomic.StoreUint64(&sm.HeadersNewSnapshot, atomic.LoadUint64(&sm.HeadersCurrentSnapshot))
				case "bodies":
					atomic.StoreUint64(&sm.BodiesNewSnapshot, atomic.LoadUint64(&sm.BodiesCurrentSnapshot))
				}

				log.Error("Error on stage. Rollback", "type", snapshotName, "err", innerErr)
			}
		}()
		for i := range stages {
			log.Info("Stage", "i", i)
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
			defer debug.LogPanic()
			readTX, err := dbi.BeginRo(context.Background())
			if err != nil {
				log.Error("begin", "err", err)
				return
			}
			defer readTX.Rollback()

			innerErr := startStages(readTX)
			if innerErr != nil {
				log.Error("Async stages", "err", innerErr)
				return
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
		var syncStages []func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error
		switch sm.snapshotType {
		case "bodies":
			syncStages = []func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error{
				func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
					log.Info("Prune db", "new", atomic.LoadUint64(&sm.BodiesNewSnapshot))
					return RemoveBlocksData(db, tx, atomic.LoadUint64(&sm.BodiesNewSnapshot))
				},
				func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
					log.Info("Save bodies snapshot", "new", common.Bytes2Hex(sm.HeadersNewSnapshotInfohash), "new", atomic.LoadUint64(&sm.HeadersNewSnapshot))
					c, err := tx.RwCursor(dbutils.BittorrentInfoBucket)
					if err != nil {
						return err
					}
					if len(sm.BodiesNewSnapshotInfohash) == 20 {
						err = c.Put(dbutils.CurrentBodiesSnapshotHash, sm.BodiesNewSnapshotInfohash)
						if err != nil {
							return err
						}
					}
					return c.Put(dbutils.CurrentBodiesSnapshotBlock, dbutils.EncodeBlockNumber(atomic.LoadUint64(&sm.BodiesNewSnapshot)))
				},
			}
		case "headers":
			syncStages = []func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error{
				func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
					log.Info("Prune headers db", "current", sm.HeadersCurrentSnapshot, "new", atomic.LoadUint64(&sm.HeadersNewSnapshot))
					return RemoveHeadersData(db, tx, sm.HeadersCurrentSnapshot, atomic.LoadUint64(&sm.HeadersNewSnapshot))
				},
				func(db ethdb.RoKV, tx ethdb.RwTx, toBlock uint64) error {
					log.Info("Save headers snapshot", "new", common.Bytes2Hex(sm.HeadersNewSnapshotInfohash), "new", atomic.LoadUint64(&sm.HeadersNewSnapshot))
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
				snapshotPath := filepath.Join(sm.snapshotsDir, snapshotName)
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
	return filepath.Join(baseDir, name) + strconv.FormatUint(blockNum, 10)
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
