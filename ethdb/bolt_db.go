// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package ethdb defines the interfaces for an Ethereum data store.
package ethdb

import (
	"bytes"
	"context"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	boltPagesAllocGauge    = metrics.NewRegisteredGauge("bolt/pages/alloc_bytes", nil)
	boltPagesFreeGauge     = metrics.NewRegisteredGauge("bolt/pages/free", nil)
	boltPagesPendingGauge  = metrics.NewRegisteredGauge("bolt/pages/pending", nil)
	boltFreelistInuseGauge = metrics.NewRegisteredGauge("bolt/freelist/inuse", nil)
	boltTxGauge            = metrics.NewRegisteredGauge("bolt/tx/total", nil)
	boltTxOpenGauge        = metrics.NewRegisteredGauge("bolt/tx/open", nil)
	boltTxCursorGauge      = metrics.NewRegisteredGauge("bolt/tx/cursors_total", nil)
	boltRebalanceGauge     = metrics.NewRegisteredGauge("bolt/rebalance/total", nil)
	boltRebalanceTimer     = metrics.NewRegisteredTimer("bolt/rebalance/time", nil)
	boltSplitGauge         = metrics.NewRegisteredGauge("bolt/split/total", nil)
	boltSpillGauge         = metrics.NewRegisteredGauge("bolt/spill/total", nil)
	boltSpillTimer         = metrics.NewRegisteredTimer("bolt/spill/time", nil)
	boltWriteGauge         = metrics.NewRegisteredGauge("bolt/write/total", nil)
	boltWriteTimer         = metrics.NewRegisteredTimer("bolt/write/time", nil)
)

// BoltDatabase is a wrapper over BoltDb,
// compatible with the Database interface.
type BoltDatabase struct {
	db  *bolt.DB   // BoltDB instance
	log log.Logger // Contextual logger tracking the database path
	id  uint64

	stopNetInterface context.CancelFunc
	netAddr          string
	stopMetrics      context.CancelFunc
	wg               *sync.WaitGroup
}

// NewBoltDatabase returns a BoltDB wrapper.
func NewWrapperBoltDatabase(db *bolt.DB) *BoltDatabase {
	logger := log.New()
	return &BoltDatabase{
		db:  db,
		log: logger,
		id:  id(),
	}
}

// NewBoltDatabase returns a BoltDB wrapper.
func NewBoltDatabase(file string) (*BoltDatabase, error) {
	logger := log.New("database", file)

	// Create necessary directories
	if err := os.MkdirAll(path.Dir(file), os.ModePerm); err != nil {
		return nil, err
	}
	// Open the db and recover any potential corruptions
	db, errOpen := bolt.Open(file, 0600, &bolt.Options{KeysPrefixCompressionDisable: true})
	// (Re)check for errors and abort if opening of the db failed
	if errOpen != nil {
		return nil, errOpen
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range dbutils.Buckets {
			if _, err := tx.CreateBucketIfNotExists(bucket, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	bdb := &BoltDatabase{
		db:  db,
		log: logger,
		id:  id(),
		wg:  &sync.WaitGroup{},
	}
	if metrics.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		bdb.stopMetrics = cancel
		bdb.wg.Add(1)
		go func() {
			defer bdb.wg.Done()
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			collectBoltMetrics(ctx, db, ticker)
		}()
	}

	return bdb, nil
}

// Put inserts or updates a single entry.
func (db *BoltDatabase) Put(bucket, key []byte, value []byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket, false)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
	return err
}

func (db *BoltDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	var savedTx *bolt.Tx
	err := db.db.Update(func(tx *bolt.Tx) error {
		for bucketStart := 0; bucketStart < len(tuples); {
			bucketEnd := bucketStart
			for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
			}
			b, err := tx.CreateBucketIfNotExists(tuples[bucketStart], false)
			if err != nil {
				return err
			}
			c := b.Cursor()
			l := (bucketEnd - bucketStart) / 3
			for i := 0; i < l; i++ {
				k := tuples[bucketStart+3*i+1]
				v := tuples[bucketStart+3*i+2]
				if v == nil {
					if err := c.Delete2(k); err != nil {
						return err
					}
				} else {
					if err := c.Put(k, v); err != nil {
						return err
					}
				}
			}
			//if err := b.MultiPut(pairs...); err != nil {
			//	return err
			//}
			bucketStart = bucketEnd
		}
		savedTx = tx
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint64(savedTx.Stats().Write), nil
}

// Type which expecting sequence of triplets: dbi, key, value, ....
// It sorts entries by dbi name, then inside dbi clusters sort by keys
type MultiPutTuples [][]byte

func (t MultiPutTuples) Len() int { return len(t) / 3 }

func (t MultiPutTuples) Less(i, j int) bool {
	cmp := bytes.Compare(t[i*3], t[j*3])
	if cmp == -1 {
		return true
	}
	if cmp == 0 {
		return bytes.Compare(t[i*3+1], t[j*3+1]) == -1
	}
	return false
}

func (t MultiPutTuples) Swap(i, j int) {
	t[i*3], t[j*3] = t[j*3], t[i*3]
	t[i*3+1], t[j*3+1] = t[j*3+1], t[i*3+1]
	t[i*3+2], t[j*3+2] = t[j*3+2], t[i*3+2]
}

func (db *BoltDatabase) Has(bucket, key []byte) (bool, error) {
	var has bool
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			has = false
		} else {
			v, _ := b.Get(key)
			has = v != nil
		}
		return nil
	})
	return has, err
}

func (db *BoltDatabase) DiskSize(_ context.Context) (common.StorageSize, error) {
	return common.StorageSize(db.db.Size()), nil
}

func (db *BoltDatabase) BucketsStat(ctx context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return db.KV().(HasStats).BucketsStat(ctx)
}

// Get returns the value for a given key if it's present.
func (db *BoltDatabase) Get(bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v, _ := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

func Get(db KV, bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.View(context.Background(), func(tx Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v, _ := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

// GetIndexChunk returns proper index chunk or return error if index is not created.
// key must contain inverted block number in the end
func (db *BoltDatabase) GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			c := b.Cursor()
			k, v := c.Seek(dbutils.IndexChunkKey(key, timestamp))
			if !bytes.HasPrefix(k, dbutils.CompositeKeyWithoutIncarnation(key)) {
				return ErrKeyNotFound
			}
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

// getChangeSetByBlockNoLock returns changeset by block and dbi
func (db *BoltDatabase) GetChangeSetByBlock(storage bool, timestamp uint64) ([]byte, error) {
	key := dbutils.EncodeTimestamp(timestamp)

	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.ChangeSetByIndexBucket(true /* plain */, storage))
		if b == nil {
			return nil
		}

		v, _ := b.Get(key)
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dat, nil
}

func HackAddRootToAccountBytes(accNoRoot []byte, root []byte) (accWithRoot []byte, err error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(accNoRoot); err != nil {
		return nil, err
	}
	acc.Root = common.BytesToHash(root)
	accWithRoot = make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(accWithRoot)
	return accWithRoot, nil
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

func (db *BoltDatabase) Walk(bucket, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
			goOn, err := walker(k, v)
			if err != nil {
				return err
			}
			if !goOn {
				break
			}
			k, v = c.Next()
		}
		return nil
	})
	return err
}

func (db *BoltDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil {
			// Adjust rangeIdx if needed
			if fixedbytes > 0 {
				cmp := int(-1)
				for cmp != 0 {
					cmp = bytes.Compare(k[:fixedbytes-1], startkey[:fixedbytes-1])
					if cmp == 0 {
						k1 := k[fixedbytes-1] & mask
						k2 := startkey[fixedbytes-1] & mask
						if k1 < k2 {
							cmp = -1
						} else if k1 > k2 {
							cmp = 1
						}
					}
					if cmp < 0 {
						k, v = c.SeekTo(startkey)
						if k == nil {
							return nil
						}
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
					}
				}
			}
			if len(v) > 0 {
				if err := walker(rangeIdx, k, v); err != nil {
					return err
				}
			}
			k, v = c.Next()
		}
		return nil
	})
	return err
}

// Delete deletes the key from the queue and database
func (db *BoltDatabase) Delete(bucket, key []byte) error {
	// Execute the actual operation
	err := db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			return b.Delete(key)
		} else {
			return nil
		}
	})
	return err
}

func (db *BoltDatabase) ClearBuckets(buckets ...[]byte) error {
	for _, bucket := range buckets {
		bucket := bucket
		if err := db.db.Update(func(tx *bolt.Tx) error {
			if err := tx.DeleteBucket(bucket); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(bucket, false); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (db *BoltDatabase) Close() {
	if db.stopMetrics != nil {
		db.stopMetrics()
	}

	if err := db.db.Close(); err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}

func (db *BoltDatabase) Keys() ([][]byte, error) {
	var keys [][]byte
	err := db.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if bolt.IsSystemBucket(name) {
				return nil
			}
			var nameCopy = make([]byte, len(name))
			copy(nameCopy, name)
			return b.ForEach(func(k, _ []byte) error {
				var kCopy = make([]byte, len(k))
				copy(kCopy, k)
				keys = append(append(keys, nameCopy), kCopy)
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

func (db *BoltDatabase) Bolt() *bolt.DB {
	return db.db
}

func (db *BoltDatabase) KV() KV {
	return &BoltKV{bolt: db.db}
}

func (db *BoltDatabase) NewBatch() DbWithPendingMutations {
	m := &mutation{
		db:   db,
		puts: newPuts(),
	}
	return m
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (db *BoltDatabase) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
}

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (db *BoltDatabase) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (db *BoltDatabase) TruncateAncients(items uint64) error {
	return errNotSupported
}

func (db *BoltDatabase) ID() uint64 {
	return db.id
}

func InspectDatabase(db Database) error {
	// FIXME: implement in Turbo-Geth
	// see https://github.com/ethereum/go-ethereum/blob/f5d89cdb72c1e82e9deb54754bef8dd20bf12591/core/rawdb/database.go#L224
	return errNotSupported
}

func NewDatabaseWithFreezer(db *ObjectDatabase, dir, suffix string) (*ObjectDatabase, error) {
	// FIXME: implement freezer in Turbo-Geth
	return db, nil
}
