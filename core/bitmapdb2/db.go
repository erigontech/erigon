package bitmapdb2

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cockroachdb/pebble"
	"github.com/ledgerwatch/log/v3"
)

// BitmapDB2 is a database that stores bitmaps.
// We do this to offload data from mdbx, which does not scale well on modern hardware by preventing parallel writes.
// Internally, the database is organized similarly to roaring bitmap, where each entry represents a container (array or bitmap).
// The key is a composite of bucket name, key, and high 16 bits (container key).
type DB struct {
	ldb *pebble.DB
}

type Batch struct {
	mutex            sync.Mutex
	db               *DB
	batch            *pebble.Batch
	autoCommitTxSize int
}

type LDBReader interface {
	NewIter(o *pebble.IterOptions) *pebble.Iterator
}

func NewBitmapDB2(datadir string) *DB {
	path := datadir + "/bitmapdb2"
	ldb, err := pebble.Open(path, &pebble.Options{
		MemTableSize: 16 * 1024 * 1024,
		Merger: &pebble.Merger{
			Name:  "bitmapdb2ContainerMerger",
			Merge: containerMerger,
		},
	})
	if err != nil {
		panic(err)
	}
	log.Info("BitmapDB2 open", "path", path)
	return &DB{
		ldb: ldb,
	}
}

func (db *DB) Close() {
	db.ldb.Close()
}

func (db *DB) NewBatch() *Batch {
	return &Batch{
		db:    db,
		batch: db.ldb.NewIndexedBatch(),
	}
}

func (db *DB) NewAutoBatch(txSize int) *Batch {
	return &Batch{
		db:               db,
		batch:            db.ldb.NewIndexedBatch(),
		autoCommitTxSize: txSize,
	}
}

func (db *DB) iterateContainers(reader LDBReader, bucket string, key []byte, from, to uint64,
	fn func(key []byte, c *container) (bool, error)) error {
	prefix := containerRowPrefix(bucket, key)
	iter := reader.NewIter(nil)
	defer iter.Close()

	fromHi := uint16(from >> 16)
	toHi := uint16(to >> 16)
	for iter.SeekGE(containerRowKey(bucket, key, fromHi)); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		hi := getHiFromRowKey(iter.Key())
		if hi > toHi {
			break
		}
		var more bool
		var err error
		if more, err = fn(iter.Key(), ContainerNoCopy(uint16(hi), iter.Value())); err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

func (db *DB) GetBitmap(bucket string, key []byte, from, to uint64) (*roaring.Bitmap, error) {
	bitmap := roaring.New()
	if err := db.iterateContainers(db.ldb, bucket, key, from, to, func(_ []byte, c *container) (bool, error) {
		c.ForEach(func(v uint32) error {
			if v >= uint32(from) && v <= uint32(to) {
				bitmap.Add(v)
			}
			return nil
		})
		return true, nil
	}); err != nil {
		return nil, err
	}
	return bitmap, nil
}

func (db *DB) GetBitmap64(bucket string, key []byte, from, to uint64) (*roaring64.Bitmap, error) {
	bitmap := roaring64.New()
	if err := db.iterateContainers(db.ldb, bucket, key, from, to, func(_ []byte, c *container) (bool, error) {
		c.ForEach(func(v uint32) error {
			if v >= uint32(from) && v <= uint32(to) {
				bitmap.Add(uint64(v))
			}
			return nil
		})
		return true, nil
	}); err != nil {
		return nil, err
	}
	return bitmap, nil
}

func (db *DB) SeekFirstGTE(bucket string, key []byte, input uint32) (uint32, error) {
	found, value := false, uint32(0)
	if err := db.iterateContainers(db.ldb, bucket, key, uint64(input), math.MaxInt32, func(_ []byte, c *container) (bool, error) {
		c.ForEach(func(v uint32) error {
			if !found && v >= input {
				found, value = true, v
			}
			return nil
		})
		return !found, nil
	}); err != nil {
		return 0, err
	}
	return value, nil
}

func (b *Batch) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := b.batch.Close(); err != nil {
		return err
	}
	b.batch = nil
	return nil
}

func (b *Batch) Commit() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	return b.commitInternal()
}

// TruncateBitmap removes all values from the bitmap that are greater than or equal to `from`.
func (b *Batch) TruncateBitmap(bucket string, key []byte, from uint64) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	b.db.iterateContainers(b.batch, bucket, key, from, math.MaxUint64, func(key []byte, c *container) (bool, error) {
		min := uint64(c.Hi) << 16
		if min < from {
			// This container is partially truncated
			var preserved []uint32
			c.ForEach(func(v uint32) error {
				if v < uint32(from) {
					preserved = append(preserved, v)
				}
				return nil
			})
			if len(preserved) > 0 {
				return true, b.batch.Set(key, ContainerFromArray(preserved).Buffer, nil)
			}
		}
		// This container is fully truncated
		return true, b.batch.Delete(key, nil)
	})
	return b.autoCommit()
}

// Value may be modified in place.  Caller must not use the value after calling this method.
func (b *Batch) UpsertBitmap(bucket string, key []byte, value *roaring.Bitmap) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	return b.upsertBitmapInternal(bucket, key, value)
}

type roaringBitmap64Reader struct {
	value *roaring64.Bitmap
}

func (r *roaringBitmap64Reader) IsEmpty() bool {
	return r.value.IsEmpty()
}

func (r *roaringBitmap64Reader) Iterate(cb func(x uint32) bool) {
	iter := r.value.Iterator()
	for iter.HasNext() {
		v := iter.Next()
		if v > math.MaxUint32 {
			panic("value too large")
		}
		if !cb(uint32(v)) {
			break
		}
	}
}

func (b *Batch) UpsertBitmap64(bucket string, key []byte, value *roaring64.Bitmap) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.batch == nil {
		return fmt.Errorf("batch is closed")
	}
	return b.upsertBitmapInternal(bucket, key, &roaringBitmap64Reader{value})
}

type RoaringBitmapReader interface {
	IsEmpty() bool
	Iterate(cb func(x uint32) bool)
}

func (b *Batch) upsertBitmapInternal(bucket string, key []byte, value RoaringBitmapReader) error {
	if value.IsEmpty() {
		return nil
	}
	containerMap := make(map[uint16]*container)
	value.Iterate(func(v uint32) bool {
		hi := uint16(v >> 16)
		c, ok := containerMap[hi]
		if !ok {
			c = NewEmptyContainer(hi)
			containerMap[hi] = c
		}
		c.Add(v)
		return true
	})

	for _, c := range containerMap {
		rowKey := containerRowKey(bucket, key, c.Hi)
		b.batch.Merge(rowKey, c.Buffer, nil)
	}
	return b.autoCommit()
}

func (b *Batch) commitInternal() error {
	startTime := time.Now()
	txSize := b.batch.Len()
	defer func() {
		log.Debug("Batch commit done", "time", time.Since(startTime), "txSize", txSize)
	}()
	return b.batch.Commit(nil)
}

func (b *Batch) autoCommit() error {
	if b.autoCommitTxSize == 0 {
		return nil
	}
	if b.autoCommitTxSize > 0 && b.autoCommitTxSize <= b.batch.Len() {
		if err := b.commitInternal(); err != nil {
			return err
		}
		b.batch = b.db.ldb.NewIndexedBatch()
	}
	return nil
}
