package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

// TxDb - provides Database interface around ethdb.Tx
// It's not thread-safe!
// TxDb not usable after .Commit()/.Rollback() call, but usable after .CommitAndBegin() call
// you can put unlimited amount of data into this class, call IdealBatchSize is unnecessary
// Walk and MultiWalk methods - work outside of Tx object yet, will implement it later
type TxDb struct {
	db       Database
	writable bool
	tx       Tx
	ParentTx Tx
	cursors  map[string]Cursor
	len      uint64
}

func (m *TxDb) Close() {
	panic("don't call me")
}

// NewTxDbWithoutTransaction creates TxDb object without opening transaction,
// such TxDb not usable before .Begin() call on it
// It allows inject TxDb object into class hierarchy, but open write transaction later
func NewTxDbWithoutTransaction(db Database) *TxDb {
	return &TxDb{db: db}
}

func (m *TxDb) Begin(ctx context.Context, writable bool) (DbWithPendingMutations, error) {
	batch := m
	if m.tx != nil {
		batch = &TxDb{db: m.db}
	}

	if err := batch.begin(ctx, m.tx, writable); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) Put(bucket string, key []byte, value []byte) error {
	if metrics.Enabled {
		if bucket == dbutils.PlainStateBucket {
			defer dbPutTimer.UpdateSince(time.Now())
		}
	}
	m.len += uint64(len(key) + len(value))
	return m.cursors[bucket].Put(key, value)
}

func (m *TxDb) Reserve(bucket string, key []byte, i int) ([]byte, error) {
	m.len += uint64(len(key) + i)
	return m.cursors[bucket].Reserve(key, i)
}

func (m *TxDb) Append(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	switch c := m.cursors[bucket].(type) {
	case CursorDupSort:
		return c.AppendDup(key, value)
	default:
		return c.Append(key, value)
	}
}

func (m *TxDb) Delete(bucket string, key []byte) error {
	m.len += uint64(len(key))
	return m.cursors[bucket].Delete(key)
}

func (m *TxDb) NewBatch() DbWithPendingMutations {
	return &mutation{
		db:   m,
		puts: newPuts(),
	}
}

func (m *TxDb) begin(ctx context.Context, parent Tx, writable bool) error {
	tx, err := m.db.(HasKV).KV().Begin(ctx, parent, writable)
	if err != nil {
		return err
	}
	m.tx = tx
	m.ParentTx = parent
	m.cursors = make(map[string]Cursor, 16)
	for name := range m.db.(HasKV).KV().AllBuckets() {
		m.cursors[name] = tx.Cursor(name)
	}
	return nil
}

func (m *TxDb) KV() KV {
	panic("not allowed to get KV interface because you will loose transaction, please use .Tx() method")
}

// Can only be called from the worker thread
func (m *TxDb) Last(bucket string) ([]byte, []byte, error) {
	c, ok := m.cursors[bucket]
	if !ok {
		panic(fmt.Sprintf("bucket doesn't exists: '%s'", bucket))
	}
	return c.Last()
}

func (m *TxDb) Get(bucket string, key []byte) ([]byte, error) {
	if metrics.Enabled {
		defer dbGetTimer.UpdateSince(time.Now())
	}

	v, err := m.cursors[bucket].SeekExact(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (m *TxDb) GetIndexChunk(bucket string, key []byte, timestamp uint64) ([]byte, error) {
	if m.db != nil {
		return m.db.GetIndexChunk(bucket, key, timestamp)
	}
	return nil, ErrKeyNotFound
}

func (m *TxDb) Has(bucket string, key []byte) (bool, error) {
	v, err := m.Get(bucket, key)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (m *TxDb) DiskSize(ctx context.Context) (common.StorageSize, error) {
	if m.db == nil {
		return 0, nil
	}
	sz, err := m.db.(HasStats).DiskSize(ctx)
	if err != nil {
		return 0, err
	}
	return common.StorageSize(sz), nil
}

func (m *TxDb) MultiPut(tuples ...[]byte) (uint64, error) {
	return 0, MultiPut(m.tx, tuples...)
}

func MultiPut(tx Tx, tuples ...[]byte) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	count := 0
	total := float64(len(tuples)) / 3
	for bucketStart := 0; bucketStart < len(tuples); {
		bucketEnd := bucketStart
		for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
		}
		c := tx.Cursor(string(tuples[bucketStart]))

		// move cursor to a first element in batch
		// if it's nil, it means all keys in batch gonna be inserted after end of bucket (batch is sorted and has no duplicates here)
		// can apply optimisations for this case
		firstKey, _, err := c.Seek(tuples[bucketStart+1])
		if err != nil {
			return err
		}
		isEndOfBucket := firstKey == nil

		l := (bucketEnd - bucketStart) / 3
		for i := 0; i < l; i++ {
			k := tuples[bucketStart+3*i+1]
			v := tuples[bucketStart+3*i+2]
			if isEndOfBucket {
				if v == nil {
					// nothing to delete after end of bucket
				} else {
					if err := c.Append(k, v); err != nil {
						return err
					}
				}
			} else {
				if v == nil {
					if err := c.Delete(k); err != nil {
						return err
					}
				} else {
					if err := c.Put(k, v); err != nil {
						return err
					}
				}
			}

			count++

			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress)
			}
		}

		bucketStart = bucketEnd
	}
	return nil
}

func (m *TxDb) BatchSize() int {
	return int(m.len)
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (m *TxDb) IdealBatchSize() int {
	panic("only mutation hast preferred batch size, because it limited by RAM")
}

func (m *TxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return Walk(m.cursors[bucket], startkey, fixedbits, walker)
}

func Walk(c Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func ForEach(c Cursor, walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, v)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func (m *TxDb) MultiWalk(bucket string, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return MultiWalk(m.cursors[bucket], startkeys, fixedbits, walker)
}

func MultiWalk(c Cursor, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
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
					k, v, err = c.Seek(startkey)
					if err != nil {
						return err
					}
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
			if err = walker(rangeIdx, k, v); err != nil {
				return err
			}
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *TxDb) CommitAndBegin(ctx context.Context) error {
	_, err := m.Commit()
	if err != nil {
		return err
	}

	return m.begin(ctx, m.ParentTx, m.writable)
}

func (m *TxDb) RollbackAndBegin(ctx context.Context) error {
	m.Rollback()
	return m.begin(ctx, m.ParentTx, m.writable)
}

func (m *TxDb) Commit() (uint64, error) {
	if metrics.Enabled {
		defer dbCommitBigBatchTimer.UpdateSince(time.Now())
	}

	if m.tx == nil {
		return 0, fmt.Errorf("second call .Commit() on same transaction")
	}
	if err := m.tx.Commit(context.Background()); err != nil {
		return 0, err
	}
	m.tx = nil
	m.ParentTx = nil
	m.cursors = nil
	m.len = 0
	return 0, nil
}

func (m *TxDb) Rollback() {
	if m.tx == nil {
		return
	}
	m.tx.Rollback()
	m.cursors = nil
	m.tx = nil
	m.ParentTx = nil
	m.len = 0
}

func (m *TxDb) Tx() Tx {
	return m.tx
}

func (m *TxDb) Keys() ([][]byte, error) {
	panic("don't use me")
}

func (m *TxDb) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (m *TxDb) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (m *TxDb) TruncateAncients(items uint64) error {
	return errNotSupported
}

func (m *TxDb) BucketExists(name string) (bool, error) {
	exists := false
	migrator, ok := m.tx.(BucketMigrator)
	if !ok {
		return false, fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
	}
	exists = migrator.ExistsBucket(name)
	return exists, nil
}

func (m *TxDb) ClearBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]

		migrator, ok := m.tx.(BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.ClearBucket(name); err != nil {
			return err
		}
	}

	return nil
}

func (m *TxDb) ClearBucketsAndCommitEvery(deleteKeysPerTx uint64, buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Clear bucket", "name", name)
		if err := m.removeBucketContentByMultipleTransactions(name, deleteKeysPerTx); err != nil {
			return err
		}
		if err := m.tx.(BucketMigrator).ClearBucket(name); err != nil {
			return err
		}
		if err := m.CommitAndBegin(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

func (m *TxDb) DropBucketsAndCommitEvery(deleteKeysPerTx uint64, buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Dropping bucket", "name", name)
		if err := m.removeBucketContentByMultipleTransactions(name, deleteKeysPerTx); err != nil {
			return err
		}
		if err := m.tx.(BucketMigrator).DropBucket(name); err != nil {
			return err
		}
		if err := m.CommitAndBegin(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

// removeBucketContentByMultipleTransactions - allows to avoid single large freelist record inside database and
// avoid "too big transaction" error
func (m *TxDb) removeBucketContentByMultipleTransactions(bucket string, deleteKeysPerTx uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	var partialDropDone bool
	for !partialDropDone {
		c := m.tx.Cursor(bucket)
		cnt, err := c.Count()
		if err != nil {
			return err
		}
		if cnt < deleteKeysPerTx {
			return nil
		}
		var deleted uint64
		for k, _, err := c.First(); k != nil; k, _, err = c.First() {
			if err != nil {
				return err
			}
			deleted++
			if deleted > deleteKeysPerTx {
				break
			}

			err = c.DeleteCurrent()
			if err != nil {
				return err
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("ClearBuckets", "bucket", bucket, "records_left", cnt-deleted)
			}
		}

		if err := m.CommitAndBegin(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func (m *TxDb) DropBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Dropping bucket", "name", name)
		migrator, ok := m.tx.(BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.DropBucket(name); err != nil {
			return err
		}
	}
	return nil
}
