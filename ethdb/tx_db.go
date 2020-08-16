package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

// TxDb - provides Database interface around ethdb.Tx
// It's not thread-safe!
// It's not usable after .Commit()/.Rollback() call
// you can put unlimited amount of data into this class, call IdealBatchSize is unnecessary
// Walk and MultiWalk methods - work outside of Tx object yet, will implement it later
type TxDb struct {
	db      Database
	Tx      Tx
	cursors map[string]*LmdbCursor
	len     uint64
}

func (m *TxDb) Close() {
	panic("don't call me")
}

func (m *TxDb) Begin() (DbWithPendingMutations, error) {
	batch := &TxDb{db: m.db, cursors: map[string]*LmdbCursor{}}
	if err := batch.begin(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) Put(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	return m.cursors[bucket].Put(key, value)
}

func (m *TxDb) Append(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	return m.cursors[bucket].Append(key, value)
}

func (m *TxDb) Delete(bucket string, key []byte) error {
	m.len += uint64(len(key))
	return m.cursors[bucket].Delete(key)
}

func (m *TxDb) NewBatch() DbWithPendingMutations {
	panic("don't call me")
}

func (m *TxDb) begin() error {
	tx, err := m.db.(HasKV).KV().Begin(context.Background(), nil, true)
	if err != nil {
		return err
	}
	m.Tx = tx
	for i := range dbutils.Buckets {
		m.cursors[dbutils.Buckets[i]] = tx.Cursor(dbutils.Buckets[i]).(*LmdbCursor)
		if err := m.cursors[dbutils.Buckets[i]].initCursor(); err != nil {
			return err
		}
	}
	return nil
}

func (m *TxDb) KV() KV {
	if casted, ok := m.db.(HasKV); ok {
		return casted.KV()
	}
	return nil
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
	v, err := m.cursors[bucket].Get(key)
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
	panic("don't use me")
}

func (m *TxDb) BatchSize() int {
	return int(m.len)
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (m *TxDb) IdealBatchSize() int {
	return int(1 * datasize.GB)
}

func (m *TxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return Walk(m.cursors[bucket], startkey, fixedbits, walker)
}

func Walk(c Cursor, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
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

func (m *TxDb) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	if m.Tx == nil {
		return 0, fmt.Errorf("second call .Commit() on same transaction")
	}
	if err := m.Tx.Commit(context.Background()); err != nil {
		return 0, err
	}
	m.Tx = nil
	m.cursors = nil
	m.len = 0
	return 0, nil
}

func (m *TxDb) Rollback() {
	if m.Tx == nil {
		return
	}
	m.Tx.Rollback()
	m.cursors = nil
	m.Tx = nil
	m.len = 0
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
