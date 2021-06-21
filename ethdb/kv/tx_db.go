package kv

import (
	"context"
	"fmt"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

// Implements ethdb.Getter for Tx
type roTxDb struct {
	tx  ethdb.Tx
	top bool
}

func NewRoTxDb(tx ethdb.Tx) *roTxDb {
	return &roTxDb{tx: tx, top: true}
}

func (m *roTxDb) GetOne(bucket string, key []byte) ([]byte, error) {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	_, v, err := c.SeekExact(key)
	return v, err
}

func (m *roTxDb) Get(bucket string, key []byte) ([]byte, error) {
	dat, err := m.GetOne(bucket, key)
	return ethdb.GetOneWrapper(dat, err)
}

func (m *roTxDb) Has(bucket string, key []byte) (bool, error) {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return false, err
	}
	defer c.Close()
	_, v, err := c.SeekExact(key)

	return v != nil, err
}

func (m *roTxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return ethdb.Walk(c, startkey, fixedbits, walker)
}

func (m *roTxDb) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return m.tx.ForEach(bucket, fromPrefix, walker)
}

func (m *roTxDb) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	return m.tx.ForPrefix(bucket, prefix, walker)
}
func (m *roTxDb) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return m.tx.ForAmount(bucket, prefix, amount, walker)
}

func (m *roTxDb) BeginGetter(ctx context.Context) (ethdb.GetterTx, error) {
	return &roTxDb{tx: m.tx, top: false}, nil
}

func (m *roTxDb) Rollback() {
	if m.top {
		m.tx.Rollback()
	}
}

func (m *roTxDb) Tx() ethdb.Tx {
	return m.tx
}

func NewRwTxDb(tx ethdb.Tx) *TxDb {
	return &TxDb{tx: tx, cursors: map[string]ethdb.Cursor{}}
}

// TxDb - provides Database interface around ethdb.Tx
// It's not thread-safe!
// TxDb not usable after .Commit()/.Rollback() call, but usable after .CommitAndBegin() call
// you can put unlimited amount of data into this class
// Walk and MultiWalk methods - work outside of Tx object yet, will implement it later
type TxDb struct {
	db      ethdb.Database
	tx      ethdb.Tx
	cursors map[string]ethdb.Cursor
	txFlags ethdb.TxFlags
	len     uint64
}

func WrapIntoTxDB(tx ethdb.RwTx) *TxDb {
	return &TxDb{tx: tx, cursors: map[string]ethdb.Cursor{}}
}

func (m *TxDb) Close() {
	panic("don't call me")
}

// NewTxDbWithoutTransaction creates TxDb object without opening transaction,
// such TxDb not usable before .Begin() call on it
// It allows inject TxDb object into class hierarchy, but open write transaction later
func NewTxDbWithoutTransaction(db ethdb.Database, flags ethdb.TxFlags) ethdb.DbWithPendingMutations {
	return &TxDb{db: db, txFlags: flags}
}

func (m *TxDb) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	batch := m
	if m.tx != nil {
		panic("nested transactions not supported")
	}

	if err := batch.begin(ctx, flags); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) BeginGetter(ctx context.Context) (ethdb.GetterTx, error) {
	batch := m
	if m.tx != nil {
		panic("nested transactions not supported")
	}

	if err := batch.begin(ctx, ethdb.RO); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) cursor(bucket string) (ethdb.Cursor, error) {
	c, ok := m.cursors[bucket]
	if !ok {
		var err error
		c, err = m.tx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		m.cursors[bucket] = c
	}
	return c, nil
}

func (m *TxDb) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	return m.tx.(ethdb.RwTx).IncrementSequence(bucket, amount)
}

func (m *TxDb) ReadSequence(bucket string) (res uint64, err error) {
	return m.tx.ReadSequence(bucket)
}

func (m *TxDb) Put(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(ethdb.RwCursor).Put(key, value)
}

func (m *TxDb) Append(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(ethdb.RwCursor).Append(key, value)
}

func (m *TxDb) AppendDup(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(ethdb.RwCursorDupSort).AppendDup(key, value)
}

func (m *TxDb) Delete(bucket string, k, v []byte) error {
	m.len += uint64(len(k))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(ethdb.RwCursor).Delete(k, v)
}

func (m *TxDb) NewBatch() ethdb.DbWithPendingMutations {
	return &mutation{
		db:   m,
		puts: btree.New(32),
	}
}

func (m *TxDb) begin(ctx context.Context, flags ethdb.TxFlags) error {
	kv := m.db.(ethdb.HasRwKV).RwKV()

	var tx ethdb.Tx
	var err error
	if flags&ethdb.RO != 0 {
		tx, err = kv.BeginRo(ctx)
	} else {
		tx, err = kv.BeginRw(ctx)
	}
	if err != nil {
		return err
	}
	m.tx = tx
	m.cursors = make(map[string]ethdb.Cursor, 16)
	return nil
}

func (m *TxDb) RwKV() ethdb.RwKV {
	panic("not allowed to get KV interface because you will loose transaction, please use .Tx() method")
}

// Last can only be called from the transaction thread
func (m *TxDb) Last(bucket string) ([]byte, []byte, error) {
	c, err := m.cursor(bucket)
	if err != nil {
		return []byte{}, nil, err
	}
	return c.Last()
}

func (m *TxDb) GetOne(bucket string, key []byte) ([]byte, error) {
	c, err := m.cursor(bucket)
	if err != nil {
		return nil, err
	}
	_, v, err := c.SeekExact(key)
	return v, err
}

func (m *TxDb) Get(bucket string, key []byte) ([]byte, error) {
	dat, err := m.GetOne(bucket, key)
	return ethdb.GetOneWrapper(dat, err)
}

func (m *TxDb) Has(bucket string, key []byte) (bool, error) {
	v, err := m.Get(bucket, key)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (m *TxDb) MultiPut(tuples ...[]byte) (uint64, error) {
	return 0, ethdb.MultiPut(m.tx.(ethdb.RwTx), tuples...)
}

func (m *TxDb) BatchSize() int {
	return int(m.len)
}

func (m *TxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	// get cursor out of pool, then calls txDb.Put/Get/Delete on same bucket inside Walk callback - will not affect state of Walk
	c, ok := m.cursors[bucket]
	if ok {
		delete(m.cursors, bucket)
	} else {
		var err error
		c, err = m.tx.Cursor(bucket)
		if err != nil {
			return err
		}
	}
	defer func() { // put cursor back to pool if can
		if _, ok = m.cursors[bucket]; ok {
			c.Close()
		} else {
			m.cursors[bucket] = c
		}
	}()
	return ethdb.Walk(c, startkey, fixedbits, walker)
}
func (m *TxDb) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return m.tx.ForEach(bucket, fromPrefix, walker)
}

func (m *TxDb) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	return m.tx.ForPrefix(bucket, prefix, walker)
}

func (m *TxDb) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return m.tx.ForAmount(bucket, prefix, amount, walker)
}

func (m *TxDb) CommitAndBegin(ctx context.Context) error {
	err := m.Commit()
	if err != nil {
		return err
	}

	return m.begin(ctx, m.txFlags)
}

func (m *TxDb) RollbackAndBegin(ctx context.Context) error {
	m.Rollback()
	return m.begin(ctx, m.txFlags)
}

func (m *TxDb) Commit() error {
	if m.tx == nil {
		return fmt.Errorf("second call .Commit() on same transaction")
	}
	if err := m.tx.Commit(); err != nil {
		return err
	}
	m.tx = nil
	m.cursors = nil
	m.len = 0
	return nil
}

func (m *TxDb) Rollback() {
	if m.tx == nil {
		return
	}
	m.tx.Rollback()
	m.cursors = nil
	m.tx = nil
	m.len = 0
}

func (m *TxDb) Tx() ethdb.Tx {
	return m.tx
}

func (m *TxDb) Keys() ([][]byte, error) {
	panic("don't use me")
}

func (m *TxDb) BucketExists(name string) (bool, error) {
	exists := false
	migrator, ok := m.tx.(ethdb.BucketMigrator)
	if !ok {
		return false, fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
	}
	exists = migrator.ExistsBucket(name)
	return exists, nil
}

func (m *TxDb) ClearBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]

		migrator, ok := m.tx.(ethdb.BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.ClearBucket(name); err != nil {
			return err
		}
	}

	return nil
}

func (m *TxDb) DropBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Dropping bucket", "name", name)
		migrator, ok := m.tx.(ethdb.BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.DropBucket(name); err != nil {
			return err
		}
	}
	return nil
}
