package olddb

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
)

// TxDb - provides Database interface around ethdb.Tx
// It's not thread-safe!
// TxDb not usable after .Commit()/.Rollback() call, but usable after .CommitAndBegin() call
// you can put unlimited amount of data into this class
// Walk and MultiWalk methods - work outside of Tx object yet, will implement it later
// Deprecated
// nolint
type TxDb struct {
	db      ethdb.Database
	tx      kv.Tx
	cursors map[string]kv.Cursor
	txFlags ethdb.TxFlags
	len     uint64
}

// nolint
func WrapIntoTxDB(tx kv.RwTx) *TxDb {
	return &TxDb{tx: tx, cursors: map[string]kv.Cursor{}}
}

func (m *TxDb) Close() {
	panic("don't call me")
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

func (m *TxDb) cursor(bucket string) (kv.Cursor, error) {
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
	return m.tx.(kv.RwTx).IncrementSequence(bucket, amount)
}

func (m *TxDb) ReadSequence(bucket string) (res uint64, err error) {
	return m.tx.ReadSequence(bucket)
}

func (m *TxDb) Put(table string, k, v []byte) error {
	m.len += uint64(len(k) + len(v))
	c, err := m.cursor(table)
	if err != nil {
		return err
	}
	return c.(kv.RwCursor).Put(k, v)
}

func (m *TxDb) Append(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(kv.RwCursor).Append(key, value)
}

func (m *TxDb) AppendDup(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(kv.RwCursorDupSort).AppendDup(key, value)
}

func (m *TxDb) Delete(table string, k []byte) error {
	m.len += uint64(len(k))
	c, err := m.cursor(table)
	if err != nil {
		return err
	}
	return c.(kv.RwCursor).Delete(k)
}

func (m *TxDb) begin(ctx context.Context, flags ethdb.TxFlags) error {
	db := m.db.(ethdb.HasRwKV).RwKV()

	var tx kv.Tx
	var err error
	if flags&ethdb.RO != 0 {
		tx, err = db.BeginRo(ctx)
	} else {
		tx, err = db.BeginRw(ctx)
	}
	if err != nil {
		return err
	}
	m.tx = tx
	m.cursors = make(map[string]kv.Cursor, 16)
	return nil
}

func (m *TxDb) RwKV() kv.RwDB {
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

func (m *TxDb) BatchSize() int {
	return int(m.len)
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

func (m *TxDb) Tx() kv.Tx {
	return m.tx
}

func (m *TxDb) BucketExists(name string) (bool, error) {
	migrator, ok := m.tx.(kv.BucketMigrator)
	if !ok {
		return false, fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
	}
	return migrator.ExistsBucket(name)
}

func (m *TxDb) ClearBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]

		migrator, ok := m.tx.(kv.BucketMigrator)
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
		migrator, ok := m.tx.(kv.BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.DropBucket(name); err != nil {
			return err
		}
	}
	return nil
}
