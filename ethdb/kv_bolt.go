package ethdb

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/bolt"
)

type BoltKV struct {
	opts Options
	bolt *bolt.DB
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *BoltKV) Close() error {
	return db.bolt.Close()
}

func (db *BoltKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	var err error
	t := &boltTx{db: db, ctx: ctx}
	t.bolt, err = db.bolt.Begin(writable)
	return t, err
}

func (db *BoltKV) Options() Options {
	return db.opts
}

type boltTx struct {
	ctx context.Context
	db  *BoltKV

	bolt *bolt.Tx
}

type boltBucket struct {
	tx *boltTx

	bolt    *bolt.Bucket
	nameLen uint
}

type boltCursor struct {
	ctx    context.Context
	bucket boltBucket
	prefix []byte

	bolt *bolt.Cursor

	k   []byte
	v   []byte
	err error
}

func (db *BoltKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &boltTx{db: db, ctx: ctx}
	return db.bolt.View(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (db *BoltKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &boltTx{db: db, ctx: ctx}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (tx *boltTx) Commit(ctx context.Context) error {
	return tx.bolt.Commit()
}

func (tx *boltTx) Rollback() error {
	return tx.bolt.Rollback()
}

func (tx *boltTx) Yield() {
	tx.bolt.Yield()
}

func (tx *boltTx) Bucket(name []byte) Bucket {
	b := boltBucket{tx: tx, nameLen: uint(len(name))}
	b.bolt = tx.bolt.Bucket(name)
	return b
}

func (c *boltCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *boltCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *boltCursor) Prefetch(v uint) Cursor {
	// nothing to do
	return c
}

func (c *boltCursor) NoValues() NoValuesCursor {
	return &noValuesBoltCursor{boltCursor: *c}
}

func (b boltBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	val, _ = b.bolt.Get(key)
	return val, err
}

func (b boltBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	return b.bolt.Put(key, value)
}

func (b boltBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	return b.bolt.Delete(key)
}

func (b boltBucket) Cursor() Cursor {
	return &boltCursor{bucket: b, ctx: b.tx.ctx, bolt: b.bolt.Cursor()}
}

func (c *boltCursor) initCursor() {
	if c.bolt != nil {
		return
	}
	c.bolt = c.bucket.bolt.Cursor()
}

func (c *boltCursor) First() ([]byte, []byte, error) {
	if len(c.prefix) == 0 {
		c.k, c.v = c.bolt.First()
		return c.k, c.v, nil
	}

	c.k, c.v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Seek(seek)
	if len(c.prefix) != 0 && !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Next()
	if len(c.prefix) != 0 && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Walk(walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
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

type noValuesBoltCursor struct {
	boltCursor
}

func (c *noValuesBoltCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
	for k, vSize, err := c.First(); k != nil || err != nil; k, vSize, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, vSize)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func (c *noValuesBoltCursor) First() ([]byte, uint32, error) {
	if len(c.prefix) == 0 {
		c.k, c.v = c.bolt.First()
		return c.k, uint32(len(c.v)), nil
	}

	c.k, c.v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, uint32(len(c.v)), nil
}

func (c *noValuesBoltCursor) Seek(seek []byte) ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Seek(seek)
	if len(c.prefix) != 0 && !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, uint32(len(c.v)), nil
}

func (c *noValuesBoltCursor) Next() ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Next()
	if len(c.prefix) != 0 && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, 0, nil
	}
	return c.k, uint32(len(c.v)), nil
}
