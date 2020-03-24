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
		defer t.cleanup()
		t.bolt = tx
		return f(t)
	})
}

func (db *BoltKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &boltTx{db: db, ctx: ctx}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		defer t.cleanup()
		t.bolt = tx
		return f(t)
	})
}

func (tx *boltTx) Bucket(name []byte) Bucket {
	b := boltBucket{tx: tx, nameLen: uint(len(name))}
	b.bolt = tx.bolt.Bucket(name)
	return b
}

func (tx *boltTx) cleanup() {
	// nothing to cleanup
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
	c := &boltCursor{bucket: b, ctx: b.tx.ctx}
	// nothing to do
	return c
}

func (c *boltCursor) initCursor() {
	if c.bolt != nil {
		return
	}
	c.bolt = c.bucket.bolt.Cursor()
}

func (c *boltCursor) First() ([]byte, []byte, error) {
	c.initCursor()

	if c.prefix != nil {
		c.k, c.v = c.bolt.Seek(c.prefix)
	} else {
		c.k, c.v = c.bolt.First()
	}
	return c.k, c.v, c.err
}

func (c *boltCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.initCursor()

	c.k, c.v = c.bolt.Seek(seek)
	return c.k, c.v, c.err
}

func (c *boltCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Next()
	if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, nil, nil
	}
	return c.k, c.v, c.err
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

func (c *noValuesBoltCursor) Walk(walker func(k []byte, vSize uint64) (bool, error)) error {
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

func (c *noValuesBoltCursor) First() ([]byte, uint64, error) {
	c.initCursor()

	var vSize uint64
	var v []byte
	if c.prefix != nil {
		c.k, v = c.bolt.Seek(c.prefix)
	} else {
		c.k, v = c.bolt.First()
	}
	vSize = uint64(len(v))
	return c.k, vSize, c.err
}

func (c *noValuesBoltCursor) Seek(seek []byte) ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.initCursor()

	var vSize uint64
	var v []byte
	c.k, v = c.bolt.Seek(seek)
	vSize = uint64(len(v))
	return c.k, vSize, c.err
}

func (c *noValuesBoltCursor) Next() ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	var vSize uint64
	var v []byte
	c.k, v = c.bolt.Next()
	vSize = uint64(len(v))
	if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, 0, nil
	}
	return c.k, vSize, c.err
}
