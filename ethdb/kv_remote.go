package ethdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type remoteDB struct {
	opts   Options
	remote *remote.DB
}

func (db *remoteDB) Options() Options {
	return db.opts
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *remoteDB) Close() error {
	return db.remote.Close()
}

type remoteTx struct {
	ctx context.Context
	db  *remoteDB

	remote *remote.Tx
}

type remoteBucket struct {
	tx *remoteTx

	nameLen uint
	remote  *remote.Bucket
}

type remoteCursor struct {
	ctx    context.Context
	bucket remoteBucket
	prefix []byte

	remoteOpts remote.CursorOpts

	remote *remote.Cursor

	k   []byte
	v   []byte
	err error
}

func (db *remoteDB) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &remoteTx{db: db, ctx: ctx}
	return db.remote.View(ctx, func(tx *remote.Tx) error {
		t.remote = tx
		return f(t)
	})
}

func (db *remoteDB) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) Bucket(name []byte) Bucket {
	b := remoteBucket{tx: tx, nameLen: uint(len(name))}
	b.remote = tx.remote.Bucket(name)
	return b
}

func (tx *remoteTx) cleanup() {
	// nothing to cleanup
}

func (c *remoteCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *remoteCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *remoteCursor) Prefetch(v uint) Cursor {
	c.remoteOpts.PrefetchSize(uint64(v))
	return c
}

func (c *remoteCursor) NoValues() NoValuesCursor {
	c.remoteOpts.PrefetchValues(false)
	return &remoteNoValuesCursor{remoteCursor: *c}
}

func (b remoteBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	val, err = b.remote.Get(key)
	return val, err
}

func (b remoteBucket) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (b remoteBucket) Delete(key []byte) error {
	panic("not supported")
}

func (b remoteBucket) Cursor() Cursor {
	c := &remoteCursor{bucket: b, ctx: b.tx.ctx}
	c.remoteOpts = remote.DefaultCursorOpts
	return c
}

func (c *remoteCursor) initCursor() {
	if c.remote != nil {
		return
	}
	c.remote = c.bucket.remote.Cursor(c.remoteOpts)
}

func (c *remoteCursor) First() ([]byte, []byte, error) {
	c.initCursor()

	if c.prefix != nil {
		c.k, c.v, c.err = c.remote.Seek(c.prefix)
	} else {
		c.k, c.v, c.err = c.remote.First()
	}
	return c.k, c.v, c.err
}

func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.initCursor()

	c.k, c.v, c.err = c.remote.Seek(seek)
	return c.k, c.v, c.err
}

func (c *remoteCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.k, c.v, c.err = c.remote.Next()
	if c.err != nil {
		return nil, nil, c.err
	}

	if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, nil, nil
	}
	return c.k, c.v, c.err
}

func (c *remoteCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

type remoteNoValuesCursor struct {
	remoteCursor
}

func (c *remoteNoValuesCursor) Walk(walker func(k []byte, vSize uint64) (bool, error)) error {
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

func (c *remoteNoValuesCursor) First() ([]byte, uint64, error) {
	c.initCursor()

	var vSize uint64
	var vIsEmpty bool
	if c.prefix != nil {
		c.k, vIsEmpty, c.err = c.remote.SeekKey(c.prefix)
	} else {
		c.k, vIsEmpty, c.err = c.remote.FirstKey()
	}
	if !vIsEmpty {
		vSize = 1
	}
	return c.k, vSize, c.err
}

func (c *remoteNoValuesCursor) Seek(seek []byte) ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.initCursor()

	var vSize uint64
	var vIsEmpty bool
	c.k, vIsEmpty, c.err = c.remote.SeekKey(seek)
	if !vIsEmpty {
		vSize = 1
	}
	return c.k, vSize, c.err
}

func (c *remoteNoValuesCursor) Next() ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	var vSize uint64
	var vIsEmpty bool
	c.k, vIsEmpty, c.err = c.remote.NextKey()
	if !vIsEmpty {
		vSize = 1
	}
	if c.err != nil {
		return nil, 0, c.err
	}
	if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, 0, nil
	}
	return c.k, vSize, c.err
}
