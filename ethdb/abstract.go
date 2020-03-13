package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger/v2"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type DB interface {
	View(ctx context.Context, f func(tx Tx) error) (err error)
	Update(ctx context.Context, f func(tx Tx) error) (err error)
	Close() error
}

type Tx interface {
	Bucket(name []byte) Bucket
}

type Bucket interface {
	Get(key []byte) (val []byte, err error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Cursor() Cursor
}

type Cursor interface {
	Prefix(v []byte) Cursor
	MatchBits(uint) Cursor
	Prefetch(v uint) Cursor
	NoValues() NoValuesCursor

	First() ([]byte, []byte, error)
	Seek(seek []byte) ([]byte, []byte, error)
	Next() ([]byte, []byte, error)
	Walk(walker func(k, v []byte) (bool, error)) error
}

type NoValuesCursor interface {
	First() ([]byte, uint64, error)
	Seek(seek []byte) ([]byte, uint64, error)
	Next() ([]byte, uint64, error)
	Walk(walker func(k []byte, vSize uint64) (bool, error)) error
}

type DbProvider uint8

const (
	Bolt DbProvider = iota
	Badger
	Remote
)

const DefaultProvider = Bolt

type Options struct {
	provider DbProvider

	Remote remote.DbOpts
	Bolt   *bolt.Options
	Badger badger.Options

	path string
}

func NewBolt() Options {
	opts := Options{provider: Bolt, Bolt: bolt.DefaultOptions}
	return opts
}

func NewBadger() Options {
	opts := Options{provider: Badger, Badger: badger.DefaultOptions("")}
	return opts
}

func NewRemote() Options {
	opts := Options{provider: Badger, Remote: remote.DefaultOpts}
	return opts
}

func NewMemDb() Options {
	return Opts().InMem(true)
}

func ProviderOpts(provider DbProvider) Options {
	switch provider {
	case Bolt:
		return NewBolt()
	case Badger:
		return NewBadger()
	case Remote:
		return NewRemote()
	default:
		panic("unknown db provider: " + strconv.Itoa(int(provider)))
	}
}

func Opts() Options {
	return ProviderOpts(DefaultProvider)
}

func (opts Options) Path(path string) Options {
	opts.path = path
	switch opts.provider {
	case Bolt:
		// nothing to do
	case Badger:
		opts.Badger = opts.Badger.WithDir(path).WithValueDir(path)
	case Remote:
		opts.Remote = opts.Remote.Addr(path)
	}
	return opts
}

func (opts Options) InMem(val bool) Options {
	switch opts.provider {
	case Bolt:
		opts.Bolt.MemOnly = val
	case Badger:
		opts.Badger = opts.Badger.WithInMemory(val)
	case Remote:
		panic("not supported")
	}
	return opts
}

type allDB struct {
	opts   Options
	bolt   *bolt.DB
	badger *badger.DB
	remote *remote.DB
}

var buckets = [][]byte{
	dbutils.IntermediateTrieHashBucket,
	dbutils.AccountsBucket,
}

func (opts Options) Open(ctx context.Context) (db *allDB, err error) {
	return Open(ctx, opts)
}

func Open(ctx context.Context, opts Options) (db *allDB, err error) {
	db = &allDB{opts: opts}

	switch db.opts.provider {
	case Bolt:
		db.bolt, err = bolt.Open(opts.path, 0600, opts.Bolt)
		if err != nil {
			return nil, err
		}
		err = db.bolt.Update(func(tx *bolt.Tx) error {
			for _, name := range buckets {
				_, createErr := tx.CreateBucketIfNotExists(name, false)
				if createErr != nil {
					return createErr
				}
			}
			return nil
		})
	case Badger:
		db.badger, err = badger.Open(opts.Badger)
	case Remote:
		db.remote, err = remote.Open(ctx, opts.Remote)
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes DB
// All transactions must be closed before closing the database.
func (db *allDB) Close() error {
	switch db.opts.provider {
	case Bolt:
		return db.bolt.Close()
	case Badger:
		return db.badger.Close()
	case Remote:
		return db.remote.Close()
	}
	return nil
}

type tx struct {
	ctx context.Context
	db  *allDB

	bolt   *bolt.Tx
	badger *badger.Txn
	remote *remote.Tx

	badgerIterators []*badger.Iterator
}

type bucket struct {
	tx *tx

	bolt         *bolt.Bucket
	badgerPrefix []byte
	nameLen      uint
	remote       *remote.Bucket
}

type cursor struct {
	ctx      context.Context
	bucket   bucket
	provider DbProvider
	prefix   []byte

	remoteOpts remote.CursorOpts
	badgerOpts badger.IteratorOptions

	bolt   *bolt.Cursor
	badger *badger.Iterator
	remote *remote.Cursor

	k   []byte
	v   []byte
	err error
}

func (db *allDB) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &tx{db: db, ctx: ctx}
	switch db.opts.provider {
	case Bolt:
		return db.bolt.View(func(tx *bolt.Tx) error {
			defer t.cleanup()
			t.bolt = tx
			return f(t)
		})
	case Badger:
		return db.badger.View(func(tx *badger.Txn) error {
			defer t.cleanup()
			t.badger = tx
			return f(t)
		})
	case Remote:
		return db.remote.View(ctx, func(tx *remote.Tx) error {
			t.remote = tx
			return f(t)
		})
	}
	return err
}

func (db *allDB) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &tx{db: db, ctx: ctx}
	switch db.opts.provider {
	case Bolt:
		return db.bolt.Update(func(tx *bolt.Tx) error {
			defer t.cleanup()
			t.bolt = tx
			return f(t)
		})
	case Badger:
		return db.badger.Update(func(tx *badger.Txn) error {
			defer t.cleanup()
			t.badger = tx
			return f(t)
		})
	case Remote:
		return fmt.Errorf("remote db provider doesn't support .Update method")
	}
	return err
}

func (tx *tx) Bucket(name []byte) Bucket {
	b := bucket{tx: tx, nameLen: uint(len(name))}
	switch tx.db.opts.provider {
	case Bolt:
		b.bolt = tx.bolt.Bucket(name)
	case Badger:
		b.badgerPrefix = name
	case Remote:
		b.remote = tx.remote.Bucket(name)
	}
	return b
}

func (tx *tx) cleanup() {
	switch tx.db.opts.provider {
	case Bolt:
		// nothing to cleanup
	case Badger:
		for _, it := range tx.badgerIterators {
			it.Close()
		}
	case Remote:
		// nothing to cleanup
	}
}

func (c *cursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *cursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *cursor) Prefetch(v uint) Cursor {
	switch c.provider {
	case Bolt:
		// nothing to do
	case Badger:
		c.badgerOpts.PrefetchSize = int(v)
	case Remote:
		c.remoteOpts.PrefetchSize(uint64(v))
	}
	return c
}

func (c *cursor) NoValues() NoValuesCursor {
	return newNoValuesCursor(c)
}

func (b bucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	switch b.tx.db.opts.provider {
	case Bolt:
		val, _ = b.bolt.Get(key)
	case Badger:
		var item *badger.Item
		b.badgerPrefix = append(b.badgerPrefix[:b.nameLen], key...)
		item, err = b.tx.badger.Get(b.badgerPrefix)
		if item != nil {
			val, err = item.ValueCopy(nil) // can improve this by using pool
		}
	case Remote:
		val, err = b.remote.Get(key)
	}
	return val, err
}

func (b bucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	switch b.tx.db.opts.provider {
	case Bolt:
		return b.bolt.Put(key, value)
	case Badger:
		b.badgerPrefix = append(b.badgerPrefix[:b.nameLen], key...)
		return b.tx.badger.Set(b.badgerPrefix, value)
	case Remote:
		panic("not supported")
	}
	return nil
}

func (b bucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	switch b.tx.db.opts.provider {
	case Bolt:
		return b.bolt.Delete(key)
	case Badger:
		b.badgerPrefix = append(b.badgerPrefix[:b.nameLen], key...)
		return b.tx.badger.Delete(b.badgerPrefix)
	case Remote:
		panic("not supported")
	}
	return nil
}

func (b bucket) Cursor() Cursor {
	c := &cursor{bucket: b, ctx: b.tx.ctx, provider: b.tx.db.opts.provider}
	switch c.provider {
	case Bolt:
		// nothing to do
	case Badger:
		c.badgerOpts = badger.DefaultIteratorOptions
		b.badgerPrefix = append(b.badgerPrefix[:b.nameLen], c.prefix...) // set bucket
		c.badgerOpts.Prefix = b.badgerPrefix                             // set bucket
	case Remote:
		c.remoteOpts = remote.DefaultCursorOpts
	}
	return c
}

func (c *cursor) initCursor() {
	switch c.provider {
	case Bolt:
		if c.bolt != nil {
			return
		}
		c.bolt = c.bucket.bolt.Cursor()
	case Badger:
		if c.badger != nil {
			return
		}
		c.badger = c.bucket.tx.badger.NewIterator(c.badgerOpts)
		// add to auto-cleanup on end of transactions
		if c.bucket.tx.badgerIterators == nil {
			c.bucket.tx.badgerIterators = make([]*badger.Iterator, 0, 1)
		}
		c.bucket.tx.badgerIterators = append(c.bucket.tx.badgerIterators, c.badger)
	case Remote:
		if c.remote != nil {
			return
		}
		c.remote = c.bucket.remote.Cursor(c.remoteOpts)
	}
}

func (c *cursor) First() ([]byte, []byte, error) {
	c.initCursor()

	switch c.provider {
	case Bolt:
		if c.prefix != nil {
			c.k, c.v = c.bolt.Seek(c.prefix)
		} else {
			c.k, c.v = c.bolt.First()
		}
	case Badger:
		c.badger.Rewind()
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		if c.badgerOpts.PrefetchValues {
			c.v, c.err = item.ValueCopy(c.v) // bech show: using .ValueCopy on same buffer has same speed as item.Value()
		}
	case Remote:
		if c.prefix != nil {
			c.k, c.v, c.err = c.remote.Seek(c.prefix)
		} else {
			c.k, c.v, c.err = c.remote.First()
		}
	}
	return c.k, c.v, c.err
}

func (c *cursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.initCursor()

	switch c.provider {
	case Bolt:
		c.k, c.v = c.bolt.Seek(seek)
	case Badger:
		c.bucket.badgerPrefix = append(c.bucket.badgerPrefix[:c.bucket.nameLen], seek...)
		c.badger.Seek(c.bucket.badgerPrefix)
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		if c.badgerOpts.PrefetchValues {
			c.v, c.err = item.ValueCopy(c.v)
		}
	case Remote:
		c.k, c.v, c.err = c.remote.Seek(seek)
	}
	return c.k, c.v, c.err
}

func (c *cursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	switch c.provider {
	case Bolt:
		c.k, c.v = c.bolt.Next()
		if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
			return nil, nil, nil
		}
	case Badger:
		c.badger.Next()
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		if c.badgerOpts.PrefetchValues {
			c.v, c.err = item.ValueCopy(c.v)
		}
	case Remote:
		c.k, c.v, c.err = c.remote.Next()
		if c.err != nil {
			return nil, nil, c.err
		}

		if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
			return nil, nil, nil
		}
	}
	return c.k, c.v, c.err
}

func (c *cursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

type noValuesCursor struct {
	cursor
}

func newNoValuesCursor(c *cursor) *noValuesCursor {
	switch c.provider {
	case Bolt:
		// nothing to do
	case Badger:
		c.badgerOpts.PrefetchValues = false
	case Remote:
		c.remoteOpts.PrefetchValues(false)
	}
	return &noValuesCursor{cursor: *c}
}

func (c *noValuesCursor) Walk(walker func(k []byte, vSize uint64) (bool, error)) error {
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

func (c *noValuesCursor) First() ([]byte, uint64, error) {
	c.initCursor()

	var vSize uint64
	switch c.provider {
	case Bolt:
		var v []byte
		if c.prefix != nil {
			c.k, v = c.bolt.Seek(c.prefix)
		} else {
			c.k, v = c.bolt.First()
		}
		vSize = uint64(len(v))
	case Badger:
		c.badger.Rewind()
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		vSize = uint64(item.ValueSize())
	case Remote:
		var vIsEmpty bool
		if c.prefix != nil {
			c.k, vIsEmpty, c.err = c.remote.SeekKey(c.prefix)
		} else {
			c.k, vIsEmpty, c.err = c.remote.FirstKey()
		}
		if !vIsEmpty {
			vSize = 1
		}
	}
	return c.k, vSize, c.err
}

func (c *noValuesCursor) Seek(seek []byte) ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.initCursor()

	var vSize uint64
	switch c.provider {
	case Bolt:
		var v []byte
		c.k, v = c.bolt.Seek(seek)
		vSize = uint64(len(v))
	case Badger:
		c.bucket.badgerPrefix = append(c.bucket.badgerPrefix[:c.bucket.nameLen], seek...)
		c.badger.Seek(c.bucket.badgerPrefix)
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		vSize = uint64(item.ValueSize())
	case Remote:
		var vIsEmpty bool
		c.k, vIsEmpty, c.err = c.remote.SeekKey(seek)
		if !vIsEmpty {
			vSize = 1
		}
	}
	return c.k, vSize, c.err
}

func (c *noValuesCursor) Next() ([]byte, uint64, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	var vSize uint64
	switch c.provider {
	case Bolt:
		var v []byte
		c.k, v = c.bolt.Next()
		vSize = uint64(len(v))
		if c.prefix != nil && !bytes.HasPrefix(c.k, c.prefix) {
			return nil, 0, nil
		}
	case Badger:
		c.badger.Next()
		if !c.badger.Valid() {
			c.k = nil
			break
		}
		item := c.badger.Item()
		c.k = item.Key()[c.bucket.nameLen:]
		vSize = uint64(item.ValueSize())
	case Remote:
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
	}
	return c.k, vSize, c.err
}
