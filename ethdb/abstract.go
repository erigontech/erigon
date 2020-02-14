package ethdb

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type DbProvider string

const (
	Bolt   DbProvider = "bolt"
	Badger DbProvider = "badger"
	Remote DbProvider = "remote"
)

const DefaultProvider = Bolt

type Opts struct {
	provider DbProvider
	Remote   remote.DbOptions
	Bolt     *bolt.Options
	Badger   badger.Options

	path string
}

func Options() Opts {
	return ProviderOptions(DefaultProvider)
}

func (opts Opts) Path(path string) Opts {
	opts.path = path
	switch opts.provider {
	case Bolt:
		// nothing to do
	case Badger:
		opts.Badger.WithDir(path).WithValueDir(path)
	case Remote:
		opts.Remote.DialAddress = path
	}
	return opts
}

func (opts Opts) InMemory(val bool) Opts {
	switch opts.provider {
	case Bolt:
		opts.Bolt.MemOnly = val
	case Badger:
		opts.Badger.WithInMemory(val)
	case Remote:
		panic("not supported")
	}
	return opts
}

func ProviderOptions(provider DbProvider) Opts {
	opts := Opts{}
	switch opts.provider {
	case Bolt:
		opts.Badger = badger.DefaultOptions(opts.path)
	case Badger:
		opts.Bolt = bolt.DefaultOptions
	case Remote:
		opts.Remote = remote.DefaultOptions()
	default:
		panic("unknown db provider: " + provider)
	}

	return opts
}

type DB struct {
	opts   Opts
	bolt   *bolt.DB
	badger *badger.DB
	remote *remote.DB
}

func Open(ctx context.Context, opts Opts) (db *DB, err error) {
	db = &DB{opts: opts}

	switch db.opts.provider {
	case Bolt:
		db.bolt, err = bolt.Open(opts.path, 0600, opts.Bolt)
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
func (db *DB) Close() error {
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

type Tx struct {
	db *DB

	bolt   *bolt.Tx
	badger *badger.Txn
	remote *remote.Tx
}

type Bucket struct {
	tx *Tx

	bolt         *bolt.Bucket
	badgerPrefix []byte
	remote       *remote.Bucket
}

type Cursor struct {
	bucket *Bucket

	bolt   *bolt.Cursor
	badger *badger.Iterator
	remote *remote.Cursor
}

func (db *DB) View(ctx context.Context, f func(tx *Tx) error) (err error) {
	t := &Tx{}
	switch db.opts.provider {
	case Bolt:
		return db.bolt.View(func(tx *bolt.Tx) error {
			t.bolt = tx
			return f(t)
		})
	case Badger:
		return db.badger.View(func(tx *badger.Txn) error {
			t.badger = tx
			return f(t)
		})
	case Remote:
		return db.remote.View(ctx, func(tx *remote.Tx) error {
			t.remote = tx
			return f(t)
		})
	}
	return nil
}

func (db *DB) Update(ctx context.Context, f func(tx *Tx) error) (err error) {
	t := &Tx{db: db}
	switch db.opts.provider {
	case Bolt:
		return db.bolt.Update(func(tx *bolt.Tx) error {
			t.bolt = tx
			return f(t)
		})
	case Badger:
		return db.badger.Update(func(tx *badger.Txn) error {
			t.badger = tx
			return f(t)
		})
	case Remote:
		return fmt.Errorf("remote db provider doesn't support .Update method")
	}
	return nil
}

func (tx *Tx) Bucket(name []byte) (b *Bucket, err error) {
	b = &Bucket{tx: tx}
	switch tx.db.opts.provider {
	case Bolt:
		b.bolt = tx.bolt.Bucket(name)
	case Badger:
		b.badgerPrefix = name
	case Remote:
		b.remote, err = tx.remote.Bucket(name)
	}
	return nil, nil
}

func (b *Bucket) CursorOptions() CursorOpts {
	c := CursorOpts{}
	switch b.tx.db.opts.provider {
	case Bolt:
		// nothing to do
	case Badger:
		opts := badger.DefaultIteratorOptions
		opts.Prefix = b.badgerPrefix
		c.badger = opts
	case Remote:
		c.remote = remote.DefaultCursorOpts
	}

	return c
}

func (b *Bucket) Cursor(opts CursorOpts) (c *Cursor, err error) {
	c = &Cursor{bucket: b}
	switch c.bucket.tx.db.opts.provider {
	case Bolt:
		c.bolt = b.bolt.Cursor()
	case Badger:
		opts.badger.Prefix = b.badgerPrefix
		c.badger = b.tx.badger.NewIterator(opts.badger)
	case Remote:
		c.remote, err = b.remote.Cursor(opts.remote)
	}
	return c, nil
}

type CursorOpts struct {
	provider DbProvider

	remote remote.CursorOpts
	badger badger.IteratorOptions

	path string
}

func (opts CursorOpts) PrefetchSize(v uint) CursorOpts {
	switch opts.provider {
	case Bolt:
		// nothing to do
	case Badger:
		opts.badger.PrefetchSize = int(v)
	case Remote:
		opts.remote.PrefetchSize(uint64(v))
	}
	return opts
}
