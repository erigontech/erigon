package ethdb

import (
	"context"

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
	Remote   *remote.DbOptions
	Bolt     *bolt.Options
	Badger   badger.Options

	path string
}

func Options() Opts {
	return Opts{provider: DefaultProvider}
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

func (opts Opts) Provider(provider DbProvider) Opts {
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

// Example of getting inMemory db:
// ethdb.Open(context.Background(), ethdb.Options().InMemory(true))
// or
// ethdb.Open(context.Background(), ethdb.Options().Provider(ethdb.Badger).InMemory(true))
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
