package ethdb

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
)

type badgerOpts struct {
	Badger badger.Options
}

func (opts badgerOpts) Path(path string) badgerOpts {
	opts.Badger = opts.Badger.WithDir(path).WithValueDir(path)
	return opts
}

func (opts badgerOpts) InMem() badgerOpts {
	opts.Badger = opts.Badger.WithInMemory(true).WithNumCompactors(0).WithCompression(options.None).WithCompactL0OnClose(false)
	return opts
}

func (opts badgerOpts) ReadOnly() badgerOpts {
	opts.Badger = opts.Badger.WithReadOnly(true)
	return opts
}

func (opts badgerOpts) Open(ctx context.Context) (KV, error) {
	logger := log.New("badger_db", opts.Badger.Dir)

	oldMaxProcs := runtime.GOMAXPROCS(0)
	if oldMaxProcs < minGoMaxProcs {
		runtime.GOMAXPROCS(minGoMaxProcs)
		logger.Info("Bumping GOMAXPROCS", "old", oldMaxProcs, "new", minGoMaxProcs)
	}
	opts.Badger = opts.Badger.WithMaxTableSize(512 << 20)

	db, err := badger.Open(opts.Badger)
	if err != nil {
		return nil, err
	}

	var gcTicker *time.Ticker
	if !opts.Badger.InMemory {
		// Start GC in backround
		go func() {
			gcTicker = time.NewTicker(gcPeriod)
			for range gcTicker.C {
				err := db.RunValueLogGC(0.5)
				if err != badger.ErrNoRewrite {
					logger.Info("Badger GC run", "err", err)
				}
			}
		}()
	}

	return &badgerDB{
		opts:     opts,
		badger:   db,
		log:      logger,
		gcTicker: gcTicker,
	}, nil
}

func (opts badgerOpts) MustOpen(ctx context.Context) KV {
	db, err := opts.Open(ctx)
	if err != nil {
		panic(err)
	}
	return db
}

type badgerDB struct {
	opts     badgerOpts
	badger   *badger.DB
	gcTicker *time.Ticker
	log      log.Logger
}

func NewBadger() badgerOpts {
	return badgerOpts{Badger: badger.DefaultOptions("")}
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *badgerDB) Close() {
	if db.gcTicker != nil {
		db.gcTicker.Stop()
	}
	if err := db.badger.Close(); err != nil {
		db.log.Warn("failed to close badger DB", "err", err)
	} else {
		db.log.Info("badger database closed")
	}
}

func (db *badgerDB) DiskSize(_ context.Context) (common.StorageSize, error) {
	lsm, vlog := db.badger.Size()
	return common.StorageSize(lsm + vlog), nil
}

func (db *badgerDB) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return map[string]common.StorageBucketWriteStats{}, nil
}

func (db *badgerDB) Begin(ctx context.Context, writable bool) (Tx, error) {
	return &badgerTx{
		db:     db,
		ctx:    ctx,
		badger: db.badger.NewTransaction(writable),
	}, nil
}

type badgerTx struct {
	ctx context.Context
	db  *badgerDB

	badger          *badger.Txn
	badgerIterators []*badger.Iterator
}

type badgerBucket struct {
	tx *badgerTx

	prefix  []byte
	nameLen uint
}

type badgerCursor struct {
	ctx    context.Context
	bucket badgerBucket
	prefix []byte

	badgerOpts badger.IteratorOptions

	badger *badger.Iterator

	k   []byte
	v   []byte
	err error
}

func (db *badgerDB) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &badgerTx{db: db, ctx: ctx}
	return db.badger.View(func(tx *badger.Txn) error {
		defer t.cleanup()
		t.badger = tx
		return f(t)
	})
}

func (db *badgerDB) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &badgerTx{db: db, ctx: ctx}
	return db.badger.Update(func(tx *badger.Txn) error {
		defer t.cleanup()
		t.badger = tx
		return f(t)
	})
}

func (tx *badgerTx) Bucket(name []byte) Bucket {
	b := badgerBucket{tx: tx, nameLen: uint(len(name))}
	b.prefix = name
	return b
}

func (tx *badgerTx) Commit(ctx context.Context) error {
	tx.cleanup()
	return tx.badger.Commit()
}

func (tx *badgerTx) Rollback() error {
	tx.cleanup()
	tx.badger.Discard()
	return nil
}

func (tx *badgerTx) cleanup() {
	for _, it := range tx.badgerIterators {
		it.Close()
	}
}

func (c *badgerCursor) Prefix(v []byte) Cursor {
	c.prefix = append(c.prefix[:0], c.bucket.prefix[:c.bucket.nameLen]...)
	c.prefix = append(c.prefix, v...)

	c.badgerOpts.Prefix = append(c.badgerOpts.Prefix[:0], c.prefix...)
	return c
}

func (c *badgerCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *badgerCursor) Prefetch(v uint) Cursor {
	c.badgerOpts.PrefetchSize = int(v)
	return c
}

func (c *badgerCursor) NoValues() NoValuesCursor {
	c.badgerOpts.PrefetchValues = false
	return &badgerNoValuesCursor{badgerCursor: *c}
}

func (b badgerBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	var item *badger.Item
	b.prefix = append(b.prefix[:b.nameLen], key...)
	item, err = b.tx.badger.Get(b.prefix)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if item != nil {
		val, err = item.ValueCopy(nil) // can improve this by using pool
	}
	if val == nil {
		val = []byte{}
	}
	return val, err
}

func (b badgerBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	b.prefix = append(b.prefix[:b.nameLen], key...) // avoid passing buffer in Put, need copy bytes
	return b.tx.badger.Set(common.CopyBytes(b.prefix), value)
}

func (b badgerBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	b.prefix = append(b.prefix[:b.nameLen], key...)
	return b.tx.badger.Delete(b.prefix)
}

func (b badgerBucket) Cursor() Cursor {
	c := &badgerCursor{bucket: b, ctx: b.tx.ctx, badgerOpts: badger.DefaultIteratorOptions}
	c.prefix = append(c.prefix[:0], c.bucket.prefix[:c.bucket.nameLen]...)
	c.badgerOpts.Prefix = append(c.badgerOpts.Prefix[:0], c.prefix...)
	return c
}

func (c *badgerCursor) initCursor() {
	if c.badger != nil {
		return
	}

	c.badger = c.bucket.tx.badger.NewIterator(c.badgerOpts)
	// add to auto-cleanup on end of transactions
	if c.bucket.tx.badgerIterators == nil {
		c.bucket.tx.badgerIterators = make([]*badger.Iterator, 0, 1)
	}
	c.bucket.tx.badgerIterators = append(c.bucket.tx.badgerIterators, c.badger)
}

func (c *badgerCursor) First() ([]byte, []byte, error) {
	c.initCursor()

	c.badger.Rewind()
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, c.v, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	if c.badgerOpts.PrefetchValues {
		c.v, c.err = item.ValueCopy(c.v) // bech show: using .ValueCopy on same buffer has same speed as item.Value()
	}
	if c.err != nil {
		return []byte{}, nil, c.err
	}
	if c.v == nil {
		c.v = []byte{}
	}
	return c.k, c.v, nil
}

func (c *badgerCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.initCursor()

	c.badger.Seek(append(c.prefix[:c.bucket.nameLen], seek...))
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, c.v, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	if c.badgerOpts.PrefetchValues {
		c.v, c.err = item.ValueCopy(c.v)
	}
	if c.err != nil {
		return []byte{}, nil, c.err
	}
	if c.v == nil {
		c.v = []byte{}
	}

	return c.k, c.v, nil
}

func (c *badgerCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *badgerCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err() // on error key should be != nil
	default:
	}

	c.badger.Next()
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, c.v, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	if c.badgerOpts.PrefetchValues {
		c.v, c.err = item.ValueCopy(c.v)
	}
	if c.err != nil {
		return []byte{}, nil, c.err // on error key should be != nil
	}
	if c.v == nil {
		c.v = []byte{}
	}
	return c.k, c.v, nil
}

func (c *badgerCursor) Delete(key []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	c.initCursor()

	return c.bucket.Delete(key)
}

func (c *badgerCursor) Put(key []byte, value []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	c.initCursor()

	return c.bucket.Put(key, value)
}

func (c *badgerCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

type badgerNoValuesCursor struct {
	badgerCursor
}

func (c *badgerNoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
	for k, vSize, err := c.First(); k != nil; k, vSize, err = c.Next() {
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

func (c *badgerNoValuesCursor) First() ([]byte, uint32, error) {
	c.initCursor()
	c.badger.Rewind()
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, 0, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	return c.k, uint32(item.ValueSize()), nil
}

func (c *badgerNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	c.initCursor()

	c.badger.Seek(append(c.prefix[:c.bucket.nameLen], seek...))
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, 0, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]

	return c.k, uint32(item.ValueSize()), nil
}

func (c *badgerNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *badgerNoValuesCursor) Next() ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	c.badger.Next()
	if !c.badger.Valid() {
		c.k, c.v = nil, nil
		return c.k, 0, nil
	}
	item := c.badger.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	return c.k, uint32(item.ValueSize()), nil
}
