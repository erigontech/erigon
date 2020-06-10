package ethdb

import (
	"bytes"
	"context"
	"os"
	"sync"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	boltTxPool     = sync.Pool{New: func() interface{} { return &boltTx{} }}
	boltCursorPool = sync.Pool{New: func() interface{} { return &boltCursor{} }}
)

type boltOpts struct {
	Bolt *bolt.Options
	path string
}

type BoltKV struct {
	opts boltOpts
	bolt *bolt.DB
	log  log.Logger
}

type boltTx struct {
	ctx     context.Context
	db      *BoltKV
	bolt    *bolt.Tx
	cursors []*boltCursor
}

type boltBucket struct {
	tx      *boltTx
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

type noValuesBoltCursor struct {
	*boltCursor
}

func (opts boltOpts) InMem() boltOpts {
	opts.Bolt.MemOnly = true
	return opts
}

func (opts boltOpts) ReadOnly() boltOpts {
	opts.Bolt.ReadOnly = true
	return opts
}

func (opts boltOpts) Path(path string) boltOpts {
	opts.path = path
	return opts
}

// WrapBoltDB provides a way for the code to gradually migrate
// to the abstract interface
func (opts boltOpts) WrapBoltDB(boltDB *bolt.DB) (KV, error) {
	if !opts.Bolt.ReadOnly {
		if err := boltDB.Update(func(tx *bolt.Tx) error {
			for _, name := range dbutils.Buckets {
				_, createErr := tx.CreateBucketIfNotExists(name, false)
				if createErr != nil {
					return createErr
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return &BoltKV{
		opts: opts,
		bolt: boltDB,
		log:  log.New("bolt_db", opts.path),
	}, nil
}

func (opts boltOpts) Open(ctx context.Context) (db KV, err error) {
	boltDB, err := bolt.Open(opts.path, 0600, opts.Bolt)
	if err != nil {
		return nil, err
	}
	return opts.WrapBoltDB(boltDB)
}

func (opts boltOpts) MustOpen(ctx context.Context) KV {
	db, err := opts.Open(ctx)
	if err != nil {
		panic(err)
	}
	return db
}

func NewBolt() boltOpts {
	o := boltOpts{Bolt: bolt.DefaultOptions}
	o.Bolt.KeysPrefixCompressionDisable = true
	return o
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *BoltKV) Close() {
	if db.bolt != nil {
		if err := db.bolt.Close(); err != nil {
			db.log.Warn("failed to close bolt DB", "err", err)
		} else {
			db.log.Info("bolt database closed")
		}
	}
}

func (db *BoltKV) DiskSize(_ context.Context) (common.StorageSize, error) {
	return common.StorageSize(db.bolt.Size()), nil
}

func (db *BoltKV) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	res := map[string]common.StorageBucketWriteStats{}
	for name, stats := range db.bolt.WriteStats() {
		res[name] = common.StorageBucketWriteStats{
			KeyN:             common.StorageCounter(stats.KeyN),
			KeyBytesN:        common.StorageSize(stats.KeyBytesN),
			ValueBytesN:      common.StorageSize(stats.ValueBytesN),
			TotalPut:         common.StorageCounter(stats.TotalPut),
			TotalDelete:      common.StorageCounter(stats.TotalDelete),
			TotalBytesPut:    common.StorageSize(stats.TotalBytesPut),
			TotalBytesDelete: common.StorageSize(stats.TotalBytesDelete),
		}
	}
	return res, nil
}

func (db *BoltKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	t := boltTxPool.Get().(*boltTx)
	t.ctx = ctx
	t.db = db
	var err error
	t.bolt, err = db.bolt.Begin(writable)
	return t, err
}

func (db *BoltKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := boltTxPool.Get().(*boltTx)
	defer boltTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	defer t.closeCursors()
	return db.bolt.View(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (db *BoltKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := boltTxPool.Get().(*boltTx)
	defer boltTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	defer t.closeCursors()
	return db.bolt.Update(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (tx *boltTx) Commit(ctx context.Context) error {
	defer tx.closeCursors()
	// could not put tx back to pool, because tx can be used by app code after commit
	return tx.bolt.Commit()
}

func (tx *boltTx) Rollback() error {
	defer tx.closeCursors()
	// could not put tx back to pool, because tx can be used by app code after rollback
	return tx.bolt.Rollback()
}

func (tx *boltTx) closeCursors() {
	for _, c := range tx.cursors {
		boltCursorPool.Put(c)
	}
	tx.cursors = tx.cursors[:0]
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
	return &noValuesBoltCursor{boltCursor: c}
}

func (b boltBucket) Size() (uint64, error) {
	st := b.bolt.Stats()
	return uint64((st.BranchPageN + st.BranchOverflowN + st.LeafPageN) * os.Getpagesize()), nil
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
	c := boltCursorPool.Get().(*boltCursor)
	c.ctx = b.tx.ctx
	c.bucket = b
	c.prefix = nil
	c.k = nil
	c.v = nil
	c.err = nil
	c.bolt = b.bolt.Cursor()
	// add to auto-close on end of transactions
	if b.tx.cursors == nil {
		b.tx.cursors = make([]*boltCursor, 0, 1)
	}
	b.tx.cursors = append(b.tx.cursors, c)
	return c
}

func (c *boltCursor) First() ([]byte, []byte, error) {
	if len(c.prefix) == 0 {
		c.k, c.v = c.bolt.First()
	} else {
		c.k, c.v = c.bolt.Seek(c.prefix)
	}

	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Seek(seek)
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.SeekTo(seek)
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Next()
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, c.v, nil
}

func (c *boltCursor) Delete(key []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	return c.bolt.Delete2(key)
}

func (c *boltCursor) Put(key []byte, value []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	return c.bolt.Put(key, value)
}

func (c *boltCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

func (c *noValuesBoltCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
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
		return []byte{}, 0, c.ctx.Err() // on error key should be != nil
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
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	c.k, c.v = c.bolt.Next()
	if len(c.prefix) != 0 && !bytes.HasPrefix(c.k, c.prefix) {
		return nil, 0, nil
	}
	return c.k, uint32(len(c.v)), nil
}
