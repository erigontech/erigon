package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"path"
	"sync"
)

type boltOpts struct {
	Bolt *bolt.Options
	path string
}

type BoltKV struct {
	opts boltOpts
	bolt *bolt.DB
	log  log.Logger
	wg   *sync.WaitGroup
}

type boltTx struct {
	ctx  context.Context
	db   *BoltKV
	bolt *bolt.Tx
}

type boltBucket struct {
	tx      *boltTx
	bolt    *bolt.Bucket
	id      int
	nameLen uint
}

type boltCursor struct {
	ctx    context.Context
	bucket boltBucket
	prefix []byte

	bolt *bolt.Cursor
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

func (opts boltOpts) Open() (KV, error) {
	if !opts.Bolt.MemOnly {
		if err := os.MkdirAll(path.Dir(opts.path), 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	boltDB, err := bolt.Open(opts.path, 0600, opts.Bolt)
	if err != nil {
		return nil, err
	}
	if !opts.Bolt.ReadOnly {
		if err := boltDB.Update(func(tx *bolt.Tx) error {
			for _, name := range dbutils.Buckets {
				_, createErr := tx.CreateBucketIfNotExists([]byte(name), false)
				if createErr != nil {
					return createErr
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	db := &BoltKV{
		opts: opts,
		bolt: boltDB,
		log:  log.New("bolt_db", opts.path),
		wg:   &sync.WaitGroup{},
	}

	return db, nil
}

func (opts boltOpts) MustOpen() KV {
	db, err := opts.Open()
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
	db.wg.Wait()

	if db.bolt != nil {
		if err := db.bolt.Close(); err != nil {
			db.log.Warn("failed to close bolt DB", "err", err)
		} else {
			db.log.Info("bolt database closed")
		}
	}
}

func (db *BoltKV) DiskSize(_ context.Context) (uint64, error) {
	return uint64(db.bolt.Size()), nil
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

func (db *BoltKV) Begin(ctx context.Context, parent Tx, writable bool) (Tx, error) {
	if db.bolt == nil {
		return nil, fmt.Errorf("db closed")
	}

	t := &boltTx{db: db, ctx: ctx}
	var err error
	t.bolt, err = db.bolt.Begin(writable)
	return t, err
}

func (db *BoltKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.bolt == nil {
		return fmt.Errorf("db closed")
	}

	t := &boltTx{db: db, ctx: ctx}
	return db.bolt.View(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (db *BoltKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.bolt == nil {
		return fmt.Errorf("db closed")
	}

	t := &boltTx{db: db, ctx: ctx}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (tx *boltTx) Commit(ctx context.Context) error {
	if tx.bolt == nil {
		return fmt.Errorf("db closed")
	}

	return tx.bolt.Commit()
}

func (tx *boltTx) Rollback() {
	if tx.bolt == nil {
		return
	}
	if err := tx.bolt.Rollback(); err != nil {
		log.Warn("bolt rollback failed", "err", err)
	}
}

func (tx *boltTx) Yield() {
	tx.bolt.Yield()
}

func (tx *boltTx) Bucket(name string) boltBucket {
	b := boltBucket{tx: tx, nameLen: uint(len(name)), id: dbutils.BucketsCfg[name].ID}
	b.bolt = tx.bolt.Bucket([]byte(name))
	return b
}

func (c *boltCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *boltCursor) Prefetch(v uint) Cursor {
	// nothing to do
	return c
}

func (c *boltCursor) NoValues() NoValuesCursor {
	return &noValuesBoltCursor{boltCursor: c}
}

func (tx *boltTx) BucketSize(name string) (uint64, error) {
	st := tx.bolt.Bucket([]byte(name)).Stats()
	return uint64((st.BranchPageN + st.BranchOverflowN + st.LeafPageN) * os.Getpagesize()), nil
}

func (b boltBucket) Clear() error {
	err := b.tx.bolt.DeleteBucket([]byte(dbutils.Buckets[b.id]))
	if err != nil {
		return err
	}
	_, err = b.tx.bolt.CreateBucket([]byte(dbutils.Buckets[b.id]), false)
	if err != nil {
		return err
	}
	return nil
}

func (tx *boltTx) Get(bucket string, key []byte) (val []byte, err error) {
	return tx.Bucket(bucket).Get(key)
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

func (tx *boltTx) Cursor(bucket string) Cursor {
	return tx.Bucket(bucket).Cursor()
}

func (b boltBucket) Cursor() Cursor {
	return &boltCursor{bucket: b, ctx: b.tx.ctx, bolt: b.bolt.Cursor()}
}

func (c *boltCursor) SeekExact(key []byte) (val []byte, err error) {
	return c.bucket.Get(key)
}

func (c *boltCursor) First() (k, v []byte, err error) {
	if len(c.prefix) == 0 {
		k, v = c.bolt.First()
		return k, v, nil
	}
	k, v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(k, c.prefix) {
		return nil, nil, nil
	}
	return k, v, nil
}

func (c *boltCursor) Seek(seek []byte) (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v = c.bolt.Seek(seek)
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, nil, nil
		}
	}
	return k, v, nil
}

func (c *boltCursor) Next() (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v = c.bolt.Next()
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			k, v = nil, nil
		}
	}
	return k, v, nil
}

func (c *boltCursor) Last() (k, v []byte, err error) {
	panic("not implemented yet")
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

func (c *boltCursor) Append(key []byte, value []byte) error {
	return c.Put(key, value)
}

func (c *noValuesBoltCursor) First() (k []byte, vSize uint32, err error) {
	var v []byte
	if len(c.prefix) == 0 {
		k, v = c.bolt.First()
		return k, uint32(len(v)), nil
	}

	k, v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(k, c.prefix) {
		return nil, 0, nil
	}
	return k, uint32(len(v)), nil
}

func (c *noValuesBoltCursor) Seek(seek []byte) (k []byte, vSize uint32, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err() // on error key should be != nil
	default:
	}

	var v []byte
	k, v = c.bolt.Seek(seek)
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, 0, nil
		}
	}
	return k, uint32(len(v)), nil
}

func (c *noValuesBoltCursor) Next() (k []byte, vSize uint32, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	var v []byte
	k, v = c.bolt.Next()
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, 0, nil
		}
	}
	return k, uint32(len(v)), nil
}
