package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

type lmdbOpts struct {
	path     string
	inMem    bool
	readOnly bool
}

func (opts lmdbOpts) Path(path string) lmdbOpts {
	opts.path = path
	return opts
}

func (opts lmdbOpts) InMem() lmdbOpts {
	opts.inMem = true
	return opts
}

func (opts lmdbOpts) ReadOnly() lmdbOpts {
	opts.readOnly = true
	return opts
}

func (opts lmdbOpts) Open(ctx context.Context) (KV, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	err = env.SetMaxDBs(100)
	if err != nil {
		return nil, err
	}

	var logger log.Logger

	if opts.inMem {
		logger = log.New("lmdb", "inMem")
		err = env.SetMapSize(1 << 22) // 4MB
		if err != nil {
			return nil, err
		}
		opts.path, _ = ioutil.TempDir(os.TempDir(), "lmdb")
		//opts.path = path.Join(os.TempDir(), "lmdb-in-memory")
		//opts.path = "lmdb_tmp"
	} else {
		logger = log.New("lmdb", path.Base(opts.path))

		err = env.SetMapSize(1 << 45) // 1TB
		if err != nil {
			return nil, err
		}
	}

	flags := uint(0)
	if opts.readOnly {
		flags |= lmdb.Readonly
	}
	if opts.inMem {
		flags |= lmdb.NoSync | lmdb.NoMetaSync | lmdb.WriteMap
	}
	if err = os.MkdirAll(opts.path, 0744); err != nil {
		return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
	}
	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, err
	}

	buckets := map[string]lmdb.DBI{}
	if !opts.readOnly {
		if err := env.Update(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), lmdb.Create)
				if createErr != nil {
					return createErr
				}
				buckets[string(name)] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	go func() {
		for {
			tick := time.NewTicker(time.Minute)
			select {
			case <-ctx.Done():
				tick.Stop()
				return
			case <-tick.C:
				// In any real application it is important to check for readers that were
				// never closed by their owning process, and for which the owning process
				// has exited.  See the documentation on transactions for more information.
				staleReaders, err2 := env.ReaderCheck()
				if err2 != nil {
					logger.Error("failed ReaderCheck", "err", err2)
				}
				if staleReaders > 0 {
					logger.Info("cleared reader slots from dead processes", "amount", staleReaders)
				}
			}
		}
	}()
	return &LmdbKV{
		opts:    opts,
		env:     env,
		log:     logger,
		buckets: buckets,
	}, nil
}

func (opts lmdbOpts) MustOpen(ctx context.Context) KV {
	db, err := opts.Open(ctx)
	if err != nil {
		panic(fmt.Errorf("fail to open lmdb: %w", err))
	}
	return db
}

type LmdbKV struct {
	opts    lmdbOpts
	env     *lmdb.Env
	log     log.Logger
	buckets map[string]lmdb.DBI
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.env != nil {
		if err := db.env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed")
		}
	}

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}
}

func (db *LmdbKV) DiskSize(_ context.Context) (common.StorageSize, error) {
	stats, err := db.env.Stat()
	if err != nil {
		return 0, fmt.Errorf("could not read database size: %w", err)
	}
	return common.StorageSize(uint64(stats.PSize) * (stats.LeafPages + stats.BranchPages + stats.OverflowPages)), nil
}

func (db *LmdbKV) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return map[string]common.StorageBucketWriteStats{}, nil
}

func (db *LmdbKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	flags := uint(0)
	if !writable {
		flags |= lmdb.Readonly
	}
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, err
	}
	tx.RawRead = true
	return &lmdbTx{
		db:  db,
		ctx: ctx,
		tx:  tx,
	}, nil
}

type lmdbTx struct {
	ctx context.Context
	db  *LmdbKV

	tx      *lmdb.Txn
	cursors []*lmdb.Cursor
}

type lmdbBucket struct {
	tx  *lmdbTx
	dbi lmdb.DBI
}

type lmdbCursor struct {
	ctx    context.Context
	bucket lmdbBucket
	prefix []byte

	cursor *lmdb.Cursor

	k   []byte
	v   []byte
	err error
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.View(func(tx *lmdb.Txn) error {
		defer t.cleanup()
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.Update(func(tx *lmdb.Txn) error {
		defer t.cleanup()
		t.tx = tx
		return f(t)
	})
}

func (tx *lmdbTx) Bucket(name []byte) Bucket {
	b, ok := tx.db.buckets[string(name)]
	if !ok {
		panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(name)))
	}
	return lmdbBucket{tx: tx, dbi: b}
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	tx.cleanup()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() error {
	tx.cleanup()
	tx.tx.Reset()
	return nil
}

func (tx *lmdbTx) cleanup() {
	for _, c := range tx.cursors {
		c.Close()
	}
}

func (c *lmdbCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *lmdbCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *lmdbCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (c *lmdbCursor) NoValues() NoValuesCursor {
	//c.cursorOpts.PrefetchValues = false
	return &lmdbNoValuesCursor{lmdbCursor: *c}
}

func (b lmdbBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	val, err = b.tx.tx.Get(b.dbi, key)
	if lmdb.IsNotFound(err) {
		return nil, nil
	}
	return val, err
}

func (b lmdbBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	err := b.tx.tx.Put(b.dbi, key, value, 0)
	if err != nil {
		return fmt.Errorf("failed LmdbKV.Put: %w", err)
	}
	return nil
}

func (b lmdbBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	err := b.tx.tx.Del(b.dbi, key, nil)
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

func (b lmdbBucket) Cursor() Cursor {
	c := &lmdbCursor{bucket: b, ctx: b.tx.ctx}
	return c
}

func (c *lmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}

	var err error
	c.cursor, err = c.bucket.tx.tx.OpenCursor(c.bucket.dbi)
	if err != nil {
		return err
	}

	// add to auto-cleanup on end of transactions
	if c.bucket.tx.cursors == nil {
		c.bucket.tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	c.bucket.tx.cursors = append(c.bucket.tx.cursors, c.cursor)
	return nil
}

func (c *lmdbCursor) First() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}

	return c.Seek(c.prefix)
}

func (c *lmdbCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}

	if seek == nil {
		c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		c.k, c.v, c.err = c.cursor.Get(seek, nil, lmdb.SetRange)

	}
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Seek(): %w, key: %x", c.err, seek)
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *lmdbCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.Next)
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", c.err)
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) Delete(key []byte) error {
	if err := c.initCursor(); err != nil {
		return err
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	k, _, err := c.Seek(key)
	if err != nil {
		return err
	}

	if !bytes.Equal(k, key) {
		return nil
	}
	return c.cursor.Del(0)
}

func (c *lmdbCursor) Put(key []byte, value []byte) error {
	if err := c.initCursor(); err != nil {
		return err
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	return c.cursor.Put(key, value, 0)
}

func (c *lmdbCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

type lmdbNoValuesCursor struct {
	lmdbCursor
}

func (c *lmdbNoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
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

func (c *lmdbNoValuesCursor) First() ([]byte, uint32, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, 0, err
	}
	if len(c.prefix) == 0 {
		c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		c.k, c.v, c.err = c.cursor.Get(c.prefix, nil, lmdb.SetKey)
	}
	if lmdb.IsNotFound(c.err) {
		return []byte{}, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, uint32(len(c.v)), c.err
}

func (c *lmdbNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	if err := c.initCursor(); err != nil {
		return []byte{}, 0, err
	}

	c.k, c.v, c.err = c.cursor.Get(seek, nil, lmdb.SetKey)
	if lmdb.IsNotFound(c.err) {
		return nil, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, uint32(len(c.v)), c.err
}

func (c *lmdbNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *lmdbNoValuesCursor) Next() ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.Next)
	if lmdb.IsNotFound(c.err) {
		return nil, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, uint32(len(c.v)), c.err
}
