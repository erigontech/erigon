package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/AskAlexSharov/lmdb-go/exp/lmdbpool"
	"github.com/AskAlexSharov/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	lmdbKvTxPool     = sync.Pool{New: func() interface{} { return &lmdbTx{} }}
	lmdbKvCursorPool = sync.Pool{New: func() interface{} { return &lmdbCursor{} }}
	lmdbKvBucketPool = sync.Pool{New: func() interface{} { return &lmdbBucket{} }}
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

func (opts lmdbOpts) Open() (KV, error) {
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
		err = env.SetMapSize(64 << 20) // 64MB
		logger = log.New("lmdb", "inMem")
		if err != nil {
			return nil, err
		}
		opts.path, _ = ioutil.TempDir(os.TempDir(), "lmdb")
	} else {
		err = env.SetMapSize(32 << 40) // 32TB
		logger = log.New("lmdb", path.Base(opts.path))
		if err != nil {
			return nil, err
		}
	}
	if err = os.MkdirAll(opts.path, 0744); err != nil {
		return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
	}

	var flags uint = lmdb.NoReadahead
	if opts.readOnly {
		flags |= lmdb.Readonly
	}
	if opts.inMem {
		flags |= lmdb.NoSync | lmdb.NoMetaSync
	}
	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, err
	}

	db := &LmdbKV{
		opts:            opts,
		env:             env,
		log:             logger,
		lmdbTxPool:      lmdbpool.NewTxnPool(env),
		lmdbCursorPools: make([]sync.Pool, len(dbutils.Buckets)),
	}

	db.buckets = make([]lmdb.DBI, len(dbutils.Buckets))
	if opts.readOnly {
		if err := env.View(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), 0)
				if createErr != nil {
					return createErr
				}
				db.buckets[dbutils.BucketsIndex[string(name)]] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := env.Update(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), lmdb.Create)
				if createErr != nil {
					return createErr
				}
				db.buckets[dbutils.BucketsIndex[string(name)]] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	if !opts.inMem {
		ctx, ctxCancel := context.WithCancel(context.Background())
		db.stopStaleReadsCheck = ctxCancel
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			db.staleReadsCheckLoop(ctx, ticker)
		}()
	}

	return db, nil
}

func (opts lmdbOpts) MustOpen() KV {
	db, err := opts.Open()
	if err != nil {
		panic(fmt.Errorf("fail to open lmdb: %w", err))
	}
	return db
}

type LmdbKV struct {
	opts                lmdbOpts
	env                 *lmdb.Env
	log                 log.Logger
	buckets             []lmdb.DBI
	lmdbTxPool          *lmdbpool.TxnPool // pool of lmdb.Txn objects
	lmdbCursorPools     []sync.Pool       // pool of lmdb.Cursor objects
	stopStaleReadsCheck context.CancelFunc
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

// staleReadsCheckLoop - In any real application it is important to check for readers that were
// never closed by their owning process, and for which the owning process
// has exited.  See the documentation on transactions for more information.
func (db *LmdbKV) staleReadsCheckLoop(ctx context.Context, ticker *time.Ticker) {
	for {
		// check once on app start
		staleReaders, err2 := db.env.ReaderCheck()
		if err2 != nil {
			db.log.Error("failed ReaderCheck", "err", err2)
		}
		if staleReaders > 0 {
			db.log.Info("cleared reader slots from dead processes", "amount", staleReaders)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.stopStaleReadsCheck != nil {
		db.stopStaleReadsCheck()
	}
	db.lmdbTxPool.Close()

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
	return common.StorageSize(0), nil
}

func (db *LmdbKV) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return map[string]common.StorageBucketWriteStats{}, nil
}

func (db *LmdbKV) dbi(bucket []byte) lmdb.DBI {
	if id, ok := dbutils.BucketsIndex[string(bucket)]; ok {
		return db.buckets[id]
	}
	panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(bucket)))
}

func (db *LmdbKV) Get(ctx context.Context, bucket, key []byte) (val []byte, err error) {
	err = db.View(ctx, func(tx Tx) error {
		v, err2 := tx.(*lmdbTx).tx.Get(db.dbi(bucket), key)
		if err2 != nil {
			if lmdb.IsNotFound(err2) {
				return nil
			}
			return err2
		}
		if v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (db *LmdbKV) Has(ctx context.Context, bucket, key []byte) (has bool, err error) {
	err = db.View(ctx, func(tx Tx) error {
		v, err2 := tx.(*lmdbTx).tx.Get(db.dbi(bucket), key)
		if err2 != nil {
			if lmdb.IsNotFound(err2) {
				return nil
			}
			return err2
		}
		has = v != nil
		return nil
	})
	if err != nil {
		return false, err
	}
	return has, nil
}

func (db *LmdbKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	flags := uint(0)
	var tx *lmdb.Txn
	if writable {
		var err error
		tx, err = db.env.BeginTxn(nil, flags)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		tx, err = db.lmdbTxPool.BeginTxn(flags | lmdb.Readonly)
		if err != nil {
			return nil, err
		}
	}

	tx.RawRead = true
	tx.Pooled = false

	t := lmdbKvTxPool.Get().(*lmdbTx)
	t.ctx = ctx
	t.tx = tx
	t.db = db
	return t, nil
}

type lmdbTx struct {
	tx      *lmdb.Txn
	ctx     context.Context
	db      *LmdbKV
	cursors []*lmdbCursor
	buckets []*lmdbBucket
}

type lmdbBucket struct {
	id  int
	tx  *lmdbTx
	dbi lmdb.DBI
}

type lmdbCursor struct {
	ctx    context.Context
	bucket *lmdbBucket
	prefix []byte

	cursor *lmdb.Cursor
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := lmdbKvTxPool.Get().(*lmdbTx)
	defer lmdbKvTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	return db.lmdbTxPool.View(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.Pooled, tx.RawRead = true, true
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := lmdbKvTxPool.Get().(*lmdbTx)
	defer lmdbKvTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	return db.env.Update(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (tx *lmdbTx) Bucket(name []byte) Bucket {
	id, ok := dbutils.BucketsIndex[string(name)]
	if !ok {
		panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(name)))
	}

	b := lmdbKvBucketPool.Get().(*lmdbBucket)
	b.tx = tx
	b.id = id
	b.dbi = tx.db.buckets[id]

	// add to auto-close on end of transactions
	if b.tx.buckets == nil {
		b.tx.buckets = make([]*lmdbBucket, 0, 1)
	}
	b.tx.buckets = append(b.tx.buckets, b)

	return b
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	tx.closeCursors()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() error {
	tx.closeCursors()
	tx.tx.Reset()
	return nil
}

func (tx *lmdbTx) closeCursors() {
	for _, c := range tx.cursors {
		if c.cursor != nil {
			if tx.tx.Pooled {
				tx.db.lmdbCursorPools[c.bucket.id].Put(c.cursor)
			} else {
				c.cursor.Close()
			}
		}
		lmdbKvCursorPool.Put(c)
	}
	tx.cursors = tx.cursors[:0]
	for _, b := range tx.buckets {
		lmdbKvBucketPool.Put(b)
	}
	tx.buckets = tx.buckets[:0]
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
	return &lmdbNoValuesCursor{lmdbCursor: c}
}

func (b lmdbBucket) Get(key []byte) (val []byte, err error) {
	val, err = b.tx.tx.Get(b.dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, err
}

func (b *lmdbBucket) Put(key []byte, value []byte) error {
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

func (b *lmdbBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	err := b.tx.tx.Del(b.dbi, key, nil)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}
	return err
}

func (b *lmdbBucket) Size() (uint64, error) {
	st, err := b.tx.tx.Stat(b.dbi)
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (b *lmdbBucket) Cursor() Cursor {
	tx := b.tx
	c := lmdbKvCursorPool.Get().(*lmdbCursor)
	c.ctx = tx.ctx
	c.bucket = b
	c.prefix = nil
	c.cursor = nil
	// add to auto-close on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*lmdbCursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c)
	return c
}

func (c *lmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}
	tx := c.bucket.tx

	if tx.tx.Pooled {
		cur := tx.db.lmdbCursorPools[c.bucket.id].Get()
		if cur != nil {
			c.cursor = cur.(*lmdb.Cursor)
			if err := c.cursor.Renew(tx.tx); err != nil {
				return err
			}
		}
	}

	if c.cursor == nil {
		var err error
		c.cursor, err = tx.tx.OpenCursor(c.bucket.dbi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *lmdbCursor) First() ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	return c.Seek(c.prefix)
}

func (c *lmdbCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if seek == nil {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		k, v, err = c.cursor.Get(seek, nil, lmdb.SetRange)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Seek(): %w, key: %x", err, seek)
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *lmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *lmdbCursor) Next() (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v, err = c.cursor.Get(nil, nil, lmdb.Next)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", err)
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *lmdbCursor) Delete(key []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
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
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
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
	*lmdbCursor
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

func (c *lmdbNoValuesCursor) First() (k []byte, v uint32, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, 0, err
		}
	}

	var val []byte
	if len(c.prefix) == 0 {
		k, val, err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		k, val, err = c.cursor.Get(c.prefix, nil, lmdb.SetKey)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return []byte{}, uint32(len(val)), nil
		}
		return []byte{}, 0, err
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, val = nil, nil
	}
	return k, uint32(len(val)), err
}

func (c *lmdbNoValuesCursor) Seek(seek []byte) (k []byte, v uint32, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, 0, err
		}
	}

	var val []byte
	k, val, err = c.cursor.Get(seek, nil, lmdb.SetKey)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return []byte{}, uint32(len(val)), nil
		}
		return []byte{}, 0, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, val = nil, nil
	}

	return k, uint32(len(val)), err
}

func (c *lmdbNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *lmdbNoValuesCursor) Next() (k []byte, v uint32, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	var val []byte
	k, val, err = c.cursor.Get(nil, nil, lmdb.Next)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return []byte{}, uint32(len(val)), nil
		}
		return []byte{}, 0, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, val = nil, nil
	}

	return k, uint32(len(val)), err
}
