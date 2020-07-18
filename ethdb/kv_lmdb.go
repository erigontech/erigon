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

	"github.com/ledgerwatch/lmdb-go/lmdb"
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
		opts: opts,
		env:  env,
		log:  logger,
		wg:   &sync.WaitGroup{},
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
		db.wg.Add(1)
		go func() {
			defer db.wg.Done()
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
	stopStaleReadsCheck context.CancelFunc
	wg                  *sync.WaitGroup
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
	db.wg.Wait()

	if db.env != nil {
		if err := db.env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed (LMDB)")
		}
		db.env = nil
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

func (db *LmdbKV) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
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
	tx      *lmdb.Txn
	ctx     context.Context
	db      *LmdbKV
	cursors []*lmdb.Cursor
}

type lmdbBucket struct {
	id  int
	tx  *lmdbTx
	dbi lmdb.DBI
}

type LmdbCursor struct {
	ctx    context.Context
	bucket *lmdbBucket
	prefix []byte

	cursor *lmdb.Cursor
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.View(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
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

	return &lmdbBucket{tx: tx, id: id, dbi: tx.db.buckets[id]}
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	tx.closeCursors()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() {
	tx.closeCursors()
	tx.tx.Reset()
}

func (tx *lmdbTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = tx.cursors[:0]
}

func (c *LmdbCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *LmdbCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *LmdbCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (c *LmdbCursor) NoValues() NoValuesCursor {
	//c.cursorOpts.PrefetchValues = false
	return &lmdbNoValuesCursor{LmdbCursor: c}
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

	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", dbutils.Buckets[b.id])
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

func (b *lmdbBucket) Clear() error {
	return b.tx.tx.Drop(b.dbi, false)
}

func (b *lmdbBucket) Cursor() Cursor {
	return &LmdbCursor{bucket: b, ctx: b.tx.ctx}
}

func (c *LmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}
	tx := c.bucket.tx

	var err error
	c.cursor, err = tx.tx.OpenCursor(c.bucket.dbi)
	if err != nil {
		return err
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c.cursor)
	return nil
}

func (c *LmdbCursor) First() ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	return c.Seek(c.prefix)
}

func (c *LmdbCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if len(seek) == 0 {
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

func (c *LmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *LmdbCursor) Next() (k, v []byte, err error) {
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

func (c *LmdbCursor) Delete(key []byte) error {
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

func (c *LmdbCursor) Put(key []byte, value []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", dbutils.Buckets[c.bucket.id])
	}
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	return c.cursor.Put(key, value, 0)
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Danger: if provided data will not sorted (or bucket have old records which mess with new in sorting manner) - db will corrupt.
func (c *LmdbCursor) Append(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", dbutils.Buckets[c.bucket.id])
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	return c.cursor.Put(key, value, lmdb.Append)
}

func (c *LmdbCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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
	*LmdbCursor
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
	return c.Seek(c.prefix)
}

func (c *lmdbNoValuesCursor) Seek(seek []byte) (k []byte, vSize uint32, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, 0, err
		}
	}

	var v []byte
	if len(seek) == 0 {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		k, v, err = c.cursor.Get(seek, nil, lmdb.SetRange)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return []byte{}, uint32(len(v)), nil
		}
		return []byte{}, 0, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, uint32(len(v)), err
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
