package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	NonExistingDBI = 999_999_999
)

var (
	LMDBMapSize = 2 * datasize.TB
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
		err = env.SetMapSize(int64(LMDBMapSize.Bytes()))
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
		flags |= lmdb.NoMetaSync
	}
	flags |= lmdb.NoSync
	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("%w, path: %s", err, opts.path)
	}

	db := &LmdbKV{
		opts:    opts,
		env:     env,
		log:     logger,
		wg:      &sync.WaitGroup{},
		buckets: map[string]lmdb.DBI{},
	}

	// Open or create buckets
	if opts.readOnly {
		if err := db.View(context.Background(), func(tx Tx) error {
			for _, name := range dbutils.Buckets {
				if err := tx.(BucketMigrator).CreateBucket(name); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := db.Update(context.Background(), func(tx Tx) error {
			for _, name := range dbutils.Buckets {
				if err := tx.(BucketMigrator).CreateBucket(name); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	// Open deprecated buckets if they exist, don't create
	if err := env.View(func(tx *lmdb.Txn) error {
		for _, name := range dbutils.DeprecatedBuckets {
			dbi, createErr := tx.OpenDBI(name, 0)
			if createErr != nil {
				if lmdb.IsNotFound(createErr) {
					db.buckets[name] = NonExistingDBI // some non-existing DBI
					continue                          // if deprecated bucket couldn't be open - then it's deleted and it's fine
				} else {
					return createErr
				}
			}
			db.buckets[name] = dbi
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if !opts.inMem {
		if staleReaders, err := db.env.ReaderCheck(); err != nil {
			db.log.Error("failed ReaderCheck", "err", err)
		} else if staleReaders > 0 {
			db.log.Debug("cleared reader slots from dead processes", "amount", staleReaders)
		}
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
	buckets             map[string]lmdb.DBI
	stopStaleReadsCheck context.CancelFunc
	wg                  *sync.WaitGroup
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.env != nil {
		db.wg.Wait()
	}

	if db.env != nil {
		env := db.env
		db.env = nil
		time.Sleep(10 * time.Millisecond) // TODO: remove after consensus/ethash/consensus.go:VerifyHeaders will spawn controllable goroutines
		if err := env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed (LMDB)")
		}
	}

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}

}

func (db *LmdbKV) DiskSize(_ context.Context) (uint64, error) {
	stats, err := db.env.Stat()
	if err != nil {
		return 0, fmt.Errorf("could not read database size: %w", err)
	}
	return uint64(stats.PSize) * (stats.LeafPages + stats.BranchPages + stats.OverflowPages), nil
}

func (db *LmdbKV) IdealBatchSize() int {
	return int(512 * datasize.MB)
}

func (db *LmdbKV) Begin(ctx context.Context, parent Tx, writable bool) (Tx, error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	isSubTx := parent != nil
	if !isSubTx {
		runtime.LockOSThread()
		db.wg.Add(1)
	}

	flags := uint(0)
	if !writable {
		flags |= lmdb.Readonly
	}
	var parentTx *lmdb.Txn
	if parent != nil {
		parentTx = parent.(*lmdbTx).tx
	}
	tx, err := db.env.BeginTxn(parentTx, flags)
	if err != nil {
		if !isSubTx {
			runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		}
		return nil, err
	}
	tx.RawRead = true
	return &lmdbTx{
		db:      db,
		ctx:     ctx,
		tx:      tx,
		isSubTx: isSubTx,
	}, nil
}

type lmdbTx struct {
	isSubTx bool
	tx      *lmdb.Txn
	ctx     context.Context
	db      *LmdbKV
	cursors []*lmdb.Cursor
}

type LmdbCursor struct {
	ctx        context.Context
	tx         *lmdbTx
	bucketName string
	dbi        lmdb.DBI
	bucketCfg  *dbutils.BucketConfigItem
	prefix     []byte

	cursor *lmdb.Cursor
}

func (db *LmdbKV) Env() *lmdb.Env {
	return db.env
}

func (db *LmdbKV) AllDBI() map[string]lmdb.DBI {
	return db.buckets
}

// All buckets stored as keys of un-named bucket
func (tx *lmdbTx) ExistingBuckets() ([]string, error) {
	var res []string
	rawTx := tx.tx
	root, err := rawTx.OpenRoot(0)
	if err != nil {
		return nil, err
	}
	c, err := rawTx.OpenCursor(root)
	if err != nil {
		return nil, err
	}
	for k, _, _ := c.Get(nil, nil, lmdb.First); k != nil; k, _, _ = c.Get(nil, nil, lmdb.Next) {
		res = append(res, string(k))
	}
	return res, nil
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.View(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	var commitTimer time.Time
	tx := &lmdbTx{db: db, ctx: ctx}
	if err := db.env.Update(func(txn *lmdb.Txn) error {
		defer tx.closeCursors()
		txn.RawRead = true
		tx.tx = txn
		if exeErr := f(tx); exeErr != nil {
			return exeErr
		}
		commitTimer = time.Now()
		return nil
	}); err != nil {
		return err
	}

	commitTook := time.Since(commitTimer)
	if commitTook > 20*time.Second {
		log.Info("Batch", "commit", commitTook)
	}

	fsyncTimer := time.Now()
	if err := tx.db.env.Sync(true); err != nil {
		log.Warn("fsync after commit failed: \n", err)
	}
	fsyncTook := time.Since(fsyncTimer)
	if fsyncTook > 20*time.Second {
		log.Info("Batch", "fsync", fsyncTook)
	}
	return nil
}

func (tx *lmdbTx) CreateBucket(name string) error {
	var flags uint = 0
	if !tx.db.opts.readOnly {
		flags |= lmdb.Create
	}
	if dbutils.BucketsCfg[name].IsDupSort {
		flags |= lmdb.DupSort
	}
	dbi, err := tx.tx.OpenDBI(name, flags)
	if err != nil {
		return err
	}
	tx.db.buckets[name] = dbi
	return nil
}

func (tx *lmdbTx) dropEvenIfBucketIsNotDeprecated(name string) error {
	dbi := tx.db.buckets[name]
	// if bucket was not open on db start, then it's may be deprecated
	// try to open it now without `Create` flag, and if fail then nothing to drop
	if dbi == NonExistingDBI {
		var err error
		dbi, err = tx.tx.OpenDBI(name, 0)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil // DBI doesn't exists means no drop needed
			}
			return err
		}
	}
	if err := tx.tx.Drop(dbi, true); err != nil {
		return err
	}
	tx.db.buckets[name] = NonExistingDBI
	return nil
}

func (tx *lmdbTx) ClearBucket(bucket string) error {
	if err := tx.dropEvenIfBucketIsNotDeprecated(bucket); err != nil {
		return nil
	}
	return tx.CreateBucket(bucket)
}

func (tx *lmdbTx) DropBucket(name string) error {
	for i := range dbutils.Buckets {
		if dbutils.Buckets[i] == name {
			return fmt.Errorf("%w, bucket: %s", ErrAttemptToDeleteNonDeprecatedBucket, name)
		}
	}

	return tx.dropEvenIfBucketIsNotDeprecated(name)
}

func (tx *lmdbTx) ExistsBucket(name string) bool {
	return tx.db.buckets[name] != NonExistingDBI
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	if tx.db.env == nil {
		return fmt.Errorf("db closed")
	}
	if tx.tx == nil {
		return nil
	}
	defer func() {
		tx.tx = nil
		if !tx.isSubTx {
			tx.db.wg.Done()
			runtime.UnlockOSThread()
		}
	}()
	tx.closeCursors()

	commitTimer := time.Now()
	if err := tx.tx.Commit(); err != nil {
		return err
	}
	commitTook := time.Since(commitTimer)
	if commitTook > 10*time.Second {
		log.Info("Batch", "commit", commitTook)
	}

	if !tx.isSubTx { // call fsync only after main transaction commit
		fsyncTimer := time.Now()
		if err := tx.db.env.Sync(true); err != nil {
			log.Warn("fsync after commit failed: \n", err)
		}
		fsyncTook := time.Since(fsyncTimer)
		if fsyncTook > 1*time.Second {
			log.Info("Batch", "fsync", fsyncTook)
		}
	}
	return nil
}

func (tx *lmdbTx) Rollback() {
	if tx.db.env == nil {
		return
	}
	if tx.tx == nil {
		return
	}
	defer func() {
		tx.tx = nil
		if !tx.isSubTx {
			tx.db.wg.Done()
			runtime.UnlockOSThread()
		}
	}()
	tx.closeCursors()
	tx.tx.Abort()
}

func (tx *lmdbTx) get(dbi lmdb.DBI, key []byte) ([]byte, error) {
	return tx.tx.Get(dbi, key)
}

func (tx *lmdbTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = []*lmdb.Cursor{}
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

func (tx *lmdbTx) Get(bucket string, key []byte) ([]byte, error) {
	dbi := tx.db.buckets[bucket]
	cfg := dbutils.BucketsCfg[bucket]
	if cfg.IsDupSort {
		return tx.getDupSort(bucket, dbi, cfg, key)
	}

	val, err := tx.get(dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (tx *lmdbTx) getDupSort(bucket string, dbi lmdb.DBI, cfg *dbutils.BucketConfigItem, key []byte) ([]byte, error) {
	from, to := cfg.DupFromLen, cfg.DupToLen
	if len(key) == from {
		c := tx.Cursor(bucket).(*LmdbCursor)
		if err := c.initCursor(); err != nil {
			return nil, err
		}
		_, v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if !bytes.Equal(key[to:], v[:from-to]) {
			return nil, nil
		}
		return v[from-to:], nil
	}

	val, err := tx.get(dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (tx *lmdbTx) BucketSize(name string) (uint64, error) {
	st, err := tx.tx.Stat(tx.db.buckets[name])
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (tx *lmdbTx) Cursor(bucket string) Cursor {
	return &LmdbCursor{bucketName: bucket, ctx: tx.ctx, tx: tx, bucketCfg: dbutils.BucketsCfg[bucket], dbi: tx.db.buckets[bucket]}
}

func (c *LmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}
	tx := c.tx

	var err error
	c.cursor, err = tx.tx.OpenCursor(c.tx.db.buckets[c.bucketName])
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

func (c *LmdbCursor) Last() ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if c.prefix != nil {
		return []byte{}, nil, fmt.Errorf(".Last doesn't support c.prefix yet")
	}

	k, v, err := c.last()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed LmdbKV cursor.Last(): %w, bucket: %s", err, c.bucketName)
		return []byte{}, nil, err
	}

	if c.bucketCfg.IsDupSort {
		if k == nil {
			return k, v, nil
		}
		k, v, err = c.lastDup(k)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			err = fmt.Errorf("failed LmdbKV cursor.Last(): %w, bucket: %s", err, c.bucketName)
			return []byte{}, nil, err
		}
	}

	return k, v, nil
}

func (c *LmdbCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if c.bucketCfg.IsDupSort {
		return c.seekDupSort(seek)
	}

	if len(seek) == 0 {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		k, v, err = c.setRange(seek)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed LmdbKV cursor.Seek(): %w, bucket: %s,  key: %x", err, c.bucketName, seek)
		return []byte{}, nil, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *LmdbCursor) seekDupSort(seek []byte) (k, v []byte, err error) {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(seek) == 0 {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, err
		}
		if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
			k, v = nil, nil
		}
		return k, v, nil
	}

	var seek1, seek2 []byte
	if len(seek) > to {
		seek1, seek2 = seek[:to], seek[to:]
	} else {
		seek1 = seek
	}
	k, v, err = c.setRange(seek1)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}

		return []byte{}, nil, err
	}

	if seek2 != nil && bytes.Equal(seek1, k) {
		k, v, err = c.getBothRange(seek1, seek2)
		if err != nil && lmdb.IsNotFound(err) {
			k, v, err = c.next()
			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil, nil, nil
				}
				return []byte{}, nil, err
			}
		} else if err != nil {
			return []byte{}, nil, err
		}
	}

	if len(k) == to {
		k2 := make([]byte, 0, len(k)+from-to)
		k2 = append(append(k2, k...), v[:from-to]...)
		v = v[from-to:]
		k = k2
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}
	return k, v, nil
}

func (c *LmdbCursor) Next() (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	if c.bucketCfg.IsDupSort {
		return c.nextDupSort()
	}

	if c.cursor == nil {
		if err = c.initCursor(); err != nil {
			log.Error("init cursor", "err", err)
		}
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

func (c *LmdbCursor) nextDupSort() (k, v []byte, err error) {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	k, v, err = c.cursor.Get(nil, nil, lmdb.NextDup)
	if err != nil && lmdb.IsNotFound(err) {
		k, v, err = c.cursor.Get(nil, nil, lmdb.Next)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", err)
		}
	} else if err != nil {
		return nil, nil, err
	}
	if len(k) == to {
		k = append(k, v[:from-to]...)
		v = v[from-to:]
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

	if c.bucketCfg.IsDupSort {
		return c.deleteDupSort(key)
	}

	_, _, err := c.cursor.Get(key, nil, lmdb.Set)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.cursor.Del(0)
}

func (c *LmdbCursor) deleteDupSort(key []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", c.bucketName, from, to, key)
	}

	if len(key) == from {
		_, v, err := c.cursor.Get(key[:to], key[to:], lmdb.GetBothRange)
		if err != nil { // if key not found, or found another one - then nothing to delete
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !bytes.Equal(v[:from-to], key[to:]) {
			return nil
		}
		return c.cursor.Del(0)
	}

	_, _, err := c.cursor.Get(key, nil, lmdb.Set)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.cursor.Del(0)
}

func (c *LmdbCursor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.IsDupSort {
		return c.putDupSort(key, value)
	}

	return c.put(key, value)
}

func (c *LmdbCursor) putDupSort(key []byte, value []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", c.bucketName, from, to, key)
	}

	if len(key) != from {
		_, _, err := c.set(key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return c.put(key, value)
			}
			return err
		}

		return c.putCurrent(key, value)
	}

	newValue := make([]byte, 0, from-to+len(value))
	newValue = append(append(newValue, key[to:]...), value...)

	key = key[:to]
	_, v, err := c.getBothRange(key, newValue[:from-to])
	if err != nil { // if key not found, or found another one - then just insert
		if lmdb.IsNotFound(err) {
			return c.put(key, newValue)
		}
		return err
	}

	if bytes.Equal(v[:from-to], newValue[:from-to]) {
		if len(v) == len(newValue) { // in DupSort case lmdb.Current works only with values of same length
			return c.putCurrent(key, newValue)
		}
		err = c.delCurrent()
		if err != nil {
			return err
		}
		return c.put(key, newValue)
	}

	return c.put(key, newValue)
}

func (c *LmdbCursor) SeekExact(key []byte) ([]byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	if c.bucketCfg.IsDupSort {
		return c.getDupSort(key)
	}

	_, v, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func (c *LmdbCursor) getDupSort(key []byte) ([]byte, error) {
	from, to := c.bucketCfg.DupFromLen, c.bucketCfg.DupToLen
	if len(key) == from {
		_, v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if !bytes.Equal(key[to:], v[:from-to]) {
			return nil, nil
		}
		return v[from-to:], nil
	}

	_, val, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (c *LmdbCursor) SeekBothRange(key, value []byte) ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.getBothRange(key, value)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.getBoth(key, value)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbCursor) set(key []byte) ([]byte, []byte, error) {
	return c.cursor.Get(key, nil, lmdb.Set)
}

func (c *LmdbCursor) setRange(key []byte) ([]byte, []byte, error) {
	return c.cursor.Get(key, nil, lmdb.SetRange)
}

func (c *LmdbCursor) next() ([]byte, []byte, error) {
	return c.cursor.Get(nil, nil, lmdb.Next)
}

func (c *LmdbCursor) getBothRange(key []byte, value []byte) ([]byte, []byte, error) {
	k, v, err := c.cursor.Get(key, value, lmdb.GetBothRange)
	if err != nil {
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbCursor) getBoth(key []byte, value []byte) ([]byte, []byte, error) {
	k, v, err := c.cursor.Get(key, value, lmdb.GetBoth)
	if err != nil {
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbCursor) last() ([]byte, []byte, error) {
	return c.cursor.Get(nil, nil, lmdb.Last)
}

func (c *LmdbCursor) lastDup(key []byte) ([]byte, []byte, error) {
	return c.cursor.Get(key, nil, lmdb.LastDup)
}

func (c *LmdbCursor) delCurrent() error {
	return c.cursor.Del(0)
}

func (c *LmdbCursor) put(key []byte, value []byte) error {
	return c.cursor.Put(key, value, 0)
}

func (c *LmdbCursor) putCurrent(key []byte, value []byte) error {
	return c.cursor.Put(key, value, lmdb.Current)
}

func (c *LmdbCursor) append(key []byte, value []byte) error {
	return c.cursor.Put(key, value, lmdb.Append)
}

func (c *LmdbCursor) appendDup(key []byte, value []byte) error {
	return c.cursor.Put(key, value, lmdb.AppendDup)
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Danger: if provided data will not sorted (or bucket have old records which mess with new in sorting manner) - db will corrupt.
func (c *LmdbCursor) Append(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if b.IsDupSort {
		if len(key) != from && len(key) >= to {
			return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", c.bucketName, from, to, key)
		}

		if len(key) == from {
			newValue := make([]byte, 0, from-to+len(value))
			newValue = append(append(newValue, key[to:]...), value...)
			key = key[:to]
			return c.appendDup(key, newValue)
		}
		return c.append(key, value)
	}
	return c.append(key, value)
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

func (c *LmdbCursor) Close() error {
	if c.cursor != nil {
		c.cursor.Close()
		c.cursor = nil
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
	k, v, err := c.LmdbCursor.Seek(seek)
	if err != nil {
		return []byte{}, 0, err
	}
	return k, uint32(len(v)), err
}

func (c *lmdbNoValuesCursor) Next() (k []byte, vSize uint32, err error) {
	k, v, err := c.LmdbCursor.Next()
	if err != nil {
		return []byte{}, 0, err
	}
	return k, uint32(len(v)), err
}
