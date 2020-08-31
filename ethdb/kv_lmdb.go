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

type BucketConfigsFunc func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg
type lmdbOpts struct {
	inMem      bool
	readOnly   bool
	path       string
	bucketsCfg BucketConfigsFunc
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

func (opts lmdbOpts) WithBucketsConfig(f BucketConfigsFunc) lmdbOpts {
	opts.bucketsCfg = f
	return opts
}

var DefaultBucketConfigs = func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	return defaultBuckets
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
		buckets: dbutils.BucketsCfg{},
	}
	customBuckets := opts.bucketsCfg(dbutils.BucketsConfigs)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	// Open or create buckets
	if opts.readOnly {
		if err := db.View(context.Background(), func(tx Tx) error {
			for name, cfg := range db.buckets {
				if cfg.IsDeprecated {
					continue
				}
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
			for name, cfg := range db.buckets {
				if cfg.IsDeprecated {
					continue
				}
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
		for name, cfg := range db.buckets {
			if !cfg.IsDeprecated {
				continue
			}
			dbi, createErr := tx.OpenDBI(name, 0)
			if createErr != nil {
				if lmdb.IsNotFound(createErr) {
					cnfCopy := db.buckets[name]
					cnfCopy.DBI = NonExistingDBI
					db.buckets[name] = cnfCopy
					continue // if deprecated bucket couldn't be open - then it's deleted and it's fine
				} else {
					return createErr
				}
			}
			cnfCopy := db.buckets[name]
			cnfCopy.DBI = dbi
			db.buckets[name] = cnfCopy
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
	buckets             dbutils.BucketsCfg
	stopStaleReadsCheck context.CancelFunc
	wg                  *sync.WaitGroup
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{bucketsCfg: DefaultBucketConfigs}
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
	bucketCfg  dbutils.BucketConfigItem
	prefix     []byte

	c *lmdb.Cursor
}

func (db *LmdbKV) Env() *lmdb.Env {
	return db.env
}

func (db *LmdbKV) AllDBI() map[string]lmdb.DBI {
	res := map[string]lmdb.DBI{}
	for name, cfg := range db.buckets {
		res[name] = cfg.DBI
	}
	return res
}

func (db *LmdbKV) AllBuckets() dbutils.BucketsCfg {
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

	// can't use db.evn.View method - because it calls commit for read transactions - it conflicts with write transactions.
	tx, err := db.Begin(ctx, nil, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	tx, err := db.Begin(ctx, nil, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = f(tx)
	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (tx *lmdbTx) CreateBucket(name string) error {
	var flags = tx.db.buckets[name].Flags
	if !tx.db.opts.readOnly {
		flags |= lmdb.Create
	}
	dbi, err := tx.tx.OpenDBI(name, flags)
	if err != nil {
		return err
	}
	cnfCopy := tx.db.buckets[name]
	cnfCopy.DBI = dbi
	tx.db.buckets[name] = cnfCopy
	return nil
}

func (tx *lmdbTx) dropEvenIfBucketIsNotDeprecated(name string) error {
	dbi := tx.db.buckets[name].DBI
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
	cnfCopy := tx.db.buckets[name]
	cnfCopy.DBI = NonExistingDBI
	tx.db.buckets[name] = cnfCopy
	return nil
}

func (tx *lmdbTx) ClearBucket(bucket string) error {
	if err := tx.dropEvenIfBucketIsNotDeprecated(bucket); err != nil {
		return nil
	}
	return tx.CreateBucket(bucket)
}

func (tx *lmdbTx) DropBucket(bucket string) error {
	if cfg, ok := tx.db.buckets[bucket]; !(ok && cfg.IsDeprecated) {
		return fmt.Errorf("%w, bucket: %s", ErrAttemptToDeleteNonDeprecatedBucket, bucket)
	}

	return tx.dropEvenIfBucketIsNotDeprecated(bucket)
}

func (tx *lmdbTx) ExistsBucket(bucket string) bool {
	if cfg, ok := tx.db.buckets[bucket]; ok {
		return cfg.DBI != NonExistingDBI
	}
	return false
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
	if commitTook > 20*time.Second {
		log.Info("Batch", "commit", commitTook)
	}

	if !tx.isSubTx { // call fsync only after main transaction commit
		fsyncTimer := time.Now()
		if err := tx.db.env.Sync(true); err != nil {
			log.Warn("fsync after commit failed: \n", err)
		}
		fsyncTook := time.Since(fsyncTimer)
		if fsyncTook > 20*time.Second {
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

func (c *LmdbCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (tx *lmdbTx) Get(bucket string, key []byte) ([]byte, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
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

	val, err := tx.get(b.DBI, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (tx *lmdbTx) BucketSize(name string) (uint64, error) {
	st, err := tx.tx.Stat(tx.db.buckets[name].DBI)
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (tx *lmdbTx) Cursor(bucket string) Cursor {
	return &LmdbCursor{bucketName: bucket, ctx: tx.ctx, tx: tx, bucketCfg: tx.db.buckets[bucket], dbi: tx.db.buckets[bucket].DBI}
}

func (tx *lmdbTx) CursorDupSort(bucket string) CursorDupSort {
	basicCursor := tx.Cursor(bucket).(*LmdbCursor)
	return &LmdbDupSortCursor{LmdbCursor: basicCursor}
}

func (tx *lmdbTx) CursorDupFixed(bucket string) CursorDupFixed {
	basicCursor := tx.CursorDupSort(bucket).(*LmdbDupSortCursor)
	return &LmdbDupFixedCursor{LmdbDupSortCursor: basicCursor}
}

func (tx *lmdbTx) CursorNoValues(bucket string) CursorNoValues {
	return &lmdbNoValuesCursor{LmdbCursor: tx.Cursor(bucket).(*LmdbCursor)}
}

// methods here help to see better pprof picture
func (c *LmdbCursor) set(k []byte) ([]byte, []byte, error) { return c.c.Get(k, nil, lmdb.Set) }
func (c *LmdbCursor) getCurrent() ([]byte, []byte, error)  { return c.c.Get(nil, nil, lmdb.GetCurrent) }
func (c *LmdbCursor) first() ([]byte, []byte, error)       { return c.c.Get(nil, nil, lmdb.First) }
func (c *LmdbCursor) next() ([]byte, []byte, error)        { return c.c.Get(nil, nil, lmdb.Next) }
func (c *LmdbCursor) nextDup() ([]byte, []byte, error)     { return c.c.Get(nil, nil, lmdb.NextDup) }
func (c *LmdbCursor) nextNoDup() ([]byte, []byte, error)   { return c.c.Get(nil, nil, lmdb.NextNoDup) }
func (c *LmdbCursor) prev() ([]byte, []byte, error)        { return c.c.Get(nil, nil, lmdb.Prev) }
func (c *LmdbCursor) prevDup() ([]byte, []byte, error)     { return c.c.Get(nil, nil, lmdb.PrevDup) }
func (c *LmdbCursor) prevNoDup() ([]byte, []byte, error)   { return c.c.Get(nil, nil, lmdb.PrevNoDup) }
func (c *LmdbCursor) last() ([]byte, []byte, error)        { return c.c.Get(nil, nil, lmdb.Last) }
func (c *LmdbCursor) delCurrent() error                    { return c.c.Del(0) }
func (c *LmdbCursor) delNoDupData() error                  { return c.c.Del(lmdb.NoDupData) }
func (c *LmdbCursor) put(k, v []byte) error                { return c.c.Put(k, v, 0) }
func (c *LmdbCursor) putCurrent(k, v []byte) error         { return c.c.Put(k, v, lmdb.Current) }
func (c *LmdbCursor) putNoOverwrite(k, v []byte) error     { return c.c.Put(k, v, lmdb.NoOverwrite) }
func (c *LmdbCursor) putNoDupData(k, v []byte) error       { return c.c.Put(k, v, lmdb.NoDupData) }
func (c *LmdbCursor) append(k, v []byte) error             { return c.c.Put(k, v, lmdb.Append) }
func (c *LmdbCursor) appendDup(k, v []byte) error          { return c.c.Put(k, v, lmdb.AppendDup) }
func (c *LmdbCursor) getBoth(k, v []byte) ([]byte, []byte, error) {
	return c.c.Get(k, v, lmdb.GetBoth)
}
func (c *LmdbCursor) setRange(k []byte) ([]byte, []byte, error) {
	return c.c.Get(k, nil, lmdb.SetRange)
}
func (c *LmdbCursor) getBothRange(k, v []byte) ([]byte, []byte, error) {
	return c.c.Get(k, v, lmdb.GetBothRange)
}
func (c *LmdbCursor) firstDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, lmdb.FirstDup)
	return v, err
}
func (c *LmdbCursor) lastDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, lmdb.LastDup)
	return v, err
}

func (c *LmdbCursor) initCursor() error {
	if c.c != nil {
		return nil
	}
	tx := c.tx

	var err error
	c.c, err = tx.tx.OpenCursor(c.tx.db.buckets[c.bucketName].DBI)
	if err != nil {
		return err
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c.c)
	return nil
}

func (c *LmdbCursor) First() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	return c.Seek(c.prefix)
}

func (c *LmdbCursor) Last() ([]byte, []byte, error) {
	if c.c == nil {
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

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	return k, v, nil
}

func (c *LmdbCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.seekDupSort(seek)
	}

	if len(seek) == 0 {
		k, v, err = c.first()
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
		k, v, err = c.first()
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
	if c.c == nil {
		if err = c.initCursor(); err != nil {
			log.Error("init cursor", "err", err)
		}
	}

	k, v, err = c.next()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", err)
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *LmdbCursor) Prev() (k, v []byte, err error) {
	if c.c == nil {
		if err = c.initCursor(); err != nil {
			log.Error("init cursor", "err", err)
		}
	}

	k, v, err = c.prev()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Prev(): %w", err)
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

// Current - return key/data at current cursor position
func (c *LmdbCursor) Current() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.getCurrent()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *LmdbCursor) Delete(key []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.deleteDupSort(key)
	}

	_, _, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.delCurrent()
}

// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
// This does not invalidate the cursor, so operations such as MDB_NEXT
// can still be used on it.
// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
// this operation.
func (c *LmdbCursor) DeleteCurrent() error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	return c.delCurrent()
}

func (c *LmdbCursor) deleteDupSort(key []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", c.bucketName, from, to, key)
	}

	if len(key) == from {
		_, v, err := c.getBothRange(key[:to], key[to:])
		if err != nil { // if key not found, or found another one - then nothing to delete
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !bytes.Equal(v[:from-to], key[to:]) {
			return nil
		}
		return c.delCurrent()
	}

	_, _, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.delCurrent()
}

func (c *LmdbCursor) PutNoOverwrite(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.putDupSort(key, value)
	}

	return c.putNoOverwrite(key, value)
}

func (c *LmdbCursor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.putDupSort(key, value)
	}

	return c.put(key, value)
}

func (c *LmdbCursor) PutCurrent(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		value = append(key[b.DupToLen:], value...)
		key = key[:b.DupToLen]
	}

	return c.putCurrent(key, value)
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
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
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

	_, v, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Danger: if provided data will not sorted (or bucket have old records which mess with new in sorting manner) - db will corrupt.
func (c *LmdbCursor) Append(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}

	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if b.AutoDupSortKeysConversion {
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

func (c *LmdbCursor) Close() error {
	if c.c != nil {
		c.c.Close()
		c.c = nil
	}
	return nil
}

type LmdbDupSortCursor struct {
	*LmdbCursor
}

func (c *LmdbDupSortCursor) initCursor() error {
	if c.c != nil {
		return nil
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return fmt.Errorf("class LmdbDupSortCursor not compatible with AutoDupSortKeysConversion buckets")
	}

	if c.bucketCfg.Flags&lmdb.DupSort == 0 {
		return fmt.Errorf("class LmdbDupSortCursor can be used only if bucket created with flag lmdb.DupSort")
	}

	return c.LmdbCursor.initCursor()
}

func (c *LmdbDupSortCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	if c.c == nil {
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

func (c *LmdbDupSortCursor) SeekBothRange(key, value []byte) ([]byte, []byte, error) {
	if c.c == nil {
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

func (c *LmdbDupSortCursor) FirstDup() ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	v, err := c.firstDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

// NextDup - iterate only over duplicates of current key
func (c *LmdbDupSortCursor) NextDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.nextDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

// NextNoDup - iterate with skipping all duplicates
func (c *LmdbDupSortCursor) NextNoDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.nextNoDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) PrevDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.prevDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) PrevNoDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.prevNoDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) LastDup() ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	v, err := c.lastDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func (c *LmdbCursor) AppendDup(key []byte, value []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	return c.appendDup(key, value)
}

func (c *LmdbDupSortCursor) PutNoDupData(key, value []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	return c.putNoDupData(key, value)
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *LmdbDupSortCursor) DeleteCurrentDuplicates() error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	return c.delNoDupData()
}

// Count returns the number of duplicates for the current key. See mdb_cursor_count
func (c *LmdbDupSortCursor) CountDuplicates() (uint64, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return 0, err
		}
	}
	return c.c.Count()
}

type LmdbDupFixedCursor struct {
	*LmdbDupSortCursor
}

func (c *LmdbDupFixedCursor) initCursor() error {
	if c.c != nil {
		return nil
	}

	if c.bucketCfg.Flags&lmdb.DupFixed == 0 {
		return fmt.Errorf("class LmdbDupSortCursor can be used only if bucket created with flag lmdb.DupSort")
	}

	return c.LmdbCursor.initCursor()
}

func (c *LmdbDupFixedCursor) GetMulti() ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}
	_, v, err := c.c.Get(nil, nil, lmdb.GetMultiple)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func (c *LmdbDupFixedCursor) NextMulti() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}
	k, v, err := c.c.Get(nil, nil, lmdb.NextMultiple)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbDupFixedCursor) PutMulti(key []byte, page []byte, stride int) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	return c.c.PutMulti(key, page, stride, 0)
}

type lmdbNoValuesCursor struct {
	*LmdbCursor
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
