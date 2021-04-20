package ethdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/prometheus/tsdb/fileutil"
)

var _ DbCopier = &LmdbKV{}

const (
	NonExistingDBI dbutils.DBI = 999_999_999
)

var (
	LMDBDefaultMapSize          = 2 * datasize.TB
	LMDBDefaultMaxFreelistReuse = uint(1000) // measured in pages
)

type BucketConfigsFunc func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg
type LmdbOpts struct {
	inMem      bool
	flags      uint
	path       string
	exclusive  bool
	bucketsCfg BucketConfigsFunc
	mapSize    datasize.ByteSize
}

func NewLMDB() LmdbOpts {
	return LmdbOpts{
		bucketsCfg: DefaultBucketConfigs,
		flags:      lmdb.NoReadahead, // do call .Sync manually after commit to measure speed of commit and speed of fsync individually
	}
}

func (opts LmdbOpts) Path(path string) LmdbOpts {
	opts.path = path
	return opts
}

func (opts LmdbOpts) Set(opt LmdbOpts) LmdbOpts {
	return opt
}

func (opts LmdbOpts) InMem() LmdbOpts {
	opts.inMem = true
	return opts
}

func (opts LmdbOpts) MapSize(sz datasize.ByteSize) LmdbOpts {
	opts.mapSize = sz
	return opts
}

func (opts LmdbOpts) Flags(f func(uint) uint) LmdbOpts {
	opts.flags = f(opts.flags)
	return opts
}

func (opts LmdbOpts) Readonly() LmdbOpts {
	opts.flags = opts.flags | lmdb.Readonly
	return opts
}

func (opts LmdbOpts) Exclusive() LmdbOpts {
	opts.exclusive = true
	return opts
}

func (opts LmdbOpts) WithBucketsConfig(f BucketConfigsFunc) LmdbOpts {
	opts.bucketsCfg = f
	return opts
}

func DefaultBucketConfigs(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	return defaultBuckets
}

func (opts LmdbOpts) Open() (kv RwKV, err error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = env.SetMaxReaders(ReadersLimit)
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
		opts.path, err = ioutil.TempDir(os.TempDir(), "lmdb")
		if err != nil {
			return nil, err
		}
	} else {
		logger = log.New("lmdb", path.Base(opts.path))
	}

	if opts.mapSize == 0 {
		if opts.inMem {
			opts.mapSize = 128 * datasize.MB
		} else {
			opts.mapSize = LMDBDefaultMapSize
		}
	}
	if err = env.SetMapSize(int64(opts.mapSize.Bytes())); err != nil {
		return nil, err
	}

	if err = env.SetMaxFreelistReuse(LMDBDefaultMaxFreelistReuse); err != nil {
		return nil, err
	}

	if opts.flags&lmdb.Readonly == 0 {
		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	var flags = opts.flags
	if opts.inMem {
		flags |= lmdb.NoMetaSync | lmdb.NoSync
	}

	var exclusiveLock fileutil.Releaser
	if opts.exclusive {
		exclusiveLock, _, err = fileutil.Flock(path.Join(opts.path, "LOCK"))
		if err != nil {
			return nil, fmt.Errorf("failed exclusive Flock for lmdb, path=%s: %w", opts.path, err)
		}
		defer func() { // if kv.Open() returns error - then kv.Close() will not called - just release lock
			if err != nil && exclusiveLock != nil {
				_ = exclusiveLock.Release()
			}
		}()
	} else { // try exclusive lock (release immediately)
		exclusiveLock, _, err = fileutil.Flock(path.Join(opts.path, "LOCK"))
		if err != nil {
			return nil, fmt.Errorf("failed exclusive Flock for lmdb, path=%s: %w", opts.path, err)
		}
		_ = exclusiveLock.Release()
	}

	db := &LmdbKV{
		exclusiveLock: exclusiveLock,
		opts:          opts,
		env:           env,
		log:           logger,
		wg:            &sync.WaitGroup{},
		buckets:       dbutils.BucketsCfg{},
	}

	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("%w, path: %s", err, opts.path)
	}

	customBuckets := opts.bucketsCfg(dbutils.BucketsConfigs)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	// Open or create buckets
	if opts.flags&lmdb.Readonly != 0 {
		tx, innerErr := db.BeginRo(context.Background())
		if innerErr != nil {
			return nil, innerErr
		}
		for name, cfg := range db.buckets {
			if cfg.IsDeprecated {
				continue
			}
			if err = tx.(BucketMigrator).CreateBucket(name); err != nil {
				return nil, err
			}
		}
		err = tx.Commit()
		if err != nil {
			return nil, err
		}
	} else {
		if err := db.Update(context.Background(), func(tx RwTx) error {
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

	// Configure buckets and open deprecated buckets
	if err := env.View(func(tx *lmdb.Txn) error {
		for name, cfg := range db.buckets {
			// Open deprecated buckets if they exist, don't create
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
			cnfCopy.DBI = dbutils.DBI(dbi)

			switch cnfCopy.CustomDupComparator {
			case dbutils.DupCmpSuffix32:
				if err := tx.SetDupCmpExcludeSuffix32(dbi); err != nil {
					return err
				}
			}
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

func (opts LmdbOpts) MustOpen() RwKV {
	db, err := opts.Open()
	if err != nil {
		panic(fmt.Errorf("fail to open lmdb: %w", err))
	}
	return db
}

type LmdbKV struct {
	opts          LmdbOpts
	env           *lmdb.Env
	log           log.Logger
	buckets       dbutils.BucketsCfg
	wg            *sync.WaitGroup
	exclusiveLock fileutil.Releaser
}

func (db *LmdbKV) NewDbWithTheSameParameters() *ObjectDatabase {
	opts := db.opts
	return NewObjectDatabase(NewLMDB().Set(opts).MustOpen())
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.env != nil {
		db.wg.Wait()
	}

	if db.exclusiveLock != nil {
		_ = db.exclusiveLock.Release()
	}

	if db.env != nil {
		env := db.env
		db.env = nil
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
	fileInfo, err := os.Stat(path.Join(db.opts.path, "data.mdb"))
	if err != nil {
		return 0, err
	}
	return uint64(fileInfo.Size()), nil
}

func (db *LmdbKV) CollectMetrics() {
	/*
		fileInfo, _ := os.Stat(path.Join(db.opts.path, "data.mdb"))
			dbSize.Update(fileInfo.Size())
			if err := db.View(context.Background(), func(tx Tx) error {
			stat, _ := tx.(*lmdbTx).BucketStat(dbutils.PlainStorageChangeSetBucket)
			tableScsLeaf.Update(int64(stat.LeafPages))
			tableScsBranch.Update(int64(stat.BranchPages))
			tableScsOverflow.Update(int64(stat.OverflowPages))
			tableScsEntries.Update(int64(stat.Entries))

			stat, _ = tx.(*lmdbTx).BucketStat(dbutils.PlainStateBucket)
			tableStateLeaf.Update(int64(stat.LeafPages))
			tableStateBranch.Update(int64(stat.BranchPages))
			tableStateOverflow.Update(int64(stat.OverflowPages))
			tableStateEntries.Update(int64(stat.Entries))

			stat, _ = tx.(*lmdbTx).BucketStat(dbutils.Log)
			tableLogLeaf.Update(int64(stat.LeafPages))
			tableLogBranch.Update(int64(stat.BranchPages))
			tableLogOverflow.Update(int64(stat.OverflowPages))
			tableLogEntries.Update(int64(stat.Entries))

			stat, _ = tx.(*lmdbTx).BucketStat(dbutils.EthTx)
			tableTxLeaf.Update(int64(stat.LeafPages))
			tableTxBranch.Update(int64(stat.BranchPages))
			tableTxOverflow.Update(int64(stat.OverflowPages))
			tableTxEntries.Update(int64(stat.Entries))

			stat, _ = tx.(*lmdbTx).BucketStat("gc")
			tableGcLeaf.Update(int64(stat.LeafPages))
			tableGcBranch.Update(int64(stat.BranchPages))
			tableGcOverflow.Update(int64(stat.OverflowPages))
			tableGcEntries.Update(int64(stat.Entries))
			return nil
		}); err != nil {
			log.Error("collecting metrics failed", "err", err)
		}
	*/
}

func (db *LmdbKV) BeginRo(_ context.Context) (txn Tx, err error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	defer func() {
		if err == nil {
			db.wg.Add(1)
		}
	}()

	tx, err := db.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return nil, err
	}
	tx.RawRead = true
	return &lmdbTx{
		db:       db,
		tx:       tx,
		readOnly: true,
	}, nil
}

func (db *LmdbKV) BeginRw(_ context.Context) (txn RwTx, err error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	runtime.LockOSThread()
	defer func() {
		if err == nil {
			db.wg.Add(1)
		}
	}()

	tx, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		return nil, err
	}
	tx.RawRead = true
	return &lmdbTx{
		db: db,
		tx: tx,
	}, nil
}

type lmdbTx struct {
	readOnly bool
	tx       *lmdb.Txn
	db       *LmdbKV
	cursors  []*lmdb.Cursor
}

type LmdbCursor struct {
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

func (db *LmdbKV) AllDBI() map[string]dbutils.DBI {
	res := map[string]dbutils.DBI{}
	for name, cfg := range db.buckets {
		res[name] = cfg.DBI
	}
	return res
}

func (db *LmdbKV) AllBuckets() dbutils.BucketsCfg {
	return db.buckets
}

func (tx *lmdbTx) Comparator(bucket string) dbutils.CmpFunc {
	b := tx.db.buckets[bucket]
	return chooseComparator(tx.tx, lmdb.DBI(b.DBI), b)
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
	c.Close()
	return res, nil
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	// can't use db.evn.View method - because it calls commit for read transactions - it conflicts with write transactions.
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx RwTx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = f(tx)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (tx *lmdbTx) CreateBucket(name string) error {
	var flags = tx.db.buckets[name].Flags
	var nativeFlags uint
	if tx.db.opts.flags&lmdb.Readonly == 0 {
		nativeFlags |= lmdb.Create
	}

	if flags&dbutils.DupSort != 0 {
		nativeFlags |= lmdb.DupSort
		flags ^= dbutils.DupSort
	}

	if flags != 0 {
		return fmt.Errorf("some not supported flag provided for bucket")
	}
	dbi, err := tx.tx.OpenDBI(name, nativeFlags)
	if err != nil {
		return err
	}
	cnfCopy := tx.db.buckets[name]
	cnfCopy.DBI = dbutils.DBI(dbi)

	switch cnfCopy.CustomDupComparator {
	case dbutils.DupCmpSuffix32:
		if err := tx.tx.SetDupCmpExcludeSuffix32(dbi); err != nil {
			return err
		}
	}

	tx.db.buckets[name] = cnfCopy

	return nil
}

func chooseComparator(tx *lmdb.Txn, dbi lmdb.DBI, cnfCopy dbutils.BucketConfigItem) dbutils.CmpFunc {
	if cnfCopy.CustomComparator == dbutils.DefaultCmp && cnfCopy.CustomDupComparator == dbutils.DefaultCmp {
		if cnfCopy.Flags&lmdb.DupSort == 0 {
			return dbutils.DefaultCmpFunc
		}
		return dbutils.DefaultDupCmpFunc
	}
	if cnfCopy.Flags&lmdb.DupSort == 0 {
		return CustomCmpFunc(tx, dbi)
	}
	return CustomDupCmpFunc(tx, dbi)
}

func CustomCmpFunc(tx *lmdb.Txn, dbi lmdb.DBI) dbutils.CmpFunc {
	return func(k1, k2, v1, v2 []byte) int {
		return tx.Cmp(dbi, k1, k2)
	}
}

func CustomDupCmpFunc(tx *lmdb.Txn, dbi lmdb.DBI) dbutils.CmpFunc {
	return func(k1, k2, v1, v2 []byte) int {
		cmp := tx.Cmp(dbi, k1, k2)
		if cmp == 0 {
			cmp = tx.DCmp(dbi, v1, v2)
		}
		return cmp
	}
}

func (tx *lmdbTx) dropEvenIfBucketIsNotDeprecated(name string) error {
	dbi := tx.db.buckets[name].DBI
	// if bucket was not open on db start, then it's may be deprecated
	// try to open it now without `Create` flag, and if fail then nothing to drop
	if dbi == NonExistingDBI {
		nativeDBI, err := tx.tx.OpenDBI(name, 0)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil // DBI doesn't exists means no drop needed
			}
			return err
		}
		dbi = dbutils.DBI(nativeDBI)
	}
	if err := tx.tx.Drop(lmdb.DBI(dbi), true); err != nil {
		return err
	}
	cnfCopy := tx.db.buckets[name]
	cnfCopy.DBI = NonExistingDBI
	tx.db.buckets[name] = cnfCopy
	return nil
}

func (tx *lmdbTx) ClearBucket(bucket string) error {
	dbi := tx.db.buckets[bucket].DBI
	if dbi == NonExistingDBI {
		return nil
	}
	return tx.tx.Drop(lmdb.DBI(dbi), false)
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

func (tx *lmdbTx) Commit() error {
	if tx.db.env == nil {
		return fmt.Errorf("db closed")
	}
	if tx.tx == nil {
		return nil
	}
	defer func() {
		tx.tx = nil
		tx.db.wg.Done()
		if !tx.readOnly {
			runtime.UnlockOSThread()
		}
	}()
	tx.closeCursors()

	commitTimer := time.Now()
	defer dbCommitBigBatchTimer.UpdateSince(commitTimer)

	if err := tx.tx.Commit(); err != nil {
		return err
	}
	commitTook := time.Since(commitTimer)
	if commitTook > 20*time.Second {
		log.Info("Batch", "commit", commitTook)
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
		tx.db.wg.Done()
		if !tx.readOnly {
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

func (tx *lmdbTx) Put(bucket string, k, v []byte) error {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion {
		c, err := tx.RwCursor(bucket)
		if err != nil {
			return err
		}
		return c.Put(k, v)
	}

	return tx.tx.Put(lmdb.DBI(b.DBI), k, v, 0)
}

func (tx *lmdbTx) Delete(bucket string, k, v []byte) error {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion {
		c, err := tx.RwCursor(bucket)
		if err != nil {
			return err
		}
		return c.Delete(k, v)
	}
	err := tx.tx.Del(lmdb.DBI(b.DBI), k, v)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

func (tx *lmdbTx) GetOne(bucket string, key []byte) ([]byte, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		c1, err := tx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		defer c1.Close()
		c := c1.(*LmdbCursor)
		v, err := c.getBothRange(key[:to], key[to:])
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

	val, err := tx.get(lmdb.DBI(b.DBI), key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (tx *lmdbTx) Has(bucket string, key []byte) (bool, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		c1, err := tx.Cursor(bucket)
		if err != nil {
			return false, err
		}
		defer c1.Close()
		c := c1.(*LmdbCursor)
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if lmdb.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return bytes.Equal(key[to:], v[:from-to]), nil
	}

	if _, err := tx.get(lmdb.DBI(b.DBI), key); err == nil {
		return true, nil
	} else if lmdb.IsNotFound(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (tx *lmdbTx) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	c, _ := tx.RwCursor(dbutils.Sequence)
	defer c.Close()
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil && !lmdb.IsNotFound(err) {
		return 0, err
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	err = c.Put([]byte(bucket), newVBytes)
	if err != nil {
		return 0, err
	}
	return currentV, nil
}

func (tx *lmdbTx) ReadSequence(bucket string) (uint64, error) {
	c, _ := tx.Cursor(dbutils.Sequence)
	defer c.Close()
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil && !lmdb.IsNotFound(err) {
		return 0, err
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

func (tx *lmdbTx) BucketSize(name string) (uint64, error) {
	st, err := tx.tx.Stat(lmdb.DBI(tx.db.buckets[name].DBI))
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (tx *lmdbTx) BucketStat(name string) (*lmdb.Stat, error) {
	if name == "freelist" || name == "gc" || name == "free_list" { //nolint:goconst
		return tx.tx.Stat(lmdb.DBI(0))
	}
	if name == "root" { //nolint:goconst
		return tx.tx.Stat(lmdb.DBI(1))
	}
	return tx.tx.Stat(lmdb.DBI(tx.db.buckets[name].DBI))
}

func (tx *lmdbTx) RwCursor(bucket string) (RwCursor, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion {
		return tx.stdCursor(bucket)
	}

	if b.Flags&dbutils.DupSort != 0 {
		return tx.RwCursorDupSort(bucket)
	}

	return tx.stdCursor(bucket)
}

func (tx *lmdbTx) Cursor(bucket string) (Cursor, error) {
	c, _ := tx.RwCursor(bucket)
	return c, nil
}

func (tx *lmdbTx) stdCursor(bucket string) (RwCursor, error) {
	b := tx.db.buckets[bucket]
	c := &LmdbCursor{bucketName: bucket, tx: tx, bucketCfg: b, dbi: lmdb.DBI(tx.db.buckets[bucket].DBI)}

	var err error
	c.c, err = tx.tx.OpenCursor(c.dbi)
	if err != nil {
		return nil, fmt.Errorf("table: %s, %w", c.bucketName, err)
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c.c)
	return c, nil
}

func (tx *lmdbTx) RwCursorDupSort(bucket string) (RwCursorDupSort, error) {
	basicCursor, err := tx.stdCursor(bucket)
	if err != nil {
		return nil, err
	}
	return &LmdbDupSortCursor{LmdbCursor: basicCursor.(*LmdbCursor)}, nil
}

func (tx *lmdbTx) CursorDupSort(bucket string) (CursorDupSort, error) {
	return tx.RwCursorDupSort(bucket)
}

func (tx *lmdbTx) CHandle() unsafe.Pointer {
	return tx.tx.CHandle()
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
func (c *LmdbCursor) delCurrent() error                    { return c.c.Del(lmdb.Current) }
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
func (c *LmdbCursor) getBothRange(k, v []byte) ([]byte, error) {
	_, v, err := c.c.Get(k, v, lmdb.GetBothRange)
	return v, err
}
func (c *LmdbCursor) firstDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, lmdb.FirstDup)
	return v, err
}
func (c *LmdbCursor) lastDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, lmdb.LastDup)
	return v, err
}

func (c *LmdbCursor) Count() (uint64, error) {
	st, err := c.tx.tx.Stat(c.dbi)
	if err != nil {
		return 0, err
	}
	return st.Entries, nil
}

func (c *LmdbCursor) First() ([]byte, []byte, error) {
	return c.Seek(c.prefix)
}

func (c *LmdbCursor) Last() ([]byte, []byte, error) {
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
		if len(k) == to {
			k2 := make([]byte, 0, len(k)+from-to)
			k2 = append(append(k2, k...), v[:from-to]...)
			v = v[from-to:]
			k = k2
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
		v, err = c.getBothRange(seek1, seek2)
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

func (c *LmdbCursor) Delete(k, v []byte) error {
	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.deleteDupSort(k)
	}

	if c.bucketCfg.Flags&lmdb.DupSort != 0 {
		_, _, err := c.getBoth(k, v)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		return c.delCurrent()
	}

	_, _, err := c.set(k)
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
	return c.delCurrent()
}

func (c *LmdbCursor) deleteDupSort(key []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, key, len(key))
	}

	if len(key) == from {
		v, err := c.getBothRange(key[:to], key[to:])
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

	if c.bucketCfg.AutoDupSortKeysConversion {
		panic("not implemented")
	}

	return c.putNoOverwrite(key, value)
}

func (c *LmdbCursor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion {
		if err := c.putDupSort(key, value); err != nil {
			return err
		}
		return nil
	}
	if err := c.put(key, value); err != nil {
		return err
	}
	return nil
}

func (c *LmdbCursor) putDupSort(key []byte, value []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, key, len(key))
	}

	if len(key) != from {
		err := c.putNoOverwrite(key, value)
		if err != nil {
			if lmdb.IsKeyExists(err) {
				return c.putCurrent(key, value)
			}
			return err
		}
		return nil
	}

	value = append(key[to:], value...)
	key = key[:to]
	v, err := c.getBothRange(key, value[:from-to])
	if err != nil { // if key not found, or found another one - then just insert
		if lmdb.IsNotFound(err) {
			return c.put(key, value)
		}
		return fmt.Errorf("getBothRange bucket: %s, %w", c.bucketName, err)
	}

	if bytes.Equal(v[:from-to], value[:from-to]) {
		if len(v) == len(value) { // in DupSort case lmdb.Current works only with values of same length
			return c.putCurrent(key, value)
		}
		err = c.delCurrent()
		if err != nil {
			return fmt.Errorf("delCurrent bucket: %s, %w", c.bucketName, err)
		}
	}

	return c.put(key, value)
}

func (c *LmdbCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, err
		}
		if !bytes.Equal(key[to:], v[:from-to]) {
			return nil, nil, nil
		}
		return key[:to], v[from-to:], nil
	}

	k, v, err := c.set(key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Return error - if provided data will not sorted (or bucket have old records which mess with new in sorting manner).
func (c *LmdbCursor) Append(k []byte, v []byte) error {
	if len(k) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucketName)
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion {
		from, to := b.DupFromLen, b.DupToLen
		if len(k) != from && len(k) >= to {
			return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, k, len(k))
		}

		if len(k) == from {
			v = append(k[to:], v...)
			k = k[:to]
		}
	}

	if b.Flags&lmdb.DupSort != 0 {
		return c.appendDup(k, v)
	}

	return c.append(k, v)
}

func (c *LmdbCursor) Close() {
	if c.c != nil {
		c.c.Close()
		l := len(c.tx.cursors)
		if l == 0 {
			c.c = nil
			return
		}
		//TODO: Find a better solution to avoid the leak?
		newCursors := make([]*lmdb.Cursor, l-1)
		i := 0
		for _, cc := range c.tx.cursors {
			if cc != c.c {
				newCursors[i] = cc
				i++
			}
		}
		c.tx.cursors = newCursors
		c.c = nil
	}
}

type LmdbDupSortCursor struct {
	*LmdbCursor
}

// DeleteExact - does delete
func (c *LmdbDupSortCursor) DeleteExact(k1, k2 []byte) error {
	_, _, err := c.getBoth(k1, k2)
	if err != nil { // if key not found, or found another one - then nothing to delete
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}
	return c.delCurrent()
}

func (c *LmdbDupSortCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	k, v, err := c.getBoth(key, value)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in SeekBothExact: %w", err)
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	v, err := c.getBothRange(key, value)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in SeekBothRange: %w", err)
	}
	return v, nil
}

func (c *LmdbDupSortCursor) FirstDup() ([]byte, error) {
	v, err := c.firstDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in FirstDup: %w", err)
	}
	return v, nil
}

// NextDup - iterate only over duplicates of current key
func (c *LmdbDupSortCursor) NextDup() ([]byte, []byte, error) {
	k, v, err := c.nextDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextDup: %w", err)
	}
	return k, v, nil
}

// NextNoDup - iterate with skipping all duplicates
func (c *LmdbDupSortCursor) NextNoDup() ([]byte, []byte, error) {
	k, v, err := c.nextNoDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextNoDup: %w", err)
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) PrevDup() ([]byte, []byte, error) {
	k, v, err := c.prevDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevDup: %w", err)
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) PrevNoDup() ([]byte, []byte, error) {
	k, v, err := c.prevNoDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevNoDup: %w", err)
	}
	return k, v, nil
}

func (c *LmdbDupSortCursor) LastDup() ([]byte, error) {
	v, err := c.lastDup()
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in LastDup: %w", err)
	}
	return v, nil
}

func (c *LmdbDupSortCursor) Append(k []byte, v []byte) error {
	if err := c.c.Put(k, v, lmdb.Append|lmdb.AppendDup); err != nil {
		return fmt.Errorf("in Append: %w", err)
	}
	return nil
}

func (c *LmdbDupSortCursor) AppendDup(k []byte, v []byte) error {
	if err := c.appendDup(k, v); err != nil {
		return fmt.Errorf("in AppendDup: %w", err)
	}
	return nil
}

func (c *LmdbDupSortCursor) PutNoDupData(key, value []byte) error {
	if err := c.putNoDupData(key, value); err != nil {
		return fmt.Errorf("in PutNoDupData: %w", err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *LmdbDupSortCursor) DeleteCurrentDuplicates() error {
	if err := c.delNoDupData(); err != nil {
		return fmt.Errorf("in DeleteCurrentDuplicates: %w", err)
	}
	return nil
}

// Count returns the number of duplicates for the current key. See mdb_cursor_count
func (c *LmdbDupSortCursor) CountDuplicates() (uint64, error) {
	res, err := c.c.Count()
	if err != nil {
		return 0, fmt.Errorf("in CountDuplicates: %w", err)
	}
	return res, nil
}
