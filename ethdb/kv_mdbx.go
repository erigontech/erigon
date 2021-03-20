//+build mdbx

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
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/ethdb/mdbx"
	"github.com/ledgerwatch/turbo-geth/log"
)

var _ DbCopier = &MdbxKV{}

type MdbxOpts struct {
	inMem             bool
	exclusive         bool
	flags             uint
	path              string
	bucketsCfg        BucketConfigsFunc
	mapSize           datasize.ByteSize
	dirtyListMaxPages uint64
	maxFreelistReuse  uint
}

func NewMDBX() MdbxOpts {
	return MdbxOpts{
		bucketsCfg:        DefaultBucketConfigs,
		flags:             mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable, // | mdbx.LifoReclaim,
		dirtyListMaxPages: 128 * 1024,
	}
}

func (opts MdbxOpts) Path(path string) MdbxOpts {
	opts.path = path
	return opts
}

func (opts MdbxOpts) Set(opt MdbxOpts) MdbxOpts {
	return opt
}

func (opts MdbxOpts) InMem() MdbxOpts {
	opts.inMem = true
	return opts
}

func (opts MdbxOpts) Exclusive() MdbxOpts {
	opts.exclusive = true
	return opts
}

func (opts MdbxOpts) Flags(f func(uint) uint) MdbxOpts {
	opts.flags = f(opts.flags)
	return opts
}

func (opts MdbxOpts) MapSize(sz datasize.ByteSize) MdbxOpts {
	opts.mapSize = sz
	return opts
}

func (opts MdbxOpts) MaxFreelistReuse(pages uint) MdbxOpts {
	opts.maxFreelistReuse = pages
	return opts
}

func (opts MdbxOpts) WithBucketsConfig(f BucketConfigsFunc) MdbxOpts {
	opts.bucketsCfg = f
	return opts
}

func (opts MdbxOpts) Open() (KV, error) {
	var logger log.Logger
	var err error
	if opts.inMem {
		logger = log.New("mdbx", "inMem")
		opts.path, err = ioutil.TempDir(os.TempDir(), "mdbx")
		if err != nil {
			return nil, err
		}
	} else {
		logger = log.New("mdbx", path.Base(opts.path))
	}

	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	//_ = env.SetDebug(mdbx.LogLvlExtra, mdbx.DbgAssert, mdbx.LoggerDoNotChange) // temporary disable error, because it works if call it 1 time, but returns error if call it twice in same process (what often happening in tests)

	if err = env.SetMaxDBs(100); err != nil {
		return nil, err
	}
	if err = env.SetOption(mdbx.OptMaxReaders, 256); err != nil {
		return nil, err
	}

	if opts.flags&mdbx.Accede == 0 {
		if opts.mapSize == 0 {
			if opts.inMem {
				opts.mapSize = 64 * datasize.MB
			} else {
				opts.mapSize = LMDBDefaultMapSize
			}
		}

		if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(2*datasize.GB), -1, 4*1024); err != nil {
			return nil, err
		}

		if err = env.SetOption(mdbx.OptRpAugmentLimit, 32*1024*1024); err != nil {
			return nil, err
		}

		if opts.maxFreelistReuse == 0 {
			opts.maxFreelistReuse = LMDBDefaultMaxFreelistReuse
		}

		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	var flags = opts.flags
	if opts.inMem {
		flags ^= mdbx.Durable
		flags |= mdbx.NoMetaSync | mdbx.SafeNoSync
		opts.dirtyListMaxPages = 8 * 1024
	}

	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("%w, path: %s", err, opts.path)
	}

	if opts.flags&mdbx.Accede == 0 {
		// 1/8 is good for transactions with a lot of modifications - to reduce invalidation size.
		// But TG app now using Batch and etl.Collectors to avoid writing to DB frequently changing data.
		// It means most of our writes are: APPEND or "single UPSERT per key during transaction"
		/*
			if err = env.SetOption(mdbx.OptSpillMinDenominator, 8); err != nil {
				return nil, err
			}
			if err = env.SetOption(mdbx.OptTxnDpInitial, 4*1024); err != nil {
				return nil, err
			}
			if err = env.SetOption(mdbx.OptDpReverseLimit, 4*1024); err != nil {
				return nil, err
			}
			if err = env.SetOption(mdbx.OptTxnDpLimit, opts.dirtyListMaxPages); err != nil {
				return nil, err
			}
		*/
	}

	db := &MdbxKV{
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
	if opts.flags&mdbx.Readonly != 0 {
		tx, innerErr := db.Begin(context.Background(), RO)
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
		err = tx.Commit(context.Background())
		if err != nil {
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

	// Configure buckets and open deprecated buckets
	if err := env.View(func(tx *mdbx.Txn) error {
		for name, cfg := range db.buckets {
			// Open deprecated buckets if they exist, don't create
			if !cfg.IsDeprecated {
				continue
			}
			cnfCopy := db.buckets[name]
			var dcmp mdbx.CmpFunc
			switch cnfCopy.CustomDupComparator {
			case dbutils.DupCmpSuffix32:
				dcmp = tx.GetCmpExcludeSuffix32()
			}

			dbi, createErr := tx.OpenDBI(name, mdbx.DBAccede, nil, dcmp)
			if createErr != nil {
				if mdbx.IsNotFound(createErr) {
					cnfCopy.DBI = NonExistingDBI
					db.buckets[name] = cnfCopy
					continue // if deprecated bucket couldn't be open - then it's deleted and it's fine
				} else {
					return fmt.Errorf("bucket: %s, %w", name, createErr)
				}
			}
			cnfCopy.DBI = dbutils.DBI(dbi)
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

func (opts MdbxOpts) MustOpen() KV {
	db, err := opts.Open()
	if err != nil {
		panic(fmt.Errorf("fail to open mdbx: %w", err))
	}
	return db
}

type MdbxKV struct {
	opts    MdbxOpts
	env     *mdbx.Env
	log     log.Logger
	buckets dbutils.BucketsCfg
	wg      *sync.WaitGroup
}

func (db *MdbxKV) NewDbWithTheSameParameters() *ObjectDatabase {
	opts := db.opts
	return NewObjectDatabase(NewMDBX().Set(opts).MustOpen())
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *MdbxKV) Close() {
	if db.env != nil {
		db.wg.Wait()
	}

	if db.env != nil {
		env := db.env
		db.env = nil
		if err := env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed (MDBX)")
		}
	}

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}

}

func (db *MdbxKV) DiskSize(_ context.Context) (uint64, error) {
	fileInfo, err := os.Stat(path.Join(db.opts.path, "mdbx.dat"))
	if err != nil {
		return 0, err
	}
	return uint64(fileInfo.Size()), nil
}

func (db *MdbxKV) CollectMetrics() {
	info, _ := db.env.Info()
	dbSize.Update(int64(info.Geo.Current))

	if err := db.View(context.Background(), func(tx Tx) error {
		stat, _ := tx.(*MdbxTx).BucketStat(dbutils.PlainStorageChangeSetBucket)
		tableScsLeaf.Update(int64(stat.LeafPages))
		tableScsBranch.Update(int64(stat.BranchPages))
		tableScsOverflow.Update(int64(stat.OverflowPages))
		tableScsEntries.Update(int64(stat.Entries))

		stat, _ = tx.(*MdbxTx).BucketStat(dbutils.PlainStateBucket)
		tableStateLeaf.Update(int64(stat.LeafPages))
		tableStateBranch.Update(int64(stat.BranchPages))
		tableStateOverflow.Update(int64(stat.OverflowPages))
		tableStateEntries.Update(int64(stat.Entries))

		stat, _ = tx.(*MdbxTx).BucketStat(dbutils.Log)
		tableLogLeaf.Update(int64(stat.LeafPages))
		tableLogBranch.Update(int64(stat.BranchPages))
		tableLogOverflow.Update(int64(stat.OverflowPages))
		tableLogEntries.Update(int64(stat.Entries))

		stat, _ = tx.(*MdbxTx).BucketStat(dbutils.EthTx)
		tableTxLeaf.Update(int64(stat.LeafPages))
		tableTxBranch.Update(int64(stat.BranchPages))
		tableTxOverflow.Update(int64(stat.OverflowPages))
		tableTxEntries.Update(int64(stat.Entries))

		stat, _ = tx.(*MdbxTx).BucketStat("gc")
		tableGcLeaf.Update(int64(stat.LeafPages))
		tableGcBranch.Update(int64(stat.BranchPages))
		tableGcOverflow.Update(int64(stat.OverflowPages))
		tableGcEntries.Update(int64(stat.Entries))
		return nil
	}); err != nil {
		panic(err)
	}
}

func (db *MdbxKV) Begin(_ context.Context, flags TxFlags) (txn Tx, err error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	runtime.LockOSThread()
	defer func() {
		if err == nil {
			db.wg.Add(1)
		}
	}()

	var ro bool
	nativeFlags := uint(0)
	if flags&RO != 0 {
		ro = true
		nativeFlags |= mdbx.Readonly
	}
	if flags&NoSync != 0 {
		nativeFlags |= mdbx.TxNoSync
	}

	tx, err := db.env.BeginTxn(nil, nativeFlags)
	if err != nil {
		runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		return nil, err
	}
	tx.RawRead = true
	return &MdbxTx{
		db:       db,
		tx:       tx,
		readOnly: ro,
	}, nil
}

type MdbxTx struct {
	readOnly bool
	tx       *mdbx.Txn
	db       *MdbxKV
	cursors  []*mdbx.Cursor
}

type MdbxCursor struct {
	tx         *MdbxTx
	bucketName string
	dbi        mdbx.DBI
	bucketCfg  dbutils.BucketConfigItem
	prefix     []byte

	c *mdbx.Cursor
}

func (db *MdbxKV) Env() *mdbx.Env {
	return db.env
}

func (db *MdbxKV) AllDBI() map[string]dbutils.DBI {
	res := map[string]dbutils.DBI{}
	for name, cfg := range db.buckets {
		res[name] = cfg.DBI
	}
	return res
}

func (db *MdbxKV) AllBuckets() dbutils.BucketsCfg {
	return db.buckets
}

func (tx *MdbxTx) Comparator(bucket string) dbutils.CmpFunc {
	b := tx.db.buckets[bucket]
	return chooseComparator2(tx.tx, mdbx.DBI(b.DBI), b)
}

// All buckets stored as keys of un-named bucket
func (tx *MdbxTx) ExistingBuckets() ([]string, error) {
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
	for k, _, _ := c.Get(nil, nil, mdbx.First); k != nil; k, _, _ = c.Get(nil, nil, mdbx.Next) {
		res = append(res, string(k))
	}
	return res, nil
}

func (db *MdbxKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	// can't use db.evn.View method - because it calls commit for read transactions - it conflicts with write transactions.
	tx, err := db.Begin(ctx, RO)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *MdbxKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()

	tx, err := db.Begin(ctx, RW)
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

func (tx *MdbxTx) CreateBucket(name string) error {
	cnfCopy := tx.db.buckets[name]

	var dcmp mdbx.CmpFunc
	switch cnfCopy.CustomDupComparator {
	case dbutils.DupCmpSuffix32:
		dcmp = tx.tx.GetCmpExcludeSuffix32()
	}

	dbi, err := tx.tx.OpenDBI(name, mdbx.DBAccede, nil, dcmp)
	if err != nil && !mdbx.IsNotFound(err) {
		return fmt.Errorf("create bucket: %s, %w", name, err)
	}
	if err == nil {
		cnfCopy.DBI = dbutils.DBI(dbi)
		var flags uint
		flags, err = tx.tx.Flags(dbi)
		if err != nil {
			return err
		}
		cnfCopy.Flags = dbutils.BucketFlags(flags)

		tx.db.buckets[name] = cnfCopy
		return nil
	}

	// if bucket doesn't exists - create it

	var flags = tx.db.buckets[name].Flags
	var nativeFlags uint
	if tx.db.opts.flags&mdbx.Readonly == 0 {
		nativeFlags |= mdbx.Create
	}

	if flags&dbutils.DupSort != 0 {
		nativeFlags |= mdbx.DupSort
		flags ^= dbutils.DupSort
	}
	if flags != 0 {
		return fmt.Errorf("some not supported flag provided for bucket")
	}

	dbi, err = tx.tx.OpenDBI(name, nativeFlags, nil, dcmp)
	if err != nil {
		return fmt.Errorf("create bucket: %s, %w", name, err)
	}
	cnfCopy.DBI = dbutils.DBI(dbi)

	tx.db.buckets[name] = cnfCopy
	return nil
}

func chooseComparator2(tx *mdbx.Txn, dbi mdbx.DBI, cnfCopy dbutils.BucketConfigItem) dbutils.CmpFunc {
	if cnfCopy.CustomComparator == dbutils.DefaultCmp && cnfCopy.CustomDupComparator == dbutils.DefaultCmp {
		if cnfCopy.Flags&mdbx.DupSort == 0 {
			return dbutils.DefaultCmpFunc
		}
		return dbutils.DefaultDupCmpFunc
	}
	if cnfCopy.Flags&mdbx.DupSort == 0 {
		return CustomCmpFunc2(tx, dbi)
	}
	return CustomDupCmpFunc2(tx, dbi)
}

func CustomCmpFunc2(tx *mdbx.Txn, dbi mdbx.DBI) dbutils.CmpFunc {
	return func(k1, k2, v1, v2 []byte) int {
		return tx.Cmp(dbi, k1, k2)
	}
}

func CustomDupCmpFunc2(tx *mdbx.Txn, dbi mdbx.DBI) dbutils.CmpFunc {
	return func(k1, k2, v1, v2 []byte) int {
		cmp := tx.Cmp(dbi, k1, k2)
		if cmp == 0 {
			cmp = tx.DCmp(dbi, v1, v2)
		}
		return cmp
	}
}

func (tx *MdbxTx) dropEvenIfBucketIsNotDeprecated(name string) error {
	dbi := tx.db.buckets[name].DBI
	// if bucket was not open on db start, then it's may be deprecated
	// try to open it now without `Create` flag, and if fail then nothing to drop
	if dbi == NonExistingDBI {
		nativeDBI, err := tx.tx.OpenDBI(name, 0, nil, nil)
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil // DBI doesn't exists means no drop needed
			}
			return fmt.Errorf("bucket: %s, %w", name, err)
		}
		dbi = dbutils.DBI(nativeDBI)
	}

	if err := tx.tx.Drop(mdbx.DBI(dbi), true); err != nil {
		return err
	}
	cnfCopy := tx.db.buckets[name]
	cnfCopy.DBI = NonExistingDBI
	tx.db.buckets[name] = cnfCopy
	return nil
}

func (tx *MdbxTx) ClearBucket(bucket string) error {
	dbi := tx.db.buckets[bucket].DBI
	if dbi == NonExistingDBI {
		return nil
	}
	return tx.tx.Drop(mdbx.DBI(dbi), false)
}

func (tx *MdbxTx) DropBucket(bucket string) error {
	if cfg, ok := tx.db.buckets[bucket]; !(ok && cfg.IsDeprecated) {
		return fmt.Errorf("%w, bucket: %s", ErrAttemptToDeleteNonDeprecatedBucket, bucket)
	}

	return tx.dropEvenIfBucketIsNotDeprecated(bucket)
}

func (tx *MdbxTx) ExistsBucket(bucket string) bool {
	if cfg, ok := tx.db.buckets[bucket]; ok {
		return cfg.DBI != NonExistingDBI
	}
	return false
}

func (tx *MdbxTx) Commit(ctx context.Context) error {
	if tx.db.env == nil {
		return fmt.Errorf("db closed")
	}
	if tx.tx == nil {
		return nil
	}
	defer func() {
		tx.tx = nil
		tx.db.wg.Done()
		runtime.UnlockOSThread()
	}()
	tx.closeCursors()

	slowTx := 10 * time.Second
	if debug.SlowCommit() > 0 {
		slowTx = debug.SlowCommit()
	}

	tx.printDebugInfo()

	latency, err := tx.tx.Commit()
	if err != nil {
		return err
	}

	if latency.Whole > slowTx {
		log.Info("Commit",
			"preparation", latency.Preparation,
			"gc", latency.GC,
			"audit", latency.Audit,
			"write", latency.Write,
			"fsync", latency.Sync,
			"ending", latency.Ending,
			"whole", latency.Whole,
		)
	}

	return nil
}

func (tx *MdbxTx) Rollback() {
	if tx.db.env == nil {
		return
	}
	if tx.tx == nil {
		return
	}
	defer func() {
		tx.tx = nil
		tx.db.wg.Done()
		runtime.UnlockOSThread()
	}()
	tx.closeCursors()
	tx.printDebugInfo()
	tx.tx.Abort()
}

func (tx *MdbxTx) printDebugInfo() {
	if debug.BigRoTxKb() > 0 || debug.BigRwTxKb() > 0 {
		txInfo, err := tx.tx.Info(true)
		if err != nil {
			panic(err)
		}

		txSize := uint(txInfo.SpaceDirty / 1024)
		doPrint := tx.readOnly && debug.BigRoTxKb() > 0 && txSize > debug.BigRoTxKb()
		doPrint = doPrint || (!tx.readOnly && debug.BigRwTxKb() > 0 && txSize > debug.BigRwTxKb())
		if doPrint {
			log.Info("Tx info",
				"id", txInfo.Id,
				"read_lag", txInfo.ReadLag,
				"ro", tx.readOnly,
				//"space_retired_mb", txInfo.SpaceRetired/1024/1024,
				"space_dirty_kb", txInfo.SpaceDirty/1024,
				"callers", debug.Callers(7),
			)
		}
	}
}

func (tx *MdbxTx) get(dbi mdbx.DBI, key []byte) ([]byte, error) {
	return tx.tx.Get(dbi, key)
}

func (tx *MdbxTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = []*mdbx.Cursor{}
}

func (c *MdbxCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *MdbxCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (tx *MdbxTx) GetOne(bucket string, key []byte) ([]byte, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		c := tx.Cursor(bucket).(*MdbxCursor)
		if err := c.initCursor(); err != nil {
			return nil, err
		}
		defer c.Close()
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if !bytes.Equal(key[to:], v[:from-to]) {
			return nil, nil
		}
		return v[from-to:], nil
	}

	val, err := tx.get(mdbx.DBI(b.DBI), key)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (tx *MdbxTx) HasOne(bucket string, key []byte) (bool, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		c := tx.Cursor(bucket).(*MdbxCursor)
		if err := c.initCursor(); err != nil {
			return false, err
		}
		defer c.Close()
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if mdbx.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return bytes.Equal(key[to:], v[:from-to]), nil
	}

	if _, err := tx.get(mdbx.DBI(b.DBI), key); err == nil {
		return true, nil
	} else if mdbx.IsNotFound(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (tx *MdbxTx) Sequence(bucket string, amount uint64) (uint64, error) {
	// non-native for now
	// return tx.tx.Sequence(mdbx.DBI(tx.db.buckets[bucket].DBI), amount)

	c := tx.Cursor(dbutils.Sequence)
	defer c.Close()
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil && !mdbx.IsNotFound(err) {
		return 0, err
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	if amount == 0 {
		return currentV, nil
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	err = c.Put([]byte(bucket), newVBytes)
	if err != nil {
		return 0, err
	}
	return currentV, nil
}

func (tx *MdbxTx) BucketSize(name string) (uint64, error) {
	st, err := tx.BucketStat(name)
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (tx *MdbxTx) BucketStat(name string) (*mdbx.Stat, error) {
	if name == "freelist" || name == "gc" || name == "free_list" {
		return tx.tx.StatDBI(mdbx.DBI(0))
	}
	if name == "root" {
		return tx.tx.StatDBI(mdbx.DBI(1))
	}
	st, err := tx.tx.StatDBI(mdbx.DBI(tx.db.buckets[name].DBI))
	if err != nil {
		return nil, fmt.Errorf("bucket: %s, %w", name, err)
	}
	return st, nil
}

func (tx *MdbxTx) Cursor(bucket string) Cursor {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion {
		return tx.stdCursor(bucket)
	}

	if b.Flags&dbutils.DupSort != 0 {
		return tx.CursorDupSort(bucket)
	}

	return tx.stdCursor(bucket)
}

func (tx *MdbxTx) stdCursor(bucket string) Cursor {
	b := tx.db.buckets[bucket]
	return &MdbxCursor{bucketName: bucket, tx: tx, bucketCfg: b, dbi: mdbx.DBI(tx.db.buckets[bucket].DBI)}
}

func (tx *MdbxTx) CursorDupSort(bucket string) CursorDupSort {
	basicCursor := tx.stdCursor(bucket).(*MdbxCursor)
	return &MdbxDupSortCursor{MdbxCursor: basicCursor}
}

func (tx *MdbxTx) CHandle() unsafe.Pointer {
	panic("not implemented yet")
}

// methods here help to see better pprof picture
func (c *MdbxCursor) set(k []byte) ([]byte, []byte, error) { return c.c.Get(k, nil, mdbx.Set) }
func (c *MdbxCursor) getCurrent() ([]byte, []byte, error)  { return c.c.Get(nil, nil, mdbx.GetCurrent) }
func (c *MdbxCursor) first() ([]byte, []byte, error)       { return c.c.Get(nil, nil, mdbx.First) }
func (c *MdbxCursor) next() ([]byte, []byte, error)        { return c.c.Get(nil, nil, mdbx.Next) }
func (c *MdbxCursor) nextDup() ([]byte, []byte, error)     { return c.c.Get(nil, nil, mdbx.NextDup) }
func (c *MdbxCursor) nextNoDup() ([]byte, []byte, error)   { return c.c.Get(nil, nil, mdbx.NextNoDup) }
func (c *MdbxCursor) prev() ([]byte, []byte, error)        { return c.c.Get(nil, nil, mdbx.Prev) }
func (c *MdbxCursor) prevDup() ([]byte, []byte, error)     { return c.c.Get(nil, nil, mdbx.PrevDup) }
func (c *MdbxCursor) prevNoDup() ([]byte, []byte, error)   { return c.c.Get(nil, nil, mdbx.PrevNoDup) }
func (c *MdbxCursor) last() ([]byte, []byte, error)        { return c.c.Get(nil, nil, mdbx.Last) }
func (c *MdbxCursor) delCurrent() error                    { return c.c.Del(mdbx.Current) }
func (c *MdbxCursor) delNoDupData() error                  { return c.c.Del(mdbx.NoDupData) }
func (c *MdbxCursor) put(k, v []byte) error                { return c.c.Put(k, v, 0) }
func (c *MdbxCursor) putCurrent(k, v []byte) error         { return c.c.Put(k, v, mdbx.Current) }
func (c *MdbxCursor) putNoOverwrite(k, v []byte) error     { return c.c.Put(k, v, mdbx.NoOverwrite) }
func (c *MdbxCursor) putNoDupData(k, v []byte) error       { return c.c.Put(k, v, mdbx.NoDupData) }
func (c *MdbxCursor) append(k, v []byte) error             { return c.c.Put(k, v, mdbx.Append) }
func (c *MdbxCursor) appendDup(k, v []byte) error          { return c.c.Put(k, v, mdbx.AppendDup) }
func (c *MdbxCursor) getBoth(k, v []byte) ([]byte, error) {
	_, v, err := c.c.Get(k, v, mdbx.GetBoth)
	return v, err
}
func (c *MdbxCursor) setRange(k []byte) ([]byte, []byte, error) {
	return c.c.Get(k, nil, mdbx.SetRange)
}
func (c *MdbxCursor) getBothRange(k, v []byte) ([]byte, error) {
	_, v, err := c.c.Get(k, v, mdbx.GetBothRange)
	return v, err
}
func (c *MdbxCursor) firstDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, mdbx.FirstDup)
	return v, err
}
func (c *MdbxCursor) lastDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, mdbx.LastDup)
	return v, err
}

func (c *MdbxCursor) initCursor() error {
	if c.c != nil {
		return nil
	}
	tx := c.tx

	var err error
	c.c, err = tx.tx.OpenCursor(c.dbi)
	if err != nil {
		return fmt.Errorf("table: %s, %w", c.bucketName, err)
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*mdbx.Cursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c.c)
	return nil
}

func (c *MdbxCursor) Count() (uint64, error) {
	st, err := c.tx.tx.StatDBI(c.dbi)
	if err != nil {
		return 0, err
	}
	return st.Entries, nil
}

func (c *MdbxCursor) First() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	return c.Seek(c.prefix)
}

func (c *MdbxCursor) Last() ([]byte, []byte, error) {
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
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed MdbxKV cursor.Last(): %w, bucket: %s", err, c.bucketName)
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

func (c *MdbxCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.c == nil {
		if err1 := c.initCursor(); err1 != nil {
			return []byte{}, nil, err1
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
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed MdbxKV cursor.Seek(): %w, bucket: %s,  key: %x", err, c.bucketName, seek)
		return []byte{}, nil, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *MdbxCursor) seekDupSort(seek []byte) (k, v []byte, err error) {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(seek) == 0 {
		k, v, err = c.first()
		if err != nil {
			if mdbx.IsNotFound(err) {
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
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}

		return []byte{}, nil, err
	}

	if seek2 != nil && bytes.Equal(seek1, k) {
		v, err = c.getBothRange(seek1, seek2)
		if err != nil && mdbx.IsNotFound(err) {
			k, v, err = c.next()
			if err != nil {
				if mdbx.IsNotFound(err) {
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

func (c *MdbxCursor) Next() (k, v []byte, err error) {
	if c.c == nil {
		if err = c.initCursor(); err != nil {
			log.Error("init cursor", "err", err)
		}
	}

	k, v, err = c.next()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Next(): %w", err)
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

func (c *MdbxCursor) Prev() (k, v []byte, err error) {
	if c.c == nil {
		if err = c.initCursor(); err != nil {
			log.Error("init cursor", "err", err)
		}
	}

	k, v, err = c.prev()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Prev(): %w", err)
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
func (c *MdbxCursor) Current() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.getCurrent()
	if err != nil {
		if mdbx.IsNotFound(err) {
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

func (c *MdbxCursor) Delete(k, v []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.deleteDupSort(k)
	}

	if c.bucketCfg.Flags&mdbx.DupSort != 0 {
		_, err := c.getBoth(k, v)
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil
			}
			return err
		}
		return c.delCurrent()
	}

	_, _, err := c.set(k)
	if err != nil {
		if mdbx.IsNotFound(err) {
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
func (c *MdbxCursor) DeleteCurrent() error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	return c.delCurrent()
}

func (c *MdbxCursor) deleteDupSort(key []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("delete from dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, key, len(key))
	}

	if len(key) == from {
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil { // if key not found, or found another one - then nothing to delete
			if mdbx.IsNotFound(err) {
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
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.delCurrent()
}

func (c *MdbxCursor) PutNoOverwrite(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("mdbx doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucketCfg.AutoDupSortKeysConversion {
		panic("not implemented")
	}

	return c.putNoOverwrite(key, value)
}

func (c *MdbxCursor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("mdbx doesn't support empty keys. bucket: %s", c.bucketName)
	}
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
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

func (c *MdbxCursor) putDupSort(key []byte, value []byte) error {
	b := c.bucketCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("put dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, key, len(key))
	}

	if len(key) != from {
		err := c.putNoOverwrite(key, value)
		if err != nil {
			if mdbx.IsKeyExists(err) {
				return c.putCurrent(key, value)
			}
			return fmt.Errorf("putNoOverwrite, bucket: %s, key: %x, val: %x, err: %w", c.bucketName, key, value, err)
		}
		return nil
	}

	value = append(key[to:], value...)
	key = key[:to]
	v, err := c.getBothRange(key, value[:from-to])
	if err != nil { // if key not found, or found another one - then just insert
		if mdbx.IsNotFound(err) {
			return c.put(key, value)
		}
		return err
	}

	if bytes.Equal(v[:from-to], value[:from-to]) {
		if len(v) == len(value) { // in DupSort case mdbx.Current works only with values of same length
			return c.putCurrent(key, value)
		}
		err = c.delCurrent()
		if err != nil {
			return err
		}
	}

	return c.put(key, value)
}

func (c *MdbxCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	b := c.bucketCfg
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if mdbx.IsNotFound(err) {
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
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

// Append - speedy feature of mdbx which is not part of KV interface.
// Cast your cursor to *MdbxCursor to use this method.
// Return error - if provided data will not sorted (or bucket have old records which mess with new in sorting manner).
func (c *MdbxCursor) Append(k []byte, v []byte) error {
	if len(k) == 0 {
		return fmt.Errorf("mdbx doesn't support empty keys. bucket: %s", c.bucketName)
	}

	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	b := c.bucketCfg
	if b.AutoDupSortKeysConversion {
		from, to := b.DupFromLen, b.DupToLen
		if len(k) != from && len(k) >= to {
			return fmt.Errorf("append dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, k, len(k))
		}

		if len(k) == from {
			v = append(k[to:], v...)
			k = k[:to]
		}
	}

	if b.Flags&mdbx.DupSort != 0 {
		if err := c.appendDup(k, v); err != nil {
			return fmt.Errorf("bucket: %s, %w", c.bucketName, err)
		}
		return nil
	}

	if err := c.append(k, v); err != nil {
		return fmt.Errorf("bucket: %s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxCursor) Close() {
	if c.c != nil {
		c.c.Close()
		l := len(c.tx.cursors)
		if l == 0 {
			c.c = nil
			return
		}
		//TODO: Find a better solution to avoid the leak?
		newCursors := make([]*mdbx.Cursor, l-1)
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

type MdbxDupSortCursor struct {
	*MdbxCursor
}

func (c *MdbxDupSortCursor) Internal() *mdbx.Cursor {
	return c.c
}

func (c *MdbxDupSortCursor) initCursor() error {
	if c.c != nil {
		return nil
	}

	return c.MdbxCursor.initCursor()
}

// DeleteExact - does delete
func (c *MdbxDupSortCursor) DeleteExact(k1, k2 []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	_, err := c.getBoth(k1, k2)
	if err != nil { // if key not found, or found another one - then nothing to delete
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	}
	return c.delCurrent()
}

func (c *MdbxDupSortCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	v, err := c.getBoth(key, value)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in SeekBothExact: %w", err)
	}
	return key, v, nil
}

func (c *MdbxDupSortCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	v, err := c.getBothRange(key, value)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in SeekBothRange: %w", err)
	}
	return v, nil
}

func (c *MdbxDupSortCursor) FirstDup() ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	v, err := c.firstDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in FirstDup: %w", err)
	}
	return v, nil
}

// NextDup - iterate only over duplicates of current key
func (c *MdbxDupSortCursor) NextDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.nextDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextDup: %w", err)
	}
	return k, v, nil
}

// NextNoDup - iterate with skipping all duplicates
func (c *MdbxDupSortCursor) NextNoDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.nextNoDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextNoDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) PrevDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.prevDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) PrevNoDup() ([]byte, []byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	k, v, err := c.prevNoDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevNoDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) LastDup() ([]byte, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return nil, err
		}
	}

	v, err := c.lastDup()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in LastDup: %w", err)
	}
	return v, nil
}

func (c *MdbxDupSortCursor) Append(k []byte, v []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if err := c.c.Put(k, v, mdbx.Append|mdbx.AppendDup); err != nil {
		return fmt.Errorf("in Append: bucket=%s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) AppendDup(k []byte, v []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if err := c.appendDup(k, v); err != nil {
		return fmt.Errorf("in AppendDup: bucket=%s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) PutNoDupData(key, value []byte) error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	if err := c.putNoDupData(key, value); err != nil {
		return fmt.Errorf("in PutNoDupData: %w", err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *MdbxDupSortCursor) DeleteCurrentDuplicates() error {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	if err := c.delNoDupData(); err != nil {
		return fmt.Errorf("in DeleteCurrentDuplicates: %w", err)
	}
	return nil
}

// Count returns the number of duplicates for the current key. See mdb_cursor_count
func (c *MdbxDupSortCursor) CountDuplicates() (uint64, error) {
	if c.c == nil {
		if err := c.initCursor(); err != nil {
			return 0, err
		}
	}
	res, err := c.c.Count()
	if err != nil {
		return 0, fmt.Errorf("in CountDuplicates: %w", err)
	}
	return res, nil
}
