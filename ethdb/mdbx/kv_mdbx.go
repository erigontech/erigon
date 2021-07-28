package mdbx

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

const expectMdbxVersionMajor = 0
const expectMdbxVersionMinor = 10
const pageSize = 4 * 1024

const NonExistingDBI kv.DBI = 999_999_999

type BucketConfigsFunc func(defaultBuckets kv.TableCfg) kv.TableCfg

func DefaultBucketConfigs(defaultBuckets kv.TableCfg) kv.TableCfg {
	return defaultBuckets
}

type MdbxOpts struct {
	bucketsCfg BucketConfigsFunc
	path       string
	inMem      bool
	label      kv.Label // marker to distinct db instances - one process may open many databases. for example to collect metrics of only 1 database
	verbosity  kv.DBVerbosityLvl
	mapSize    datasize.ByteSize
	flags      uint
	log        log.Logger
}

func testKVPath() string {
	dir, err := ioutil.TempDir(os.TempDir(), "erigon-test-db")
	if err != nil {
		panic(err)
	}
	return dir
}

func NewMDBX(log log.Logger) MdbxOpts {
	return MdbxOpts{
		bucketsCfg: DefaultBucketConfigs,
		flags:      mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable,
		log:        log,
	}
}

func (opts MdbxOpts) Label(label kv.Label) MdbxOpts {
	opts.label = label
	return opts
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
	opts.flags = opts.flags | mdbx.Exclusive
	return opts
}

func (opts MdbxOpts) Flags(f func(uint) uint) MdbxOpts {
	opts.flags = f(opts.flags)
	return opts
}

func (opts MdbxOpts) Readonly() MdbxOpts {
	opts.flags = opts.flags | mdbx.Readonly
	return opts
}

func (opts MdbxOpts) DBVerbosity(v kv.DBVerbosityLvl) MdbxOpts {
	opts.verbosity = v
	return opts
}

func (opts MdbxOpts) MapSize(sz datasize.ByteSize) MdbxOpts {
	opts.mapSize = sz
	return opts
}

func (opts MdbxOpts) WithBucketsConfig(f BucketConfigsFunc) MdbxOpts {
	opts.bucketsCfg = f
	return opts
}

func (opts MdbxOpts) Open() (kv.RwDB, error) {
	if expectMdbxVersionMajor != mdbx.Major || expectMdbxVersionMinor != mdbx.Minor {
		return nil, fmt.Errorf("unexpected mdbx version: %d.%d, expected %d %d. Please run 'make mdbx'", mdbx.Major, mdbx.Minor, expectMdbxVersionMajor, expectMdbxVersionMinor)
	}

	var err error
	if opts.inMem {
		opts.path = testKVPath()
	}

	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	if opts.verbosity != -1 {
		err = env.SetDebug(mdbx.LogLvl(opts.verbosity), mdbx.DbgDoNotChange, mdbx.LoggerDoNotChange) // temporary disable error, because it works if call it 1 time, but returns error if call it twice in same process (what often happening in tests)
		if err != nil {
			return nil, fmt.Errorf("db verbosity set: %w", err)
		}
	}
	if err = env.SetOption(mdbx.OptMaxDB, 100); err != nil {
		return nil, err
	}
	if err = env.SetOption(mdbx.OptMaxReaders, kv.ReadersLimit); err != nil {
		return nil, err
	}

	if opts.mapSize == 0 {
		if opts.inMem {
			opts.mapSize = 64 * datasize.MB
		} else {
			opts.mapSize = 2 * datasize.TB
		}
	}
	if opts.flags&mdbx.Accede == 0 {
		if opts.inMem {
			if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(2*datasize.MB), 0, 4*1024); err != nil {
				return nil, err
			}
		} else {
			if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(2*datasize.GB), -1, pageSize); err != nil {
				return nil, err
			}
		}
		if err = env.SetOption(mdbx.OptRpAugmentLimit, 32*1024*1024); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	err = env.Open(opts.path, opts.flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label.String(), debug.Callers(10))
	}

	defaultDirtyPagesLimit, err := env.GetOption(mdbx.OptTxnDpLimit)
	if err != nil {
		return nil, err
	}

	if opts.flags&mdbx.Accede == 0 && opts.flags&mdbx.Readonly == 0 {
		// 1/8 is good for transactions with a lot of modifications - to reduce invalidation size.
		// But Erigon app now using Batch and etl.Collectors to avoid writing to DB frequently changing data.
		// It means most of our writes are: APPEND or "single UPSERT per key during transaction"
		//if err = env.SetOption(mdbx.OptSpillMinDenominator, 8); err != nil {
		//	return nil, err
		//}
		if err = env.SetOption(mdbx.OptTxnDpInitial, 16*1024); err != nil {
			return nil, err
		}
		if err = env.SetOption(mdbx.OptDpReverseLimit, 16*1024); err != nil {
			return nil, err
		}
		if err = env.SetOption(mdbx.OptTxnDpLimit, defaultDirtyPagesLimit*2); err != nil { // default is RAM/42
			return nil, err
		}
		// must be in the range from 12.5% (almost empty) to 50% (half empty)
		// which corresponds to the range from 8192 and to 32768 in units respectively
		if err = env.SetOption(mdbx.OptMergeThreshold16dot16Percent, 32768); err != nil {
			return nil, err
		}
	}

	dirtyPagesLimit, err := env.GetOption(mdbx.OptTxnDpLimit)
	if err != nil {
		return nil, err
	}

	db := &MdbxKV{
		opts:    opts,
		env:     env,
		log:     opts.log,
		wg:      &sync.WaitGroup{},
		buckets: kv.TableCfg{},
		txSize:  dirtyPagesLimit * pageSize,
	}
	customBuckets := opts.bucketsCfg(kv.BucketsConfigs)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	buckets := bucketSlice(db.buckets)
	// Open or create buckets
	if opts.flags&mdbx.Readonly != 0 {
		tx, innerErr := db.BeginRo(context.Background())
		if innerErr != nil {
			return nil, innerErr
		}
		for _, name := range buckets {
			if db.buckets[name].IsDeprecated {
				continue
			}
			if err = tx.(kv.BucketMigrator).CreateBucket(name); err != nil {
				return nil, err
			}
		}
		err = tx.Commit()
		if err != nil {
			return nil, err
		}
	} else {
		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			for _, name := range buckets {
				if db.buckets[name].IsDeprecated {
					continue
				}
				if err := tx.(kv.BucketMigrator).CreateBucket(name); err != nil {
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
		for _, name := range buckets {
			// Open deprecated buckets if they exist, don't create
			if !db.buckets[name].IsDeprecated {
				continue
			}
			cnfCopy := db.buckets[name]
			dbi, createErr := tx.OpenDBI(name, mdbx.DBAccede, nil, nil)
			if createErr != nil {
				if mdbx.IsNotFound(createErr) {
					cnfCopy.DBI = NonExistingDBI
					db.buckets[name] = cnfCopy
					continue // if deprecated bucket couldn't be open - then it's deleted and it's fine
				} else {
					return fmt.Errorf("bucket: %s, %w", name, createErr)
				}
			}
			cnfCopy.DBI = kv.DBI(dbi)
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
			db.log.Info("[db] cleared reader slots from dead processes", "amount", staleReaders)
		}
	}
	return db, nil
}

func (opts MdbxOpts) MustOpen() kv.RwDB {
	db, err := opts.Open()
	if err != nil {
		panic(fmt.Errorf("fail to open mdbx: %w", err))
	}
	return db
}

type MdbxKV struct {
	env     *mdbx.Env
	log     log.Logger
	wg      *sync.WaitGroup
	buckets kv.TableCfg
	opts    MdbxOpts
	txSize  uint64
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *MdbxKV) Close() {
	if db.env == nil {
		return
	}

	db.wg.Wait()
	db.env.Close()
	db.env = nil

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	} else {
		db.log.Info("database closed (MDBX)")
	}
}

func (db *MdbxKV) BeginRo(_ context.Context) (txn kv.Tx, err error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	defer func() {
		if err == nil {
			db.wg.Add(1)
		}
	}()

	tx, err := db.env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, db.opts.label.String(), debug.Callers(10))
	}
	tx.RawRead = true
	return &MdbxTx{
		db:       db,
		tx:       tx,
		readOnly: true,
	}, nil
}

func (db *MdbxKV) BeginRw(_ context.Context) (txn kv.RwTx, err error) {
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
		return nil, fmt.Errorf("%w, lable: %s, trace: %s", err, db.opts.label.String(), debug.Callers(10))
	}
	tx.RawRead = true
	return &MdbxTx{
		db: db,
		tx: tx,
	}, nil
}

type MdbxTx struct {
	tx               *mdbx.Txn
	db               *MdbxKV
	cursors          map[uint64]*mdbx.Cursor
	statelessCursors map[string]kv.Cursor
	readOnly         bool
	cursorID         uint64
}

type MdbxCursor struct {
	tx         *MdbxTx
	c          *mdbx.Cursor
	bucketName string
	bucketCfg  kv.TableConfigItem
	dbi        mdbx.DBI
	id         uint64
}

func (db *MdbxKV) Env() *mdbx.Env {
	return db.env
}

func (db *MdbxKV) AllDBI() map[string]kv.DBI {
	res := map[string]kv.DBI{}
	for name, cfg := range db.buckets {
		res[name] = cfg.DBI
	}
	return res
}

func (db *MdbxKV) AllBuckets() kv.TableCfg {
	return db.buckets
}

func (tx *MdbxTx) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (tx *MdbxTx) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}
func (tx *MdbxTx) ForAmount(bucket string, fromPrefix []byte, amount uint32, walker func(k, v []byte) error) error {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil && amount > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
		amount--
	}
	return nil
}

func (tx *MdbxTx) CollectMetrics() {
	if tx.db.opts.label != kv.ChainDB {
		return
	}

	info, err := tx.db.env.Info(tx.tx)
	if err != nil {
		return
	}
	if info.SinceReaderCheck.Hours() > 1 {
		if staleReaders, err := tx.db.env.ReaderCheck(); err != nil {
			tx.db.log.Error("failed ReaderCheck", "err", err)
		} else if staleReaders > 0 {
			tx.db.log.Info("[db] cleared reader slots from dead processes", "amount", staleReaders)
		}
	}

	if !metrics.Enabled {
		return
	}
	kv.DbSize.Update(int64(info.Geo.Current))
	kv.DbPgopsNewly.Update(int64(info.PageOps.Newly))
	kv.DbPgopsCow.Update(int64(info.PageOps.Cow))
	kv.DbPgopsClone.Update(int64(info.PageOps.Clone))
	kv.DbPgopsSplit.Update(int64(info.PageOps.Split))
	kv.DbPgopsMerge.Update(int64(info.PageOps.Merge))
	kv.DbPgopsSpill.Update(int64(info.PageOps.Spill))
	kv.DbPgopsUnspill.Update(int64(info.PageOps.Unspill))
	kv.DbPgopsWops.Update(int64(info.PageOps.Wops))

	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return
	}

	kv.TxDirty.Update(int64(txInfo.SpaceDirty))
	kv.TxLimit.Update(int64(tx.db.txSize))
	kv.TxSpill.Update(int64(txInfo.Spill))
	kv.TxUnspill.Update(int64(txInfo.Unspill))

	gc, err := tx.BucketStat("gc")
	if err != nil {
		return
	}
	kv.GcLeafMetric.Update(int64(gc.LeafPages))
	kv.GcOverflowMetric.Update(int64(gc.OverflowPages))
	kv.GcPagesMetric.Update(int64((gc.LeafPages + gc.OverflowPages) * pageSize / 8))

	{
		st, err := tx.BucketStat(kv.PlainStateBucket)
		if err != nil {
			return
		}
		kv.TableStateLeaf.Update(int64(st.LeafPages))
		kv.TableStateBranch.Update(int64(st.BranchPages))
		kv.TableStateEntries.Update(int64(st.Entries))
		kv.TableStateSize.Update(int64(st.LeafPages+st.BranchPages+st.OverflowPages) * pageSize)
	}
	{
		st, err := tx.BucketStat(kv.StorageChangeSet)
		if err != nil {
			return
		}
		kv.TableScsLeaf.Update(int64(st.LeafPages))
		kv.TableScsBranch.Update(int64(st.BranchPages))
		kv.TableScsEntries.Update(int64(st.Entries))
		kv.TableScsSize.Update(int64(st.LeafPages+st.BranchPages+st.OverflowPages) * pageSize)
	}
	{
		st, err := tx.BucketStat(kv.EthTx)
		if err != nil {
			return
		}
		kv.TableTxLeaf.Update(int64(st.LeafPages))
		kv.TableTxBranch.Update(int64(st.BranchPages))
		kv.TableTxOverflow.Update(int64(st.OverflowPages))
		kv.TableTxEntries.Update(int64(st.Entries))
		kv.TableTxSize.Update(int64(st.LeafPages+st.BranchPages+st.OverflowPages) * pageSize)
	}
	{
		st, err := tx.BucketStat(kv.Log)
		if err != nil {
			return
		}
		kv.TableLogLeaf.Update(int64(st.LeafPages))
		kv.TableLogBranch.Update(int64(st.BranchPages))
		kv.TableLogOverflow.Update(int64(st.OverflowPages))
		kv.TableLogEntries.Update(int64(st.Entries))
		kv.TableLogSize.Update(int64(st.LeafPages+st.BranchPages+st.OverflowPages) * pageSize)
	}
}

// ExistingBuckets - all buckets stored as keys of un-named bucket
func (tx *MdbxTx) ListBuckets() ([]string, error) {
	return tx.tx.ListDBI()
}

func (db *MdbxKV) View(ctx context.Context, f func(tx kv.Tx) error) (err error) {
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

func (db *MdbxKV) Update(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
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

func (tx *MdbxTx) CreateBucket(name string) error {
	cnfCopy := tx.db.buckets[name]
	dbi, err := tx.tx.OpenDBI(name, mdbx.DBAccede, nil, nil)
	if err != nil && !mdbx.IsNotFound(err) {
		return fmt.Errorf("create bucket: %s, %w", name, err)
	}
	if err == nil {
		cnfCopy.DBI = kv.DBI(dbi)
		var flags uint
		flags, err = tx.tx.Flags(dbi)
		if err != nil {
			return err
		}
		cnfCopy.Flags = kv.TableFlags(flags)

		tx.db.buckets[name] = cnfCopy
		return nil
	}

	// if bucket doesn't exists - create it

	var flags = tx.db.buckets[name].Flags
	var nativeFlags uint
	if tx.db.opts.flags&mdbx.Readonly == 0 {
		nativeFlags |= mdbx.Create
	}

	if flags&kv.DupSort != 0 {
		nativeFlags |= mdbx.DupSort
		flags ^= kv.DupSort
	}
	if flags != 0 {
		return fmt.Errorf("some not supported flag provided for bucket")
	}

	dbi, err = tx.tx.OpenDBI(name, nativeFlags, nil, nil)

	if err != nil {
		return fmt.Errorf("create bucket: %s, %w", name, err)
	}
	cnfCopy.DBI = kv.DBI(dbi)

	tx.db.buckets[name] = cnfCopy
	return nil
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
		dbi = kv.DBI(nativeDBI)
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
		return fmt.Errorf("%w, bucket: %s", kv.ErrAttemptToDeleteNonDeprecatedBucket, bucket)
	}

	return tx.dropEvenIfBucketIsNotDeprecated(bucket)
}

func (tx *MdbxTx) ExistsBucket(bucket string) (bool, error) {
	if cfg, ok := tx.db.buckets[bucket]; ok {
		return cfg.DBI != NonExistingDBI, nil
	}
	return false, nil
}

func (tx *MdbxTx) Commit() error {
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

	slowTx := 10 * time.Second
	if debug.SlowCommit() > 0 {
		slowTx = debug.SlowCommit()
	}

	if debug.BigRoTxKb() > 0 || debug.BigRwTxKb() > 0 {
		tx.PrintDebugInfo()
	}
	tx.CollectMetrics()

	latency, err := tx.tx.Commit()
	if err != nil {
		return err
	}

	if tx.db.opts.label == kv.ChainDB {
		kv.DbCommitPreparation.Update(latency.Preparation)
		kv.DbCommitGc.Update(latency.GC)
		kv.DbCommitAudit.Update(latency.Audit)
		kv.DbCommitWrite.Update(latency.Write)
		kv.DbCommitSync.Update(latency.Sync)
		kv.DbCommitEnding.Update(latency.Ending)
		kv.DbCommitBigBatchTimer.Update(latency.Whole)
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
		if !tx.readOnly {
			runtime.UnlockOSThread()
		}
	}()
	tx.closeCursors()
	//tx.printDebugInfo()
	tx.tx.Abort()
}

//nolint
func (tx *MdbxTx) SpaceDirty() (uint64, uint64, error) {
	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return 0, 0, err
	}

	return txInfo.SpaceDirty, tx.db.txSize, nil
}

//nolint
func (tx *MdbxTx) PrintDebugInfo() {
	txInfo, err := tx.tx.Info(true)
	if err != nil {
		panic(err)
	}

	txSize := uint(txInfo.SpaceDirty / 1024)
	doPrint := debug.BigRoTxKb() == 0 && debug.BigRwTxKb() == 0 ||
		tx.readOnly && debug.BigRoTxKb() > 0 && txSize > debug.BigRoTxKb() ||
		(!tx.readOnly && debug.BigRwTxKb() > 0 && txSize > debug.BigRwTxKb())
	if doPrint {
		tx.db.log.Info("Tx info",
			"id", txInfo.Id,
			"read_lag", txInfo.ReadLag,
			"ro", tx.readOnly,
			//"space_retired_mb", txInfo.SpaceRetired/1024/1024,
			"space_dirty_mb", txInfo.SpaceDirty/1024/1024,
			//"callers", debug.Callers(7),
		)
	}
}

func (tx *MdbxTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = nil
	tx.statelessCursors = nil
}

func (tx *MdbxTx) statelessCursor(bucket string) (kv.RwCursor, error) {
	if tx.statelessCursors == nil {
		tx.statelessCursors = make(map[string]kv.Cursor)
	}
	c, ok := tx.statelessCursors[bucket]
	if !ok {
		var err error
		c, err = tx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		tx.statelessCursors[bucket] = c
	}
	return c.(kv.RwCursor), nil
}

func (tx *MdbxTx) Put(bucket string, k, v []byte) error {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return err
	}
	return c.Put(k, v)
}

func (tx *MdbxTx) Delete(bucket string, k, v []byte) error {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return err
	}
	return c.Delete(k, v)
}

func (tx *MdbxTx) GetOne(bucket string, k []byte) ([]byte, error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return nil, err
	}
	_, v, err := c.SeekExact(k)
	return v, err
}

func (tx *MdbxTx) Has(bucket string, key []byte) (bool, error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return false, err
	}
	k, _, err := c.Seek(key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, k), nil
}

func (tx *MdbxTx) Append(bucket string, k, v []byte) error {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return err
	}
	return c.Append(k, v)
}
func (tx *MdbxTx) AppendDup(bucket string, k, v []byte) error {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return err
	}
	return c.(*MdbxDupSortCursor).AppendDup(k, v)
}

func (tx *MdbxTx) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	c, err := tx.statelessCursor(kv.Sequence)
	if err != nil {
		return 0, err
	}
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil {
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

func (tx *MdbxTx) ReadSequence(bucket string) (uint64, error) {
	c, err := tx.statelessCursor(kv.Sequence)
	if err != nil {
		return 0, err
	}
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil && !mdbx.IsNotFound(err) {
		return 0, err
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

func (tx *MdbxTx) BucketSize(name string) (uint64, error) {
	st, err := tx.BucketStat(name)
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * pageSize, nil
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

func (tx *MdbxTx) RwCursor(bucket string) (kv.RwCursor, error) {
	b := tx.db.buckets[bucket]
	if b.AutoDupSortKeysConversion {
		return tx.stdCursor(bucket)
	}

	if b.Flags&kv.DupSort != 0 {
		return tx.RwCursorDupSort(bucket)
	}

	return tx.stdCursor(bucket)
}

func (tx *MdbxTx) Cursor(bucket string) (kv.Cursor, error) {
	return tx.RwCursor(bucket)
}

func (tx *MdbxTx) stdCursor(bucket string) (kv.RwCursor, error) {
	b := tx.db.buckets[bucket]
	c := &MdbxCursor{bucketName: bucket, tx: tx, bucketCfg: b, dbi: mdbx.DBI(tx.db.buckets[bucket].DBI), id: tx.cursorID}
	tx.cursorID++

	var err error
	c.c, err = tx.tx.OpenCursor(c.dbi)
	if err != nil {
		return nil, fmt.Errorf("table: %s, %w", c.bucketName, err)
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = map[uint64]*mdbx.Cursor{}
	}
	tx.cursors[c.id] = c.c
	return c, nil
}

func (tx *MdbxTx) RwCursorDupSort(bucket string) (kv.RwCursorDupSort, error) {
	basicCursor, err := tx.stdCursor(bucket)
	if err != nil {
		return nil, err
	}
	return &MdbxDupSortCursor{MdbxCursor: basicCursor.(*MdbxCursor)}, nil
}

func (tx *MdbxTx) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	return tx.RwCursorDupSort(bucket)
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

func (c *MdbxCursor) Count() (uint64, error) {
	st, err := c.tx.tx.StatDBI(c.dbi)
	if err != nil {
		return 0, err
	}
	return st.Entries, nil
}

func (c *MdbxCursor) First() ([]byte, []byte, error) {
	return c.Seek(nil)
}

func (c *MdbxCursor) Last() ([]byte, []byte, error) {
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

	return k, v, nil
}

func (c *MdbxCursor) Next() (k, v []byte, err error) {
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

	return k, v, nil
}

func (c *MdbxCursor) Prev() (k, v []byte, err error) {
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

	return k, v, nil
}

// Current - return key/data at current cursor position
func (c *MdbxCursor) Current() ([]byte, []byte, error) {
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

	return k, v, nil
}

func (c *MdbxCursor) Delete(k, v []byte) error {
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
	if c.bucketCfg.AutoDupSortKeysConversion {
		panic("not implemented")
	}

	return c.putNoOverwrite(key, value)
}

func (c *MdbxCursor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("mdbx doesn't support empty keys. bucket: %s", c.bucketName)
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
		delete(c.tx.cursors, c.id)
		c.c = nil
	}
}

type MdbxDupSortCursor struct {
	*MdbxCursor
}

func (c *MdbxDupSortCursor) Internal() *mdbx.Cursor {
	return c.c
}

// DeleteExact - does delete
func (c *MdbxDupSortCursor) DeleteExact(k1, k2 []byte) error {
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
	if err := c.c.Put(k, v, mdbx.Append|mdbx.AppendDup); err != nil {
		return fmt.Errorf("in Append: bucket=%s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) AppendDup(k []byte, v []byte) error {
	if err := c.appendDup(k, v); err != nil {
		return fmt.Errorf("in AppendDup: bucket=%s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) PutNoDupData(key, value []byte) error {
	if err := c.putNoDupData(key, value); err != nil {
		return fmt.Errorf("in PutNoDupData: %w", err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *MdbxDupSortCursor) DeleteCurrentDuplicates() error {
	if err := c.delNoDupData(); err != nil {
		return fmt.Errorf("in DeleteCurrentDuplicates: %w", err)
	}
	return nil
}

// Count returns the number of duplicates for the current key. See mdb_cursor_count
func (c *MdbxDupSortCursor) CountDuplicates() (uint64, error) {
	res, err := c.c.Count()
	if err != nil {
		return 0, fmt.Errorf("in CountDuplicates: %w", err)
	}
	return res, nil
}

func bucketSlice(b kv.TableCfg) []string {
	buckets := make([]string, 0, len(b))
	for name := range b {
		buckets = append(buckets, name)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return strings.Compare(buckets[i], buckets[j]) < 0
	})
	return buckets
}
