/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mdbx

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"
	stack2 "github.com/go-stack/stack"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/log/v3"
	"github.com/pbnjay/memory"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/semaphore"
)

const NonExistingDBI kv.DBI = 999_999_999

type TableCfgFunc func(defaultBuckets kv.TableCfg) kv.TableCfg

func WithChaindataTables(defaultBuckets kv.TableCfg) kv.TableCfg {
	return defaultBuckets
}

type MdbxOpts struct {
	// must be in the range from 12.5% (almost empty) to 50% (half empty)
	// which corresponds to the range from 8192 and to 32768 in units respectively
	log             log.Logger
	roTxsLimiter    *semaphore.Weighted
	bucketsCfg      TableCfgFunc
	path            string
	syncPeriod      time.Duration
	mapSize         datasize.ByteSize
	growthStep      datasize.ByteSize
	shrinkThreshold int
	flags           uint
	pageSize        uint64
	dirtySpace      uint64 // if exeed this space, modified pages will `spill` to disk
	mergeThreshold  uint64
	verbosity       kv.DBVerbosityLvl
	label           kv.Label // marker to distinct db instances - one process may open many databases. for example to collect metrics of only 1 database
	inMem           bool
}

func NewMDBX(log log.Logger) MdbxOpts {
	opts := MdbxOpts{
		bucketsCfg: WithChaindataTables,
		flags:      mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable,
		log:        log,
		pageSize:   kv.DefaultPageSize(),

		// default is (TOTAL_RAM+AVAILABLE_RAM)/42/pageSize
		// but for reproducibility of benchmarks - please don't rely on Available RAM
		dirtySpace: 2 * (memory.TotalMemory() / 42),

		mapSize:         2 * datasize.TB,
		growthStep:      2 * datasize.GB,
		mergeThreshold:  3 * 8192,
		shrinkThreshold: -1, // default
		label:           kv.InMem,
	}
	return opts
}

func (opts MdbxOpts) GetLabel() kv.Label  { return opts.label }
func (opts MdbxOpts) GetInMem() bool      { return opts.inMem }
func (opts MdbxOpts) GetPageSize() uint64 { return opts.pageSize }

func (opts MdbxOpts) Label(label kv.Label) MdbxOpts {
	opts.label = label
	return opts
}

func (opts MdbxOpts) DirtySpace(s uint64) MdbxOpts {
	opts.dirtySpace = s
	return opts
}

func (opts MdbxOpts) RoTxsLimiter(l *semaphore.Weighted) MdbxOpts {
	opts.roTxsLimiter = l
	return opts
}

func (opts MdbxOpts) PageSize(v uint64) MdbxOpts {
	opts.pageSize = v
	return opts
}

func (opts MdbxOpts) GrowthStep(v datasize.ByteSize) MdbxOpts {
	opts.growthStep = v
	return opts
}

func (opts MdbxOpts) Path(path string) MdbxOpts {
	opts.path = path
	return opts
}

func (opts MdbxOpts) Set(opt MdbxOpts) MdbxOpts {
	return opt
}

func (opts MdbxOpts) InMem(tmpDir string) MdbxOpts {
	if tmpDir != "" {
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			panic(err)
		}
	}
	path, err := os.MkdirTemp(tmpDir, "erigon-memdb-")
	if err != nil {
		panic(err)
	}
	opts.path = path
	opts.inMem = true
	opts.flags = mdbx.UtterlyNoSync | mdbx.NoMetaSync | mdbx.LifoReclaim | mdbx.NoMemInit
	opts.growthStep = 2 * datasize.MB
	opts.mapSize = 512 * datasize.MB
	opts.shrinkThreshold = 0 // disable
	opts.label = kv.InMem
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

func (opts MdbxOpts) HasFlag(flag uint) bool { return opts.flags&flag != 0 }
func (opts MdbxOpts) Readonly() MdbxOpts {
	opts.flags = opts.flags | mdbx.Readonly
	return opts
}

func (opts MdbxOpts) SyncPeriod(period time.Duration) MdbxOpts {
	opts.syncPeriod = period
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

func (opts MdbxOpts) WriteMap() MdbxOpts {
	opts.flags |= mdbx.WriteMap
	return opts
}

func (opts MdbxOpts) WriteMergeThreshold(v uint64) MdbxOpts {
	opts.mergeThreshold = v
	return opts
}

func (opts MdbxOpts) WithTableCfg(f TableCfgFunc) MdbxOpts {
	opts.bucketsCfg = f
	return opts
}

var pathDbMap = map[string]kv.RoDB{}
var pathDbMapLock sync.Mutex

func addToPathDbMap(path string, db kv.RoDB) {
	pathDbMapLock.Lock()
	defer pathDbMapLock.Unlock()
	pathDbMap[path] = db
}

func removeFromPathDbMap(path string) {
	pathDbMapLock.Lock()
	defer pathDbMapLock.Unlock()
	delete(pathDbMap, path)
}

func PathDbMap() map[string]kv.RoDB {
	pathDbMapLock.Lock()
	defer pathDbMapLock.Unlock()
	return maps.Clone(pathDbMap)
}

func (opts MdbxOpts) Open() (kv.RwDB, error) {
	if dbg.WriteMap() {
		opts = opts.WriteMap() //nolint
	}
	if dbg.DirtySpace() > 0 {
		opts = opts.DirtySpace(dbg.DirtySpace()) //nolint
	}
	if dbg.NoSync() {
		opts = opts.Flags(func(u uint) uint { return u | mdbx.SafeNoSync }) //nolint
	}
	if dbg.MergeTr() > 0 {
		opts = opts.WriteMergeThreshold(uint64(dbg.MergeTr() * 8192)) //nolint
	}
	if dbg.MdbxReadAhead() {
		opts = opts.Flags(func(u uint) uint { return u &^ mdbx.NoReadahead }) //nolint
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
	if err = env.SetOption(mdbx.OptMaxDB, 200); err != nil {
		return nil, err
	}
	if err = env.SetOption(mdbx.OptMaxReaders, kv.ReadersLimit); err != nil {
		return nil, err
	}

	if opts.flags&mdbx.Accede == 0 {
		if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(opts.growthStep), opts.shrinkThreshold, int(opts.pageSize)); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	err = env.Open(opts.path, opts.flags, 0664)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label.String(), stack2.Trace().String())
		}
	}

	// mdbx will not change pageSize if db already exists. means need read real value after env.open()
	in, err := env.Info(nil)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label.String(), stack2.Trace().String())
		}
	}

	opts.pageSize = uint64(in.PageSize)

	//nolint
	if opts.flags&mdbx.Accede == 0 && opts.flags&mdbx.Readonly == 0 {
	}
	// erigon using big transactions
	// increase "page measured" options. need do it after env.Open() because default are depend on pageSize known only after env.Open()
	if opts.flags&mdbx.Readonly == 0 {
		// 1/8 is good for transactions with a lot of modifications - to reduce invalidation size.
		// But Erigon app now using Batch and etl.Collectors to avoid writing to DB frequently changing data.
		// It means most of our writes are: APPEND or "single UPSERT per key during transaction"
		//if err = env.SetOption(mdbx.OptSpillMinDenominator, 8); err != nil {
		//	return nil, err
		//}

		txnDpInitial, err := env.GetOption(mdbx.OptTxnDpInitial)
		if err != nil {
			return nil, err
		}
		if err = env.SetOption(mdbx.OptTxnDpInitial, txnDpInitial*2); err != nil {
			return nil, err
		}
		dpReserveLimit, err := env.GetOption(mdbx.OptDpReverseLimit)
		if err != nil {
			return nil, err
		}
		if err = env.SetOption(mdbx.OptDpReverseLimit, dpReserveLimit*2); err != nil {
			return nil, err
		}

		if err = env.SetOption(mdbx.OptTxnDpLimit, opts.dirtySpace/opts.pageSize); err != nil {
			return nil, err
		}
		// must be in the range from 12.5% (almost empty) to 50% (half empty)
		// which corresponds to the range from 8192 and to 32768 in units respectively
		if err = env.SetOption(mdbx.OptMergeThreshold16dot16Percent, opts.mergeThreshold); err != nil {
			return nil, err
		}
	}

	dirtyPagesLimit, err := env.GetOption(mdbx.OptTxnDpLimit)
	if err != nil {
		return nil, err
	}

	if opts.syncPeriod != 0 {
		if err = env.SetSyncPeriod(opts.syncPeriod); err != nil {
			env.Close()
			return nil, err
		}
	}
	//if err := env.SetOption(mdbx.OptSyncBytes, uint64(math2.MaxUint64)); err != nil {
	//	return nil, err
	//}

	if opts.roTxsLimiter == nil {
		targetSemCount := int64(runtime.GOMAXPROCS(-1) * 16)
		opts.roTxsLimiter = semaphore.NewWeighted(targetSemCount) // 1 less than max to allow unlocking to happen
	}
	db := &MdbxKV{
		opts:         opts,
		env:          env,
		log:          opts.log,
		wg:           &sync.WaitGroup{},
		buckets:      kv.TableCfg{},
		txSize:       dirtyPagesLimit * opts.pageSize,
		roTxsLimiter: opts.roTxsLimiter,

		leakDetector: dbg.NewLeakDetector("db."+opts.label.String(), dbg.SlowTx()),
	}

	customBuckets := opts.bucketsCfg(kv.ChaindataTablesCfg)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	buckets := bucketSlice(db.buckets)
	if err := db.openDBIs(buckets); err != nil {
		return nil, err
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
			db.log.Info("cleared reader slots from dead processes", "amount", staleReaders)
		}

	}
	db.path = opts.path
	addToPathDbMap(opts.path, db)
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
	log          log.Logger
	env          *mdbx.Env
	wg           *sync.WaitGroup
	buckets      kv.TableCfg
	roTxsLimiter *semaphore.Weighted // does limit amount of concurrent Ro transactions - in most casess runtime.NumCPU() is good value for this channel capacity - this channel can be shared with other components (like Decompressor)
	opts         MdbxOpts
	txSize       uint64
	closed       atomic.Bool
	path         string

	leakDetector *dbg.LeakDetector
}

func (db *MdbxKV) PageSize() uint64 { return db.opts.pageSize }
func (db *MdbxKV) ReadOnly() bool   { return db.opts.HasFlag(mdbx.Readonly) }

// openDBIs - first trying to open existing DBI's in RO transaction
// otherwise re-try by RW transaction
// it allow open DB from another process - even if main process holding long RW transaction
func (db *MdbxKV) openDBIs(buckets []string) error {
	if db.ReadOnly() {
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			for _, name := range buckets {
				if db.buckets[name].IsDeprecated {
					continue
				}
				if err := tx.(kv.BucketMigrator).CreateBucket(name); err != nil {
					return err
				}
			}
			return tx.Commit() // when open db as read-only, commit of this RO transaction is required
		}); err != nil {
			return err
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
			return err
		}
	}
	return nil
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *MdbxKV) Close() {
	if ok := db.closed.CompareAndSwap(false, true); !ok {
		return
	}
	db.wg.Wait()
	db.env.Close()
	db.env = nil

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}
	removeFromPathDbMap(db.path)
}

func (db *MdbxKV) BeginRo(ctx context.Context) (txn kv.Tx, err error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("db closed")
	}

	// don't try to acquire if the context is already done
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// otherwise carry on
	}

	// will return nil err if context is cancelled (may appear to acquire the semaphore)
	if semErr := db.roTxsLimiter.Acquire(ctx, 1); semErr != nil {
		return nil, semErr
	}

	defer func() {
		if txn == nil {
			// on error, or if there is whatever reason that we don't return a tx,
			// we need to free up the limiter slot, otherwise it could lead to deadlocks
			db.roTxsLimiter.Release(1)
		}
	}()

	tx, err := db.env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, db.opts.label.String(), stack2.Trace().String())
	}
	db.wg.Add(1)
	return &MdbxTx{
		ctx:      ctx,
		db:       db,
		tx:       tx,
		readOnly: true,
		id:       db.leakDetector.Add(),
	}, nil
}

func (db *MdbxKV) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.beginRw(ctx, 0)
}
func (db *MdbxKV) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.beginRw(ctx, mdbx.TxNoSync)
}

func (db *MdbxKV) beginRw(ctx context.Context, flags uint) (txn kv.RwTx, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if db.closed.Load() {
		return nil, fmt.Errorf("db closed")
	}
	runtime.LockOSThread()
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		return nil, fmt.Errorf("%w, lable: %s, trace: %s", err, db.opts.label.String(), stack2.Trace().String())
	}
	db.wg.Add(1)
	return &MdbxTx{
		db:  db,
		tx:  tx,
		ctx: ctx,
		id:  db.leakDetector.Add(),
	}, nil
}

type MdbxTx struct {
	tx               *mdbx.Txn
	db               *MdbxKV
	cursors          map[uint64]*mdbx.Cursor
	streams          []kv.Closer
	statelessCursors map[string]kv.RwCursor
	readOnly         bool
	cursorID         uint64
	ctx              context.Context
	id               uint64 // set only if TRACE_TX=true
}

type MdbxCursor struct {
	tx         *MdbxTx
	c          *mdbx.Cursor
	bucketName string
	bucketCfg  kv.TableCfgItem
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

func (db *MdbxKV) AllTables() kv.TableCfg {
	return db.buckets
}

func (tx *MdbxTx) ViewID() uint64 { return tx.tx.ID() }

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
			tx.db.log.Info("cleared reader slots from dead processes", "amount", staleReaders)
		}
	}

	kv.DbSize.Set(info.Geo.Current)
	kv.DbPgopsNewly.Set(info.PageOps.Newly)
	kv.DbPgopsCow.Set(info.PageOps.Cow)
	kv.DbPgopsClone.Set(info.PageOps.Clone)
	kv.DbPgopsSplit.Set(info.PageOps.Split)
	kv.DbPgopsMerge.Set(info.PageOps.Merge)
	kv.DbPgopsSpill.Set(info.PageOps.Spill)
	kv.DbPgopsUnspill.Set(info.PageOps.Unspill)
	kv.DbPgopsWops.Set(info.PageOps.Wops)

	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return
	}

	kv.TxDirty.Set(txInfo.SpaceDirty)
	kv.TxLimit.Set(tx.db.txSize)
	kv.TxSpill.Set(txInfo.Spill)
	kv.TxUnspill.Set(txInfo.Unspill)

	gc, err := tx.BucketStat("gc")
	if err != nil {
		return
	}
	kv.GcLeafMetric.Set(gc.LeafPages)
	kv.GcOverflowMetric.Set(gc.OverflowPages)
	kv.GcPagesMetric.Set((gc.LeafPages + gc.OverflowPages) * tx.db.opts.pageSize / 8)
}

// ListBuckets - all buckets stored as keys of un-named bucket
func (tx *MdbxTx) ListBuckets() ([]string, error) {
	return tx.tx.ListDBI()
}

func (db *MdbxKV) View(ctx context.Context, f func(tx kv.Tx) error) (err error) {
	// can't use db.env.View method - because it calls commit for read transactions - it conflicts with write transactions.
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *MdbxKV) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
	tx, err := db.BeginRwNosync(ctx)
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

func (db *MdbxKV) Update(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
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
		return fmt.Errorf("create table: %s, %w", name, err)
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
	if !tx.db.ReadOnly() {
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
		return fmt.Errorf("create table: %s, %w", name, err)
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
	if tx.tx == nil {
		return nil
	}
	defer func() {
		tx.tx = nil
		tx.db.wg.Done()
		if tx.readOnly {
			tx.db.roTxsLimiter.Release(1)
		} else {
			runtime.UnlockOSThread()
		}
		tx.db.leakDetector.Del(tx.id)
	}()
	tx.closeCursors()

	//slowTx := 10 * time.Second
	//if debug.SlowCommit() > 0 {
	//	slowTx = debug.SlowCommit()
	//}
	//
	//if debug.BigRoTxKb() > 0 || debug.BigRwTxKb() > 0 {
	//	tx.PrintDebugInfo()
	//}
	tx.CollectMetrics()

	latency, err := tx.tx.Commit()
	if err != nil {
		return err
	}

	if tx.db.opts.label == kv.ChainDB {
		kv.DbCommitPreparation.Update(latency.Preparation.Seconds())
		//kv.DbCommitAudit.Update(latency.Audit.Seconds())
		kv.DbCommitWrite.Update(latency.Write.Seconds())
		kv.DbCommitSync.Update(latency.Sync.Seconds())
		kv.DbCommitEnding.Update(latency.Ending.Seconds())
		kv.DbCommitTotal.Update(latency.Whole.Seconds())

		//kv.DbGcWorkPnlMergeTime.Update(latency.GCDetails.WorkPnlMergeTime.Seconds())
		//kv.DbGcWorkPnlMergeVolume.Set(uint64(latency.GCDetails.WorkPnlMergeVolume))
		//kv.DbGcWorkPnlMergeCalls.Set(uint64(latency.GCDetails.WorkPnlMergeCalls))
		//
		//kv.DbGcSelfPnlMergeTime.Update(latency.GCDetails.SelfPnlMergeTime.Seconds())
		//kv.DbGcSelfPnlMergeVolume.Set(uint64(latency.GCDetails.SelfPnlMergeVolume))
		//kv.DbGcSelfPnlMergeCalls.Set(uint64(latency.GCDetails.SelfPnlMergeCalls))
	}

	return nil
}

func (tx *MdbxTx) Rollback() {
	if tx.tx == nil {
		return
	}
	defer func() {
		tx.tx = nil
		tx.db.wg.Done()
		if tx.readOnly {
			tx.db.roTxsLimiter.Release(1)
		} else {
			runtime.UnlockOSThread()
		}
		tx.db.leakDetector.Del(tx.id)
	}()
	tx.closeCursors()
	//tx.printDebugInfo()
	tx.tx.Abort()
}

func (tx *MdbxTx) SpaceDirty() (uint64, uint64, error) {
	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return 0, 0, err
	}

	return txInfo.SpaceDirty, tx.db.txSize, nil
}

func (tx *MdbxTx) PrintDebugInfo() {
	/*
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
	*/
}

func (tx *MdbxTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = nil
	for _, c := range tx.streams {
		if c != nil {
			c.Close()
		}
	}
	tx.statelessCursors = nil
}

func (tx *MdbxTx) statelessCursor(bucket string) (kv.RwCursor, error) {
	if tx.statelessCursors == nil {
		tx.statelessCursors = make(map[string]kv.RwCursor)
	}
	c, ok := tx.statelessCursors[bucket]
	if !ok {
		var err error
		c, err = tx.RwCursor(bucket)
		if err != nil {
			return nil, err
		}
		tx.statelessCursors[bucket] = c
	}
	return c, nil
}

func (tx *MdbxTx) Put(table string, k, v []byte) error {
	c, err := tx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.Put(k, v)
}

func (tx *MdbxTx) Delete(table string, k []byte) error {
	c, err := tx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.Delete(k)
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

	var currentV uint64
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
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * tx.db.opts.pageSize, nil
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

func (tx *MdbxTx) DBSize() (uint64, error) {
	info, err := tx.db.env.Info(tx.tx)
	if err != nil {
		return 0, err
	}
	return info.Geo.Current, err
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
		return nil, fmt.Errorf("table: %s, %w, stack: %s", c.bucketName, err, dbg.Stack())
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
func (c *MdbxCursor) delAllDupData() error                 { return c.c.Del(mdbx.AllDups) }
func (c *MdbxCursor) put(k, v []byte) error                { return c.c.Put(k, v, 0) }
func (c *MdbxCursor) putCurrent(k, v []byte) error         { return c.c.Put(k, v, mdbx.Current) }
func (c *MdbxCursor) putNoOverwrite(k, v []byte) error     { return c.c.Put(k, v, mdbx.NoOverwrite) }
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

func (c *MdbxCursor) Delete(k []byte) error {
	if c.bucketCfg.AutoDupSortKeysConversion {
		return c.deleteDupSort(k)
	}

	_, _, err := c.set(k)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	}

	if c.bucketCfg.Flags&mdbx.DupSort != 0 {
		return c.delAllDupData()
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
	if c.bucketCfg.AutoDupSortKeysConversion {
		panic("not implemented")
	}

	return c.putNoOverwrite(key, value)
}

func (c *MdbxCursor) Put(key []byte, value []byte) error {
	b := c.bucketCfg
	if b.AutoDupSortKeysConversion {
		if err := c.putDupSort(key, value); err != nil {
			return err
		}
		return nil
	}
	if err := c.put(key, value); err != nil {
		return fmt.Errorf("table: %s, err: %w", c.bucketName, err)
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
	if c.bucketCfg.AutoDupSortKeysConversion {
		b := c.bucketCfg
		from, to := b.DupFromLen, b.DupToLen
		if len(k) != from && len(k) >= to {
			return fmt.Errorf("append dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.bucketName, from, to, k, len(k))
		}

		if len(k) == from {
			v = append(k[to:], v...)
			k = k[:to]
		}
	}

	if c.bucketCfg.Flags&mdbx.DupSort != 0 {
		if err := c.c.Put(k, v, mdbx.AppendDup); err != nil {
			return fmt.Errorf("bucket: %s, %w", c.bucketName, err)
		}
		return nil
	}

	if err := c.c.Put(k, v, mdbx.Append); err != nil {
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
		return nil, fmt.Errorf("in SeekBothRange, table=%s: %w", c.bucketName, err)
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
	if err := c.c.Put(k, v, mdbx.AppendDup); err != nil {
		return fmt.Errorf("in AppendDup: bucket=%s, %w", c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) PutNoDupData(k, v []byte) error {
	if err := c.c.Put(k, v, mdbx.NoDupData); err != nil {
		return fmt.Errorf("in PutNoDupData: %w", err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *MdbxDupSortCursor) DeleteCurrentDuplicates() error {
	if err := c.delAllDupData(); err != nil {
		return fmt.Errorf("in DeleteCurrentDuplicates: %w", err)
	}
	return nil
}

// CountDuplicates returns the number of duplicates for the current key. See mdb_cursor_count
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

func (tx *MdbxTx) Prefix(table string, prefix []byte) (iter.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return tx.Range(table, prefix, nil)
	}
	return tx.Range(table, prefix, nextPrefix)
}

func (tx *MdbxTx) Range(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	return tx.RangeAscend(table, fromPrefix, toPrefix, -1)
}
func (tx *MdbxTx) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Asc, limit)
}
func (tx *MdbxTx) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Desc, limit)
}

type cursor2iter struct {
	c                                  kv.Cursor
	fromPrefix, toPrefix, nextK, nextV []byte
	err                                error
	orderAscend                        order.By
	limit                              int64
	ctx                                context.Context
}

func (tx *MdbxTx) rangeOrderLimit(table string, fromPrefix, toPrefix []byte, orderAscend order.By, limit int) (*cursor2iter, error) {
	s := &cursor2iter{ctx: tx.ctx, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: orderAscend, limit: int64(limit)}
	tx.streams = append(tx.streams, s)
	return s.init(table, tx)
}
func (s *cursor2iter) init(table string, tx kv.Tx) (*cursor2iter, error) {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.Cursor(table)
	if err != nil {
		return s, err
	}
	s.c = c

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextK, s.nextV, s.err = s.c.First()
		} else {
			s.nextK, s.nextV, s.err = s.c.Last()
		}
		return s, s.err
	}

	if s.orderAscend {
		s.nextK, s.nextV, s.err = s.c.Seek(s.fromPrefix)
		return s, s.err
	} else {
		// seek exactly to given key or previous one
		s.nextK, s.nextV, s.err = s.c.SeekExact(s.fromPrefix)
		if s.err != nil {
			return s, s.err
		}
		if s.nextK != nil { // go to last value of this key
			if casted, ok := s.c.(kv.CursorDupSort); ok {
				s.nextV, s.err = casted.LastDup()
			}
		} else { // key not found, go to prev one
			s.nextK, s.nextV, s.err = s.c.Prev()
		}
		return s, s.err
	}
}

func (s *cursor2iter) Close() {
	if s.c != nil {
		s.c.Close()
	}
}
func (s *cursor2iter) HasNext() bool {
	if s.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextK == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	cmp := bytes.Compare(s.nextK, s.toPrefix)
	return (bool(s.orderAscend) && cmp < 0) || (!bool(s.orderAscend) && cmp > 0)
}
func (s *cursor2iter) Next() (k, v []byte, err error) {
	select {
	case <-s.ctx.Done():
		return nil, nil, s.ctx.Err()
	default:
	}
	s.limit--
	k, v, err = s.nextK, s.nextV, s.err
	if s.orderAscend {
		s.nextK, s.nextV, s.err = s.c.Next()
	} else {
		s.nextK, s.nextV, s.err = s.c.Prev()
	}
	return k, v, err
}

func (tx *MdbxTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	s := &cursorDup2iter{ctx: tx.ctx, key: key, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: bool(asc), limit: int64(limit)}
	tx.streams = append(tx.streams, s)
	return s.init(table, tx)
}

type cursorDup2iter struct {
	c                           kv.CursorDupSort
	key                         []byte
	fromPrefix, toPrefix, nextV []byte
	err                         error
	orderAscend                 bool
	limit                       int64
	ctx                         context.Context
}

func (s *cursorDup2iter) init(table string, tx kv.Tx) (*cursorDup2iter, error) {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.CursorDupSort(table)
	if err != nil {
		return s, err
	}
	s.c = c
	k, _, err := c.SeekExact(s.key)
	if err != nil {
		return s, err
	}
	if k == nil {
		return s, nil
	}

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextV, s.err = s.c.FirstDup()
		} else {
			s.nextV, s.err = s.c.LastDup()
		}
		return s, s.err
	}

	if s.orderAscend {
		s.nextV, s.err = s.c.SeekBothRange(s.key, s.fromPrefix)
		return s, s.err
	} else {
		// seek exactly to given key or previous one
		_, s.nextV, s.err = s.c.SeekBothExact(s.key, s.fromPrefix)
		if s.nextV == nil { // no such key
			_, s.nextV, s.err = s.c.PrevDup()
		}
		return s, s.err
	}
}

func (s *cursorDup2iter) Close() {
	if s.c != nil {
		s.c.Close()
	}
}
func (s *cursorDup2iter) HasNext() bool {
	if s.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextV == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	cmp := bytes.Compare(s.nextV, s.toPrefix)
	return (s.orderAscend && cmp < 0) || (!s.orderAscend && cmp > 0)
}
func (s *cursorDup2iter) Next() (k, v []byte, err error) {
	select {
	case <-s.ctx.Done():
		return nil, nil, s.ctx.Err()
	default:
	}
	s.limit--
	v, err = s.nextV, s.err
	if s.orderAscend {
		_, s.nextV, s.err = s.c.NextDup()
	} else {
		_, s.nextV, s.err = s.c.PrevDup()
	}
	return s.key, v, err
}

func (tx *MdbxTx) ForAmount(bucket string, fromPrefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
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
