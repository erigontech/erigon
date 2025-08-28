// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package mdbx

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/estimate"

	"github.com/erigontech/mdbx-go/mdbx"
	stack2 "github.com/go-stack/stack"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func init() {
	mdbx.MapFullErrorMessage += " You can try remove the database files (e.g., by running rm -rf /path/to/db)"
}

const NonExistingDBI kv.DBI = 999_999_999

type TableCfgFunc func(defaultBuckets kv.TableCfg) kv.TableCfg

func WithChaindataTables(defaultBuckets kv.TableCfg) kv.TableCfg {
	return defaultBuckets
}

type MdbxOpts struct {
	// must be in the range from 12.5% (almost empty) to 50% (half empty)
	// which corresponds to the range from 8192 and to 32768 in units respectively
	log             log.Logger
	bucketsCfg      TableCfgFunc
	path            string
	syncPeriod      time.Duration // to be used only in combination with SafeNoSync flag. The dirty data will automatically be flushed to disk periodically in the background.
	syncBytes       *uint         // to be used only in combination with SafeNoSync flag. The dirty data will be flushed to disk when this threshold is reached.
	mapSize         datasize.ByteSize
	growthStep      datasize.ByteSize
	shrinkThreshold int
	flags           uint
	pageSize        datasize.ByteSize
	dirtySpace      uint64 // if exceed this space, modified pages will `spill` to disk
	mergeThreshold  uint64
	verbosity       kv.DBVerbosityLvl
	label           kv.Label // marker to distinct db instances - one process may open many databases. for example to collect metrics of only 1 database
	inMem           bool

	// roTxsLimiter - without this limiter - it's possible to reach 10K threads (if 10K rotx will wait for IO) - and golang will crush https://groups.google.com/g/golang-dev/c/igMoDruWNwo
	// most of db must set explicit `roTxsLimiter <= 9K`.
	// There is way to increase the 10,000 thread limit: https://golang.org/pkg/runtime/debug/#SetMaxThreads
	roTxsLimiter *semaphore.Weighted

	metrics bool
}

const DefaultMapSize = 2 * datasize.TB
const DefaultGrowthStep = 1 * datasize.GB

func New(label kv.Label, log log.Logger) MdbxOpts {
	opts := MdbxOpts{
		bucketsCfg: WithChaindataTables,
		flags:      mdbx.NoReadahead | mdbx.Durable,
		log:        log,
		pageSize:   kv.DefaultPageSize(),

		mapSize:         DefaultMapSize,
		growthStep:      DefaultGrowthStep,
		mergeThreshold:  2 * 8192,
		shrinkThreshold: -1, // default
		label:           label,
		metrics:         label == kv.ChainDB,
	}
	if label == kv.ChainDB {
		opts = opts.RemoveFlags(mdbx.NoReadahead) // enable readahead for chaindata by default. Erigon3 require fast updates and prune. Also it's chaindata is small (doesen GB)
	}
	return opts
}

func (opts MdbxOpts) GetLabel() kv.Label             { return opts.label }
func (opts MdbxOpts) GetPageSize() datasize.ByteSize { return opts.pageSize }

// Setters
func (opts MdbxOpts) DirtySpace(s uint64) MdbxOpts                { opts.dirtySpace = s; return opts }
func (opts MdbxOpts) RoTxsLimiter(l *semaphore.Weighted) MdbxOpts { opts.roTxsLimiter = l; return opts }
func (opts MdbxOpts) PageSize(v datasize.ByteSize) MdbxOpts       { opts.pageSize = v; return opts }
func (opts MdbxOpts) GrowthStep(v datasize.ByteSize) MdbxOpts     { opts.growthStep = v; return opts }
func (opts MdbxOpts) Path(path string) MdbxOpts                   { opts.path = path; return opts }
func (opts MdbxOpts) SyncPeriod(period time.Duration) MdbxOpts    { opts.syncPeriod = period; return opts }
func (opts MdbxOpts) SyncBytes(threshold uint) MdbxOpts           { opts.syncBytes = &threshold; return opts }
func (opts MdbxOpts) DBVerbosity(v kv.DBVerbosityLvl) MdbxOpts    { opts.verbosity = v; return opts }
func (opts MdbxOpts) MapSize(sz datasize.ByteSize) MdbxOpts       { opts.mapSize = sz; return opts }
func (opts MdbxOpts) WriteMergeThreshold(v uint64) MdbxOpts       { opts.mergeThreshold = v; return opts }
func (opts MdbxOpts) WithTableCfg(f TableCfgFunc) MdbxOpts        { opts.bucketsCfg = f; return opts }
func (opts MdbxOpts) WithMetrics() MdbxOpts                       { opts.metrics = true; return opts }

// Flags
func (opts MdbxOpts) HasFlag(flag uint) bool           { return opts.flags&flag != 0 }
func (opts MdbxOpts) Flags(f func(uint) uint) MdbxOpts { opts.flags = f(opts.flags); return opts }
func (opts MdbxOpts) AddFlags(flags uint) MdbxOpts     { opts.flags = opts.flags | flags; return opts }
func (opts MdbxOpts) RemoveFlags(flags uint) MdbxOpts  { opts.flags = opts.flags &^ flags; return opts }
func (opts MdbxOpts) boolToFlag(enabled bool, flag uint) MdbxOpts {
	if enabled {
		return opts.AddFlags(flag)
	}
	return opts.RemoveFlags(flag)
}
func (opts MdbxOpts) WriteMap(v bool) MdbxOpts  { return opts.boolToFlag(v, mdbx.WriteMap) }
func (opts MdbxOpts) Exclusive(v bool) MdbxOpts { return opts.boolToFlag(v, mdbx.Exclusive) }
func (opts MdbxOpts) Readonly(v bool) MdbxOpts  { return opts.boolToFlag(v, mdbx.Readonly) }
func (opts MdbxOpts) Accede(v bool) MdbxOpts    { return opts.boolToFlag(v, mdbx.Accede) }

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
	opts.flags = mdbx.UtterlyNoSync | mdbx.NoMetaSync | mdbx.NoMemInit
	opts.growthStep = 2 * datasize.MB
	opts.mapSize = 16 * datasize.GB
	opts.dirtySpace = uint64(32 * datasize.MB)
	opts.shrinkThreshold = 0 // disable
	opts.pageSize = 4096
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

var ErrDBDoesNotExists = errors.New("can't create database - because opening in `Accede` mode. probably another (main) process can create it")

func (opts MdbxOpts) Open(ctx context.Context) (kv.RwDB, error) {
	if dbg.DirtySpace() > 0 {
		opts = opts.DirtySpace(dbg.DirtySpace()) //nolint
	}
	if dbg.MergeTr() > 0 {
		opts = opts.WriteMergeThreshold(uint64(dbg.MergeTr() * 8192)) //nolint
	}

	if opts.metrics {
		kv.InitSummaries(opts.label)
	}
	if opts.HasFlag(mdbx.Accede) || opts.HasFlag(mdbx.Readonly) {
		for retry := 0; ; retry++ {
			exists, err := dir.FileExist(filepath.Join(opts.path, "mdbx.dat"))
			if err != nil {
				return nil, err
			}
			if exists {
				break
			}
			if retry >= 5 {
				return nil, fmt.Errorf("%w, label: %s, path: %s", ErrDBDoesNotExists, opts.label, opts.path)
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

	}

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		return nil, err
	}
	if opts.label == kv.ChainDB && opts.verbosity != -1 {
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
	if err = env.SetOption(mdbx.OptRpAugmentLimit, 1_000_000_000); err != nil { //default: 262144
		return nil, err
	}

	exists, err := dir.FileExist(filepath.Join(opts.path, "mdbx.dat"))
	if err != nil {
		return nil, err
	}

	if !opts.HasFlag(mdbx.Accede) && !exists {
		if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(opts.growthStep), opts.shrinkThreshold, int(opts.pageSize)); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	} else if exists {
		if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(opts.growthStep), opts.shrinkThreshold, -1); err != nil {
			return nil, err
		}
	}

	// erigon using big transactions
	// increase "page measured" options. need do it after env.Open() because default are depend on pageSize known only after env.Open()
	if !opts.HasFlag(mdbx.Readonly) {
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
		if opts.label == kv.ChainDB {
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
		}

		// before env.Open() we don't know real pageSize. but will be implemented soon: https://gitflic.ru/project/erthink/libmdbx/issue/15
		// but we want call all `SetOption` before env.Open(), because:
		//   - after they will require rwtx-lock, which is not acceptable in ACCEDEE mode.
		pageSize := opts.pageSize
		if pageSize == 0 {
			pageSize = kv.DefaultPageSize()
		}
		var dirtySpace uint64
		if opts.dirtySpace > 0 {
			dirtySpace = opts.dirtySpace
		} else {
			dirtySpace = estimate.TotalMemory() / 42 // it's default of mdbx, but our package also supports cgroups and GOMEMLIMIT
			// clamp to max size
			const dirtySpaceMaxChainDB = uint64(1 * datasize.GB)
			const dirtySpaceMaxDefault = uint64(64 * datasize.MB)

			if opts.label == kv.ChainDB && dirtySpace > dirtySpaceMaxChainDB {
				dirtySpace = dirtySpaceMaxChainDB
			} else if opts.label != kv.ChainDB && dirtySpace > dirtySpaceMaxDefault {
				dirtySpace = dirtySpaceMaxDefault
			}
		}
		//can't use real pagesize here - it will be known only after env.Open()
		if err = env.SetOption(mdbx.OptTxnDpLimit, dirtySpace/pageSize.Bytes()); err != nil {
			return nil, err
		}

		// must be in the range from 12.5% (almost empty) to 50% (half empty)
		// which corresponds to the range from 8192 and to 32768 in units respectively
		if err = env.SetOption(mdbx.OptMergeThreshold16dot16Percent, opts.mergeThreshold); err != nil {
			return nil, err
		}
	}

	err = env.Open(opts.path, opts.flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label, stack2.Trace().String())
	}

	// mdbx will not change pageSize if db already exists. means need read real value after env.open()
	in, err := env.Info(nil)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label, stack2.Trace().String())
	}

	opts.pageSize = datasize.ByteSize(in.PageSize)
	opts.mapSize = datasize.ByteSize(in.MapSize)
	if opts.label == kv.ChainDB {
		opts.log.Info("[db] open", "label", opts.label, "sizeLimit", opts.mapSize, "pageSize", opts.pageSize)
	} else {
		opts.log.Debug("[db] open", "label", opts.label, "sizeLimit", opts.mapSize, "pageSize", opts.pageSize)
	}

	dirtyPagesLimit, err := env.GetOption(mdbx.OptTxnDpLimit)
	if err != nil {
		return nil, err
	}

	if opts.HasFlag(mdbx.SafeNoSync) && opts.syncPeriod != 0 {
		if err = env.SetSyncPeriod(opts.syncPeriod); err != nil {
			env.Close()
			return nil, err
		}
	}

	if opts.HasFlag(mdbx.SafeNoSync) && opts.syncBytes != nil {
		if err = env.SetSyncBytes(*opts.syncBytes); err != nil {
			env.Close()
			return nil, err
		}
	}

	if opts.roTxsLimiter == nil {
		targetSemCount := int64(runtime.GOMAXPROCS(-1) * 16)
		opts.roTxsLimiter = semaphore.NewWeighted(targetSemCount) // 1 less than max to allow unlocking to happen
	}

	txsCountMutex := &sync.Mutex{}

	db := &MdbxKV{
		opts:         opts,
		env:          env,
		log:          opts.log,
		buckets:      kv.TableCfg{},
		txSize:       dirtyPagesLimit * opts.pageSize.Bytes(),
		roTxsLimiter: opts.roTxsLimiter,

		txsCountMutex:         txsCountMutex,
		txsAllDoneOnCloseCond: sync.NewCond(txsCountMutex),

		leakDetector: dbg.NewLeakDetector("db."+string(opts.label), dbg.SlowTx()),

		MaxBatchSize:  DefaultMaxBatchSize,
		MaxBatchDelay: DefaultMaxBatchDelay,
	}

	customBuckets := opts.bucketsCfg(kv.TablesCfgByLabel(opts.label))
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
			dbi, createErr := tx.OpenDBISimple(name, mdbx.DBAccede)
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
	if dbg.MdbxLockInRam() && opts.label == kv.ChainDB {
		log.Info("[dbg] locking db in mem", "label", opts.label)
		if err := db.View(ctx, func(tx kv.Tx) error { return tx.(*MdbxTx).LockDBInRam() }); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (opts MdbxOpts) MustOpen() kv.RwDB {
	db, err := opts.Open(context.Background())
	if err != nil {
		panic(fmt.Errorf("fail to open mdbx: %w", err))
	}
	return db
}

type MdbxKV struct {
	log          log.Logger
	env          *mdbx.Env
	buckets      kv.TableCfg
	roTxsLimiter *semaphore.Weighted // does limit amount of concurrent Ro transactions - in most casess runtime.NumCPU() is good value for this channel capacity - this channel can be shared with other components (like Decompressor)
	opts         MdbxOpts
	txSize       uint64
	closed       atomic.Bool
	path         string

	txsCount              uint
	txsCountMutex         *sync.Mutex
	txsAllDoneOnCloseCond *sync.Cond

	leakDetector *dbg.LeakDetector

	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchDelay time.Duration

	batchMu sync.Mutex
	batch   *batch
}

func (db *MdbxKV) Path() string                { return db.opts.path }
func (db *MdbxKV) PageSize() datasize.ByteSize { return db.opts.pageSize }
func (db *MdbxKV) ReadOnly() bool              { return db.opts.HasFlag(mdbx.Readonly) }
func (db *MdbxKV) Accede() bool                { return db.opts.HasFlag(mdbx.Accede) }

func (db *MdbxKV) CHandle() unsafe.Pointer {
	return db.env.CHandle()
}

// openDBIs - first trying to open existing DBI's in RO transaction
// otherwise re-try by RW transaction
// it allow open DB from another process - even if main process holding long RW transaction
func (db *MdbxKV) openDBIs(buckets []string) error {
	if db.ReadOnly() || db.Accede() {
		return db.View(context.Background(), func(tx kv.Tx) error {
			for _, name := range buckets {
				if db.buckets[name].IsDeprecated {
					continue
				}
				if err := tx.(kv.BucketMigrator).CreateTable(name); err != nil {
					return err
				}
			}
			return tx.(*MdbxTx).Commit() // when open db as read-only, commit of this RO transaction is required
		})
	}

	return db.Update(context.Background(), func(tx kv.RwTx) error {
		for _, name := range buckets {
			if db.buckets[name].IsDeprecated {
				continue
			}
			if err := tx.(kv.BucketMigrator).CreateTable(name); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *MdbxKV) trackTxBegin() bool {
	db.txsCountMutex.Lock()
	defer db.txsCountMutex.Unlock()

	isOpen := !db.closed.Load()
	if isOpen {
		db.txsCount++
	}
	return isOpen
}

func (db *MdbxKV) hasTxsAllDoneAndClosed() bool {
	return (db.txsCount == 0) && db.closed.Load()
}

func (db *MdbxKV) trackTxEnd() {
	db.txsCountMutex.Lock()
	defer db.txsCountMutex.Unlock()

	if db.txsCount > 0 {
		db.txsCount--
	} else {
		panic("MdbxKV: unmatched trackTxEnd")
	}

	if db.hasTxsAllDoneAndClosed() {
		db.txsAllDoneOnCloseCond.Signal()
	}
}

func (db *MdbxKV) waitTxsAllDoneOnClose() {
	db.txsCountMutex.Lock()
	defer db.txsCountMutex.Unlock()

	for !db.hasTxsAllDoneAndClosed() {
		db.txsAllDoneOnCloseCond.Wait()
	}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *MdbxKV) Close() {
	if ok := db.closed.CompareAndSwap(false, true); !ok {
		return
	}
	db.waitTxsAllDoneOnClose()

	db.env.Close()
	db.env = nil

	if db.opts.inMem {
		if err := dir.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}
	removeFromPathDbMap(db.path)
}

func (db *MdbxKV) BeginRo(ctx context.Context) (txn kv.Tx, err error) {
	// don't try to acquire if the context is already done
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// otherwise carry on
	}

	if !db.trackTxBegin() {
		return nil, errors.New("db closed")
	}

	// will return nil err if context is cancelled (may appear to acquire the semaphore)
	if semErr := db.roTxsLimiter.Acquire(ctx, 1); semErr != nil {
		db.trackTxEnd()
		return nil, fmt.Errorf("mdbx.MdbxKV.BeginRo: roTxsLimiter error %w", semErr)
	}

	defer func() {
		if txn == nil {
			// on error, or if there is whatever reason that we don't return a tx,
			// we need to free up the limiter slot, otherwise it could lead to deadlocks
			db.roTxsLimiter.Release(1)
			db.trackTxEnd()
		}
	}()

	tx, err := db.env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, db.opts.label, stack2.Trace().String())
	}

	return &MdbxTx{
		ctx:      ctx,
		db:       db,
		tx:       tx,
		readOnly: true,
		traceID:  db.leakDetector.Add(),
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

	if !db.trackTxBegin() {
		return nil, errors.New("db closed")
	}

	runtime.LockOSThread()
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		db.trackTxEnd()
		return nil, fmt.Errorf("%w, lable: %s, trace: %s", err, db.opts.label, stack2.Trace().String())
	}

	return &MdbxTx{
		db:      db,
		tx:      tx,
		ctx:     ctx,
		traceID: db.leakDetector.Add(),
	}, nil
}

type MdbxTx struct {
	tx               *mdbx.Txn
	traceID          uint64 // set only if TRACE_TX=true
	db               *MdbxKV
	statelessCursors map[string]kv.RwCursor
	readOnly         bool
	ctx              context.Context

	toCloseMap map[uint64]kv.Closer
	cursorID   uint64
}

type MdbxCursor struct {
	toCloseMap map[uint64]kv.Closer
	c          *mdbx.Cursor
	bucketName string
	isDupSort  bool
	id         uint64
	label      kv.Label // marker to distinct db instances - one process may open many databases. for example to collect metrics of only 1 database
}

func (db *MdbxKV) Env() *mdbx.Env { return db.env }
func (db *MdbxKV) AllTables() kv.TableCfg {
	return db.buckets
}
func (tx *MdbxTx) IsRo() bool                    { return tx.readOnly }
func (tx *MdbxTx) ViewID() uint64                { return tx.tx.ID() }
func (tx *MdbxTx) ListTables() ([]string, error) { return tx.tx.ListDBI() }

func (db *MdbxKV) AllDBI() map[string]kv.DBI {
	res := map[string]kv.DBI{}
	for name, cfg := range db.buckets {
		res[name] = cfg.DBI
	}
	return res
}

func (tx *MdbxTx) Count(bucket string) (uint64, error) {
	st, err := tx.tx.StatDBI(mdbx.DBI(tx.db.buckets[bucket].DBI))
	if err != nil {
		return 0, err
	}
	return st.Entries, nil
}

func (tx *MdbxTx) CollectMetrics() {
	if !tx.db.opts.metrics {
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

	var dbLabel = string(tx.db.opts.label)
	kv.MDBXGauges.DbSize.WithLabelValues(dbLabel).SetUint64(info.Geo.Current)
	kv.MDBXGauges.DbPgopsNewly.WithLabelValues(dbLabel).SetUint64(info.PageOps.Newly)
	kv.MDBXGauges.DbPgopsCow.WithLabelValues(dbLabel).SetUint64(info.PageOps.Cow)
	kv.MDBXGauges.DbPgopsClone.WithLabelValues(dbLabel).SetUint64(info.PageOps.Clone)
	kv.MDBXGauges.DbPgopsSplit.WithLabelValues(dbLabel).SetUint64(info.PageOps.Split)
	kv.MDBXGauges.DbPgopsMerge.WithLabelValues(dbLabel).SetUint64(info.PageOps.Merge)
	kv.MDBXGauges.DbPgopsSpill.WithLabelValues(dbLabel).SetUint64(info.PageOps.Spill)
	kv.MDBXGauges.DbPgopsUnspill.WithLabelValues(dbLabel).SetUint64(info.PageOps.Unspill)
	kv.MDBXGauges.DbPgopsWops.WithLabelValues(dbLabel).SetUint64(info.PageOps.Wops)
	kv.MDBXGauges.UnsyncedBytes.WithLabelValues(dbLabel).SetUint64(uint64(info.UnsyncedBytes))

	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return
	}

	kv.MDBXGauges.TxDirty.WithLabelValues(dbLabel).SetUint64(txInfo.SpaceDirty)
	kv.MDBXGauges.TxRetired.WithLabelValues(dbLabel).SetUint64(txInfo.SpaceRetired)
	kv.MDBXGauges.TxLimit.WithLabelValues(dbLabel).SetUint64(tx.db.txSize)
	kv.MDBXGauges.TxSpill.WithLabelValues(dbLabel).SetUint64(txInfo.Spill)
	kv.MDBXGauges.TxUnspill.WithLabelValues(dbLabel).SetUint64(txInfo.Unspill)

	gc, err := tx.BucketStat("gc")
	if err != nil {
		return
	}
	kv.MDBXGauges.GcLeafMetric.WithLabelValues(dbLabel).SetUint64(gc.LeafPages)
	kv.MDBXGauges.GcOverflowMetric.WithLabelValues(dbLabel).SetUint64(gc.OverflowPages)
	kv.MDBXGauges.GcPagesMetric.WithLabelValues(dbLabel).SetUint64((gc.LeafPages + gc.OverflowPages) * tx.db.opts.pageSize.Bytes() / 8)
}

func (tx *MdbxTx) WarmupDB(force bool) error {
	if force {
		return tx.tx.EnvWarmup(mdbx.WarmupForce|mdbx.WarmupOomSafe, time.Hour)
	}
	return tx.tx.EnvWarmup(mdbx.WarmupDefault, time.Hour)
}
func (tx *MdbxTx) LockDBInRam() error     { return tx.tx.EnvWarmup(mdbx.WarmupLock, time.Hour) }
func (tx *MdbxTx) UnlockDBFromRam() error { return tx.tx.EnvWarmup(mdbx.WarmupRelease, time.Hour) }

func (db *MdbxKV) View(ctx context.Context, f func(tx kv.Tx) error) (err error) {
	// can't use db.env.View method - because it calls commit for read transactions - it conflicts with write transactions.
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (tx *MdbxTx) Apply(_ context.Context, f func(tx kv.Tx) error) (err error) {
	return f(tx)
}

func (tx *MdbxTx) ApplyRw(_ context.Context, f func(tx kv.RwTx) error) (err error) {
	return f(tx)
}

type TxApplySource interface {
	ApplyChan() TxApplyChan
}

type TxApplyChan chan apply

type apply interface {
	Apply()
}

type applyTx struct {
	err chan error
	tx  kv.Tx
	f   func(kv.Tx) error
}

func (a *applyTx) Apply() {
	defer func() { // Would prefer this not to crash but rather log the error
		r := recover()
		if r != nil {
			a.err <- fmt.Errorf("apply paniced: %s", r)
		}
	}()
	a.err <- a.f(a.tx)
}

type applyRwTx struct {
	err chan error
	tx  kv.RwTx
	f   func(kv.RwTx) error
}

func (a *applyRwTx) Apply() {
	defer func() { // Would prefer this not to crash but rather log the error
		r := recover()
		if r != nil {
			a.err <- fmt.Errorf("apply paniced: %s", r)
		}
	}()
	a.err <- a.f(a.tx)
}

type asyncTx struct {
	kv.Tx
	requests chan apply
}

func NewAsyncTx(tx kv.Tx, queueSize int) *asyncTx {
	return &asyncTx{tx, make(chan apply, queueSize)}
}

func (a *asyncTx) Apply(ctx context.Context, f func(kv.Tx) error) error {
	rc := make(chan error)
	a.requests <- &applyTx{rc, a.Tx, f}
	select {
	case err := <-rc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *asyncTx) ApplyChan() TxApplyChan {
	return a.requests
}

type asyncRwTx struct {
	kv.RwTx
	requests chan apply
}

func NewAsyncRwTx(tx kv.RwTx, queueSize int) *asyncRwTx {
	return &asyncRwTx{tx, make(chan apply, queueSize)}
}

func (a *asyncRwTx) Apply(ctx context.Context, f func(kv.Tx) error) error {
	rc := make(chan error)
	a.requests <- &applyTx{rc, a.RwTx, f}
	select {
	case err := <-rc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *asyncRwTx) ApplyRw(ctx context.Context, f func(kv.RwTx) error) error {
	rc := make(chan error)
	a.requests <- &applyRwTx{rc, a.RwTx, f}
	select {
	case err := <-rc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *asyncRwTx) ApplyChan() TxApplyChan {
	return a.requests
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
	err = tx.(*MdbxTx).Commit()
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
	err = tx.(*MdbxTx).Commit()
	if err != nil {
		return err
	}
	return nil
}

func (tx *MdbxTx) CreateTable(name string) error {
	cnfCopy := tx.db.buckets[name]
	dbi, err := tx.tx.OpenDBISimple(name, mdbx.DBAccede)
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
	if !(tx.db.ReadOnly() || tx.db.Accede()) {
		nativeFlags |= mdbx.Create
	}

	if flags&kv.DupSort != 0 {
		nativeFlags |= mdbx.DupSort
		flags ^= kv.DupSort
	}
	if flags != 0 {
		return errors.New("some not supported flag provided for bucket")
	}

	dbi, err = tx.tx.OpenDBISimple(name, nativeFlags)

	if err != nil {
		return fmt.Errorf("db-table doesn't exists: %s, label: %s, %w. Tip: try run `integration run_migrations` to create non-existing tables", name, tx.db.opts.label, err)
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
		nativeDBI, err := tx.tx.OpenDBISimple(name, 0)
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

func (tx *MdbxTx) ClearTable(bucket string) error {
	dbi := tx.db.buckets[bucket].DBI
	if dbi == NonExistingDBI {
		return nil
	}
	return tx.tx.Drop(mdbx.DBI(dbi), false)
}

func (tx *MdbxTx) DropTable(bucket string) error {
	if cfg, ok := tx.db.buckets[bucket]; !(ok && cfg.IsDeprecated) {
		return fmt.Errorf("%w, bucket: %s", kv.ErrAttemptToDeleteNonDeprecatedBucket, bucket)
	}

	return tx.dropEvenIfBucketIsNotDeprecated(bucket)
}

func (tx *MdbxTx) ExistsTable(bucket string) (bool, error) {
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
		tx.db.trackTxEnd()
		if tx.readOnly {
			tx.db.roTxsLimiter.Release(1)
		} else {
			runtime.UnlockOSThread()
		}
		tx.db.leakDetector.Del(tx.traceID)
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
		return fmt.Errorf("label: %s, %w", tx.db.opts.label, err)
	}

	if tx.db.opts.metrics {
		dbLabel := tx.db.opts.label
		err = kv.RecordSummaries(dbLabel, latency)
		if err != nil {
			tx.db.opts.log.Error("failed to record mdbx summaries", "err", err)
		}

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
		tx.db.trackTxEnd()
		if tx.readOnly {
			tx.db.roTxsLimiter.Release(1)
		} else {
			runtime.UnlockOSThread()
		}
		tx.db.leakDetector.Del(tx.traceID)
	}()
	tx.closeCursors()
	tx.tx.Abort()
}

func (tx *MdbxTx) SpaceDirty() (uint64, uint64, error) {
	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return 0, 0, err
	}

	return txInfo.SpaceDirty, tx.db.txSize, nil
}

func (tx *MdbxTx) closeCursors() {
	for _, c := range tx.toCloseMap {
		if c != nil {
			c.Close()
		}
	}
	tx.toCloseMap = nil
	tx.statelessCursors = nil
}

func (tx *MdbxTx) statelessCursor(bucket string) (kv.RwCursor, error) {
	if tx.statelessCursors == nil {
		tx.statelessCursors = make(map[string]kv.RwCursor)
	}
	c, ok := tx.statelessCursors[bucket]
	if !ok {
		var err error
		c, err = tx.RwCursor(bucket) //nolint:gocritic
		if err != nil {
			return nil, err
		}
		tx.statelessCursors[bucket] = c
	}
	return c, nil
}

func (tx *MdbxTx) Put(table string, k, v []byte) error {
	return tx.tx.Put(mdbx.DBI(tx.db.buckets[table].DBI), k, v, 0)
}

func (tx *MdbxTx) Delete(table string, k []byte) error {
	err := tx.tx.Del(mdbx.DBI(tx.db.buckets[table].DBI), k, nil)
	if mdbx.IsNotFound(err) {
		return nil
	}
	return err
}

func (tx *MdbxTx) GetOne(bucket string, k []byte) ([]byte, error) {
	v, err := tx.tx.Get(mdbx.DBI(tx.db.buckets[bucket].DBI), k)
	if mdbx.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("label: %s, table: %s, %w", tx.db.opts.label, bucket, err)
	}
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

func (tx *MdbxTx) ResetSequence(bucket string, newValue uint64) error {
	c, err := tx.statelessCursor(kv.Sequence)
	if err != nil {
		return err
	}
	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, newValue)

	return c.Put([]byte(bucket), newVBytes)
}

func (tx *MdbxTx) ReadSequence(bucket string) (uint64, error) {
	c, err := tx.statelessCursor(kv.Sequence)
	if err != nil {
		return 0, err
	}
	_, v, err := c.SeekExact([]byte(bucket))
	if err != nil {
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
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * tx.db.opts.pageSize.Bytes(), nil
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
	c := &MdbxCursor{bucketName: bucket, toCloseMap: tx.toCloseMap, label: tx.db.opts.label, isDupSort: tx.db.buckets[bucket].Flags&mdbx.DupSort != 0, id: tx.cursorID}
	tx.cursorID++

	if tx.tx == nil {
		panic("assert: tx.tx nil. seems this `tx` was Rollback'ed")
	}
	var err error
	c.c, err = tx.tx.OpenCursor(mdbx.DBI(tx.db.buckets[c.bucketName].DBI))
	if err != nil {
		return nil, fmt.Errorf("table: %s, %w, stack: %s", c.bucketName, err, dbg.Stack())
	}

	// add to auto-cleanup on end of transactions
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[c.id] = c
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

func (c *MdbxCursor) First() ([]byte, []byte, error) {
	return c.Seek(nil)
}

func (c *MdbxCursor) Last() ([]byte, []byte, error) {
	k, v, err := c.c.Get(nil, nil, mdbx.Last)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed MdbxKV cursor.Last(): %w, bucket: %s", err, c.bucketName)
		return []byte{}, nil, err
	}

	return k, v, nil
}

func (c *MdbxCursor) Seek(seek []byte) (k, v []byte, err error) {
	if len(seek) == 0 {
		k, v, err = c.c.Get(nil, nil, mdbx.First)
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, fmt.Errorf("cursor.First: %w, bucket: %s, key: %x", err, c.bucketName, seek)
		}
		return k, v, nil
	}

	k, v, err = c.c.Get(seek, nil, mdbx.SetRange)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("cursor.SetRange: %w, bucket: %s, key: %x", err, c.bucketName, seek)
	}
	return k, v, nil
}

func (c *MdbxCursor) Next() (k, v []byte, err error) {
	k, v, err = c.c.Get(nil, nil, mdbx.Next)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Next(): %w", err)
	}
	return k, v, nil
}

func (c *MdbxCursor) Prev() (k, v []byte, err error) {
	k, v, err = c.c.Get(nil, nil, mdbx.Prev)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Prev(): %w", err)
	}
	return k, v, nil
}

// Current - return key/data at current cursor position
func (c *MdbxCursor) Current() ([]byte, []byte, error) {
	k, v, err := c.c.Get(nil, nil, mdbx.GetCurrent)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *MdbxCursor) Delete(k []byte) error {
	_, _, err := c.c.Get(k, nil, mdbx.Set)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	}

	if c.isDupSort {
		return c.c.Del(mdbx.AllDups)
	}

	return c.c.Del(mdbx.Current)
}

// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
// This does not invalidate the cursor, so operations such as MDB_NEXT
// can still be used on it.
// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
// this operation.
func (c *MdbxCursor) DeleteCurrent() error             { return c.c.Del(mdbx.Current) }
func (c *MdbxCursor) PutNoOverwrite(k, v []byte) error { return c.c.Put(k, v, mdbx.NoOverwrite) }

func (c *MdbxCursor) Put(key []byte, value []byte) error {
	if err := c.c.Put(key, value, 0); err != nil {
		return fmt.Errorf("label: %s, table: %s, err: %w", c.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	k, v, err := c.c.Get(key, nil, mdbx.Set)
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
	if err := c.c.Put(k, v, mdbx.Append); err != nil {
		return fmt.Errorf("label: %s, bucket: %s, %w", c.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxCursor) Close() {
	if c.c != nil {
		c.c.Close()
		delete(c.toCloseMap, c.id)
		c.c = nil
	}
}

func (c *MdbxCursor) IsClosed() bool { return c.c == nil }

type MdbxDupSortCursor struct {
	*MdbxCursor
}

// DeleteExact - does delete
func (c *MdbxDupSortCursor) DeleteExact(k1, k2 []byte) error {
	_, _, err := c.c.Get(k1, k2, mdbx.GetBoth)
	if err != nil { // if key not found, or found another one - then nothing to delete
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	}
	return c.c.Del(mdbx.Current)
}

func (c *MdbxDupSortCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	_, v, err := c.c.Get(key, value, mdbx.GetBoth)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in SeekBothExact: %w", err)
	}
	return key, v, nil
}

func (c *MdbxDupSortCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	_, v, err := c.c.Get(key, value, mdbx.GetBothRange)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in SeekBothRange, table=%s: %w", c.bucketName, err)
	}
	return v, nil
}

func (c *MdbxDupSortCursor) FirstDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, mdbx.FirstDup)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("in FirstDup: tbl=%s, %w", c.bucketName, err)
	}
	return v, nil
}

// NextDup - iterate only over duplicates of current key
func (c *MdbxDupSortCursor) NextDup() ([]byte, []byte, error) {
	k, v, err := c.c.Get(nil, nil, mdbx.NextDup)
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
	k, v, err := c.c.Get(nil, nil, mdbx.NextNoDup)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextNoDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) PrevDup() ([]byte, []byte, error) {
	k, v, err := c.c.Get(nil, nil, mdbx.PrevDup)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) PrevNoDup() ([]byte, []byte, error) {
	k, v, err := c.c.Get(nil, nil, mdbx.PrevNoDup)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevNoDup: %w", err)
	}
	return k, v, nil
}

func (c *MdbxDupSortCursor) LastDup() ([]byte, error) {
	_, v, err := c.c.Get(nil, nil, mdbx.LastDup)
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
		return fmt.Errorf("label: %s, in Append: bucket=%s, %w", c.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) AppendDup(k []byte, v []byte) error {
	if err := c.c.Put(k, v, mdbx.AppendDup); err != nil {
		return fmt.Errorf("label: %s, in AppendDup: bucket=%s, %w", c.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) PutNoDupData(k, v []byte) error {
	if err := c.c.Put(k, v, mdbx.NoDupData); err != nil {
		return fmt.Errorf("label: %s, in PutNoDupData: %w", c.label, err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *MdbxDupSortCursor) DeleteCurrentDuplicates() error {
	if err := c.c.Del(mdbx.AllDups); err != nil {
		return fmt.Errorf("label: %s,in DeleteCurrentDuplicates: %w", c.label, err)
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

func (tx *MdbxTx) Prefix(table string, prefix []byte) (stream.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return tx.Range(table, prefix, nil, order.Asc, -1)
	}
	return tx.Range(table, prefix, nextPrefix, order.Asc, -1)
}

func (tx *MdbxTx) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s := &cursor2iter{ctx: tx.ctx, tx: tx, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: asc, limit: int64(limit), id: tx.cursorID}
	tx.cursorID++
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[s.id] = s
	if err := s.init(table, tx); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	if !s.tx.readOnly {
		s.nextK, s.nextV = common.Copy(s.nextK), common.Copy(s.nextV)
	}
	return s, nil
}

type cursor2iter struct {
	c  kv.Cursor
	id uint64
	tx *MdbxTx

	fromPrefix, toPrefix, nextK, nextV []byte
	orderAscend                        order.By
	limit                              int64
	ctx                                context.Context
}

func (s *cursor2iter) init(table string, tx kv.Tx) error {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.Cursor(table) //nolint:gocritic
	if err != nil {
		return err
	}
	s.c = c

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextK, s.nextV, err = s.c.First()
		} else {
			s.nextK, s.nextV, err = s.c.Last()
		}
		if err != nil {
			return err
		}
		return nil
	}

	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Seek(s.fromPrefix)
		if err != nil {
			return err
		}
		return nil
	}

	// `Seek(s.fromPrefix)` find first key with prefix `s.fromPrefix`, but we need LAST one.
	// `Seek(nextPrefix)+Prev()` will do the job.
	nextPrefix, ok := kv.NextSubtree(s.fromPrefix)
	if !ok { // end of table
		s.nextK, s.nextV, err = s.c.Last()
		if err != nil {
			return err
		}
		if s.nextK == nil {
			return nil
		}

		// go to last value of this key
		if casted, ok := s.c.(kv.CursorDupSort); ok {
			s.nextV, err = casted.LastDup()
			if err != nil {
				return err
			}
		}
		return nil
	}

	s.nextK, s.nextV, err = s.c.Seek(nextPrefix)
	if err != nil {
		return err
	}
	if s.nextK == nil {
		s.nextK, s.nextV, err = s.c.Last()
	} else {
		s.nextK, s.nextV, err = s.c.Prev()
	}
	if err != nil {
		return err
	}
	if s.nextK == nil {
		return nil
	}

	// go to last value of this key
	if casted, ok := s.c.(kv.CursorDupSort); ok {
		s.nextV, err = casted.LastDup()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *cursor2iter) advance() (err error) {
	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Next()
		if err != nil {
			return err
		}
	} else {
		s.nextK, s.nextV, err = s.c.Prev()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *cursor2iter) Close() {
	if s == nil {
		return
	}
	if s.c != nil {
		s.c.Close()
		delete(s.tx.toCloseMap, s.id)
		s.c = nil
	}
}

func (s *cursor2iter) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextK == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
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
	k, v = s.nextK, s.nextV
	if err = s.advance(); err != nil {
		return nil, nil, err
	}
	if !s.tx.readOnly {
		s.nextK, s.nextV = common.Copy(s.nextK), common.Copy(s.nextV)
	}
	return k, v, nil
}

func (tx *MdbxTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s := &cursorDup2iter{ctx: tx.ctx, tx: tx, key: key, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: bool(asc), limit: int64(limit), id: tx.cursorID}
	tx.cursorID++
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[s.id] = s
	if err := s.init(table, tx); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	if !s.tx.readOnly {
		s.nextV = common.Copy(s.nextV)
	}
	return s, nil
}

type cursorDup2iter struct {
	c  kv.CursorDupSort
	id uint64
	tx *MdbxTx

	key                         []byte
	fromPrefix, toPrefix, nextV []byte
	orderAscend                 bool
	limit                       int64
	ctx                         context.Context
}

func (s *cursorDup2iter) init(table string, tx kv.Tx) error {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.CursorDupSort(table) //nolint:gocritic
	if err != nil {
		return err
	}
	s.c = c
	k, _, err := c.SeekExact(s.key)
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextV, err = s.c.FirstDup()
			if err != nil {
				return err
			}
		} else {
			s.nextV, err = s.c.LastDup()
			if err != nil {
				return err
			}
		}
		return nil
	}

	if s.orderAscend {
		s.nextV, err = s.c.SeekBothRange(s.key, s.fromPrefix)
		if err != nil {
			return err
		}
		return nil
	}

	// to find LAST key with given prefix:
	nextSubtree, ok := kv.NextSubtree(s.fromPrefix)
	if !ok {
		_, s.nextV, err = s.c.PrevDup()
		if err != nil {
			return err
		}
		return nil
	}

	s.nextV, err = s.c.SeekBothRange(s.key, nextSubtree)
	if err != nil {
		return err
	}
	if s.nextV != nil {
		_, s.nextV, err = s.c.PrevDup()
		if err != nil {
			return err
		}
		return nil
	}

	k, s.nextV, err = s.c.SeekExact(s.key)
	if err != nil {
		return err
	}
	if k == nil {
		s.nextV = nil
		return nil
	}
	s.nextV, err = s.c.LastDup()
	if err != nil {
		return err
	}
	return nil
}

func (s *cursorDup2iter) advance() (err error) {
	if s.orderAscend {
		_, s.nextV, err = s.c.NextDup()
		if err != nil {
			return err
		}
	} else {
		_, s.nextV, err = s.c.PrevDup()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *cursorDup2iter) Close() {
	if s == nil {
		return
	}
	if s.c != nil {
		s.c.Close()
		delete(s.tx.toCloseMap, s.id)
		s.c = nil
	}
}
func (s *cursorDup2iter) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextV == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
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
	v = s.nextV
	if err = s.advance(); err != nil {
		return nil, nil, err
	}
	if !s.tx.readOnly {
		s.nextV = common.Copy(s.nextV)
	}
	return s.key, v, nil
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

func (tx *MdbxTx) CHandle() unsafe.Pointer {
	return tx.tx.CHandle()
}
