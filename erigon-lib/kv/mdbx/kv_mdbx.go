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
	"github.com/erigontech/mdbx-go/mdbx"
	stack2 "github.com/go-stack/stack"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/mmap"
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

const DefaultMapSize = 2 * datasize.TB
const DefaultGrowthStep = 1 * datasize.GB

func NewMDBX(log log.Logger) MdbxOpts {
	opts := MdbxOpts{
		bucketsCfg: WithChaindataTables,
		flags:      mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable,
		log:        log,
		pageSize:   kv.DefaultPageSize(),

		mapSize:         DefaultMapSize,
		growthStep:      DefaultGrowthStep,
		mergeThreshold:  2 * 8192,
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
	opts.flags = mdbx.UtterlyNoSync | mdbx.NoMetaSync | mdbx.NoMemInit
	opts.growthStep = 2 * datasize.MB
	opts.mapSize = 512 * datasize.MB
	opts.dirtySpace = uint64(128 * datasize.MB)
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
func (opts MdbxOpts) Accede() MdbxOpts {
	opts.flags = opts.flags | mdbx.Accede
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

func (opts MdbxOpts) WriteMap(flag bool) MdbxOpts {
	if flag {
		opts.flags |= mdbx.WriteMap
	}
	return opts
}
func (opts MdbxOpts) LifoReclaim() MdbxOpts {
	opts.flags |= mdbx.LifoReclaim
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

var ErrDBDoesNotExists = errors.New("can't create database - because opening in `Accede` mode. probably another (main) process can create it")

func (opts MdbxOpts) Open(ctx context.Context) (kv.RwDB, error) {
	opts = opts.WriteMap(dbg.WriteMap())
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
	} else {
		if opts.label == kv.ChainDB {
			opts = opts.Flags(func(u uint) uint { return u &^ mdbx.NoReadahead }) //nolint
		}
	}
	if opts.flags&mdbx.Accede != 0 || opts.flags&mdbx.Readonly != 0 {
		for retry := 0; ; retry++ {
			exists, err := dir.FileExist(filepath.Join(opts.path, "mdbx.dat"))
			if err != nil {
				return nil, err
			}
			if exists {
				break
			}
			if retry >= 5 {
				return nil, fmt.Errorf("%w, label: %s, path: %s", ErrDBDoesNotExists, opts.label.String(), opts.path)
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

	}

	env, err := mdbx.NewEnv()
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

	if !opts.HasFlag(mdbx.Accede) {
		if err = env.SetGeometry(-1, -1, int(opts.mapSize), int(opts.growthStep), opts.shrinkThreshold, int(opts.pageSize)); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(opts.path, 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
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
			dirtySpace = mmap.TotalMemory() / 42 // it's default of mdbx, but our package also supports cgroups and GOMEMLIMIT
			// clamp to max size
			const dirtySpaceMaxChainDB = uint64(1 * datasize.GB)
			const dirtySpaceMaxDefault = uint64(128 * datasize.MB)

			if opts.label == kv.ChainDB && dirtySpace > dirtySpaceMaxChainDB {
				dirtySpace = dirtySpaceMaxChainDB
			} else if opts.label != kv.ChainDB && dirtySpace > dirtySpaceMaxDefault {
				dirtySpace = dirtySpaceMaxDefault
			}
		}
		//can't use real pagesize here - it will be known only after env.Open()
		if err = env.SetOption(mdbx.OptTxnDpLimit, dirtySpace/pageSize); err != nil {
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
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label.String(), stack2.Trace().String())
	}

	// mdbx will not change pageSize if db already exists. means need read real value after env.open()
	in, err := env.Info(nil)
	if err != nil {
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, opts.label.String(), stack2.Trace().String())
	}

	opts.pageSize = uint64(in.PageSize)
	opts.mapSize = datasize.ByteSize(in.MapSize)
	if opts.label == kv.ChainDB {
		opts.log.Info("[db] open", "label", opts.label, "sizeLimit", opts.mapSize, "pageSize", opts.pageSize, "stack", dbg.Stack())
	} else {
		opts.log.Debug("[db] open", "label", opts.label, "sizeLimit", opts.mapSize, "pageSize", opts.pageSize, "stack", dbg.Stack())
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

	txsCountMutex := &sync.Mutex{}

	db := &MdbxKV{
		opts:         opts,
		env:          env,
		log:          opts.log,
		buckets:      kv.TableCfg{},
		txSize:       dirtyPagesLimit * opts.pageSize,
		roTxsLimiter: opts.roTxsLimiter,

		txsCountMutex:         txsCountMutex,
		txsAllDoneOnCloseCond: sync.NewCond(txsCountMutex),

		leakDetector: dbg.NewLeakDetector("db."+opts.label.String(), dbg.SlowTx()),

		MaxBatchSize:  DefaultMaxBatchSize,
		MaxBatchDelay: DefaultMaxBatchDelay,
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

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
)

type batch struct {
	db    *MdbxKV
	timer *time.Timer
	start sync.Once
	calls []call
}

type call struct {
	fn  func(kv.RwTx) error
	err chan<- error
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(context.Background(), func(tx kv.RwTx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(tx kv.RwTx) error, tx kv.RwTx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Batch is only useful when there are multiple goroutines calling it.
// It behaves similar to Update, except:
//
// 1. concurrent Batch calls can be combined into a single RwTx.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// Example of bad side-effects: print messages, mutate external counters `i++`
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
func (db *MdbxKV) Batch(fn func(tx kv.RwTx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if errors.Is(err, trySolo) {
		err = db.Update(context.Background(), fn)
	}
	return err
}

func (db *MdbxKV) Path() string     { return db.opts.path }
func (db *MdbxKV) PageSize() uint64 { return db.opts.pageSize }
func (db *MdbxKV) ReadOnly() bool   { return db.opts.HasFlag(mdbx.Readonly) }
func (db *MdbxKV) Accede() bool     { return db.opts.HasFlag(mdbx.Accede) }

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
				if err := tx.(kv.BucketMigrator).CreateBucket(name); err != nil {
					return err
				}
			}
			return tx.Commit() // when open db as read-only, commit of this RO transaction is required
		})
	}

	return db.Update(context.Background(), func(tx kv.RwTx) error {
		for _, name := range buckets {
			if db.buckets[name].IsDeprecated {
				continue
			}
			if err := tx.(kv.BucketMigrator).CreateBucket(name); err != nil {
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
		if err := os.RemoveAll(db.opts.path); err != nil {
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
		return nil, fmt.Errorf("%w, label: %s, trace: %s", err, db.opts.label.String(), stack2.Trace().String())
	}

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

	if !db.trackTxBegin() {
		return nil, errors.New("db closed")
	}

	runtime.LockOSThread()
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		runtime.UnlockOSThread() // unlock only in case of error. normal flow is "defer .Rollback()"
		db.trackTxEnd()
		return nil, fmt.Errorf("%w, lable: %s, trace: %s", err, db.opts.label.String(), stack2.Trace().String())
	}

	return &MdbxTx{
		db:  db,
		tx:  tx,
		ctx: ctx,
		id:  db.leakDetector.Add(),
	}, nil
}

type MdbxTx struct {
	tx               *mdbx.Txn
	id               uint64 // set only if TRACE_TX=true
	db               *MdbxKV
	statelessCursors map[string]kv.RwCursor
	readOnly         bool
	ctx              context.Context

	toCloseMap map[uint64]kv.Closer
	ID         uint64
}

type MdbxCursor struct {
	tx         *MdbxTx
	c          *mdbx.Cursor
	bucketName string
	bucketCfg  kv.TableCfgItem
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

func (tx *MdbxTx) Count(bucket string) (uint64, error) {
	st, err := tx.tx.StatDBI(mdbx.DBI(tx.db.buckets[bucket].DBI))
	if err != nil {
		return 0, err
	}
	return st.Entries, nil
}

func (db *MdbxKV) AllTables() kv.TableCfg {
	return db.buckets
}

func (tx *MdbxTx) IsRo() bool     { return tx.readOnly }
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

	kv.DbSize.SetUint64(info.Geo.Current)
	kv.DbPgopsNewly.SetUint64(info.PageOps.Newly)
	kv.DbPgopsCow.SetUint64(info.PageOps.Cow)
	kv.DbPgopsClone.SetUint64(info.PageOps.Clone)
	kv.DbPgopsSplit.SetUint64(info.PageOps.Split)
	kv.DbPgopsMerge.SetUint64(info.PageOps.Merge)
	kv.DbPgopsSpill.SetUint64(info.PageOps.Spill)
	kv.DbPgopsUnspill.SetUint64(info.PageOps.Unspill)
	kv.DbPgopsWops.SetUint64(info.PageOps.Wops)

	txInfo, err := tx.tx.Info(true)
	if err != nil {
		return
	}

	kv.TxDirty.SetUint64(txInfo.SpaceDirty)
	kv.TxLimit.SetUint64(tx.db.txSize)
	kv.TxSpill.SetUint64(txInfo.Spill)
	kv.TxUnspill.SetUint64(txInfo.Unspill)

	gc, err := tx.BucketStat("gc")
	if err != nil {
		return
	}
	kv.GcLeafMetric.SetUint64(gc.LeafPages)
	kv.GcOverflowMetric.SetUint64(gc.OverflowPages)
	kv.GcPagesMetric.SetUint64((gc.LeafPages + gc.OverflowPages) * tx.db.opts.pageSize / 8)
}

// ListBuckets - all buckets stored as keys of un-named bucket
func (tx *MdbxTx) ListBuckets() ([]string, error) { return tx.tx.ListDBI() }

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

	dbi, err = tx.tx.OpenDBI(name, nativeFlags, nil, nil)

	if err != nil {
		return fmt.Errorf("db-talbe doesn't exists: %s, %w. Tip: try run `integration run_migrations` to create non-existing tables", name, err)
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
		tx.db.trackTxEnd()
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
		return fmt.Errorf("label: %s, %w", tx.db.opts.label, err)
	}

	if tx.db.opts.label == kv.ChainDB {
		kv.DbCommitPreparation.Observe(latency.Preparation.Seconds())
		//kv.DbCommitAudit.Update(latency.Audit.Seconds())
		kv.DbCommitWrite.Observe(latency.Write.Seconds())
		kv.DbCommitSync.Observe(latency.Sync.Seconds())
		kv.DbCommitEnding.Observe(latency.Ending.Seconds())
		kv.DbCommitTotal.Observe(latency.Whole.Seconds())

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
		c, err = tx.RwCursor(bucket)
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
	//TODO: revise the logic, why we should drop not found err? maybe we need another function for get with key error
	if mdbx.IsNotFound(err) {
		return nil
	}
	return err
}

func (tx *MdbxTx) GetOne(bucket string, k []byte) ([]byte, error) {
	v, err := tx.tx.Get(mdbx.DBI(tx.db.buckets[bucket].DBI), k)
	//TODO: revise the logic, why we should drop not found err? maybe we need another function for get with key error
	if mdbx.IsNotFound(err) {
		return nil, nil
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
	c := &MdbxCursor{bucketName: bucket, tx: tx, bucketCfg: b, id: tx.ID}
	tx.ID++

	var err error
	c.c, err = tx.tx.OpenCursor(mdbx.DBI(tx.db.buckets[c.bucketName].DBI))
	if err != nil {
		return nil, fmt.Errorf("table: %s, %w, stack: %s", c.bucketName, err, dbg.Stack())
	}

	// add to auto-cleanup on end of transactions
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[c.id] = c.c
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
func (c *MdbxCursor) putNoOverwrite(k, v []byte) error     { return c.c.Put(k, v, mdbx.NoOverwrite) }
func (c *MdbxCursor) getBoth(k, v []byte) ([]byte, error) {
	_, v, err := c.c.Get(k, v, mdbx.GetBoth)
	return v, err
}
func (c *MdbxCursor) getBothRange(k, v []byte) ([]byte, error) {
	_, v, err := c.c.Get(k, v, mdbx.GetBothRange)
	return v, err
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
	k, v, err = c.next()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Next(): %w", err)
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

	return k, v, nil
}

func (c *MdbxCursor) Delete(k []byte) error {
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

func (c *MdbxCursor) PutNoOverwrite(key []byte, value []byte) error {
	return c.putNoOverwrite(key, value)
}

func (c *MdbxCursor) Put(key []byte, value []byte) error {
	if err := c.put(key, value); err != nil {
		return fmt.Errorf("label: %s, table: %s, err: %w", c.tx.db.opts.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxCursor) SeekExact(key []byte) ([]byte, []byte, error) {
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
	if err := c.c.Put(k, v, mdbx.Append); err != nil {
		return fmt.Errorf("label: %s, bucket: %s, %w", c.tx.db.opts.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxCursor) Close() {
	if c.c != nil {
		c.c.Close()
		delete(c.tx.toCloseMap, c.id)
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
		return fmt.Errorf("label: %s, in Append: bucket=%s, %w", c.tx.db.opts.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) AppendDup(k []byte, v []byte) error {
	if err := c.c.Put(k, v, mdbx.AppendDup); err != nil {
		return fmt.Errorf("label: %s, in AppendDup: bucket=%s, %w", c.tx.db.opts.label, c.bucketName, err)
	}
	return nil
}

func (c *MdbxDupSortCursor) PutNoDupData(k, v []byte) error {
	if err := c.c.Put(k, v, mdbx.NoDupData); err != nil {
		return fmt.Errorf("label: %s, in PutNoDupData: %w", c.tx.db.opts.label, err)
	}

	return nil
}

// DeleteCurrentDuplicates - delete all of the data items for the current key.
func (c *MdbxDupSortCursor) DeleteCurrentDuplicates() error {
	if err := c.delAllDupData(); err != nil {
		return fmt.Errorf("label: %s,in DeleteCurrentDuplicates: %w", c.tx.db.opts.label, err)
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
		return tx.Range(table, prefix, nil)
	}
	return tx.Range(table, prefix, nextPrefix)
}

func (tx *MdbxTx) Range(table string, fromPrefix, toPrefix []byte) (stream.KV, error) {
	return tx.RangeAscend(table, fromPrefix, toPrefix, -1)
}
func (tx *MdbxTx) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Asc, limit)
}
func (tx *MdbxTx) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Desc, limit)
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

func (tx *MdbxTx) rangeOrderLimit(table string, fromPrefix, toPrefix []byte, orderAscend order.By, limit int) (*cursor2iter, error) {
	s := &cursor2iter{ctx: tx.ctx, tx: tx, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: orderAscend, limit: int64(limit), id: tx.ID}
	tx.ID++
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[s.id] = s
	if err := s.init(table, tx); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}
func (s *cursor2iter) init(table string, tx kv.Tx) error {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.Cursor(table)
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
	return k, v, nil
}

func (tx *MdbxTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s := &cursorDup2iter{ctx: tx.ctx, tx: tx, key: key, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: bool(asc), limit: int64(limit), id: tx.ID}
	tx.ID++
	if tx.toCloseMap == nil {
		tx.toCloseMap = make(map[uint64]kv.Closer)
	}
	tx.toCloseMap[s.id] = s
	if err := s.init(table, tx); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
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
	c, err := tx.CursorDupSort(table)
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
