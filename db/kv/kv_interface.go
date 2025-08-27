// Copyright 2022 The Erigon Authors
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

package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/version"
)

/*
Naming:
 tx - Database Transaction
 txn - Ethereum Transaction (and TxNum - is also number of Ethereum Transaction)
 blockNum - Ethereum block number - same across all nodes. blockID - auto-increment ID - which can be different across all nodes
 txNum/txID - same
 RoTx - Read-Only Database Transaction. RwTx - read-write
 k, v - key, value
 ts - TimeStamp. Usually it's Ethereum's TransactionNumber (auto-increment ID). Or BlockNumber
 step - amount of txNums in the smallest file
 Table - collection of key-value pairs. In LMDB - it's `dbi`. Analog of SQL's Table. Keys are sorted and unique
 DupSort - if table created `Sorted Duplicates` option: then 1 key can have multiple (sorted and unique) values
 Cursor - low-level mdbx-tide api to navigate over Table
 Stream - high-level iterator-like api over Table/InvertedIndex/History/Domain. Server-side-streaming-friendly. See package `stream`

Methods Naming:
 Prune: delete old data
 Unwind: delete recent data
 Get: exact match of criteria
 Range: [from, to). from=nil means StartOfTable, to=nil means EndOfTable, rangeLimit=-1 means Unlimited
     Range is analog of SQL's: SELECT * FROM Table WHERE k>=from AND k<to ORDER BY k ASC/DESC LIMIT n
 Prefix: `Range(Table, prefix, kv.NextSubtree(prefix))`

Abstraction Layers:
LowLevel:
   1. DB/Tx - low-level key-value database
   2. Snapshots/FrozenData - immutable files with historical data. May be downloaded at first App
        start or auto-generate by moving old data from DB to Snapshots.
        Most important difference between DB and Snapshots: creation of
        snapshot files (build/merge) doesn't mutate any existing files - only producing new one!
        It means we don't need concept of "RwTx" for Snapshots.
        Files can become useless/garbage (merged to bigger file) - last reader of this file will
        remove it from FileSystem on tx.Rollback().
        Invariant: existing readers can't see new files, new readers can't see garbage files

MediumLevel:
   1. TemporalDB - abstracting DB+Snapshots. Target is:
        - provide 'time-travel' API for data: consistent snapshot of data as of given Timestamp.
        - auto-close streams on Commit/Rollback
        - auto-open/close agg.BeginRo() on Begin/Commit/Rollback
        - to keep DB small - only for Hot/Recent data (can be update/delete by re-org).
        - And TemporalRoTx/TemporalRwTx actually open Read-Only files view (BeginRo) - no concept of "Read-Write view of snapshot files".
        - using next entities:
              - InvertedIndex: supports range-scans
              - History: can return value of key K as of given TimeStamp. Doesn't know about latest/current
                  value of key K. Returns NIL if K not changed after TimeStamp.
              - Domain: as History but also aware about latest/current value of key K. Can move
                  cold (updated long time ago) parts of state from db to snapshots.

HighLevel:
     1. Application - rely on TemporalDB (Ex: ExecutionLayer) or just DB (Ex: TxPool, Sentry, Downloader).

General advise: for deeper understanding read `mdbx.h`
*/

/*
RoDB low-level interface - main target is - to provide common abstraction over top of MDBX and RemoteKV.
Warning: can't move `tx` between goroutines. ReadOnly transactions do not lock goroutine to thread, RwTx does.
Lifetime: read data valid until end of transaction.
Example:

	tx, err := db.BeginRo()
	if err != nil {
		return err
	}
	defer tx.Rollback() // it's safe to Rollback after `tx.Commit()`

	... application logic using `tx`

	if err := tx.Commit(); err != nil {
		return err
	}
*/
type RoDB interface {
	Closer
	BeginRo(ctx context.Context) (Tx, error)

	// View like BeginRo but for short-living transactions. Example:
	//	 if err := db.View(ctx, func(tx ethdb.Tx) error {
	//	    ... code which uses database in transaction
	//	 }); err != nil {
	//			return err
	//	}
	View(ctx context.Context, f func(tx Tx) error) error

	ReadOnly() bool
	AllTables() TableCfg
	PageSize() datasize.ByteSize

	// CHandle pointer to the underlying C environment handle, if applicable (e.g. *C.MDBX_env)
	CHandle() unsafe.Pointer
}

type RwDB interface {
	RoDB

	Update(ctx context.Context, f func(tx RwTx) error) error
	UpdateNosync(ctx context.Context, f func(tx RwTx) error) error

	// BeginRw - creates transaction
	// A transaction and its cursors must only be used by a single
	// 	thread (not goroutine), and a thread may only have a single transaction at a time.
	//  It happens automatically by - because this method calls runtime.LockOSThread() inside (Rollback/Commit releases it)
	//  By this reason application code can't call runtime.UnlockOSThread() - it leads to undefined behavior.
	BeginRw(ctx context.Context) (RwTx, error)
	BeginRwNosync(ctx context.Context) (RwTx, error)
}

// Tx
// WARNING:
//   - Tx is not threadsafe and may only be used in the goroutine that created it
//   - ReadOnly transactions do not lock goroutine to thread, RwTx does
type Tx interface {
	Getter

	// Cursor - creates cursor object on top of given table. Type of cursor - depends on table configuration.
	// If table was created with mdbx.DupSort flag, then cursor with interface CursorDupSort created
	// Otherwise - object of interface Cursor created
	//
	// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
	// long keys into DupSort key/values. See docs for `tables.go:TableCfgItem`
	Cursor(table string) (Cursor, error)
	CursorDupSort(table string) (CursorDupSort, error) // CursorDupSort - can be used if bucket has mdbx.DupSort flag

	// --- High-Level methods: 1request -> stream of server-side pushes ---

	// Range [from, to)
	// Range(from, nil) means [from, EndOfTable)
	// Range(nil, to)   means [StartOfTable, to)
	// if `order.Desc` expecting `from`<`to`
	// Limit -1 means Unlimited
	// Designed for requesting huge data (Example: full table scan). Client can't stop it.
	// Example: RangeDescend("Table", "B", "A", order.Asc, -1)
	Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error)
	// StreamDescend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error)

	// Prefix - is exactly Range(Table, prefix, kv.NextSubtree(prefix))
	Prefix(table string, prefix []byte) (stream.KV, error)

	// RangeDupSort - like Range but for fixed single key and iterating over range of values
	RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error)

	// --- High-Level methods: 1request -> 1page of values in response -> send next page request ---
	// Paginate(table string, fromPrefix, toPrefix []byte) (PairsStream, error)

	BucketSize(table string) (uint64, error)
	Count(bucket string) (uint64, error)

	ListTables() ([]string, error)

	// ViewID returns the identifier associated with this transaction. For a
	// read-only transaction, this corresponds to the snapshot being read;
	// concurrent readers will frequently have the same transaction ID.
	ViewID() uint64
	// CHandle pointer to the underlying C transaction handle (e.g. *C.MDBX_txn)
	CHandle() unsafe.Pointer

	Apply(ctx context.Context, f func(tx Tx) error) error
}

// RwTx
//
// WARNING:
//   - RwTx is not threadsafe and may only be used in the goroutine that created it.
//   - ReadOnly transactions do not lock goroutine to thread, RwTx does
//   - User Can't call runtime.LockOSThread/runtime.UnlockOSThread in same goroutine until RwTx Commit/Rollback
type RwTx interface {
	Tx
	Putter
	BucketMigrator

	RwCursor(table string) (RwCursor, error)
	RwCursorDupSort(table string) (RwCursorDupSort, error)

	Commit() error // Commit all the operations of a transaction into the database.

	ApplyRw(ctx context.Context, f func(tx RwTx) error) error
}

/*
Cursor - low-level api to navigate through a db table
If methods (like First/Next/seekInFiles) return error, then returned key SHOULD not be nil (can be []byte{} for example).
Exmaple iterate table:

	c := db.Cursor(tableName)
	defer c.Close()
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
	   if err != nil {
		   return err
	   }
	   ... logic using `k` and `v` (key and value)
	}
*/
type Cursor interface {
	First() ([]byte, []byte, error)               // First - position at first key/data item
	Seek(seek []byte) ([]byte, []byte, error)     // Seek - position at first key greater than or equal to specified key
	SeekExact(key []byte) ([]byte, []byte, error) // SeekExact - position at exact matching key if exists
	Next() ([]byte, []byte, error)                // Next - position at next key/value (can iterate over DupSort key/values automatically)
	Prev() ([]byte, []byte, error)                // Prev - position at previous key
	Last() ([]byte, []byte, error)                // Last - position at last key and last possible value
	Current() ([]byte, []byte, error)             // Current - return key/data at current cursor position

	Close()
}

type RwCursor interface {
	Cursor

	Put(k, v []byte) error           // Put - based on order
	Append(k []byte, v []byte) error // Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
	Delete(k []byte) error           // Delete - short version of SeekExact+DeleteCurrent or SeekBothExact+DeleteCurrent

	// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
	// This does not invalidate the cursor, so operations such as MDB_NEXT
	// can still be used on it.
	// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
	// this operation.
	DeleteCurrent() error
}

/*
CursorDupSort
Example iterate over DupSort table:

	for k, v, err = cursor.First(); k != nil; k, v, err = cursor.NextNoDup() {
		if err != nil {
			return err
		}
		// iterate over all values of key `k`
		for ; v != nil; _, v, err = cursor.NextDup() {
			if err != nil {
				return err
			}
			// use
		}
	}
*/
type CursorDupSort interface {
	Cursor

	// SeekBothExact -
	// second parameter can be nil only if searched key has no duplicates, or return error
	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, error) // SeekBothRange - exact match of the key, but range match of the value
	FirstDup() ([]byte, error)                       // FirstDup - position at first data item of current key
	NextDup() ([]byte, []byte, error)                // NextDup - position at next data item of current key
	NextNoDup() ([]byte, []byte, error)              // NextNoDup - position at first data item of next key
	PrevDup() ([]byte, []byte, error)
	PrevNoDup() ([]byte, []byte, error)
	LastDup() ([]byte, error) // LastDup - position at last data item of current key

	CountDuplicates() (uint64, error) // CountDuplicates - number of duplicates for the current key
}

type RwCursorDupSort interface {
	CursorDupSort
	RwCursor

	PutNoDupData(key, value []byte) error // PutNoDupData - inserts key without dupsort
	DeleteCurrentDuplicates() error       // DeleteCurrentDuplicates - deletes all values of the current key
	DeleteExact(k1, k2 []byte) error      // DeleteExact - delete 1 value from given key
	AppendDup(key, value []byte) error    // AppendDup - same as Append, but for sorted dup data
}

const Unlim int = -1 // const Unbounded/EOF/EndOfTable []byte = nil

type StatelessRwTx interface {
	Getter
	Putter
}

type GetPut interface {
	Getter
	Putter
}
type Getter interface {
	// Has indicates whether a key exists in the database.
	Has(table string, key []byte) (bool, error)

	// GetOne references a readonly section of memory that must not be accessed after txn has terminated
	GetOne(table string, key []byte) (val []byte, err error)

	Rollback() // Rollback - abandon all the operations of the transaction instead of saving them.

	// ReadSequence - allows to create a linear sequence of unique positive integers for each table (AutoIncrement).
	// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
	// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
	// Starts from 0.
	ReadSequence(table string) (uint64, error)

	// --- High-Level deprecated methods ---

	// ForEach iterates over entries with keys greater or equal to fromPrefix.
	// walker is called for each eligible entry.
	// If walker returns an error:
	//   - implementations of local db - stop
	//   - implementations of remote db - do not handle this error and may finish (send all entries to client) before error happen.
	ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error
	ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error
}

// Putter wraps the database write operations.
type Putter interface {
	// Put inserts or updates a single entry.
	Put(table string, k, v []byte) error

	// Delete removes a single entry.
	Delete(table string, k []byte) error

	/*
		IncrementSequence - AutoIncrement generator.
		Example reserve 1 ID:
		id, err := tx.IncrementSequence(table, 1)
		if err != nil {
			return err
		}
		// use id

		Example reserving N ID's:
		baseId, err := tx.IncrementSequence(table, N)
		if err != nil {
		   return err
		}
		for i := 0; i < N; i++ {    // if N == 0, it will work as expected
			id := baseId + i
			// use id
		}
	*/
	IncrementSequence(table string, amount uint64) (uint64, error)

	// ResetSequence allow set arbitrary value to sequence (for example to decrement it to exact value)
	ResetSequence(table string, newValue uint64) error
	Append(table string, k, v []byte) error
	AppendDup(table string, k, v []byte) error

	// CollectMetrics - does collect all DB-related and Tx-related metrics
	// this method exists only in RwTx to avoid concurrency
	CollectMetrics()
}

// ---- Temporal part

// Step - amount of txNums in the smallest file
type Step uint64

func (s Step) ToTxNum(stepSize uint64) uint64 { return uint64(s) * stepSize }

type (
	Domain      uint16
	Appendable  uint16
	InvertedIdx uint16
	ForkableId  uint16
)

type TemporalGetter interface {
	GetLatest(name Domain, k []byte) (v []byte, step Step, err error)
	HasPrefix(name Domain, prefix []byte) (firstKey []byte, firstVal []byte, hasPrefix bool, err error)
}
type TemporalTx interface {
	Tx
	TemporalGetter
	WithFreezeInfo

	// GetAsOf - state as of given `ts`
	// Example: GetAsOf(Account, key, txNum) - returns account's value before `txNum` transaction changed it
	// To re-execute `txNum` on historical state - do `DomainGetAsOf(key, txNum)` to read state
	// `ok = false` means: key not found. or "future txNum" passed.
	GetAsOf(name Domain, k []byte, ts uint64) (v []byte, ok bool, err error)
	RangeAsOf(name Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error)

	// IndexRange - return iterator over range of inverted index for given key `k`
	// Asc semantic:  [from, to) AND from > to
	// Desc semantic: [from, to) AND from < to
	// Limit -1 means Unlimited
	// from -1, to -1 means unbounded (StartOfTable, EndOfTable)
	// Example: IndexRange("IndexName", 10, 5, order.Desc, -1)
	// Example: IndexRange("IndexName", -1, -1, order.Asc, 10)
	IndexRange(name InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error)

	// HistorySeek - like `GetAsOf` but without latest state - only for `History`
	// `ok == true && v != nil && len(v) == 0` means key-creation even
	HistorySeek(name Domain, k []byte, ts uint64) (v []byte, ok bool, err error)

	// HistoryRange - producing "state patch": sorted and deduplicated list of keys updated at [fromTs,toTs) with their most-recent value
	HistoryRange(name Domain, fromTs, toTs int, asc order.By, limit int) (it stream.KV, err error)

	Debug() TemporalDebugTx
	AggTx() any

	AggForkablesTx(ForkableId) any // any forkableId, returns that group
	Unmarked(ForkableId) UnmarkedTx
}

// TemporalDebugTx - set of slow low-level funcs for debug purposes
type TemporalDebugTx interface {
	RangeLatest(domain Domain, from, to []byte, limit int) (stream.KV, error)
	GetLatestFromDB(domain Domain, k []byte) (v []byte, step Step, found bool, err error)
	GetLatestFromFiles(domain Domain, k []byte, maxTxNum uint64) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error)

	DomainFiles(domain ...Domain) VisibleFiles
	CurrentDomainVersion(domain Domain) version.Version
	TxNumsInFiles(domains ...Domain) (minTxNum uint64)

	// HistoryStartFrom return the earliest known txnum in history of a given domain
	HistoryStartFrom(domainName Domain) uint64

	DomainProgress(domain Domain) (txNum uint64)
	IIProgress(name InvertedIdx) (txNum uint64)
	StepSize() uint64

	CanUnwindToBlockNum() (uint64, error)
	CanUnwindBeforeBlockNum(blockNum uint64) (unwindableBlockNum uint64, ok bool, err error)
}

type TemporalDebugDB interface {
	DomainTables(names ...Domain) []string
	InvertedIdxTables(names ...InvertedIdx) []string
	BuildMissedAccessors(ctx context.Context, workers int) error
	ReloadFiles() error
	EnableReadAhead() TemporalDebugDB
	DisableReadAhead()

	Files() []string
	MergeLoop(ctx context.Context) error
}

type WithFreezeInfo interface {
	FreezeInfo() FreezeInfo
}

type FreezeInfo interface {
	AllFiles() VisibleFiles
	Files(domainName Domain) VisibleFiles
}

type TemporalRwTx interface {
	RwTx
	TemporalTx
	TemporalPutDel

	UnmarkedRw(ForkableId) UnmarkedRwTx

	GreedyPruneHistory(ctx context.Context, domain Domain) error
	PruneSmallBatches(ctx context.Context, timeout time.Duration) (haveMore bool, err error)
	Unwind(ctx context.Context, txNumUnwindTo uint64, changeset *[DomainLen][]DomainEntryDiff) error
}

type TemporalPutDel interface {
	// DomainPut
	// Optimizations:
	//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
	//   - user can append k2 into k1, then underlying methods will not perform append
	DomainPut(domain Domain, k, v []byte, txNum uint64, prevVal []byte, prevStep Step) error
	//DomainPut2(domain Domain, k1 []byte, val []byte, ts uint64) error

	// DomainDel
	// Optimizations:
	//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
	//   - user can append k2 into k1, then underlying methods will not perform append
	//   - if `val == nil` it will call DomainDel
	DomainDel(domain Domain, k []byte, txNum uint64, prevVal []byte, prevStep Step) error
	DomainDelPrefix(domain Domain, prefix []byte, txNum uint64) error
}

type TemporalRoDB interface {
	RoDB
	SnapshotNotifier
	ViewTemporal(ctx context.Context, f func(tx TemporalTx) error) error
	BeginTemporalRo(ctx context.Context) (TemporalTx, error)
	Debug() TemporalDebugDB
}
type TemporalRwDB interface {
	RwDB
	TemporalRoDB
	BeginTemporalRw(ctx context.Context) (TemporalRwTx, error)
	UpdateTemporal(ctx context.Context, f func(tx TemporalRwTx) error) error
}

// ---- non-important utilities

type TxnId uint64 // internal auto-increment ID. can't cast to eth-network canonical blocks txNum

type HasSpaceDirty interface {
	SpaceDirty() (uint64, uint64, error)
}

// BucketMigrator used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	ListTables() ([]string, error)
	DropTable(string) error
	CreateTable(string) error
	ExistsTable(string) (bool, error)
	ClearTable(string) error
}

// PendingMutations in-memory storage of changes
// Later they can either be flushed to the database or abandon
type PendingMutations interface {
	Putter
	// Flush all in-memory data into `tx`
	Flush(ctx context.Context, tx RwTx) error
	Close()
	BatchSize() int
}

type DBVerbosityLvl int8
type Label string

const (
	ChainDB         = "chaindata"
	TxPoolDB        = "txpool"
	SentryDB        = "sentry"
	ConsensusDB     = "consensus"
	DownloaderDB    = "downloader"
	HeimdallDB      = "heimdall"
	DiagnosticsDB   = "diagnostics"
	PolygonBridgeDB = "polygon-bridge"
	CaplinDB        = "caplin"
	TemporaryDB     = "temporary"
)

const ReadersLimit = 32000 // MDBX_READERS_LIMIT=32767
const dbLabelName = "db"

type DBGauges struct { // these gauges are shared by all MDBX instances, but need to be filtered by label
	DbSize        *metrics.GaugeVec
	TxLimit       *metrics.GaugeVec
	TxSpill       *metrics.GaugeVec
	TxUnspill     *metrics.GaugeVec
	TxDirty       *metrics.GaugeVec
	TxRetired     *metrics.GaugeVec
	UnsyncedBytes *metrics.GaugeVec

	DbPgopsNewly   *metrics.GaugeVec
	DbPgopsCow     *metrics.GaugeVec
	DbPgopsClone   *metrics.GaugeVec
	DbPgopsSplit   *metrics.GaugeVec
	DbPgopsMerge   *metrics.GaugeVec
	DbPgopsSpill   *metrics.GaugeVec
	DbPgopsUnspill *metrics.GaugeVec
	DbPgopsWops    *metrics.GaugeVec

	GcLeafMetric     *metrics.GaugeVec
	GcOverflowMetric *metrics.GaugeVec
	GcPagesMetric    *metrics.GaugeVec
}

type DBSummaries struct { // the summaries are particular to a DB instance
	DbCommitPreparation metrics.Summary
	DbCommitWrite       metrics.Summary
	DbCommitSync        metrics.Summary
	DbCommitEnding      metrics.Summary
	DbCommitTotal       metrics.Summary
}

// InitMDBXMGauges this only needs to be called once during startup
func InitMDBXMGauges() *DBGauges {
	return &DBGauges{
		DbSize:         metrics.GetOrCreateGaugeVec(`db_size`, []string{dbLabelName}),
		TxLimit:        metrics.GetOrCreateGaugeVec(`tx_limit`, []string{dbLabelName}),
		TxSpill:        metrics.GetOrCreateGaugeVec(`tx_spill`, []string{dbLabelName}),
		TxUnspill:      metrics.GetOrCreateGaugeVec(`tx_unspill`, []string{dbLabelName}),
		TxDirty:        metrics.GetOrCreateGaugeVec(`tx_dirty`, []string{dbLabelName}),
		UnsyncedBytes:  metrics.GetOrCreateGaugeVec(`unsynced_bytes`, []string{dbLabelName}),
		TxRetired:      metrics.GetOrCreateGaugeVec(`tx_retired`, []string{dbLabelName}),
		DbPgopsNewly:   metrics.GetOrCreateGaugeVec(`db_pgops{phase="newly"}`, []string{dbLabelName}),
		DbPgopsCow:     metrics.GetOrCreateGaugeVec(`db_pgops{phase="cow"}`, []string{dbLabelName}),
		DbPgopsClone:   metrics.GetOrCreateGaugeVec(`db_pgops{phase="clone"}`, []string{dbLabelName}),
		DbPgopsSplit:   metrics.GetOrCreateGaugeVec(`db_pgops{phase="split"}`, []string{dbLabelName}),
		DbPgopsMerge:   metrics.GetOrCreateGaugeVec(`db_pgops{phase="merge"}`, []string{dbLabelName}),
		DbPgopsSpill:   metrics.GetOrCreateGaugeVec(`db_pgops{phase="spill"}`, []string{dbLabelName}),
		DbPgopsUnspill: metrics.GetOrCreateGaugeVec(`db_pgops{phase="unspill"}`, []string{dbLabelName}),
		DbPgopsWops:    metrics.GetOrCreateGaugeVec(`db_pgops{phase="wops"}`, []string{dbLabelName}),

		GcLeafMetric:     metrics.GetOrCreateGaugeVec(`db_gc_leaf`, []string{dbLabelName}),
		GcOverflowMetric: metrics.GetOrCreateGaugeVec(`db_gc_overflow`, []string{dbLabelName}),
		GcPagesMetric:    metrics.GetOrCreateGaugeVec(`db_gc_pages`, []string{dbLabelName}),
	}
}

func InitSummaries(dbLabel Label) {
	_, ok := MDBXSummaries.Load(dbLabel)
	if !ok {
		dbName := string(dbLabel)
		MDBXSummaries.Store(dbName, &DBSummaries{
			DbCommitPreparation: metrics.GetOrCreateSummaryWithLabels(`db_commit_seconds`, []string{dbLabelName, "phase"}, []string{dbName, "preparation"}),
			DbCommitWrite:       metrics.GetOrCreateSummaryWithLabels(`db_commit_seconds`, []string{dbLabelName, "phase"}, []string{dbName, "write"}),
			DbCommitSync:        metrics.GetOrCreateSummaryWithLabels(`db_commit_seconds`, []string{dbLabelName, "phase"}, []string{dbName, "sync"}),
			DbCommitEnding:      metrics.GetOrCreateSummaryWithLabels(`db_commit_seconds`, []string{dbLabelName, "phase"}, []string{dbName, "ending"}),
			DbCommitTotal:       metrics.GetOrCreateSummaryWithLabels(`db_commit_seconds`, []string{dbLabelName, "phase"}, []string{dbName, "total"}),
		})
	}
}

func RecordSummaries(dbLabel Label, latency mdbx.CommitLatency) error {
	_summaries, ok := MDBXSummaries.Load(string(dbLabel))
	if !ok {
		return fmt.Errorf("MDBX summaries not initialized yet for db=%s", string(dbLabel))
	}
	// cast to *DBSummaries
	summaries, ok := _summaries.(*DBSummaries)
	if !ok {
		return fmt.Errorf("type casting to *DBSummaries failed")
	}

	summaries.DbCommitPreparation.Observe(latency.Preparation.Seconds())
	summaries.DbCommitWrite.Observe(latency.Write.Seconds())
	summaries.DbCommitSync.Observe(latency.Sync.Seconds())
	summaries.DbCommitEnding.Observe(latency.Ending.Seconds())
	summaries.DbCommitTotal.Observe(latency.Whole.Seconds())
	return nil

}

var MDBXGauges = InitMDBXMGauges() // global mdbx gauges. each gauge can be filtered by db name
var MDBXSummaries sync.Map         // dbName => Summaries mapping

var (
	ErrAttemptToDeleteNonDeprecatedBucket = errors.New("only buckets from dbutils.ChaindataDeprecatedTables can be deleted")
	/*
		DbPgopsPrefault = metrics.NewCounter(`db_pgops{phase="prefault"}`) //nolint
		DbPgopsMinicore = metrics.NewCounter(`db_pgops{phase="minicore"}`) //nolint
		DbPgopsMsync    = metrics.NewCounter(`db_pgops{phase="msync"}`)    //nolint
		DbPgopsFsync    = metrics.NewCounter(`db_pgops{phase="fsync"}`)    //nolint
		DbMiLastPgNo    = metrics.NewCounter(`db_mi_last_pgno`)            //nolint

		DbGcWorkRtime    = metrics.GetOrCreateSummary(`db_gc_seconds{phase="work_rtime"}`) //nolint
		DbGcWorkRsteps   = metrics.NewCounter(`db_gc{phase="work_rsteps"}`)                //nolint
		DbGcWorkRxpages  = metrics.NewCounter(`db_gc{phase="work_rxpages"}`)               //nolint
		DbGcSelfRtime    = metrics.GetOrCreateSummary(`db_gc_seconds{phase="self_rtime"}`) //nolint
		DbGcSelfXtime    = metrics.GetOrCreateSummary(`db_gc_seconds{phase="self_xtime"}`) //nolint
		DbGcWorkXtime    = metrics.GetOrCreateSummary(`db_gc_seconds{phase="work_xtime"}`) //nolint
		DbGcSelfRsteps   = metrics.NewCounter(`db_gc{phase="self_rsteps"}`)                //nolint
		DbGcWloops       = metrics.NewCounter(`db_gc{phase="wloop"}`)                      //nolint
		DbGcCoalescences = metrics.NewCounter(`db_gc{phase="coalescences"}`)               //nolint
		DbGcWipes        = metrics.NewCounter(`db_gc{phase="wipes"}`)                      //nolint
		DbGcFlushes      = metrics.NewCounter(`db_gc{phase="flushes"}`)                    //nolint
		DbGcKicks        = metrics.NewCounter(`db_gc{phase="kicks"}`)                      //nolint
		DbGcWorkMajflt   = metrics.NewCounter(`db_gc{phase="work_majflt"}`)                //nolint
		DbGcSelfMajflt   = metrics.NewCounter(`db_gc{phase="self_majflt"}`)                //nolint
		DbGcWorkCounter  = metrics.NewCounter(`db_gc{phase="work_counter"}`)               //nolint
		DbGcSelfCounter  = metrics.NewCounter(`db_gc{phase="self_counter"}`)               //nolint
		DbGcSelfXpages   = metrics.NewCounter(`db_gc{phase="self_xpages"}`)                //nolint
	*/

	//DbGcWorkPnlMergeTime   = metrics.GetOrCreateSummary(`db_gc_pnl_seconds{phase="work_merge_time"}`) //nolint
	//DbGcWorkPnlMergeVolume = metrics.NewCounter(`db_gc_pnl{phase="work_merge_volume"}`)               //nolint
	//DbGcWorkPnlMergeCalls  = metrics.NewCounter(`db_gc{phase="work_merge_calls"}`)                    //nolint
	//DbGcSelfPnlMergeTime   = metrics.GetOrCreateSummary(`db_gc_pnl_seconds{phase="slef_merge_time"}`) //nolint
	//DbGcSelfPnlMergeVolume = metrics.NewCounter(`db_gc_pnl{phase="self_merge_volume"}`)               //nolint
	//DbGcSelfPnlMergeCalls  = metrics.NewCounter(`db_gc_pnl{phase="slef_merge_calls"}`)                //nolint
)

type Closer interface {
	Close()
}

type OnFilesChange func(frozenFileNames []string)
type SnapshotNotifier interface {
	OnFilesChange(onChange OnFilesChange, onDelete OnFilesChange)
}
