package mdbx

/*
#include <stdlib.h>
#include <stdio.h>
#include "mdbxgo.h"
*/
import "C"

import (
	"log"
	"runtime"
	"time"
	"unsafe"
)

// This flags are used exclusively for Txn.OpenDBI and Txn.OpenRoot.  The
// Create flag must always be supplied when opening a non-root DBI for the
// first time.
//
// BUG(bmatsuo):
// MDBX_INTEGERKEY and MDBX_INTEGERDUP aren't usable. I'm not sure they would be
// faster with the cgo bridge.  They need to be tested and benchmarked.
const (
	// Flags for Txn.OpenDBI.

	ReverseKey = C.MDBX_REVERSEKEY // Use reverse string keys.
	DupSort    = C.MDBX_DUPSORT    // Use sorted duplicates.
	DupFixed   = C.MDBX_DUPFIXED   // Duplicate items have a fixed size (DupSort).
	ReverseDup = C.MDBX_REVERSEDUP // Reverse duplicate values (DupSort).
	Create     = C.MDBX_CREATE     // Create DB if not already existing.
	DBAccede   = C.MDBX_DB_ACCEDE  // Use sorted duplicates.
)

const (
	TxRW        = C.MDBX_TXN_READWRITE
	TxRO        = C.MDBX_TXN_RDONLY
	TxPrepareRO = C.MDBX_TXN_RDONLY_PREPARE

	TxTry        = C.MDBX_TXN_TRY
	TxNoMetaSync = C.MDBX_TXN_NOMETASYNC
	TxNoSync     = C.MDBX_TXN_NOSYNC
)

// Txn is a database transaction in an environment.
//
// WARNING: A writable Txn is not threadsafe and may only be used in the
// goroutine that created it.
//
// See MDBX_txn.
type Txn struct {
	// If RawRead is true []byte values retrieved from Get() calls on the Txn
	// and its cursors will point directly into the memory-mapped structure.
	// Such slices will be readonly and must only be referenced wthin the
	// transaction's lifetime.
	RawRead bool

	// Pooled may be set to true while a Txn is stored in a sync.Pool, after
	// Txn.Reset reset has been called and before Txn.Renew.  This will keep
	// the Txn finalizer from unnecessarily warning the application about
	// finalizations.
	Pooled bool

	managed  bool
	readonly bool

	// The value of Txn.ID() is cached so that the cost of cgo does not have to
	// be paid.  The id of a Txn cannot change over its life, even if it is
	// reset/renewed
	id uintptr

	env  *Env
	_txn *C.MDBX_txn
	key  *C.MDBX_val
	val  *C.MDBX_val

	errLogf func(format string, v ...interface{})
}

// beginTxn does not lock the OS thread which is a prerequisite for creating a
// write transaction.
func beginTxn(env *Env, flags uint) (*Txn, error) {
	txn := &Txn{
		readonly: flags&Readonly != 0,
		env:      env,
	}

	if flags&Readonly == 0 {
		// In a write Txn we can use the shared, C-allocated key and value
		// allocated by env, and freed when it is closed.
		txn.key = env.ckey
		txn.val = env.cval
	} else {
		// It is not easy to share C.MDBX_val values in this scenario unless
		// there is a synchronized pool involved, which will increase
		// overhead.  Further, allocating these values with C will add
		// overhead both here and when the values are freed.
		txn.key = new(C.MDBX_val)
		txn.val = new(C.MDBX_val)
	}
	ret := C.mdbx_txn_begin(env._env, nil, C.MDBX_txn_flags_t(flags), &txn._txn)
	if ret != success {
		return nil, operrno("mdbx_txn_begin", ret)
	}
	return txn, nil
}

// ID returns the identifier for txn.  A view transaction identifier
// corresponds to the Env snapshot being viewed and may be shared with other
// view transactions.
//
// See mdbx_txn_id.
func (txn *Txn) ID() uintptr {
	// It is possible for a txn to legitimately have ID 0 if it a readonly txn
	// created before any updates.  In practice this does not really happen
	// because an application typically must do an initial update to initialize
	// application dbis.  Even so, calling C.mdbx_txn_id excessively isn't
	// actually harmful, it is just slow.
	if txn.id == 0 {
		txn.id = txn.getID()
	}

	return txn.id
}

func (txn *Txn) getID() uintptr {
	return uintptr(C.mdbx_txn_id(txn._txn))
}

// RunOp executes fn with txn as an argument.  During the execution of fn no
// goroutine may call the Commit, Abort, Reset, and Renew methods on txn.
// RunOp returns the result of fn without any further action.  RunOp will not
// abort txn if fn returns an error, unless terminate is true.  If terminate is
// true then RunOp will attempt to commit txn if fn is successful, otherwise
// RunOp will abort txn before returning any failure encountered.
//
// RunOp primarily exists to allow applications and other packages to provide
// variants of the managed transactions provided by lmdb (i.e. View, Update,
// etc).  For example, the lmdbpool package uses RunOp to provide an
// Txn-friendly sync.Pool and a function analogous to Env.View that uses
// transactions from that pool.
func (txn *Txn) RunOp(fn TxnOp, terminate bool) error {
	if terminate {
		return txn.runOpTerm(fn)
	}
	return txn.runOp(fn)
}

func (txn *Txn) runOpTerm(fn TxnOp) error {
	if txn.managed {
		panic("managed transaction cannot be terminated directly")
	}
	defer txn.abort()

	// There is no need to restore txn.managed after fn has executed because
	// the Txn will terminate one way or another using methods which don't
	// check txn.managed.
	txn.managed = true

	err := fn(txn)
	if err != nil {
		return err
	}
	_, err = txn.commit()
	return err
}

func (txn *Txn) runOp(fn TxnOp) error {
	if !txn.managed {
		// Restoring txn.managed must be done in a deferred call otherwise the
		// caller may not be able to abort the transaction if a runtime panic
		// occurs (attempting to do so would cause another panic).
		txn.managed = true
		defer func() {
			txn.managed = false
		}()
	}
	return fn(txn)
}

// Commit persists all transaction operations to the database and clears the
// finalizer on txn.  A Txn cannot be used again after Commit is called.
//
// See mdbx_txn_commit.
func (txn *Txn) Commit() (CommitLatency, error) {
	if txn.managed {
		panic("managed transaction cannot be committed directly")
	}

	runtime.SetFinalizer(txn, nil)
	return txn.commit()
}

type CommitLatency struct {
	Preparation time.Duration
	GC          time.Duration
	Audit       time.Duration
	Write       time.Duration
	Sync        time.Duration
	Ending      time.Duration
	Whole       time.Duration
}

func toDuration(seconds16dot16 C.uint32_t) time.Duration {
	return time.Duration((uint64(1000000000)*uint64(seconds16dot16) + 32768) >> 16)
}

func (txn *Txn) commit() (CommitLatency, error) {
	var _stat C.MDBX_commit_latency
	ret := C.mdbx_txn_commit_ex(txn._txn, &_stat)
	txn.clearTxn()
	s := CommitLatency{
		Preparation: toDuration(_stat.preparation),
		GC:          toDuration(_stat.gc),
		Audit:       toDuration(_stat.audit),
		Write:       toDuration(_stat.write),
		Sync:        toDuration(_stat.sync),
		Ending:      toDuration(_stat.ending),
		Whole:       toDuration(_stat.whole),
	}
	if ret != success {
		return s, operrno("mdbx_txn_commit_ex", ret)
	}
	return s, nil
}

// Abort discards pending writes in the transaction and clears the finalizer on
// txn.  A Txn cannot be used again after Abort is called.
//
// See mdbx_txn_abort.
func (txn *Txn) Abort() {
	if txn.managed {
		panic("managed transaction cannot be aborted directly")
	}

	runtime.SetFinalizer(txn, nil)
	txn.abort()
}

func (txn *Txn) abort() {
	if txn._txn == nil {
		return
	}

	// Get a read-lock on the environment so we can abort txn if needed.
	// txn.env **should** terminate all readers otherwise when it closes.
	txn.env.closeLock.RLock()
	if txn.env._env != nil {
		C.mdbx_txn_abort(txn._txn)
	}
	txn.env.closeLock.RUnlock()

	txn.clearTxn()
}

func (txn *Txn) clearTxn() {
	// Clear the C object to prevent any potential future use of the freed
	// pointer.
	txn._txn = nil

	// Clear txn.id because it no longer matches the value of txn._txn (and
	// future calls to txn.ID() should not see the stale id).  Instead of
	// returning the old ID future calls to txn.ID() will query LMDB to make
	// sure the value returned for an invalid Txn is more or less consistent
	// for people familiar with the C semantics.
	txn.resetID()
}

// resetID has to be called anytime the value of Txn.getID() may change
// otherwise the cached value may diverge from the actual value and the
// abstraction has failed.
func (txn *Txn) resetID() {
	txn.id = 0
}

// Reset aborts the transaction clears internal state so the transaction may be
// reused by calling Renew.  If txn is not going to be reused txn.Abort() must
// be called to release its slot in the lock table and free its memory.  Reset
// panics if txn is managed by Update, View, etc.
//
// See mdbx_txn_reset.
func (txn *Txn) Reset() {
	if txn.managed {
		panic("managed transaction cannot be reset directly")
	}

	txn.reset()
}

func (txn *Txn) reset() {
	C.mdbx_txn_reset(txn._txn)
}

// Renew reuses a transaction that was previously reset by calling txn.Reset().
// Renew panics if txn is managed by Update, View, etc.
//
// See mdbx_txn_renew.
func (txn *Txn) Renew() error {
	if txn.managed {
		panic("managed transaction cannot be renewed directly")
	}

	return txn.renew()
}

func (txn *Txn) renew() error {
	ret := C.mdbx_txn_renew(txn._txn)

	// mdbx_txn_renew causes txn._txn to pick up a new transaction ID.  It's
	// slightly confusing in the LMDB docs.  Txn ID corresponds to database
	// snapshot the reader is holding, which is good because renewed
	// transactions can see updates which happened since they were created (or
	// since they were last renewed).  It should follow that renewing a Txn
	// results in the freeing of stale pages the Txn has been holding, though
	// this has not been confirmed in any way by bmatsuo as of 2017-02-15.
	txn.resetID()

	return operrno("mdbx_txn_renew", ret)
}

// OpenDBI opens a named database in the environment.  An error is returned if
// name is empty.  The DBI returned by OpenDBI can be used in other
// transactions but not before Txn has terminated.
//
// OpenDBI can only be called after env.SetMaxDBs() has been called to set the
// maximum number of named databases.
//
// The C API uses null terminated strings for database names.  A consequence is
// that names cannot contain null bytes themselves. OpenDBI does not check for
// null bytes in the name argument.
//
// See mdbx_dbi_open.
func (txn *Txn) OpenDBI(name string, flags uint, cmp, dcmp CmpFunc) (DBI, error) {
	cname := C.CString(name)
	dbi, err := txn.openDBI(cname, flags, (*C.MDBX_cmp_func)(unsafe.Pointer(cmp)), (*C.MDBX_cmp_func)(unsafe.Pointer(dcmp)))
	C.free(unsafe.Pointer(cname))
	return dbi, err
}

func (txn *Txn) OpenDBISimple(name string, flags uint) (DBI, error) {
	cname := C.CString(name)
	dbi, err := txn.openDBISimple(cname, flags)
	C.free(unsafe.Pointer(cname))
	return dbi, err
}

// CreateDBI is a shorthand for OpenDBI that passed the flag lmdb.Create.
func (txn *Txn) CreateDBI(name string) (DBI, error) {
	return txn.OpenDBI(name, Create, nil, nil)
}

// Flags returns the database flags for handle dbi.
func (txn *Txn) Flags(dbi DBI) (uint, error) {
	var cflags C.uint
	ret := C.mdbx_dbi_flags(txn._txn, C.MDBX_dbi(dbi), &cflags)
	return uint(cflags), operrno("mdbx_dbi_flags", ret)
}

// OpenRoot opens the root database.  OpenRoot behaves similarly to OpenDBI but
// does not require env.SetMaxDBs() to be called beforehand.  And, OpenRoot can
// be called without flags in a View transaction.
func (txn *Txn) OpenRoot(flags uint) (DBI, error) {
	return txn.openDBI(nil, flags, nil, nil)
}

type Cmp func(k1, k2 []byte) int

// openDBI returns returns whatever DBI value was set by mdbx_open_dbi.  In an
// error case, LMDB does not currently set DBI in case of failure, so zero is
// returned in those cases.  This is not a big deal for now because
// applications are expected to handle any error encountered opening a
// database.
func (txn *Txn) openDBI(cname *C.char, flags uint, cmp, dcmp *C.MDBX_cmp_func) (DBI, error) {
	var dbi C.MDBX_dbi
	ret := C.mdbx_dbi_open_ex(txn._txn, cname, C.MDBX_db_flags_t(flags), &dbi, cmp, dcmp)
	return DBI(dbi), operrno("mdbx_dbi_open", ret)
}

func (txn *Txn) openDBISimple(cname *C.char, flags uint) (DBI, error) {
	var dbi C.MDBX_dbi
	ret := C.mdbx_dbi_open(txn._txn, cname, C.MDBX_db_flags_t(flags), &dbi)
	return DBI(dbi), operrno("mdbx_dbi_open", ret)
}

type TxInfo struct {
	Id uint64 // The ID of the transaction. For a READ-ONLY transaction, this corresponds to the snapshot being read
	/** For READ-ONLY transaction: the lag from a recent MVCC-snapshot, i.e. the
	  number of committed transaction since read transaction started. For WRITE
	  transaction (provided if `scan_rlt=true`): the lag of the oldest reader
	  from current transaction (i.e. at least 1 if any reader running). */
	ReadLag uint64
	/** Used space by this transaction, i.e. corresponding to the last used
	 * database page. */
	SpaceUsed uint64
	/** Current size of database file. */
	SpaceLimitSoft uint64
	/** Upper bound for size the database file, i.e. the value `size_upper`
	  argument of the appropriate call of \ref mdbx_env_set_geometry(). */
	SpaceLimitHard uint64
	/** For READ-ONLY transaction: The total size of the database pages that were
	  retired by committed write transactions after the reader's MVCC-snapshot,
	  i.e. the space which would be freed after the Reader releases the
	  MVCC-snapshot for reuse by completion read transaction.
	  For WRITE transaction: The summarized size of the database pages that were
	  retired for now due Copy-On-Write during this transaction. */
	SpaceRetired uint64
	/** For READ-ONLY transaction: the space available for writer(s) and that
	  must be exhausted for reason to call the Handle-Slow-Readers callback for
	  this read transaction. For WRITE transaction: the space inside transaction
	  that left to `MDBX_TXN_FULL` error. */
	SpaceLeftover uint64
	/** For READ-ONLY transaction (provided if `scan_rlt=true`): The space that
	  actually become available for reuse when only this transaction will be
	  finished.
	  For WRITE transaction: The summarized size of the dirty database
	  pages that generated during this transaction. */
	SpaceDirty uint64
}

// scan_rlt   The boolean flag controls the scan of the read lock
//  table to provide complete information. Such scan
//  is relatively expensive and you can avoid it
//  if corresponding fields are not needed.
//  See description of \ref MDBX_txn_info.
func (txn *Txn) Info(scanRlt bool) (*TxInfo, error) {
	var _stat C.MDBX_txn_info
	ret := C.mdbx_txn_info(txn._txn, &_stat, C.bool(scanRlt))
	if ret != success {
		return nil, operrno("mdbx_txn_info", ret)
	}
	return &TxInfo{
		Id:             uint64(_stat.txn_id),
		ReadLag:        uint64(_stat.txn_reader_lag),
		SpaceUsed:      uint64(_stat.txn_space_used),
		SpaceLimitSoft: uint64(_stat.txn_space_limit_soft),
		SpaceLimitHard: uint64(_stat.txn_space_limit_hard),
		SpaceRetired:   uint64(_stat.txn_space_retired),
		SpaceLeftover:  uint64(_stat.txn_space_leftover),
		SpaceDirty:     uint64(_stat.txn_space_dirty),
	}, nil
}

func (txn *Txn) StatDBI(dbi DBI) (*Stat, error) {
	var _stat C.MDBX_stat
	ret := C.mdbx_dbi_stat(txn._txn, C.MDBX_dbi(dbi), &_stat, C.size_t(unsafe.Sizeof(_stat)))
	if ret != success {
		return nil, operrno("mdbx_stat", ret)
	}
	stat := Stat{PSize: uint(_stat.ms_psize),
		Depth:         uint(_stat.ms_depth),
		BranchPages:   uint64(_stat.ms_branch_pages),
		LeafPages:     uint64(_stat.ms_leaf_pages),
		OverflowPages: uint64(_stat.ms_overflow_pages),
		Entries:       uint64(_stat.ms_entries)}
	return &stat, nil
}

// Drop empties the database if del is false.  Drop deletes and closes the
// database if del is true.
//
// See mdbx_drop.
func (txn *Txn) Drop(dbi DBI, del bool) error {
	ret := C.mdbx_drop(txn._txn, C.MDBX_dbi(dbi), C.bool(del))
	return operrno("mdbx_drop", ret)
}

func (txn *Txn) bytes(val *C.MDBX_val) []byte {
	if txn.RawRead {
		return getBytes(val)
	}
	return getBytesCopy(val)
}

// Get retrieves items from database dbi.  If txn.RawRead is true the slice
// returned by Get references a readonly section of memory that must not be
// accessed after txn has terminated.
//
// See mdbx_get.
func (txn *Txn) Get(dbi DBI, key []byte) ([]byte, error) {
	kdata, kn := valBytes(key)
	ret := C.mdbxgo_get(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&kdata[0])), C.size_t(kn),
		txn.val,
	)
	err := operrno("mdbx_get", ret)
	if err != nil {
		*txn.val = C.MDBX_val{}
		return nil, err
	}
	b := txn.bytes(txn.val)
	*txn.val = C.MDBX_val{}
	return b, nil
}

func (txn *Txn) putNilKey(dbi DBI, flags uint) error {
	// mdbx_put with an empty key will always fail
	ret := C.mdbxgo_put2(txn._txn, C.MDBX_dbi(dbi), nil, 0, nil, 0, C.MDBX_put_flags_t(flags))
	return operrno("mdbx_put", ret)
}

// Put stores an item in database dbi.
//
// See mdbx_put.
func (txn *Txn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	kn := len(key)
	if kn == 0 {
		return txn.putNilKey(dbi, flags)
	}
	vn := len(val)
	if vn == 0 {
		val = []byte{0}
	}

	ret := C.mdbxgo_put2(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(kn),
		(*C.char)(unsafe.Pointer(&val[0])), C.size_t(vn),
		C.MDBX_put_flags_t(flags),
	)
	return operrno("mdbx_put", ret)
}

// PutReserve returns a []byte of length n that can be written to, potentially
// avoiding a memcopy.  The returned byte slice is only valid in txn's thread,
// before it has terminated.
func (txn *Txn) PutReserve(dbi DBI, key []byte, n int, flags uint) ([]byte, error) {
	if len(key) == 0 {
		return nil, txn.putNilKey(dbi, flags)
	}
	txn.val.iov_len = C.size_t(n)
	ret := C.mdbxgo_put1(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		txn.val,
		C.MDBX_put_flags_t(flags|C.MDBX_RESERVE),
	)
	err := operrno("mdbx_put", ret)
	if err != nil {
		*txn.val = C.MDBX_val{}
		return nil, err
	}
	b := getBytes(txn.val)
	*txn.val = C.MDBX_val{}
	return b, nil
}

// Del deletes an item from database dbi.  Del ignores val unless dbi has the
// DupSort flag.
//
// See mdbx_del.
func (txn *Txn) Del(dbi DBI, key, val []byte) error {
	kdata, kn := valBytes(key)
	if val == nil {
		ret := C.mdbxgo_del(
			txn._txn, C.MDBX_dbi(dbi),
			(*C.char)(unsafe.Pointer(&kdata[0])), C.size_t(kn),
			nil, 0,
		)
		return operrno("mdbx_del", ret)
	}
	vdata, vn := valBytes(val)
	ret := C.mdbxgo_del(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&kdata[0])), C.size_t(kn),
		(*C.char)(unsafe.Pointer(&vdata[0])), C.size_t(vn),
	)
	return operrno("mdbx_del", ret)
}

// OpenCursor allocates and initializes a Cursor to database dbi.
//
// See mdbx_cursor_open.
func (txn *Txn) OpenCursor(dbi DBI) (*Cursor, error) {
	cur, err := openCursor(txn, dbi)
	if cur != nil && txn.readonly {
		runtime.SetFinalizer(cur, (*Cursor).close)
	}
	return cur, err
}

func (txn *Txn) errf(format string, v ...interface{}) {
	if txn.errLogf != nil {
		txn.errLogf(format, v...)
		return
	}
	log.Printf(format, v...)
}

func (txn *Txn) finalize() {
	if txn._txn != nil {
		if !txn.Pooled {
			txn.errf("lmdb: aborting unreachable transaction %#x", uintptr(unsafe.Pointer(txn)))
		}

		txn.abort()
	}
}

// TxnOp is an operation applied to a managed transaction.  The Txn passed to a
// TxnOp is managed and the operation must not call Commit, Abort, Renew, or
// Reset on it.
//
// IMPORTANT:
// TxnOps that write to the database (those passed to Env.Update or Txn.Sub)
// must not use the Txn in another goroutine (passing it directly or otherwise
// through closure).  Doing so has undefined results.
type TxnOp func(txn *Txn) error

type CmpFunc *C.MDBX_cmp_func

func (txn *Txn) GetCmpExcludeSuffix32() CmpFunc {
	return C.mdbxgo_get_cmp_exclude_suffix32()
}

// Cmp - this func follow bytes.Compare return style: The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (txn *Txn) Cmp(dbi DBI, a []byte, b []byte) int {
	adata, an := valBytes(a)
	bdata, bn := valBytes(b)
	ret := int(C.mdbxgo_cmp(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&adata[0])), C.size_t(an),
		(*C.char)(unsafe.Pointer(&bdata[0])), C.size_t(bn),
	))
	if ret > 0 {
		return 1
	}
	if ret < 0 {
		return -1
	}
	return 0
}

// DCmp - this func follow bytes.Compare return style: The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (txn *Txn) DCmp(dbi DBI, a []byte, b []byte) int {
	adata, an := valBytes(a)
	bdata, bn := valBytes(b)
	ret := int(C.mdbxgo_dcmp(
		txn._txn, C.MDBX_dbi(dbi),
		(*C.char)(unsafe.Pointer(&adata[0])), C.size_t(an),
		(*C.char)(unsafe.Pointer(&bdata[0])), C.size_t(bn),
	))
	if ret > 0 {
		return 1
	}
	if ret < 0 {
		return -1
	}
	return 0
}

func (txn *Txn) Sequence(dbi DBI, increment uint64) (uint64, error) {
	var res C.uint64_t
	ret := C.mdbx_dbi_sequence(txn._txn, C.MDBX_dbi(dbi), &res, C.uint64_t(increment))
	if ret != 0 {
		return uint64(res), operrno("mdbx_dbi_sequence", ret)
	}
	return uint64(res), nil
}
