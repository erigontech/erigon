package mdbx

/*
#include <stdlib.h>
#include <stdio.h>
#include "mdbxgo.h"
*/
import "C"
import (
	"errors"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

// success is a value returned from the LMDB API to indicate a successful call.
// The functions in this API this behavior and its use is not required.
const success = C.MDBX_SUCCESS

const (
	// Flags for Env.Open.
	//
	// See mdbx_env_open

	EnvDefaults = C.MDBX_ENV_DEFAULTS
	LifoReclaim = C.MDBX_LIFORECLAIM
	//FixedMap    = C.MDBX_FIXEDMAP   // Danger zone. Map memory at a fixed address.
	NoSubdir      = C.MDBX_NOSUBDIR // Argument to Open is a file, not a directory.
	Accede        = C.MDBX_ACCEDE
	Coalesce      = C.MDBX_COALESCE
	Readonly      = C.MDBX_RDONLY     // Used in several functions to denote an object as readonly.
	WriteMap      = C.MDBX_WRITEMAP   // Use a writable memory map.
	NoMetaSync    = C.MDBX_NOMETASYNC // Don't fsync metapage after commit.
	UtterlyNoSync = C.MDBX_UTTERLY_NOSYNC
	SafeNoSync    = C.MDBX_SAFE_NOSYNC
	Durable       = C.MDBX_SYNC_DURABLE
	NoTLS         = C.MDBX_NOTLS // Danger zone. When unset reader locktable slots are tied to their thread.
	//NoLock      = C.MDBX_NOLOCK     // Danger zone. LMDB does not use any locks.
	NoReadahead = C.MDBX_NORDAHEAD // Disable readahead. Requires OS support.
	NoMemInit   = C.MDBX_NOMEMINIT // Disable LMDB memory initialization.
	Exclusive   = C.MDBX_EXCLUSIVE // Disable LMDB memory initialization.
)

const (
	MinPageSize = C.MDBX_MIN_PAGESIZE
	MaxPageSize = C.MDBX_MAX_PAGESIZE
	MaxDbi      = C.MDBX_MAX_DBI
)

// These flags are exclusively used in the Env.CopyFlags and Env.CopyFDFlags
// methods.
const (
	// Flags for Env.CopyFlags
	//
	// See mdbx_env_copy2

	CopyCompact = C.MDBX_CP_COMPACT // Perform compaction while copying
)

const (
	AllowTxOverlap = C.MDBX_DBG_LEGACY_OVERLAP
)

const (
	LogLvlFatal       = C.MDBX_LOG_FATAL
	LogLvlError       = C.MDBX_LOG_ERROR
	LogLvlWarn        = C.MDBX_LOG_WARN
	LogLvlNotice      = C.MDBX_LOG_NOTICE
	LogLvlVerbose     = C.MDBX_LOG_VERBOSE
	LogLvlDebug       = C.MDBX_LOG_DEBUG
	LogLvlTrace       = C.MDBX_LOG_TRACE
	LogLvlExtra       = C.MDBX_LOG_EXTRA
	LogLvlDoNotChange = C.MDBX_LOG_DONTCHANGE
)

const (
	DbgAssert          = C.MDBX_DBG_ASSERT
	DbgAudit           = C.MDBX_DBG_AUDIT
	DbgJitter          = C.MDBX_DBG_JITTER
	DbgDump            = C.MDBX_DBG_DUMP
	DbgLegacyMultiOpen = C.MDBX_DBG_LEGACY_MULTIOPEN
	DbgLegacyTxOverlap = C.MDBX_DBG_LEGACY_OVERLAP
	DbgDoNotChange     = C.MDBX_DBG_DONTCHANGE
)

const (
	OptMaxDB                        = C.MDBX_opt_max_db
	OptMaxReaders                   = C.MDBX_opt_max_readers
	OptSyncBytes                    = C.MDBX_opt_sync_bytes
	OptSyncPeriod                   = C.MDBX_opt_sync_period
	OptRpAugmentLimit               = C.MDBX_opt_rp_augment_limit
	OptLooseLimit                   = C.MDBX_opt_loose_limit
	OptDpReverseLimit               = C.MDBX_opt_dp_reserve_limit
	OptTxnDpLimit                   = C.MDBX_opt_txn_dp_limit
	OptTxnDpInitial                 = C.MDBX_opt_txn_dp_initial
	OptSpillMaxDenominator          = C.MDBX_opt_spill_max_denominator
	OptSpillMinDenominator          = C.MDBX_opt_spill_min_denominator
	OptSpillParent4ChildDenominator = C.MDBX_opt_spill_parent4child_denominator
)

var (
	LoggerDoNotChange = C.MDBX_LOGGER_DONTCHANGE
)

// DBI is a handle for a database in an Env.
//
// See MDBX_dbi
type DBI C.MDBX_dbi

// Env is opaque structure for a database environment.  A DB environment
// supports multiple databases, all residing in the same shared-memory map.
//
// See MDBX_env.
type Env struct {
	_env *C.MDBX_env

	// closeLock is used to allow the Txn finalizer to check if the Env has
	// been closed, so that it may know if it must abort.
	closeLock sync.RWMutex

	ckey *C.MDBX_val
	cval *C.MDBX_val
}

// NewEnv allocates and initializes a new Env.
//
// See mdbx_env_create.
func NewEnv() (*Env, error) {
	env := new(Env)
	ret := C.mdbx_env_create(&env._env)
	if ret != success {
		return nil, operrno("mdbx_env_create", ret)
	}
	env.ckey = (*C.MDBX_val)(C.malloc(C.size_t(unsafe.Sizeof(C.MDBX_val{}))))
	env.cval = (*C.MDBX_val)(C.malloc(C.size_t(unsafe.Sizeof(C.MDBX_val{}))))

	runtime.SetFinalizer(env, (*Env).Close)
	return env, nil
}

// Open an environment handle. If this function fails Close() must be called to
// discard the Env handle.  Open passes flags|NoTLS to mdbx_env_open.
//
// See mdbx_env_open.
func (env *Env) Open(path string, flags uint, mode os.FileMode) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdbx_env_open(env._env, cpath, C.MDBX_env_flags_t(NoTLS|flags), C.mdbx_mode_t(mode))
	return operrno("mdbx_env_open", ret)
}

var errNotOpen = errors.New("enivornment is not open")
var errNegSize = errors.New("negative size")

// FD returns the open file descriptor (or Windows file handle) for the given
// environment.  An error is returned if the environment has not been
// successfully Opened (where C API just retruns an invalid handle).
//
// See mdbx_env_get_fd.
func (env *Env) FD() (uintptr, error) {
	// fdInvalid is the value -1 as a uintptr, which is used by LMDB in the
	// case that env has not been opened yet.  the strange construction is done
	// to avoid constant value overflow errors at compile time.
	const fdInvalid = ^uintptr(0)

	mf := new(C.mdbx_filehandle_t)
	ret := C.mdbx_env_get_fd(env._env, mf)
	err := operrno("mdbx_env_get_fd", ret)
	if err != nil {
		return 0, err
	}
	fd := uintptr(*mf)

	if fd == fdInvalid {
		return 0, errNotOpen
	}
	return fd, nil
}

// ReaderList dumps the contents of the reader lock table as text.  Readers
// start on the second line as space-delimited fields described by the first
// line.
//
// See mdbx_reader_list.
//func (env *Env) ReaderList(fn func(string) error) error {
//	ctx, done := newMsgFunc(fn)
//	defer done()
//	if fn == nil {
//		ctx = 0
//	}
//
//	ret := C.mdbxgo_reader_list(env._env, C.size_t(ctx))
//	if ret >= 0 {
//		return nil
//	}
//	if ret < 0 && ctx != 0 {
//		err := ctx.get().err
//		if err != nil {
//			return err
//		}
//	}
//	return operrno("mdbx_reader_list", ret)
//}

// ReaderCheck clears stale entries from the reader lock table and returns the
// number of entries cleared.
//
// See mdbx_reader_check()
func (env *Env) ReaderCheck() (int, error) {
	var _dead C.int
	ret := C.mdbx_reader_check(env._env, &_dead)
	return int(_dead), operrno("mdbx_reader_check", ret)
}

func (env *Env) close() bool {
	if env._env == nil {
		return false
	}

	env.closeLock.Lock()
	C.mdbx_env_close(env._env)
	env._env = nil
	env.closeLock.Unlock()

	C.free(unsafe.Pointer(env.ckey))
	C.free(unsafe.Pointer(env.cval))
	env.ckey = nil
	env.cval = nil
	return true
}

// Close shuts down the environment, releases the memory map, and clears the
// finalizer on env.
//
// See mdbx_env_close.
func (env *Env) Close() error {
	if env.close() {
		runtime.SetFinalizer(env, nil)
		return nil
	}
	return errors.New("environment is already closed")
}

// CopyFD copies env to the the file descriptor fd.
//
// See mdbx_env_copyfd.
//func (env *Env) CopyFD(fd uintptr) error {
//	ret := C.mdbx_env_copyfd(env._env, C.mdbx_filehandle_t(fd))
//	return operrno("mdbx_env_copyfd", ret)
//}

// CopyFDFlag copies env to the file descriptor fd, with options.
//
// See mdbx_env_copyfd2.
//func (env *Env) CopyFDFlag(fd uintptr, flags uint) error {
//	ret := C.mdbx_env_copyfd2(env._env, C.mdbx_filehandle_t(fd), C.uint(flags))
//	return operrno("mdbx_env_copyfd2", ret)
//}

// Copy copies the data in env to an environment at path.
//
// See mdbx_env_copy.
//func (env *Env) Copy(path string) error {
//	cpath := C.CString(path)
//	defer C.free(unsafe.Pointer(cpath))
//	ret := C.mdbx_env_copy(env._env, cpath)
//	return operrno("mdbx_env_copy", ret)
//}

// CopyFlag copies the data in env to an environment at path created with flags.
//
// See mdbx_env_copy2.
//func (env *Env) CopyFlag(path string, flags uint) error {
//	cpath := C.CString(path)
//	defer C.free(unsafe.Pointer(cpath))
//	ret := C.mdbx_env_copy2(env._env, cpath, C.uint(flags))
//	return operrno("mdbx_env_copy2", ret)
//}

// Stat contains database status information.
//
// See MDBX_stat.
type Stat struct {
	PSize         uint   // Size of a database page. This is currently the same for all databases.
	Depth         uint   // Depth (height) of the B-tree
	BranchPages   uint64 // Number of internal (non-leaf) pages
	LeafPages     uint64 // Number of leaf pages
	OverflowPages uint64 // Number of overflow pages
	Entries       uint64 // Number of data items
	LastTxId      uint64 // Transaction ID of commited last modification
}

// Stat returns statistics about the environment.
//
// See mdbx_env_stat.
func (env *Env) Stat() (*Stat, error) {
	var _stat C.MDBX_stat
	var ret C.int
	ret = C.mdbx_env_stat_ex(env._env, nil, &_stat, C.size_t(unsafe.Sizeof(_stat)))
	if ret != success {
		return nil, operrno("mdbx_env_stat_ex", ret)
	}
	stat := Stat{PSize: uint(_stat.ms_psize),
		Depth:         uint(_stat.ms_depth),
		BranchPages:   uint64(_stat.ms_branch_pages),
		LeafPages:     uint64(_stat.ms_leaf_pages),
		OverflowPages: uint64(_stat.ms_overflow_pages),
		Entries:       uint64(_stat.ms_entries),
		LastTxId:      uint64(_stat.ms_mod_txnid)}
	return &stat, nil
}

// EnvInfo contains information an environment.
//
// See MDBX_envinfo.
type EnvInfo struct {
	MapSize int64 // Size of the data memory map
	LastPNO int64 // ID of the last used page
	Geo     struct {
		Lower   uint64
		Upper   uint64
		Current uint64
		Shrink  uint64
		Grow    uint64
	}
	LastTxnID                      int64 // ID of the last committed transaction
	MaxReaders                     uint  // maximum number of threads for the environment
	NumReaders                     uint  // maximum number of threads used in the environment
	PageSize                       uint  //
	SystemPageSize                 uint  //
	AutoSyncThreshold              uint  //
	SinceSyncSeconds16dot16        uint  //
	AutosyncPeriodSeconds16dot16   uint  //
	SinceReaderCheckSeconds16dot16 uint  //
	Flags                          uint  //
}

// Info returns information about the environment.
//
// See mdbx_env_info.
func (env *Env) Info() (*EnvInfo, error) {
	var _info C.MDBX_envinfo
	var ret C.int
	if err := env.View(func(txn *Txn) error {
		ret = C.mdbx_env_info_ex(env._env, txn._txn, &_info, C.size_t(unsafe.Sizeof(_info)))
		return nil
	}); err != nil {
		return nil, err
	}
	if ret != success {
		return nil, operrno("mdbx_env_info", ret)
	}
	info := EnvInfo{
		MapSize: int64(_info.mi_mapsize),
		Geo: struct {
			Lower   uint64
			Upper   uint64
			Current uint64
			Shrink  uint64
			Grow    uint64
		}{
			Lower:   uint64(_info.mi_geo.lower),
			Upper:   uint64(_info.mi_geo.upper),
			Current: uint64(_info.mi_geo.current),
			Shrink:  uint64(_info.mi_geo.shrink),
			Grow:    uint64(_info.mi_geo.grow),
		},
		LastPNO:        int64(_info.mi_last_pgno),
		LastTxnID:      int64(_info.mi_recent_txnid),
		MaxReaders:     uint(_info.mi_maxreaders),
		NumReaders:     uint(_info.mi_numreaders),
		PageSize:       uint(_info.mi_dxb_pagesize),
		SystemPageSize: uint(_info.mi_sys_pagesize),

		AutoSyncThreshold:              uint(_info.mi_autosync_threshold),
		SinceSyncSeconds16dot16:        uint(_info.mi_since_sync_seconds16dot16),
		AutosyncPeriodSeconds16dot16:   uint(_info.mi_autosync_period_seconds16dot16),
		SinceReaderCheckSeconds16dot16: uint(_info.mi_since_reader_check_seconds16dot16),
		Flags:                          uint(_info.mi_mode),
	}
	return &info, nil
}

// Sync flushes buffers to disk.  If force is true a synchronous flush occurs
// and ignores any NoSync or MapAsync flag on the environment.
//
// See mdbx_env_sync.
func (env *Env) Sync(force bool, nonblock bool) error {
	ret := C.mdbx_env_sync_ex(env._env, C.bool(force), C.bool(nonblock))
	return operrno("mdbx_env_sync_ex", ret)
}

// SetFlags sets flags in the environment.
//
// See mdbx_env_set_flags.
func (env *Env) SetFlags(flags uint) error {
	ret := C.mdbx_env_set_flags(env._env, C.MDBX_env_flags_t(flags), true)
	return operrno("mdbx_env_set_flags", ret)
}

// UnsetFlags clears flags in the environment.
//
// See mdbx_env_set_flags.
func (env *Env) UnsetFlags(flags uint) error {
	ret := C.mdbx_env_set_flags(env._env, C.MDBX_env_flags_t(flags), false)
	return operrno("mdbx_env_set_flags", ret)
}

// Flags returns the flags set in the environment.
//
// See mdbx_env_get_flags.
func (env *Env) Flags() (uint, error) {
	var _flags C.uint
	ret := C.mdbx_env_get_flags(env._env, &_flags)
	if ret != success {
		return 0, operrno("mdbx_env_get_flags", ret)
	}
	return uint(_flags), nil
}

func (env *Env) SetDebug(logLvl int, dbg int, logger *C.MDBX_debug_func) error {
	ret := C.mdbx_setup_debug(C.MDBX_log_level_t(logLvl), C.MDBX_debug_flags_t(dbg), logger)
	return operrno("mdbx_setup_debug", ret)
}

// Path returns the path argument passed to Open.  Path returns a non-nil error
// if env.Open() was not previously called.
//
// See mdbx_env_get_path.
func (env *Env) Path() (string, error) {
	var cpath *C.char
	ret := C.mdbx_env_get_path(env._env, &cpath)
	if ret != success {
		return "", operrno("mdbx_env_get_path", ret)
	}
	if cpath == nil {
		return "", errNotOpen
	}
	return C.GoString(cpath), nil
}

// SetMaxFreelistReuse sets the size of the environment memory map.
//
// Find a big enough contiguous page range for large values in freelist is hard
//        just allocate new pages and even don't try to search if value is bigger than this limit.
//        measured in pages
//func (env *Env) SetMaxFreelistReuse(pagesLimit uint) error {
//	ret := C.mdbx_env_set_maxfree_reuse(env._env, C.uint(pagesLimit))
//	return operrno("mdbx_env_set_maxfree_reuse", ret)
//}

// MaxFreelistReuse
//func (env *Env) MaxFreelistReuse() (uint, error) {
//	var pages C.uint
//	ret := C.mdbx_env_get_maxfree_reuse(env._env, &pages)
//	return uint(pages), operrno("mdbx_env_get_maxreaders", ret)
//}

// SetMapSize sets the size of the environment memory map.
//
// See mdbx_env_set_mapsize.
//func (env *Env) SetMapSize(size int64) error {
//	if size < 0 {
//		return errNegSize
//	}
//	ret := C.mdbx_env_set_mapsize(env._env, C.size_t(size))
//	return operrno("mdbx_env_set_mapsize", ret)
//}

func (env *Env) SetOption(option uint, value uint64) error {
	ret := C.mdbx_env_set_option(env._env, C.MDBX_option_t(option), C.uint64_t(value))
	return operrno("mdbx_env_set_option", ret)
}

func (env *Env) SetGeometry(sizeLower int, sizeNow int, sizeUpper int, growthStep int, shrinkThreshold int, pageSize int) error {
	ret := C.mdbx_env_set_geometry(env._env,
		C.intptr_t(sizeLower),
		C.intptr_t(sizeNow),
		C.intptr_t(sizeUpper),
		C.intptr_t(growthStep),
		C.intptr_t(shrinkThreshold),
		C.intptr_t(pageSize))
	return operrno("mdbx_env_set_geometry", ret)
}

// SetMaxReaders sets the maximum number of reader slots in the environment.
//
// See mdbx_env_set_maxreaders.
func (env *Env) SetMaxReaders(size int) error {
	if size < 0 {
		return errNegSize
	}
	ret := C.mdbx_env_set_maxreaders(env._env, C.uint(size))
	return operrno("mdbx_env_set_maxreaders", ret)
}

// MaxReaders returns the maximum number of reader slots for the environment.
//
// See mdbx_env_get_maxreaders.
func (env *Env) MaxReaders() (int, error) {
	var max C.uint
	ret := C.mdbx_env_get_maxreaders(env._env, &max)
	return int(max), operrno("mdbx_env_get_maxreaders", ret)
}

// MaxKeySize returns the maximum allowed length for a key.
//
// See mdbx_env_get_maxkeysize.
func (env *Env) MaxKeySize() int {
	if env == nil {
		return int(C.mdbx_env_get_maxkeysize_ex(nil, 0))
	}
	return int(C.mdbx_env_get_maxkeysize_ex(env._env, 0))
}

// SetMaxDBs sets the maximum number of named databases for the environment.
//
// See mdbx_env_set_maxdbs.
func (env *Env) SetMaxDBs(size int) error {
	if size < 0 {
		return errNegSize
	}
	ret := C.mdbx_env_set_maxdbs(env._env, C.MDBX_dbi(size))
	return operrno("mdbx_env_set_maxdbs", ret)
}

// BeginTxn is an unsafe, low-level method to initialize a new transaction on
// env.  The Txn returned by BeginTxn is unmanaged and must be terminated by
// calling either its Abort or Commit methods to ensure that its resources are
// released.
//
// BeginTxn does not call runtime.LockOSThread.  Unless the Readonly flag is
// passed goroutines must call runtime.LockOSThread before calling BeginTxn and
// the returned Txn must not have its methods called from another goroutine.
// Failure to meet these restrictions can have undefined results that may
// include deadlocking your application.
//
// Instead of calling BeginTxn users should prefer calling the View and Update
// methods, which assist in management of Txn objects and provide OS thread
// locking required for write transactions.
//
// A finalizer detects unreachable, live transactions and logs thems to
// standard error.  The transactions are aborted, but their presence should be
// interpreted as an application error which should be patched so transactions
// are terminated explicitly.  Unterminated transactions can adversly effect
// database performance and cause the database to grow until the map is full.
//
// See mdbx_txn_begin.
func (env *Env) BeginTxn(parent *Txn, flags uint) (*Txn, error) {
	return beginTxn(env, parent, flags)
}

// RunTxn creates a new Txn and calls fn with it as an argument.  Run commits
// the transaction if fn returns nil otherwise the transaction is aborted.
// Because RunTxn terminates the transaction goroutines should not retain
// references to it or its data after fn returns.
//
// RunTxn does not call runtime.LockOSThread.  Unless the Readonly flag is
// passed the calling goroutine should ensure it is locked to its thread and
// any goroutines started by fn must not call methods on the Txn object it is
// passed.
//
// See mdbx_txn_begin.
func (env *Env) RunTxn(flags uint, fn TxnOp) error {
	return env.run(false, flags, fn)
}

// View creates a readonly transaction with a consistent view of the
// environment and passes it to fn.  View terminates its transaction after fn
// returns.  Any error encountered by View is returned.
//
// Unlike with Update transactions, goroutines created by fn are free to call
// methods on the Txn passed to fn provided they are synchronized in their
// accesses (e.g. using a mutex or channel).
//
// Any call to Commit, Abort, Reset or Renew on a Txn created by View will
// panic.
func (env *Env) View(fn TxnOp) error {
	return env.run(false, Readonly, fn)
}

// Update calls fn with a writable transaction.  Update commits the transaction
// if fn returns a nil error otherwise Update aborts the transaction and
// returns the error.
//
// Update calls runtime.LockOSThread to lock the calling goroutine to its
// thread and until fn returns and the transaction has been terminated, at
// which point runtime.UnlockOSThread is called.  If the calling goroutine is
// already known to be locked to a thread, use UpdateLocked instead to avoid
// premature unlocking of the goroutine.
//
// Neither Update nor UpdateLocked cannot be called safely from a goroutine
// where it isn't known if runtime.LockOSThread has been called.  In such
// situations writes must either be done in a newly created goroutine which can
// be safely locked, or through a worker goroutine that accepts updates to
// apply and delivers transaction results using channels.  See the package
// documentation and examples for more details.
//
// Goroutines created by the operation fn must not use methods on the Txn
// object that fn is passed.  Doing so would have undefined and unpredictable
// results for your program (likely including data loss, deadlock, etc).
//
// Any call to Commit, Abort, Reset or Renew on a Txn created by Update will
// panic.
func (env *Env) Update(fn TxnOp) error {
	return env.run(true, 0, fn)
}

// UpdateLocked behaves like Update but does not lock the calling goroutine to
// its thread.  UpdateLocked should be used if the calling goroutine is already
// locked to its thread for another purpose.
//
// Neither Update nor UpdateLocked cannot be called safely from a goroutine
// where it isn't known if runtime.LockOSThread has been called.  In such
// situations writes must either be done in a newly created goroutine which can
// be safely locked, or through a worker goroutine that accepts updates to
// apply and delivers transaction results using channels.  See the package
// documentation and examples for more details.
//
// Goroutines created by the operation fn must not use methods on the Txn
// object that fn is passed.  Doing so would have undefined and unpredictable
// results for your program (likely including data loss, deadlock, etc).
//
// Any call to Commit, Abort, Reset or Renew on a Txn created by UpdateLocked
// will panic.
func (env *Env) UpdateLocked(fn TxnOp) error {
	return env.run(false, 0, fn)
}

func (env *Env) run(lock bool, flags uint, fn TxnOp) error {
	if lock {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	txn, err := beginTxn(env, nil, flags)
	if err != nil {
		return err
	}
	return txn.runOpTerm(fn)
}

// CloseDBI closes the database handle, db.  Normally calling CloseDBI
// explicitly is not necessary.
//
// It is the caller's responsibility to serialize calls to CloseDBI.
//
// See mdbx_dbi_close.
func (env *Env) CloseDBI(db DBI) {
	C.mdbx_dbi_close(env._env, C.MDBX_dbi(db))
}
