package mdbx

/*
#include <stdlib.h>
#include <stdio.h>
#include "mdbxgo.h"
*/
import "C"
import (
	"unsafe"
)

const (
	// Flags for Cursor.Get
	//
	// See MDB_cursor_op.

	First        = C.MDBX_FIRST          // The first item.
	FirstDup     = C.MDBX_FIRST_DUP      // The first value of current key (DupSort).
	GetBoth      = C.MDBX_GET_BOTH       // Get the key as well as the value (DupSort).
	GetBothRange = C.MDBX_GET_BOTH_RANGE // Get the key and the nearsest value (DupSort).
	GetCurrent   = C.MDBX_GET_CURRENT    // Get the key and value at the current position.
	GetMultiple  = C.MDBX_GET_MULTIPLE   // Get up to a page dup values for key at current position (DupFixed).
	Last         = C.MDBX_LAST           // Last item.
	LastDup      = C.MDBX_LAST_DUP       // Position at last value of current key (DupSort).
	Next         = C.MDBX_NEXT           // Next value.
	NextDup      = C.MDBX_NEXT_DUP       // Next value of the current key (DupSort).
	NextMultiple = C.MDBX_NEXT_MULTIPLE  // Get key and up to a page of values from the next cursor position (DupFixed).
	NextNoDup    = C.MDBX_NEXT_NODUP     // The first value of the next key (DupSort).
	Prev         = C.MDBX_PREV           // The previous item.
	PrevDup      = C.MDBX_PREV_DUP       // The previous item of the current key (DupSort).
	PrevNoDup    = C.MDBX_PREV_NODUP     // The last data item of the previous key (DupSort).
	PrevMultiple = C.MDBX_PREV_MULTIPLE  //
	Set          = C.MDBX_SET            // The specified key.
	SetKey       = C.MDBX_SET_KEY        // Get key and data at the specified key.
	SetRange     = C.MDBX_SET_RANGE      // The first key no less than the specified key.
)

// The MDB_MULTIPLE and MDB_RESERVE flags are special and do not fit the
// calling pattern of other calls to Put.  They are not exported because they
// require special methods, PutMultiple and PutReserve in which the flag is
// implied and does not need to be passed.
const (
	// Flags for Txn.Put and Cursor.Put.
	//
	// See mdb_put and mdb_cursor_put.
	Upsert      = C.MDBX_UPSERT      // Replace the item at the current key position (Cursor only)
	Current     = C.MDBX_CURRENT     // Replace the item at the current key position (Cursor only)
	NoDupData   = C.MDBX_NODUPDATA   // Store the key-value pair only if key is not present (DupSort).
	NoOverwrite = C.MDBX_NOOVERWRITE // Store a new key-value pair only if key is not present.
	Append      = C.MDBX_APPEND      // Append an item to the database.
	AppendDup   = C.MDBX_APPENDDUP   // Append an item to the database (DupSort).
	AllDups     = C.MDBX_ALLDUPS
)

// Cursor operates on data inside a transaction and holds a position in the
// database.
//
// See MDB_cursor.
type Cursor struct {
	txn *Txn
	_c  *C.MDBX_cursor
}

func openCursor(txn *Txn, db DBI) (*Cursor, error) {
	c := &Cursor{txn: txn}
	ret := C.mdbx_cursor_open(txn._txn, C.MDBX_dbi(db), &c._c)
	if ret != success {
		return nil, operrno("mdbx_cursor_open", ret)
	}
	return c, nil
}

// Renew associates readonly cursor with txn.
//
// See mdb_cursor_renew.
func (c *Cursor) Renew(txn *Txn) error {
	ret := C.mdbx_cursor_renew(txn._txn, c._c)
	err := operrno("mdbx_cursor_renew", ret)
	if err != nil {
		return err
	}
	c.txn = txn
	return nil
}

// Close the cursor handle and clear the finalizer on c.  Cursors belonging to
// write transactions are closed automatically when the transaction is
// terminated.
//
// See mdb_cursor_close.
func (c *Cursor) Close() {
	if c._c != nil {
		if c.txn._txn == nil && !c.txn.readonly {
			// the cursor has already been released by MDBX.
		} else {
			C.mdbx_cursor_close(c._c)
		}
		c.txn = nil
		c._c = nil
	}
}

// Txn returns the cursor's transaction.
func (c *Cursor) Txn() *Txn {
	return c.txn
}

// DBI returns the cursor's database handle.  If c has been closed than an
// invalid DBI is returned.
func (c *Cursor) DBI() DBI {
	// dbiInvalid is an invalid DBI (the max value for the type).  it shouldn't
	// be possible to create a database handle with value dbiInvalid because
	// the process address space would be exhausted.  it is also impractical to
	// have many open databases in an environment.
	const dbiInvalid = ^DBI(0)

	// mdb_cursor_dbi segfaults when passed a nil value
	if c._c == nil {
		return dbiInvalid
	}
	return DBI(C.mdbx_cursor_dbi(c._c))
}

// Get retrieves items from the database. If c.Txn().RawRead is true the slices
// returned by Get reference readonly sections of memory that must not be
// accessed after the transaction has terminated.
//
// In a Txn with RawRead set to true the Set op causes the returned key to
// share its memory with setkey (making it writable memory). In a Txn with
// RawRead set to false the Set op returns key values with memory distinct from
// setkey, as is always the case when using RawRead.
//
// Get ignores setval if setkey is empty.
//
// See mdb_cursor_get.
func (c *Cursor) Get(setkey, setval []byte, op uint) (key, val []byte, err error) {
	switch {
	case len(setkey) == 0:
		err = c.getVal0(op)
	case len(setval) == 0:
		err = c.getVal1(setkey, op)
	default:
		err = c.getVal2(setkey, setval, op)
	}
	if err != nil {
		*c.txn.key = C.MDBX_val{}
		*c.txn.val = C.MDBX_val{}
		return nil, nil, err
	}

	// When MDB_SET is passed to mdb_cursor_get its first argument will be
	// returned unchanged.  Unfortunately, the normal slice copy/extraction
	// routines will be bad for the Go runtime when operating on Go memory
	// (panic or potentially garbage memory reference).
	if op == Set {
		if c.txn.RawRead {
			key = setkey
		} else {
			p := make([]byte, len(setkey))
			copy(p, setkey)
			key = p
		}
	} else {
		key = c.txn.bytes(c.txn.key)
	}
	val = c.txn.bytes(c.txn.val)

	// Clear transaction storage record storage area for future use and to
	// prevent dangling references.
	*c.txn.key = C.MDBX_val{}
	*c.txn.val = C.MDBX_val{}

	return key, val, nil
}

// getVal0 retrieves items from the database without using given key or value
// data for reference (Next, First, Last, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal0(op uint) error {
	ret := C.mdbx_cursor_get(c._c, c.txn.key, c.txn.val, C.MDBX_cursor_op(op))
	return operrno("mdbx_cursor_get", ret)
}

// getVal1 retrieves items from the database using key data for reference
// (Set, SetRange, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal1(setkey []byte, op uint) error {
	ret := C.mdbxgo_cursor_get1(
		c._c,
		(*C.char)(unsafe.Pointer(&setkey[0])), C.size_t(len(setkey)),
		c.txn.key,
		c.txn.val,
		C.MDBX_cursor_op(op),
	)
	return operrno("mdbx_cursor_get", ret)
}

// getVal2 retrieves items from the database using key and value data for
// reference (GetBoth, GetBothRange, etc).
//
// See mdb_cursor_get.
func (c *Cursor) getVal2(setkey, setval []byte, op uint) error {
	ret := C.mdbxgo_cursor_get2(
		c._c,
		(*C.char)(unsafe.Pointer(&setkey[0])), C.size_t(len(setkey)),
		(*C.char)(unsafe.Pointer(&setval[0])), C.size_t(len(setval)),
		c.txn.key, c.txn.val,
		C.MDBX_cursor_op(op),
	)
	return operrno("mdbx_cursor_get", ret)
}

func (c *Cursor) putNilKey(flags uint) error {
	ret := C.mdbxgo_cursor_put2(c._c, nil, 0, nil, 0, C.MDBX_put_flags_t(flags))
	return operrno("mdbx_cursor_put", ret)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (c *Cursor) Put(key, val []byte, flags uint) error {
	if len(key) == 0 {
		return c.putNilKey(flags)
	}
	vn := len(val)
	if vn == 0 {
		val = []byte{0}
	}
	ret := C.mdbxgo_cursor_put2(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val)),
		C.MDBX_put_flags_t(flags),
	)
	return operrno("mdbx_cursor_put", ret)
}

// PutReserve returns a []byte of length n that can be written to, potentially
// avoiding a memcopy.  The returned byte slice is only valid in txn's thread,
// before it has terminated.
func (c *Cursor) PutReserve(key []byte, n int, flags uint) ([]byte, error) {
	if len(key) == 0 {
		return nil, c.putNilKey(flags)
	}

	c.txn.val.iov_len = C.size_t(n)
	ret := C.mdbxgo_cursor_put1(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		c.txn.val,
		C.MDBX_put_flags_t(flags|C.MDBX_RESERVE),
	)
	err := operrno("mdbx_cursor_put", ret)
	if err != nil {
		*c.txn.val = C.MDBX_val{}
		return nil, err
	}
	b := getBytes(c.txn.val)
	*c.txn.val = C.MDBX_val{}
	return b, nil
}

// PutMulti stores a set of contiguous items with stride size under key.
// PutMulti panics if len(page) is not a multiple of stride.  The cursor's
// database must be DupFixed and DupSort.
//
// See mdb_cursor_put.
func (c *Cursor) PutMulti(key []byte, page []byte, stride int, flags uint) error {
	if len(key) == 0 {
		return c.putNilKey(flags)
	}
	if len(page) == 0 {
		page = []byte{0}
	}

	vn := WrapMulti(page, stride).Len()
	ret := C.mdbxgo_cursor_putmulti(
		c._c,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&page[0])), C.size_t(vn), C.size_t(stride),
		C.MDBX_put_flags_t(flags|C.MDBX_MULTIPLE),
	)
	return operrno("mdbxgo_cursor_putmulti", ret)
}

// Del deletes the item referred to by the cursor from the database.
//
// See mdb_cursor_del.
func (c *Cursor) Del(flags uint) error {
	ret := C.mdbx_cursor_del(c._c, C.MDBX_put_flags_t(flags))
	return operrno("mdbx_cursor_del", ret)
}

// Count returns the number of duplicates for the current key.
//
// See mdb_cursor_count.
func (c *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdbx_cursor_count(c._c, &_size)
	if ret != success {
		return 0, operrno("mdbx_cursor_count", ret)
	}
	return uint64(_size), nil
}
