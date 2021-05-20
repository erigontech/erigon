package mdbx

/*
#include <stdlib.h>
#include <stdio.h>
#include "mdbxgo.h"
#include "dist/mdbx.h"
*/
import "C"

import (
	"unsafe"

	"github.com/ledgerwatch/erigon/ethdb/mdbx/mdbxarch"
)

// Just for docs:
//struct MDBX_val {
//	void *iov_base; /**< pointer to some data */
//	size_t iov_len; /**< the length of data in bytes */
//};

// valSizeBits is the number of bits which constraining the length of the
// single values in an LMDB database, either 32 or 31 depending on the
// platform.  valMaxSize is the largest data size allowed based.  See runtime
// source file malloc.go and the compiler typecheck.go for more information
// about memory limits and array bound limits.
//
//		https://github.com/golang/go/blob/a03bdc3e6bea34abd5077205371e6fb9ef354481/src/runtime/malloc.go#L151-L164
//		https://github.com/golang/go/blob/36a80c5941ec36d9c44d6f3c068d13201e023b5f/src/cmd/compile/internal/gc/typecheck.go#L383
//
// On 64-bit systems, luckily, the value 2^32-1 coincides with the maximum data
// size for LMDB (MAXDATASIZE).
const (
	valSizeBits = mdbxarch.Width64*32 + (1-mdbxarch.Width64)*31
	valMaxSize  = 1<<valSizeBits - 1
)

// Multi is a wrapper for a contiguous page of sorted, fixed-length values
// passed to Cursor.PutMulti or retrieved using Cursor.Get with the
// GetMultiple/NextMultiple flag.
//
// Multi values are only useful in databases opened with DupSort|DupFixed.
type Multi struct {
	page   []byte
	stride int
}

// WrapMulti converts a page of contiguous values with stride size into a
// Multi.  WrapMulti panics if len(page) is not a multiple of stride.
//
//		_, val, _ := cursor.Get(nil, nil, lmdb.FirstDup)
//		_, page, _ := cursor.Get(nil, nil, lmdb.GetMultiple)
//		multi := lmdb.WrapMulti(page, len(val))
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapMulti(page []byte, stride int) *Multi {
	if len(page)%stride != 0 {
		panic("incongruent arguments")
	}
	return &Multi{page: page, stride: stride}
}

// Vals returns a slice containing the values in m.  The returned slice has
// length m.Len() and each item has length m.Stride().
func (m *Multi) Vals() [][]byte {
	n := m.Len()
	ps := make([][]byte, n)
	for i := 0; i < n; i++ {
		ps[i] = m.Val(i)
	}
	return ps
}

// Val returns the value at index i.  Val panics if i is out of range.
func (m *Multi) Val(i int) []byte {
	off := i * m.stride
	return m.page[off : off+m.stride]
}

// Len returns the number of values in the Multi.
func (m *Multi) Len() int {
	return len(m.page) / m.stride
}

// Stride returns the length of an individual value in the m.
func (m *Multi) Stride() int {
	return m.stride
}

// Size returns the total size of the Multi data and is equal to
//
//		m.Len()*m.Stride()
//
func (m *Multi) Size() int {
	return len(m.page)
}

// Page returns the Multi page data as a raw slice of bytes with length
// m.Size().
func (m *Multi) Page() []byte {
	return m.page[:len(m.page):len(m.page)]
}

var eb = []byte{0}

func valBytes(b []byte) ([]byte, int) {
	if len(b) == 0 {
		return eb, 0
	}
	return b, len(b)
}

func wrapVal(b []byte) *C.MDBX_val {
	p, n := valBytes(b)
	return &C.MDBX_val{
		iov_base: unsafe.Pointer(&p[0]),
		iov_len:  C.size_t(n),
	}
}

func getBytes(val *C.MDBX_val) []byte {
	return (*[valMaxSize]byte)(val.iov_base)[:val.iov_len:val.iov_len]
}

func getBytesCopy(val *C.MDBX_val) []byte {
	return C.GoBytes(val.iov_base, C.int(val.iov_len))
}
