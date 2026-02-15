package sais

import (
	"index/suffixarray"
	"unsafe"
)

// Sais computes the suffix array of data using the stdlib's SA-IS implementation.
// The caller must provide sa with len(sa) == len(data).
func Sais(data []byte, sa []int32) error {
	idx := suffixarray.New(data)

	// Extract the internal []int32 suffix array from suffixarray.Index.
	// Layout (from Go src/index/suffixarray/suffixarray.go):
	//   type Index struct { data []byte; sa ints }
	//   type ints struct { int32 []int32; int64 []int64 }
	// For len(data) <= math.MaxInt32, sa.int32 is populated.
	type intsHeader struct {
		int32Ptr unsafe.Pointer
		int32Len int
		int32Cap int
		int64Ptr unsafe.Pointer
		int64Len int
		int64Cap int
	}
	type indexHeader struct {
		dataPtr unsafe.Pointer
		dataLen int
		dataCap int
		sa      intsHeader
	}
	h := (*indexHeader)(unsafe.Pointer(idx))
	internal := unsafe.Slice((*int32)(h.sa.int32Ptr), h.sa.int32Len)
	copy(sa, internal)
	return nil
}
