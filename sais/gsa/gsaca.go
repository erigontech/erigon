package gsa

/*
#include "gsacak.h"
*/
import "C"
import (
	"unsafe"
)

// Implementation from https://github.com/felipelouza/gsufsort

func SaSize(l int) int {
	var a uint
	return l * int(unsafe.Sizeof(a))
}
func LcpSize(l int) int {
	var a uint
	return l * int(unsafe.Sizeof(a))
}
func GSA(data []byte, sa []uint, lcp []int, da []int32) error {
	tPtr := unsafe.Pointer(&data[0]) // source "text"
	var lcpPtr, saPtr, daPtr unsafe.Pointer
	if sa != nil {
		saPtr = unsafe.Pointer(&sa[0])
	}
	if lcp != nil {
		lcpPtr = unsafe.Pointer(&lcp[0])
	}
	if da != nil {
		daPtr = unsafe.Pointer(&da[0])
	}
	depth := C.gsacak(
		(*C.uchar)(tPtr),
		(*C.uint_t)(saPtr),
		(*C.int_t)(lcpPtr),
		(*C.int_da)(daPtr),
		C.uint_t(len(data)),
	)
	_ = depth
	return nil
}

func ConcatAll(R [][]byte) (str []byte, n int) {
	for i := 0; i < len(R); i++ {
		n += len(R[i]) + 1
	}

	n++ //add 0 at the end
	str = make([]byte, n)
	var l, max int
	k := len(R)

	for i := 0; i < k; i++ {
		m := len(R[i])
		if m > max {
			max = m
		}
		for j := 0; j < m; j++ {
			if R[i][j] < 255 && R[i][j] > 1 {
				str[l] = R[i][j] + 1
				l++
			}
		}
		if m > 0 {
			if str[l-1] > 1 {
				str[l] = 1
				l++
			} //add 1 as separator (ignores empty entries)
		}
	}
	str[l] = 0
	l++
	n = l
	return str, n
}
