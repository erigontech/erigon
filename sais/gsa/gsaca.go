package gsa

/*
#include "gsacak.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Implementation from https://github.com/felipelouza/gsufsort
// see also: https://almob.biomedcentral.com/track/pdf/10.1186/s13015-020-00177-y.pdf
// see also: https://almob.biomedcentral.com/track/pdf/10.1186/s13015-017-0117-9.pdf

func SaSize(l int) int {
	var a uint
	return l * int(unsafe.Sizeof(a))
}
func LcpSize(l int) int {
	var a uint
	return l * int(unsafe.Sizeof(a))
}
func DaSize(l int) int {
	var a C.int_da
	return l * int(unsafe.Sizeof(a))
}
func PrintArrays(str []byte, sa []uint, lcp []int, da []int32, n int) {
	fmt.Printf("i\t")
	if da != nil {
		fmt.Printf("da\t")
	}
	if lcp != nil {
		fmt.Printf("lcp\t")
	}
	fmt.Printf("gsa\t")
	fmt.Printf("suffixes\t")
	fmt.Printf("\n")
	for i := 0; i < n; i++ {
		fmt.Printf("%d\t", i)
		if lcp != nil {
			fmt.Printf("%d\t", lcp[i])
		}

		if da != nil {
			pos := sa[da[i]]
			value := sa[i]
			if da[i] != 0 {
				value = sa[i] - pos - 1
			}
			fmt.Printf("(%d %d)\t", da[i], value)
		} else {
			fmt.Printf("%d\t", sa[i])
		}
		/*
			if(gsa){
			      da_value = (light)?rankbv_rank1(rbv,SA[i]):DA[i];
			      printf("(%" PRIdA ", ", da_value);

			      int_t pos;
			      if(last_end) pos=SA[da_value];
			      else pos=SA[da_value-1];

			      int_t value = (da_value==0)?SA[i]:SA[i]-pos-1;
			      printf("%" PRIdN ")   \t", value);
			    }
		*/
		/*
		  if(bwt){
		      char c = (SA[i])? T[SA[i]-1]-1:terminal;
		      if(c==0) c = '$';
		      printf("%c\t",c);
		    }
		*/
		for j := sa[i]; int(j) < n; j++ {
			if str[j] == 1 {
				fmt.Printf("$")
				break
			} else if str[j] == 0 {
				fmt.Printf("#")
			} else {
				fmt.Printf("%c", str[j]-1)
			}
		}
		fmt.Printf("\n")
	}
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
