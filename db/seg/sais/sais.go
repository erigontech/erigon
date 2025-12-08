package sais

/*
#include "sais.h"
#include "utils.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func Sais(data []byte, sa []int32) error {
	size := C.int(len(data))
	tPtr := unsafe.Pointer(&data[0]) // source "text"
	saPtr := unsafe.Pointer(&sa[0])

	result := C.sais(
		(*C.uchar)(tPtr),
		(*C.int)(saPtr),
		size,
	)
	if int(result) != 0 {
		return fmt.Errorf("sais returned: %d", result)
	}
	return nil
}
