package semantics

/*
#include <stdlib.h>
#cgo CFLAGS: -I./libevmsem/src/
#cgo CFLAGS: -I./libevmsem/
#include "./libevmsem/src/sem.h"
#include "./libevmsem/src/sem.c"
*/
import "C"

// Sem calls into C function
func Sem(x int) int {
	return int(C.semantics_entry(C.int(x)))
}
