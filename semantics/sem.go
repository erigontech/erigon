package semantics

/*
#include <stdlib.h>
#cgo CFLAGS: -I${SRCDIR}/libevmsem/src/
#cgo CFLAGS: -I${SRCDIR}/libevmsem/
#cgo CFLAGS: -I${SRCDIR}/z3/src/api
#cgo LDFLAGS: ${SRCDIR}/z3/build/libz3.a -lstdc++ -lm
#include "libevmsem/src/sem.h"
#include "libevmsem/src/sem.c"
#include "z3/src/api/z3.h"
*/
import "C"
import (
	"unsafe"
)

// Initialise the term sequence for semantic execution
func Initialise(stateRoot [32]byte, from [20]byte, to [20]byte, toPresent bool, value [16]byte, txData []byte, gasPrice uint64, gas uint64) int {
	stateRootPtr := C.CBytes(stateRoot[:])
	txDataPtr := C.CBytes(txData)
	fromPtr := C.CBytes(from[:])
	var toPtr unsafe.Pointer
	if toPresent {
		toPtr = C.CBytes(to[:])
	}
	result := int(C.initialise(stateRootPtr, fromPtr, toPtr, value, C.int(len(txData)), txDataPtr, C.__uint64_t(gasPrice), C.__uint64_t(gas)))
	if toPresent {
		C.free(toPtr)
	}
	C.free(fromPtr)
	C.free(txDataPtr)
	C.free(stateRootPtr)
	return result
}

// Cleanup release any dynamic memory allocated during the initialisation and semantic execution
func Cleanup() {
	C.cleanup()
}

// Init creates z3 context, sorts and datatypes
func Init() {
	C.init()
}

// Destroy deletes z3 contrxt, sorts and datatypes
func Destroy() {
	C.destroy()
}