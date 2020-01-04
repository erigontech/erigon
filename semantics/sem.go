package semantics

/*
#include <stdlib.h>
#cgo CFLAGS: -I./libevmsem/src/
#cgo CFLAGS: -I./libevmsem/
#include "./libevmsem/src/sem.h"
#include "./libevmsem/src/sem.c"
*/
import "C"

// Initialise the term sequence for semantic execution
func Initialise(stateRoot [32]byte, txData []byte, gas uint64) int {
	stateRootPtr := C.CBytes(stateRoot[:])
	txDataPtr := C.CBytes(txData)
	result := int(C.initialise(stateRootPtr, C.int(len(txData)), txDataPtr, C.ulonglong(gas)))
	C.free(txDataPtr)
	C.free(stateRootPtr)
	return result
}

// Cleanup release any dynamic memory allocated during the initialisation and semantic execution
func Cleanup() {
	C.cleanup()
}