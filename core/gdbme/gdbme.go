package gdbme

/*
#cgo CFLAGS: -g -O0 -fno-omit-frame-pointer
#include <stdlib.h>
#include "crashhelper.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func RestartWithGDB(argc int, argv []string) {
	fmt.Println("Restarting with gdb...")

	// Convert Go args to C
	cArgv := make([]*C.char, len(argv))
	for i, arg := range argv {
		cArgv[i] = C.CString(arg)
		defer C.free(unsafe.Pointer(cArgv[i]))
	}

	// Ensure C API gets the correct argument list
	var cArgvPtr **C.char
	if len(cArgv) > 0 {
		cArgvPtr = &cArgv[0]
	}

	C.check_and_restart(C.int(argc), cArgvPtr)
}
