package silkworm

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const funcName = "silkworm_execute_blocks"

func LoadExecutionFunctionPointer(dllPath string) (unsafe.Pointer, error) {
	cPath := C.CString(dllPath)
	defer C.free(unsafe.Pointer(cPath))
	dllHandle := C.dlopen(cPath, C.RTLD_LAZY)
	if dllHandle == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to load dynamic library %s: %s", dllPath, err)
	}

	cName := C.CString(funcName)
	defer C.free(unsafe.Pointer(cName))
	funcPtr := C.dlsym(dllHandle, cName)
	if funcPtr == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to find the %s function: %s", funcName, err)
	}

	return funcPtr, nil
}
