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

func OpenLibrary(dllPath string) (unsafe.Pointer, error) {
	cPath := C.CString(dllPath)
	defer C.free(unsafe.Pointer(cPath))
	dllHandle := C.dlopen(cPath, C.RTLD_LAZY)
	if dllHandle == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to load dynamic library %s: %s", dllPath, err)
	}
	return dllHandle, nil
}

func LoadFunction(dllHandle unsafe.Pointer, funcName string) (unsafe.Pointer, error) {
	cName := C.CString(funcName)
	defer C.free(unsafe.Pointer(cName))
	funcPtr := C.dlsym(dllHandle, cName)
	if funcPtr == nil {
		err := C.GoString(C.dlerror())
		return nil, fmt.Errorf("failed to find the %s function: %s", funcName, err)
	}
	return funcPtr, nil
}
