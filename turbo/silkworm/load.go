// +build !linux

package silkworm

import (
	"unsafe"
)

func LoadExecutionFunctionPointer(dllPath string) (unsafe.Pointer, error) {
	// Silkworm is only supported on Linux at the moment
	// See https://github.com/golang/go/issues/28024
	return nil, nil
}
