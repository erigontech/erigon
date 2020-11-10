// +build !linux

package silkworm

import (
	"errors"
	"unsafe"
)

func LoadExecutionFunctionPointer(dllPath string) (unsafe.Pointer, error) {
	// See https://github.com/golang/go/issues/28024
	return nil, errors.New("Silkworm is only supported on Linux")
}
