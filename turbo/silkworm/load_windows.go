//go:build windows

package silkworm

import (
	"errors"
	"unsafe"
)

func OpenLibrary(dllPath string) (unsafe.Pointer, error) {
	return nil, errors.New("not implemented")
}

func LoadFunction(dllHandle unsafe.Pointer, funcName string) (unsafe.Pointer, error) {
	return nil, errors.New("not implemented")
}
