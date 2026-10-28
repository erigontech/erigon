//go:build !linux

package mmap

import (
	"os"
	"unsafe"
)

var PopulateEnabled = false

func Populate(unsafe.Pointer) {}

func registerMapping([]byte, *os.File) {}

func unregisterMapping([]byte) {}
