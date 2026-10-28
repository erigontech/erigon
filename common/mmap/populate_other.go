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

var BatchPrefetch = false

type ColdPage uintptr

func Probing() bool { return false }

func BeginProbe() (end func(), ok bool) { return nil, false }

func ReadPages([]uintptr) {}
