//go:build !linux

package mmap

import (
	"os"
	"unsafe"
)

func registerMapping([]byte, *os.File) {}

func unregisterMapping([]byte) {}

var BatchPrefetch = false

type ColdPage uintptr

type Probe struct{}

func NewProbe() *Probe { return nil }

func (*Probe) Touch(unsafe.Pointer) {}

func ReadPages([]uintptr) {}
