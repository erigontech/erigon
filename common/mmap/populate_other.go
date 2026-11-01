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

type Probe struct{}

func NewProbe() *Probe { return nil }

func (*Probe) Touch(unsafe.Pointer) {}

func (*Probe) TouchData(unsafe.Pointer) {}

func (*Probe) ColdData() bool { return false }

func ReadPages([]uintptr) {}
