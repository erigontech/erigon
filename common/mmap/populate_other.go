//go:build !linux

package mmap

import (
	"os"
	"sync/atomic"
	"unsafe"
)

func registerMapping([]byte, *os.File) {}

func unregisterMapping([]byte) {}

var BatchPrefetch = false

type ColdPage uintptr

type Probe struct{}

func NewProbe() *Probe { return nil }

func (*Probe) Touch(unsafe.Pointer) {}

func (*Probe) Len() int { return 0 }

func ReadPages([]uintptr) error { return nil }

var ProbeEntries, PagesMax atomic.Int64

func NoteMax(v *atomic.Int64, n int64) {
	for {
		cur := v.Load()
		if n <= cur || v.CompareAndSwap(cur, n) {
			return
		}
	}
}
