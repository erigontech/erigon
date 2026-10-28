package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// PopulateEnabled makes Populate fault mapped pages in from a syscall, so the disk wait releases the goroutine's P.
var PopulateEnabled = os.Getenv("MMAP_POPULATE") == "true"

// Populate faults in the page holding p via MADV_POPULATE_READ; a no-op unless PopulateEnabled.
func Populate(p unsafe.Pointer) {
	if !PopulateEnabled {
		return
	}
	_, _, _ = unix.Syscall(unix.SYS_MADVISE, uintptr(p)&^(osPageSize-1), osPageSize, unix.MADV_POPULATE_READ)
}
