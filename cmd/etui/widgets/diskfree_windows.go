//go:build windows

package widgets

import (
	"syscall"
	"unsafe"
)

// diskFreeGB returns the available disk space in GB for the given directory.
func diskFreeGB(dir string) (float64, error) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx := kernel32.NewProc("GetDiskFreeSpaceExW")

	dirPtr, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailable uint64
	r1, _, lastErr := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(dirPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)
	if r1 == 0 {
		return 0, lastErr
	}
	return float64(freeBytesAvailable) / (1 << 30), nil
}
