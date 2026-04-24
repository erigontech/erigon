//go:build linux

package mincore

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

var pageSize = int64(os.Getpagesize())

// Residency returns a per-page residency slice (true = page is in RAM) and the
// file size. For files larger than sampleThreshold the scan uses a stride of
// sampleStride pages so that the caller knows this is approximate.
func Residency(path string) (residency []bool, fileSize int64, sampled bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, false, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, false, err
	}
	fileSize = fi.Size()
	if fileSize == 0 {
		return nil, 0, false, nil
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fileSize, false, fmt.Errorf("mmap: %w", err)
	}
	defer unix.Munmap(data) //nolint:errcheck

	nPages := (fileSize + pageSize - 1) / pageSize
	vec := make([]byte, nPages)

	// unix.Mincore signature: Mincore(addr uintptr, length uintptr, vec []byte) error
	if err := unix.Mincore(uintptr(unsafe.Pointer(&data[0])), uintptr(fileSize), vec); err != nil {
		return nil, fileSize, false, fmt.Errorf("mincore: %w", err)
	}

	const sampleThreshold = 50 << 30 // 50 GB
	if fileSize > sampleThreshold {
		return sampleResidency(vec, nPages), fileSize, true, nil
	}

	residency = make([]bool, nPages)
	for i, b := range vec {
		residency[i] = b&1 != 0
	}
	return residency, fileSize, false, nil
}

// sampleResidency strides over vec at every sampleStride pages.
const sampleStride = 8

func sampleResidency(vec []byte, nPages int64) []bool {
	sampled := make([]bool, nPages)
	for i := int64(0); i < nPages; i += sampleStride {
		v := vec[i] & 1
		end := i + sampleStride
		if end > nPages {
			end = nPages
		}
		for j := i; j < end; j++ {
			sampled[j] = v != 0
		}
	}
	return sampled
}

// PageSize returns the OS page size in bytes.
func PageSize() int64 { return pageSize }
