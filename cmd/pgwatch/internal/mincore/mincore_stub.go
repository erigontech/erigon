//go:build !linux

package mincore

import "errors"

var errNotLinux = errors.New("pgwatch requires Linux (mincore syscall not available on this platform)")

// Residency is not supported on non-Linux platforms.
func Residency(path string) (residency []bool, fileSize int64, sampled bool, err error) {
	return nil, 0, false, errNotLinux
}

// PageSize returns a placeholder value on non-Linux platforms.
func PageSize() int64 { return 4096 }
