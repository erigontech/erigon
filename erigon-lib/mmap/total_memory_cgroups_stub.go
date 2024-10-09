//go:build !linux

package mmap

import (
	"errors"
)

func cgroupsMemoryLimit() (uint64, error) {
	return 0, errors.New("cgroups not supported in this environment")
}
