//go:build !linux

package iouring

// WarmOne is a no-op on non-linux; returns false so callers fall back to pread.
func WarmOne(fd int, off int64, length int) bool { return false }
