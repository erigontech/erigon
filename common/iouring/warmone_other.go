//go:build !linux

package iouring

// WarmOne is a no-op on non-linux; returns false so callers fall back to pread.
func WarmOne(fd int, off int64, length int) bool { return false }

// Req mirrors the linux type so callers compile on all platforms.
type Req struct {
	Fd  int
	Off int64
	Len int
}

// WarmMany is a no-op on non-linux.
func WarmMany(reqs []Req) bool { return false }
