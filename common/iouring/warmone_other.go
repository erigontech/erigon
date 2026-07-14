//go:build !linux

package iouring

// WarmOne is unreachable off Linux: the residency probe (mmap.Resident) is a
// no-op there, so the gate never warms. It panics rather than silently no-op
// because there is no fallback path — io_uring is Linux-only.
func WarmOne(fd int, off int64, length int) {
	panic("iouring: io_uring warming is only available on linux")
}
