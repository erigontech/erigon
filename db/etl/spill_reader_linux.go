package etl

import (
	"os"

	"golang.org/x/sys/unix"
)

var discardSpillRange = func(file *os.File, data []byte, start, end int) error {
	if err := unix.Madvise(data[start:end], unix.MADV_DONTNEED); err != nil {
		return err
	}
	return unix.Fadvise(int(file.Fd()), int64(start), int64(end-start), unix.FADV_DONTNEED)
}
