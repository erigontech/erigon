package dir

import (
	"syscall"

	"github.com/erigontech/erigon/common/log/v3"
)

func logFilesystemStats(dirPath string) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dirPath, &stat); err != nil {
		return
	}
	// On Darwin, Blocks/Bavail are counted in Bsize units.
	blockSize := uint64(stat.Bsize)
	log.Warn("[ENOSPC] filesystem", "dir", dirPath,
		"diskTotal", byteCount(int64(stat.Blocks*blockSize)),
		"diskAvail", byteCount(int64(stat.Bavail*blockSize)),
		"inodesTotal", stat.Files, "inodesFree", stat.Ffree)
}
