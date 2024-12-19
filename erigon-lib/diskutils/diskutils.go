//go:build !darwin

package diskutils

import (
	"github.com/erigontech/erigon-lib/log/v3"
)

func MountPointForDirPath(dirPath string) string {
	log.Debug("[diskutils] Implemented only for darwin")
	return "/"
}

func DiskInfo(disk string) (string, error) {
	log.Debug("[diskutils] Implemented only for darwin")
	return "", nil
}
