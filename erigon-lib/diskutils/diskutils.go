//go:build !darwin

package diskutils

import (
	"github.com/ledgerwatch/log/v3"
)

func MountPointForDirPath(dirPath string) string {
	log.Debug("[diskutils] Implemented only for darwin")
	return "/"
}
