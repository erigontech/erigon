//go:build !darwin

package diskutils

import "github.com/ledgerwatch/erigon-lib/log/v3"

func MountPointForDirPath(dirPath string) string {
	log.Debug("[diskutils] Implemented only for darwin")
	return "/"
}
