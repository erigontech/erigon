package check

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

func Check(filesDir string) error {
	log.Info("Checking", "directory", filesDir)
	hd := headerdownload.NewHeaderDownload(
		common.Hash{}, /* initialHash */
		filesDir,
		32*1024, /* bufferLimit */
		16*1024, /* tipLimit */
		1024,    /* initPowDepth */
		nil,
		nil,
		3600, /* newAnchor future limit */
		3600, /* newAnchor past limit */
	)
	if err := hd.RecoverFromFiles(uint64(time.Now().Unix()), make(map[common.Hash]headerdownload.HeaderRecord)); err != nil {
		log.Error("Recovery from file failed, will start from scratch", "error", err)
	}
	log.Info(hd.AnchorState())
	return nil
}
