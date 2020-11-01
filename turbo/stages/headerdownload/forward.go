package headerdownload

import (
	"os"

	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func Forward(db ethdb.Database, files []string, buffer []byte) error {
	count := 0
	var highest uint64
	log.Info("Processing headers...")
	if _, _, err := ReadFilesAndBuffer(files, buffer, func(header *types.Header, blockHeight uint64) error {
		count++
		if blockHeight > highest {
			highest = blockHeight
		}
		return nil
	}); err != nil {
		return err
	}
	log.Info("Would have processed", "header", count, "highest", highest)
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Error("Could not remove", "file", file, "error", err)
		}
	}
	return nil
}
