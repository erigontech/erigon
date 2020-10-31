package headerdownload

import (
	"os"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func Forward(db ethdb.Database, files []string, buffer []byte) error {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Error("Could not remove", "file", file, "error", err)
		}
	}
	return nil
}
