package stages

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

func StageLoop(ctx context.Context, db ethdb.Database, hd *headerdownload.HeaderDownload) {
	files, buffer := hd.PrepareStageData()
	for {
		if len(files) > 0 || len(buffer) > 0 {
			if err := headerdownload.Forward(db, files, buffer); err != nil {
				log.Error("heeader download forward failed", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-hd.StageReadyChannel():
		}
		files, buffer = hd.PrepareStageData()
	}

}
