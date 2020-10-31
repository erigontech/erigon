package stages

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

func StageLoop(ctx context.Context, db ethdb.Database, hd *headerdownload.HeaderDownload) {
	files, buffer := hd.PrepareStageData()
	for {
		if len(files) > 0 || len(buffer) > 0 {
			headerdownload.HeaderDownloadForward(db, files, buffer)
		}
		select {
		case <-ctx.Done():
			return
		case <-hd.StageReadyChannel():
		}
		files, buffer = hd.PrepareStageData()
	}

}
