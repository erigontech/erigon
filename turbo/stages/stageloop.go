package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(ctx context.Context, db ethdb.Database, hd *headerdownload.HeaderDownload, bd *bodydownload.BodyDownload, bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte, penalise func([]byte), wakeUpChan chan struct{}, timeout int) error {
	if _, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false, false /* overwrite */); err != nil {
		return fmt.Errorf("setup genesis block: %w", err)
	}
	files, buffer := hd.PrepareStageData()
	for {
		if len(files) > 0 || len(buffer) > 0 {
			if err := headerdownload.Forward("1/14 Headers", db, files, buffer); err != nil {
				log.Error("header download forward failed", "error", err)
			}
			if err := bodydownload.Forward("2/14 Bodies", ctx, db, bd, bodyReqSend, penalise, wakeUpChan, timeout); err != nil {
				log.Error("body download forward failes", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-hd.StageReadyChannel():
		}
		files, buffer = hd.PrepareStageData()
	}
}
