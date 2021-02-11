package stages

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	hd *headerdownload.HeaderDownload,
	bd *bodydownload.BodyDownload,
	headerReqSend func(context.Context, []*headerdownload.HeaderRequest),
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(uint64, common.Hash, *big.Int),
	wakeUpChan chan struct{},
	timeout int,
) error {
	if _, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false, false /* overwrite */); err != nil {
		return fmt.Errorf("setup genesis block: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := headerdownload.Forward("1/14 Headers", ctx, db, hd, headerReqSend, wakeUpChan); err != nil {
			log.Error("header download forward failed", "error", err)
		}
		if err := bodydownload.Forward("2/14 Bodies", ctx, db, bd, bodyReqSend, penalise, updateHead, wakeUpChan, timeout); err != nil {
			log.Error("body download forward failes", "error", err)
		}
	}
}
