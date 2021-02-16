package stages

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
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
	updateHead func(context.Context, uint64, common.Hash, *big.Int),
	wakeUpChan chan struct{},
	timeout int,
) error {
	if _, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false /* history */, false /* overwrite */); err != nil {
		return fmt.Errorf("setup genesis block: %w", err)
	}
	st, err1 := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{},
	).Prepare(nil, nil, nil, &vm.Config{}, db, db, "downloader", ethdb.DefaultStorageMode, ".", nil, 512*1024*1024, make(chan struct{}), nil, nil, func() error { return nil }, nil)
	if err1 != nil {
		return fmt.Errorf("prepare staged sync: %w", err1)
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
		s, err := st.StageState(stages.BlockHashes, db)
		if err != nil {
			return fmt.Errorf("create block hashes stage state: %w", err)
		}
		if err = stagedsync.SpawnBlockHashStage(s, db, ".", make(chan struct{})); err != nil {
			log.Error("block hashes forward failed", "error", err)
		}
		if err = bodydownload.Forward("2/14 Bodies", ctx, db, bd, bodyReqSend, penalise, updateHead, wakeUpChan, timeout); err != nil {
			log.Error("body download forward failes", "error", err)
		}
	}
}
