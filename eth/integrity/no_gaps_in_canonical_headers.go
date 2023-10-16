package integrity

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func NoGapsInCanonicalHeaders(tx kv.Tx, ctx context.Context, br services.BlockReader) {
	firstBlockInDB := br.(*freezeblocks.BlockReader).FrozenBlocks() + 1
	lastBlockNum, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		panic(err)
	}
	for i := firstBlockInDB; i < lastBlockNum; i++ {
		header := rawdb.ReadHeaderByNumber(tx, i)
		if header == nil {
			err = fmt.Errorf("header not found: %d\n", i)
			panic(err)
		}
		body, _, _, err := rawdb.ReadBodyByNumber(tx, i)
		if err != nil {
			panic(err)
		}
		if body == nil {
			err = fmt.Errorf("header not found: %d\n", i)
			panic(err)
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
