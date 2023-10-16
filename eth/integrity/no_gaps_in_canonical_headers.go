package integrity

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func NoGapsInCanonicalHeaders(tx kv.Tx, ctx context.Context) {
	lastBlockNum, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		panic(err)
	}
	for i := uint64(0); i < lastBlockNum; i++ {
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
