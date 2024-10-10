package stagedsync

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
)

func unwindFinishZk(u *UnwindState, tx kv.RwTx, cfg FinishCfg, ctx context.Context) (err error) {
	rawdb.WriteHeadBlockHash(tx, rawdb.ReadHeadHeaderHash(tx))
	return nil
}
