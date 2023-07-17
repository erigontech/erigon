package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

func BorHeimdallForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	logger log.Logger,
) (err error) {
	return
}

func BorHeimdallUnwind(u *UnwindState, s *StageState, tx kv.RwTx) (err error) {
	return
}

func BorHeimdallPrune(s *PruneState, ctx context.Context, tx kv.RwTx) (err error) {
	return
}
