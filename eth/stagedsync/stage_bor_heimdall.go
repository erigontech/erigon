package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

type BorHeimdallCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
}

func StageBorHeimdallCfg(
	db kv.RwDB,
	chainConfig chain.Config) BorHeimdallCfg {
	return BorHeimdallCfg{
		db:          db,
		chainConfig: chainConfig,
	}
}

func BorHeimdallForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logger log.Logger,
) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	return
}

func BorHeimdallUnwind(u *UnwindState, s *StageState, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	return
}

func BorHeimdallPrune(s *PruneState, ctx context.Context, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	return
}
