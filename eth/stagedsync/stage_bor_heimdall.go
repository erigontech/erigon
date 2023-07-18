package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/log/v3"
)

type BorHeimdallCfg struct {
	db             kv.RwDB
	chainConfig    chain.Config
	heimdallClient bor.IHeimdallClient
}

func StageBorHeimdallCfg(
	db kv.RwDB,
	chainConfig chain.Config,
	heimdallClient bor.IHeimdallClient) BorHeimdallCfg {
	return BorHeimdallCfg{
		db:             db,
		chainConfig:    chainConfig,
		heimdallClient: heimdallClient,
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
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
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
