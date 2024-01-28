package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/whitelist"
)

func MiningBorHeimdallForward(
	ctx context.Context,
	cfg BorHeimdallCfg,
	stageStage *StageState,
	unwinder Unwinder,
	tx kv.RwTx,
	logger log.Logger,
) error {
	if cfg.borConfig == nil || cfg.heimdallClient == nil {
		return nil
	}

	logPrefix := stageStage.LogPrefix()
	headerStageProgress, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}

	header := cfg.miningState.MiningBlock.Header
	headerNum := header.Number.Uint64()
	if headerNum <= headerStageProgress {
		return fmt.Errorf("attempting to mine %d, which is behind current head: %d", headerNum, headerStageProgress)
	}

	// Whitelist service is called to check if the bor chain is on the canonical chain according to milestones
	whitelistService := whitelist.GetWhitelistingService()
	if whitelistService != nil && !whitelistService.IsValidChain(headerNum, []*types.Header{header}) {
		hash := header.Hash()
		logger.Debug(
			fmt.Sprintf("[%s] Verification failed for mined header", logPrefix),
			"hash", hash,
			"height", headerNum,
			"err", err,
		)
		dataflow.HeaderDownloadStates.AddChange(headerNum, dataflow.HeaderInvalidated)
		unwinder.UnwindTo(headerNum-1, ForkReset(hash))
		return fmt.Errorf("mining on a wrong fork %d:%x", headerNum, hash)
	}

	lastSpanID, err := fetchRequiredHeimdallSpansIfNeeded(ctx, headerNum, tx, cfg, logPrefix, logger)
	if err != nil {
		return err
	}

	lastStateSyncEventID, _, err := cfg.blockReader.LastEventId(ctx, tx)

	if err != nil {
		return err
	}

	lastStateSyncEventID, records, fetchTime, err := fetchRequiredHeimdallStateSyncEventsIfNeeded(
		ctx,
		header,
		tx,
		cfg,
		logPrefix,
		logger,
		lastStateSyncEventID,
	)
	if err != nil {
		return err
	}

	if err = stageStage.Update(tx, headerNum); err != nil {
		return err
	}

	logger.Info(
		fmt.Sprintf("[%s] Finished processing", logPrefix),
		"progress", headerNum,
		"lastSpanID", lastSpanID,
		"lastStateSyncEventID", lastStateSyncEventID,
		"stateSyncEventTotalRecords", records,
		"stateSyncEventFetchTime", fetchTime,
	)

	return nil
}
