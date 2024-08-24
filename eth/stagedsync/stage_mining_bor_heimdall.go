// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/dataflow"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
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
		if err := unwinder.UnwindTo(headerNum-1, ForkReset(hash), tx); err != nil {
			return err
		}
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

	lastStateSyncEventID, records, _, fetchTime, err := fetchRequiredHeimdallStateSyncEventsIfNeeded(
		ctx,
		header,
		tx,
		cfg.borConfig,
		cfg.blockReader,
		cfg.heimdallClient,
		cfg.chainConfig.ChainID.String(),
		logPrefix,
		logger,
		lastStateSyncEventID,
		0,
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
