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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var (
	ErrHeaderValidatorsLengthMismatch = errors.New("header validators length mismatch")
	ErrHeaderValidatorsBytesMismatch  = errors.New("header validators bytes mismatch")
)

func FetchSpanZeroForMiningIfNeeded(
	ctx context.Context,
	db kv.RwDB,
	blockReader services.FullBlockReader,
	heimdallClient heimdall.HeimdallClient,
	logger log.Logger,
) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		_, err := blockReader.Span(ctx, tx, 0)
		if err != nil {
			if errors.Is(err, freezeblocks.ErrSpanNotFound) {
				_, err = fetchAndWriteHeimdallSpan(ctx, 0, tx, heimdallClient, "FetchSpanZeroForMiningIfNeeded", logger)
				return err
			}

			return err
		}

		return nil
	})
}

func fetchRequiredHeimdallSpansIfNeeded(
	ctx context.Context,
	toBlockNum uint64,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {
	requiredSpanID := heimdall.SpanIdAt(toBlockNum)
	if requiredSpanID == 0 && toBlockNum >= cfg.borConfig.CalculateSprintLength(toBlockNum) {
		// when in span 0 we fetch the next span (span 1) at the beginning of sprint 2 (block 16 or later)
		requiredSpanID++
	} else if heimdall.IsBlockInLastSprintOfSpan(toBlockNum, cfg.borConfig) {
		// for subsequent spans, we always fetch the next span at the beginning of the last sprint of a span
		requiredSpanID++
	}

	lastSpanID, exists, err := cfg.blockReader.LastSpanId(ctx, tx)
	if err != nil {
		return 0, err
	}

	if exists && requiredSpanID <= heimdall.SpanId(lastSpanID) {
		return lastSpanID, nil
	}

	var from heimdall.SpanId
	if lastSpanID > 0 {
		from = heimdall.SpanId(lastSpanID + 1)
	} // else fetch from span 0

	logger.Info(fmt.Sprintf("[%s] Processing spans...", logPrefix), "from", from, "to", requiredSpanID)
	for spanID := from; spanID <= requiredSpanID; spanID++ {
		if _, err = fetchAndWriteHeimdallSpan(ctx, uint64(spanID), tx, cfg.heimdallClient, logPrefix, logger); err != nil {
			return 0, err
		}
	}

	return uint64(requiredSpanID), err
}

func fetchAndWriteHeimdallSpan(
	ctx context.Context,
	spanID uint64,
	tx kv.RwTx,
	heimdallClient heimdall.HeimdallClient,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {
	response, err := heimdallClient.FetchSpan(ctx, spanID)
	if err != nil {
		return 0, err
	}

	spanBytes, err := json.Marshal(response)
	if err != nil {
		return 0, err
	}

	var spanIDBytes [8]byte
	binary.BigEndian.PutUint64(spanIDBytes[:], spanID)
	if err = tx.Put(kv.BorSpans, spanIDBytes[:], spanBytes); err != nil {
		return 0, err
	}

	if spanID%100 == 0 {
		logger.Debug(fmt.Sprintf("[%s] Wrote span", logPrefix), "id", spanID)
	}
	return spanID, nil
}

func fetchAndWriteHeimdallCheckpointsIfNeeded(
	ctx context.Context,
	toBlockNum uint64,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {

	lastId, exists, err := cfg.blockReader.LastCheckpointId(ctx, tx)

	if err != nil {
		return 0, err
	}

	var lastCheckpoint *heimdall.Checkpoint

	if exists {
		data, err := cfg.blockReader.Checkpoint(ctx, tx, lastId)

		if err != nil {
			return 0, err
		}

		var checkpoint heimdall.Checkpoint

		if err := json.Unmarshal(data, &checkpoint); err != nil {
			return 0, err
		}

		lastCheckpoint = &checkpoint
	}

	logTimer := time.NewTicker(logInterval)
	defer logTimer.Stop()

	count, err := cfg.heimdallClient.FetchCheckpointCount(ctx)

	if err != nil {
		return 0, err
	}

	logger.Info(fmt.Sprintf("[%s] Processing checkpoints...", logPrefix), "from", lastId+1, "to", toBlockNum, "count", count)

	var lastBlockNum uint64

	for checkpointId := lastId + 1; checkpointId <= uint64(count) && (lastCheckpoint == nil || lastCheckpoint.EndBlock().Uint64() < toBlockNum); checkpointId++ {
		if _, lastCheckpoint, err = fetchAndWriteHeimdallCheckpoint(ctx, checkpointId, tx, cfg.heimdallClient, logPrefix, logger); err != nil {
			if !errors.Is(err, heimdall.ErrNotInCheckpointList) {
				return 0, err
			}

			return lastId, err
		}

		lastId = checkpointId

		select {
		default:
		case <-logTimer.C:
			if lastCheckpoint != nil {
				lastBlockNum = lastCheckpoint.EndBlock().Uint64()
			}

			logger.Info(
				fmt.Sprintf("[%s] Checkpoint Progress", logPrefix),
				"progress", lastBlockNum,
				"lastCheckpointId", lastId,
			)
		}
	}

	return lastId, err
}

func fetchAndWriteHeimdallCheckpoint(
	ctx context.Context,
	checkpointId uint64,
	tx kv.RwTx,
	heimdallClient heimdall.HeimdallClient,
	logPrefix string,
	logger log.Logger,
) (uint64, *heimdall.Checkpoint, error) {
	response, err := heimdallClient.FetchCheckpoint(ctx, int64(checkpointId))
	if err != nil {
		return 0, nil, err
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		return 0, nil, err
	}

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], checkpointId)
	if err = tx.Put(kv.BorCheckpoints, idBytes[:], bytes); err != nil {
		return 0, nil, err
	}

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], response.EndBlock().Uint64())
	if err = tx.Put(kv.BorCheckpointEnds, blockNumBuf[:], idBytes[:]); err != nil {
		return 0, nil, err
	}

	logger.Trace(fmt.Sprintf("[%s] Wrote checkpoint", logPrefix), "id", checkpointId, "start", response.StartBlock(), "end", response.EndBlock())
	return checkpointId, response, nil
}

func fetchAndWriteHeimdallMilestonesIfNeeded(
	ctx context.Context,
	toBlockNum uint64,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {

	lastId, exists, err := cfg.blockReader.LastMilestoneId(ctx, tx)

	if err != nil {
		return 0, err
	}

	var lastMilestone *heimdall.Milestone

	if exists {
		data, err := cfg.blockReader.Milestone(ctx, tx, lastId)

		if err != nil {
			return 0, err
		}

		if len(data) > 0 {
			var milestone heimdall.Milestone

			if err := json.Unmarshal(data, &milestone); err != nil {
				return 0, err
			}

			lastMilestone = &milestone
		}
	}

	logger.Info(fmt.Sprintf("[%s] Processing milestones...", logPrefix), "from", lastId+1, "to", toBlockNum)

	count, err := cfg.heimdallClient.FetchMilestoneCount(ctx)

	if err != nil {
		return 0, err
	}

	// it seems heimdall does not keep may live milestones - if
	// you try to get one before this you get an error on the api

	lastActive := uint64(count) - activeMilestones

	if lastId < lastActive {
		for lastActive <= uint64(count) {
			lastMilestone, err = cfg.heimdallClient.FetchMilestone(ctx, int64(lastActive))

			if err != nil {
				if !errors.Is(err, heimdall.ErrNotInMilestoneList) {
					return lastId, err
				}

				lastActive++
				continue
			}

			break
		}

		if lastMilestone == nil || toBlockNum < lastMilestone.StartBlock().Uint64() {
			return lastId, nil
		}

		lastId = lastActive - 1
	}

	for milestoneId := lastId + 1; milestoneId <= uint64(count) && (lastMilestone == nil || lastMilestone.EndBlock().Uint64() < toBlockNum); milestoneId++ {
		if _, lastMilestone, err = fetchAndWriteHeimdallMilestone(ctx, milestoneId, tx, cfg.heimdallClient, logPrefix, logger); err != nil {
			if !errors.Is(err, heimdall.ErrNotInMilestoneList) {
				return 0, err
			}

			return lastId, nil
		}

		lastId = milestoneId
	}

	return lastId, err
}

var activeMilestones uint64 = 100

func fetchAndWriteHeimdallMilestone(
	ctx context.Context,
	milestoneId uint64,
	tx kv.RwTx,
	heimdallClient heimdall.HeimdallClient,
	logPrefix string,
	logger log.Logger,
) (uint64, *heimdall.Milestone, error) {
	response, err := heimdallClient.FetchMilestone(ctx, int64(milestoneId))

	if err != nil {
		return 0, nil, err
	}

	bytes, err := json.Marshal(response)

	if err != nil {
		return 0, nil, err
	}

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], milestoneId)
	if err = tx.Put(kv.BorMilestones, idBytes[:], bytes); err != nil {
		return 0, nil, err
	}

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], response.EndBlock().Uint64())
	if err = tx.Put(kv.BorMilestoneEnds, blockNumBuf[:], idBytes[:]); err != nil {
		return 0, nil, err
	}

	logger.Trace(fmt.Sprintf("[%s] Wrote milestone", logPrefix), "id", milestoneId, "start", response.StartBlock(), "end", response.EndBlock())
	return milestoneId, response, nil
}

func fetchRequiredHeimdallStateSyncEventsIfNeeded(
	ctx context.Context,
	header *types.Header,
	tx kv.RwTx,
	borConfig *borcfg.BorConfig,
	blockReader services.FullBlockReader,
	heimdallClient heimdall.HeimdallClient,
	chainID string,
	logPrefix string,
	logger log.Logger,
	lastStateSyncEventID uint64,
	skipCount int,
) (uint64, int, int, time.Duration, error) {

	headerNum := header.Number.Uint64()
	if headerNum == 0 || !borConfig.IsSprintStart(headerNum) {
		// we fetch events only at beginning of each sprint with blockNum > 0
		return lastStateSyncEventID, 0, skipCount, 0, nil
	}

	return fetchAndWriteHeimdallStateSyncEvents(
		ctx,
		header,
		lastStateSyncEventID,
		skipCount,
		tx,
		borConfig,
		blockReader,
		heimdallClient,
		chainID,
		logPrefix,
		logger,
	)
}

func fetchAndWriteHeimdallStateSyncEvents(
	ctx context.Context,
	header *types.Header,
	lastStateSyncEventID uint64,
	skipCount int,
	tx kv.RwTx,
	config *borcfg.BorConfig,
	blockReader services.FullBlockReader,
	heimdallClient heimdall.HeimdallClient,
	chainID string,
	logPrefix string,
	logger log.Logger) (uint64, int, int, time.Duration, error) {
	fetchStart := time.Now()
	// Find out the latest eventId
	var fromId uint64

	blockNum := header.Number.Uint64()

	if blockNum == 0 || !config.IsSprintStart(blockNum) {
		// we fetch events only at beginning of each sprint with blockNum > 0
		return lastStateSyncEventID, 0, skipCount, 0, nil
	}

	from, to, err := bor.CalculateEventWindow(ctx, config, header, tx, blockReader)

	if err != nil {
		return lastStateSyncEventID, 0, skipCount, time.Since(fetchStart), err
	}

	fetchTo := to
	var fetchLimit int

	/* TODO
	// we want to get as many historical events as we can in
	// each call to heimdall - but we need to make sure we
	// don't type to get too recent a sync otherwise it will
	// return a nil response

	// to implement this we need to do block processing vs the
	// local db to set the blocknu index based on iterating
	// the local DB rather than the data received from the
	// client
	if time.Since(fetchTo) > (30 * time.Minute) {
		fetchTo = time.Now().Add(-30 * time.Minute)
		fetchLimit = 1000
	}
	*/

	fromId = lastStateSyncEventID + 1

	logger.Trace(
		fmt.Sprintf("[%s] Fetching state updates from Heimdall", logPrefix),
		"fromId", fromId,
		"to", to.Format(time.RFC3339),
	)

	eventRecords, err := heimdallClient.FetchStateSyncEvents(ctx, fromId, fetchTo, fetchLimit)
	if err != nil {
		return lastStateSyncEventID, 0, skipCount, time.Since(fetchStart), err
	}

	var overrideCount int

	if config.OverrideStateSyncRecords != nil {
		if val, ok := config.OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]; ok {
			overrideCount = len(eventRecords) - val //nolint
			eventRecords = eventRecords[0:val]
		}
	}

	if len(eventRecords) > 0 {
		var key, val [8]byte
		binary.BigEndian.PutUint64(key[:], blockNum)
		binary.BigEndian.PutUint64(val[:], lastStateSyncEventID+1)
	}

	var initialRecordTime *time.Time
	var lastEventRecord *heimdall.EventRecordWithTime

	for i, eventRecord := range eventRecords {
		if eventRecord.ID <= lastStateSyncEventID {
			continue
		}

		// Note: this check is only valid for events with eventRecord.ID > lastStateSyncEventID
		var afterCheck = func(limitTime time.Time, eventTime time.Time, initialTime *time.Time) bool {
			if initialTime == nil {
				return eventTime.After(from)
			}

			return initialTime.After(from)
		}

		// don't apply this for devnets we may have looser state event constraints
		// (TODO these probably needs fixing)
		if skipCount > 0 {
			skipCount--
		} else if !(chainID == "1337") {
			if lastStateSyncEventID+1 != eventRecord.ID || eventRecord.ChainID != chainID ||
				!(afterCheck(from, eventRecord.Time, initialRecordTime) && eventRecord.Time.Before(to)) {
				return lastStateSyncEventID, i, overrideCount, time.Since(fetchStart), fmt.Errorf(
					"invalid event record received %s, %s, %s, %s",
					fmt.Sprintf("blockNum=%d", blockNum),
					fmt.Sprintf("eventId=%d (exp %d)", eventRecord.ID, lastStateSyncEventID+1),
					fmt.Sprintf("chainId=%s (exp %s)", eventRecord.ChainID, chainID),
					fmt.Sprintf("time=%s (exp from %s, to %s)", eventRecord.Time, from, to),
				)
			}
		}

		data, err := eventRecord.MarshallBytes()
		if err != nil {
			logger.Error(fmt.Sprintf("[%s] Unable to pack txn for commitState", logPrefix), "err", err)
			return lastStateSyncEventID, i, skipCount + overrideCount, time.Since(fetchStart), err
		}

		eventIdBytes := eventRecord.MarshallIdBytes()
		if err = tx.Put(kv.BorEvents, eventIdBytes, data); err != nil {
			return lastStateSyncEventID, i, skipCount + overrideCount, time.Since(fetchStart), err
		}

		if initialRecordTime == nil {
			eventTime := eventRecord.Time
			initialRecordTime = &eventTime
		}

		lastStateSyncEventID++
		lastEventRecord = eventRecord
	}

	skipCount += overrideCount

	if lastEventRecord != nil {
		logger.Debug("putting state sync events", "blockNum", blockNum, "lastID", lastEventRecord.ID)

		var blockNumBuf [8]byte
		binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
		eventIdBytes := lastEventRecord.MarshallIdBytes()
		if err = tx.Put(kv.BorEventNums, blockNumBuf[:], eventIdBytes); err != nil {
			return lastStateSyncEventID, len(eventRecords), skipCount, time.Since(fetchStart), err
		}
	}

	return lastStateSyncEventID, len(eventRecords), skipCount, time.Since(fetchStart), nil
}
