package stagedsync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
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
	requiredSpanID := bor.SpanIDAt(toBlockNum)
	if requiredSpanID == 0 && toBlockNum >= cfg.borConfig.CalculateSprintLength(toBlockNum) {
		// when in span 0 we fetch the next span (span 1) at the beginning of sprint 2 (block 16 or later)
		requiredSpanID++
	} else if bor.IsBlockInLastSprintOfSpan(toBlockNum, cfg.borConfig) {
		// for subsequent spans, we always fetch the next span at the beginning of the last sprint of a span
		requiredSpanID++
	}

	lastSpanID, exists, err := cfg.blockReader.LastSpanId(ctx, tx)
	if err != nil {
		return 0, err
	}

	if exists && heimdall.SpanId(requiredSpanID) <= heimdall.SpanId(lastSpanID) {
		return lastSpanID, nil
	}

	var from heimdall.SpanId
	if lastSpanID > 0 {
		from = heimdall.SpanId(lastSpanID + 1)
	} // else fetch from span 0

	logger.Info(fmt.Sprintf("[%s] Processing spans...", logPrefix), "from", from, "to", requiredSpanID)
	for spanID := from; spanID <= heimdall.SpanId(requiredSpanID); spanID++ {
		if _, err = fetchAndWriteHeimdallSpan(ctx, uint64(spanID), tx, cfg.heimdallClient, logPrefix, logger); err != nil {
			return 0, err
		}
	}

	return requiredSpanID, err
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

	logger.Trace(fmt.Sprintf("[%s] Wrote span", logPrefix), "id", spanID)
	return spanID, nil
}

func fetchRequiredHeimdallStateSyncEventsIfNeeded(
	ctx context.Context,
	header *types.Header,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
	lastStateSyncEventID uint64,
) (uint64, int, time.Duration, error) {

	headerNum := header.Number.Uint64()
	if headerNum%cfg.borConfig.CalculateSprintLength(headerNum) != 0 || headerNum == 0 {
		// we fetch events only at beginning of each sprint
		return lastStateSyncEventID, 0, 0, nil
	}

	return fetchAndWriteHeimdallStateSyncEvents(ctx, header, lastStateSyncEventID, tx, cfg, logPrefix, logger)
}

func fetchAndWriteHeimdallStateSyncEvents(
	ctx context.Context,
	header *types.Header,
	lastStateSyncEventID uint64,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
) (uint64, int, time.Duration, error) {
	fetchStart := time.Now()
	config := cfg.borConfig
	blockReader := cfg.blockReader
	heimdallClient := cfg.heimdallClient
	chainID := cfg.chainConfig.ChainID.String()
	stateReceiverABI := cfg.stateReceiverABI
	// Find out the latest eventId
	var (
		from uint64
		to   time.Time
	)

	blockNum := header.Number.Uint64()

	if config.IsIndore(blockNum) {
		stateSyncDelay := config.CalculateStateSyncDelay(blockNum)
		to = time.Unix(int64(header.Time-stateSyncDelay), 0)
	} else {
		pHeader, err := blockReader.HeaderByNumber(ctx, tx, blockNum-config.CalculateSprintLength(blockNum))
		if err != nil {
			return lastStateSyncEventID, 0, time.Since(fetchStart), err
		}
		to = time.Unix(int64(pHeader.Time), 0)
	}

	fetchTo := to
	var fetchLimit int

	/* TODO
	// we want to get as many historical events as we can in
	// each call to heimdall - but we need to make sure we
	// don't type to get too recent a sync othewise it will
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

	from = lastStateSyncEventID + 1

	logger.Debug(
		fmt.Sprintf("[%s] Fetching state updates from Heimdall", logPrefix),
		"fromID", from,
		"to", to.Format(time.RFC3339),
	)

	eventRecords, err := heimdallClient.FetchStateSyncEvents(ctx, from, fetchTo, fetchLimit)
	if err != nil {
		return lastStateSyncEventID, 0, time.Since(fetchStart), err
	}

	if config.OverrideStateSyncRecords != nil {
		if val, ok := config.OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]; ok {
			eventRecords = eventRecords[0:val]
		}
	}

	if len(eventRecords) > 0 {
		var key, val [8]byte
		binary.BigEndian.PutUint64(key[:], blockNum)
		binary.BigEndian.PutUint64(val[:], lastStateSyncEventID+1)
	}

	wroteIndex := false
	for i, eventRecord := range eventRecords {
		if eventRecord.ID <= lastStateSyncEventID {
			continue
		}

		if lastStateSyncEventID+1 != eventRecord.ID || eventRecord.ChainID != chainID || !eventRecord.Time.Before(to) {
			return lastStateSyncEventID, i, time.Since(fetchStart), fmt.Errorf(
				"invalid event record received %s, %s, %s, %s",
				fmt.Sprintf("blockNum=%d", blockNum),
				fmt.Sprintf("eventId=%d (exp %d)", eventRecord.ID, lastStateSyncEventID+1),
				fmt.Sprintf("chainId=%s (exp %s)", eventRecord.ChainID, chainID),
				fmt.Sprintf("time=%s (exp to %s)", eventRecord.Time, to),
			)
		}

		eventRecordWithoutTime := eventRecord.BuildEventRecord()

		recordBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
		if err != nil {
			return lastStateSyncEventID, i, time.Since(fetchStart), err
		}

		data, err := stateReceiverABI.Pack("commitState", big.NewInt(eventRecord.Time.Unix()), recordBytes)
		if err != nil {
			logger.Error(fmt.Sprintf("[%s] Unable to pack tx for commitState", logPrefix), "err", err)
			return lastStateSyncEventID, i, time.Since(fetchStart), err
		}

		var eventIdBuf [8]byte
		binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
		if err = tx.Put(kv.BorEvents, eventIdBuf[:], data); err != nil {
			return lastStateSyncEventID, i, time.Since(fetchStart), err
		}

		if !wroteIndex {
			var blockNumBuf [8]byte
			binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
			binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
			if err = tx.Put(kv.BorEventNums, blockNumBuf[:], eventIdBuf[:]); err != nil {
				return lastStateSyncEventID, i, time.Since(fetchStart), err
			}

			wroteIndex = true
		}

		lastStateSyncEventID++
	}

	return lastStateSyncEventID, len(eventRecords), time.Since(fetchStart), nil
}
