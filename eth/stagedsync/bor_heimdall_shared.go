package stagedsync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
)

var (
	ErrHeaderValidatorsLengthMismatch = errors.New("header validators length mismatch")
	ErrHeaderValidatorsBytesMismatch  = errors.New("header validators bytes mismatch")
)

// LastSpanID TODO - move to block reader
func LastSpanID(tx kv.RwTx, blockReader services.FullBlockReader) (uint64, bool, error) {
	sCursor, err := tx.Cursor(kv.BorSpans)
	if err != nil {
		return 0, false, err
	}

	defer sCursor.Close()
	k, _, err := sCursor.Last()
	if err != nil {
		return 0, false, err
	}

	var lastSpanId uint64
	if k != nil {
		lastSpanId = binary.BigEndian.Uint64(k)
	}

	// TODO tidy this out when moving to block reader
	type LastFrozen interface {
		LastFrozenSpanID() uint64
	}

	snapshotLastSpanId := blockReader.(LastFrozen).LastFrozenSpanID()
	if snapshotLastSpanId > lastSpanId {
		return snapshotLastSpanId, true, nil
	}

	return lastSpanId, k != nil, nil
}

// LastStateSyncEventID TODO - move to block reader
func LastStateSyncEventID(tx kv.RwTx, blockReader services.FullBlockReader) (uint64, error) {
	cursor, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return 0, err
	}

	defer cursor.Close()
	k, _, err := cursor.Last()
	if err != nil {
		return 0, err
	}

	var lastEventId uint64
	if k != nil {
		lastEventId = binary.BigEndian.Uint64(k)
	}

	// TODO tidy this out when moving to block reader
	type LastFrozen interface {
		LastFrozenEventID() uint64
	}

	snapshotLastEventId := blockReader.(LastFrozen).LastFrozenEventID()
	if snapshotLastEventId > lastEventId {
		return snapshotLastEventId, nil
	}

	return lastEventId, nil
}

func FetchSpanZeroForMiningIfNeeded(
	ctx context.Context,
	db kv.RwDB,
	blockReader services.FullBlockReader,
	heimdallClient heimdall.HeimdallClient,
	logger log.Logger,
) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		_, err := blockReader.Span(ctx, tx, 0)
		if err == nil {
			return err
		}

		// TODO refactor to use errors.Is
		if !strings.Contains(err.Error(), "not found") {
			// span exists, no need to fetch
			return nil
		}

		_, err = fetchAndWriteHeimdallSpan(ctx, 0, tx, heimdallClient, "FetchSpanZeroForMiningIfNeeded", logger)
		return err
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
	// This check handles the case when we're in the last sprint of the current span
	// and need to commit next span.
	if bor.IsBlockInLastSprintOfSpan(toBlockNum, cfg.borConfig) {
		requiredSpanID++
	}

	// This check handles the case when we need to fetch 1st span when we're starting
	// the second sprint (of span 0, a special case to fetch span 1).
	if bor.IsSecondSprintStart(toBlockNum, cfg.borConfig) {
		requiredSpanID++
	}

	lastSpanID, exists, err := LastSpanID(tx, cfg.blockReader)
	if err != nil {
		return 0, err
	}

	if exists && requiredSpanID <= lastSpanID {
		return lastSpanID, nil
	}

	var from uint64
	if lastSpanID > 0 {
		from = lastSpanID + 1
	} // else fetch from span 0

	logger.Info(fmt.Sprintf("[%s] Processing spans...", logPrefix), "from", from, "to", requiredSpanID)
	for spanID := from; spanID <= requiredSpanID; spanID++ {
		if _, err = fetchAndWriteHeimdallSpan(ctx, spanID, tx, cfg.heimdallClient, logPrefix, logger); err != nil {
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

	logger.Debug(fmt.Sprintf("[%s] Wrote span", logPrefix), "id", spanID)
	return spanID, nil
}

func fetchRequiredHeimdallStateSyncEventsIfNeeded(
	ctx context.Context,
	header *types.Header,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logPrefix string,
	logger log.Logger,
	lastStateSyncEventIDGetter func() (uint64, error),
) (uint64, int, time.Duration, error) {
	lastStateSyncEventID, err := lastStateSyncEventIDGetter()
	if err != nil {
		return 0, 0, 0, err
	}

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

	from = lastStateSyncEventID + 1

	logger.Debug(
		fmt.Sprintf("[%s] Fetching state updates from Heimdall", logPrefix),
		"fromID", from,
		"to", to.Format(time.RFC3339),
	)

	eventRecords, err := heimdallClient.FetchStateSyncEvents(ctx, from, to, 0)
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

	const method = "commitState"
	wroteIndex := false
	for i, eventRecord := range eventRecords {
		if eventRecord.ID <= lastStateSyncEventID {
			continue
		}

		if lastStateSyncEventID+1 != eventRecord.ID || eventRecord.ChainID != chainID || !eventRecord.Time.Before(to) {
			return lastStateSyncEventID, i, time.Since(fetchStart), fmt.Errorf(fmt.Sprintf(
				"invalid event record received %s, %s, %s, %s",
				fmt.Sprintf("blockNum=%d", blockNum),
				fmt.Sprintf("eventId=%d (exp %d)", eventRecord.ID, lastStateSyncEventID+1),
				fmt.Sprintf("chainId=%s (exp %s)", eventRecord.ChainID, chainID),
				fmt.Sprintf("time=%s (exp to %s)", eventRecord.Time, to),
			))
		}

		eventRecordWithoutTime := eventRecord.BuildEventRecord()

		recordBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
		if err != nil {
			return lastStateSyncEventID, i, time.Since(fetchStart), err
		}

		data, err := stateReceiverABI.Pack(method, big.NewInt(eventRecord.Time.Unix()), recordBytes)
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
