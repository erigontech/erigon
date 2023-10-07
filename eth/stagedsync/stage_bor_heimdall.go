package stagedsync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/borfinality/generics"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

type BorHeimdallCfg struct {
	db               kv.RwDB
	miningState      MiningState
	chainConfig      chain.Config
	heimdallClient   heimdall.IHeimdallClient
	blockReader      services.FullBlockReader
	hd               *headerdownload.HeaderDownload
	penalize         func(context.Context, []headerdownload.PenaltyItem)
	stateReceiverABI abi.ABI
}

func StageBorHeimdallCfg(
	db kv.RwDB,
	miningState MiningState,
	chainConfig chain.Config,
	heimdallClient heimdall.IHeimdallClient,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	penalize func(context.Context, []headerdownload.PenaltyItem),
) BorHeimdallCfg {
	return BorHeimdallCfg{
		db:               db,
		miningState:      miningState,
		chainConfig:      chainConfig,
		heimdallClient:   heimdallClient,
		blockReader:      blockReader,
		hd:               hd,
		penalize:         penalize,
		stateReceiverABI: contract.StateReceiver(),
	}
}

func BorHeimdallForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	mine bool,
	logger log.Logger,
) (err error) {
	processStart := time.Now()

	if cfg.chainConfig.Bor == nil {
		return
	}
	if cfg.heimdallClient == nil {
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

	var header *types.Header
	var headNumber uint64

	headNumber, err = stages.GetStageProgress(tx, stages.Headers)

	hash, err := cfg.blockReader.CanonicalHash(ctx, tx, headNumber)

	if err != nil {
		return err
	}

	service := whitelist.GetWhitelistingService()

	if generics.BorMilestoneRewind.Load() != nil && *generics.BorMilestoneRewind.Load() != 0 {
		// TODO set the hash and the headNumber to the milestone fork point

		unwindPoint := *generics.BorMilestoneRewind.Load()
		var reset uint64 = 0
		generics.BorMilestoneRewind.Store(&reset)
		s.state.UnwindTo(unwindPoint, hash)

		if unwindPoint < headNumber {
			for blockNum := unwindPoint + 1; blockNum <= headNumber; blockNum++ {
				if header, err = cfg.blockReader.HeaderByNumber(ctx, tx, blockNum); err == nil {
					logger.Debug("[BorHeimdall] Verification failed for header", "hash", header.Hash(), "height", blockNum)
					cfg.penalize(ctx, []headerdownload.PenaltyItem{
						{Penalty: headerdownload.BadBlockPenalty, PeerID: cfg.hd.SourcePeerId(header.Hash())}})
					dataflow.HeaderDownloadStates.AddChange(blockNum, dataflow.HeaderInvalidated)
				}
			}
			return fmt.Errorf("milestone block mismatch at %d", headNumber)
		} else {
			return
		}
	}

	if mine {
		minedHeader := cfg.miningState.MiningBlock.Header

		if minedHeadNumber := minedHeader.Number.Uint64(); minedHeadNumber > headNumber {
			// Whitelist service is called to check if the bor chain is
			// on the cannonical chain according to milestones
			if service != nil {
				if !service.IsValidChain(minedHeadNumber, []*types.Header{minedHeader}) {
					logger.Debug("[BorHeimdall] Verification failed for mined header", "hash", minedHeader.Hash(), "height", minedHeadNumber, "err", err)
					dataflow.HeaderDownloadStates.AddChange(minedHeadNumber, dataflow.HeaderInvalidated)
					s.state.UnwindTo(minedHeadNumber-1, minedHeader.Hash())
					return err
				}
			}
		} else {
			return fmt.Errorf("attempting to mine %d, which is behind current head: %d", minedHeadNumber, headNumber)
		}
	}

	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}

	if s.BlockNumber == headNumber {
		return nil
	}

	// Find out the latest event Id
	cursor, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return err
	}
	defer cursor.Close()
	k, _, err := cursor.Last()
	if err != nil {
		return err
	}

	var lastEventId uint64
	if k != nil {
		lastEventId = binary.BigEndian.Uint64(k)
	}
	type LastFrozenEvent interface {
		LastFrozenEventID() uint64
	}
	snapshotLastEventId := cfg.blockReader.(LastFrozenEvent).LastFrozenEventID()
	if snapshotLastEventId > lastEventId {
		lastEventId = snapshotLastEventId
	}
	lastBlockNum := s.BlockNumber
	if cfg.blockReader.FrozenBorBlocks() > lastBlockNum {
		lastBlockNum = cfg.blockReader.FrozenBorBlocks()
	}

	if !mine {
		logger.Info("["+s.LogPrefix()+"] Processng sync events...", "from", lastBlockNum+1)
	}

	var blockNum uint64
	var fetchTime time.Duration
	var eventRecords int
	var lastSpanId uint64

	logTimer := time.NewTicker(30 * time.Second)
	defer logTimer.Stop()

	for blockNum = lastBlockNum + 1; blockNum <= headNumber; blockNum++ {
		select {
		default:
		case <-logTimer.C:
			logger.Info("["+s.LogPrefix()+"] StateSync Progress", "progress", blockNum, "lastSpanId", lastSpanId, "lastEventId", lastEventId, "total records", eventRecords, "fetch time", fetchTime, "process time", time.Since(processStart))
		}

		if !mine {
			header, err = cfg.blockReader.HeaderByNumber(ctx, tx, blockNum)
			if err != nil {
				return err
			}

			// Whitelist service is called to check if the bor chain is
			// on the cannonical chain according to milestones
			if service != nil {
				if !service.IsValidChain(blockNum, []*types.Header{header}) {
					logger.Debug("["+s.LogPrefix()+"] Verification failed for header", "height", blockNum, "hash", header.Hash())
					cfg.penalize(ctx, []headerdownload.PenaltyItem{
						{Penalty: headerdownload.BadBlockPenalty, PeerID: cfg.hd.SourcePeerId(header.Hash())}})
					dataflow.HeaderDownloadStates.AddChange(blockNum, dataflow.HeaderInvalidated)
					s.state.UnwindTo(blockNum-1, header.Hash())
					return fmt.Errorf("verification failed for header %d: %x", blockNum, header.Hash())
				}
			}
		}

		if blockNum%cfg.chainConfig.Bor.CalculateSprint(blockNum) == 0 {
			var callTime time.Duration
			var records int
			if lastEventId, records, callTime, err = fetchAndWriteBorEvents(ctx, cfg.blockReader, cfg.chainConfig.Bor, header, lastEventId, cfg.chainConfig.ChainID.String(), tx, cfg.heimdallClient, cfg.stateReceiverABI, s.LogPrefix(), logger); err != nil {
				return err
			}

			eventRecords += records
			fetchTime += callTime
		}

		if blockNum == 1 || (blockNum > zerothSpanEnd && ((blockNum-zerothSpanEnd-1)%spanLength) == 0) {
			if lastSpanId, err = fetchAndWriteSpans(ctx, blockNum, tx, cfg.heimdallClient, s.LogPrefix(), logger); err != nil {
				return err
			}
		}
	}

	if err = s.Update(tx, headNumber); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	logger.Info("["+s.LogPrefix()+"] Sync events processed", "progress", blockNum-1, "lastSpanId", lastSpanId, "lastEventId", lastEventId, "total records", eventRecords, "fetch time", fetchTime, "process time", time.Since(processStart))

	return
}

func fetchAndWriteBorEvents(
	ctx context.Context,
	blockReader services.FullBlockReader,
	config *chain.BorConfig,
	header *types.Header,
	lastEventId uint64,
	chainID string,
	tx kv.RwTx,
	heimdallClient heimdall.IHeimdallClient,
	stateReceiverABI abi.ABI,
	logPrefix string,
	logger log.Logger,
) (uint64, int, time.Duration, error) {
	fetchStart := time.Now()

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
		pHeader, err := blockReader.HeaderByNumber(ctx, tx, blockNum-config.CalculateSprint(blockNum))
		if err != nil {
			return lastEventId, 0, time.Since(fetchStart), err
		}
		to = time.Unix(int64(pHeader.Time), 0)
	}

	from = lastEventId + 1

	logger.Debug(
		fmt.Sprintf("[%s] Fetching state updates from Heimdall", logPrefix),
		"fromID", from,
		"to", to.Format(time.RFC3339),
	)

	eventRecords, err := heimdallClient.StateSyncEvents(ctx, from, to.Unix())

	if err != nil {
		return lastEventId, 0, time.Since(fetchStart), err
	}

	if config.OverrideStateSyncRecords != nil {
		if val, ok := config.OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]; ok {
			eventRecords = eventRecords[0:val]
		}
	}

	if len(eventRecords) > 0 {
		var key, val [8]byte
		binary.BigEndian.PutUint64(key[:], blockNum)
		binary.BigEndian.PutUint64(val[:], lastEventId+1)
	}
	const method = "commitState"

	wroteIndex := false
	for i, eventRecord := range eventRecords {
		if eventRecord.ID <= lastEventId {
			continue
		}
		if lastEventId+1 != eventRecord.ID || eventRecord.ChainID != chainID || !eventRecord.Time.Before(to) {
			return lastEventId, i, time.Since(fetchStart), fmt.Errorf("invalid event record received blockNum=%d, eventId=%d (exp %d), chainId=%s (exp %s), time=%s (exp to %s)", blockNum, eventRecord.ID, lastEventId+1, eventRecord.ChainID, chainID, eventRecord.Time, to)
		}

		eventRecordWithoutTime := eventRecord.BuildEventRecord()

		recordBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
		if err != nil {
			return lastEventId, i, time.Since(fetchStart), err
		}

		data, err := stateReceiverABI.Pack(method, big.NewInt(eventRecord.Time.Unix()), recordBytes)
		if err != nil {
			logger.Error(fmt.Sprintf("[%s] Unable to pack tx for commitState", logPrefix), "err", err)
			return lastEventId, i, time.Since(fetchStart), err
		}
		var eventIdBuf [8]byte
		binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
		if err = tx.Put(kv.BorEvents, eventIdBuf[:], data); err != nil {
			return lastEventId, i, time.Since(fetchStart), err
		}
		if !wroteIndex {
			var blockNumBuf [8]byte
			binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
			binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
			if err = tx.Put(kv.BorEventNums, blockNumBuf[:], eventIdBuf[:]); err != nil {
				return lastEventId, i, time.Since(fetchStart), err
			}
			wroteIndex = true
		}

		lastEventId++
	}

	return lastEventId, len(eventRecords), time.Since(fetchStart), nil
}

func fetchAndWriteSpans(
	ctx context.Context,
	blockNum uint64,
	tx kv.RwTx,
	heimdallClient heimdall.IHeimdallClient,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {
	var spanId uint64
	if blockNum > zerothSpanEnd {
		spanId = 1 + (blockNum-zerothSpanEnd-1)/spanLength
	}
	logger.Debug(fmt.Sprintf("[%s] Fetching span", logPrefix), "id", spanId)
	response, err := heimdallClient.Span(ctx, spanId)
	if err != nil {
		return 0, err
	}
	spanBytes, err := json.Marshal(response)
	if err != nil {
		return 0, err
	}
	var spanIDBytes [8]byte
	binary.BigEndian.PutUint64(spanIDBytes[:], spanId)
	if err = tx.Put(kv.BorSpans, spanIDBytes[:], spanBytes); err != nil {
		return 0, err
	}
	return spanId, nil
}

func BorHeimdallUnwind(u *UnwindState, ctx context.Context, s *StageState, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	cursor, err := tx.RwCursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], u.UnwindPoint+1)
	k, v, err := cursor.Seek(blockNumBuf[:])
	if err != nil {
		return err
	}
	if k != nil {
		// v is the encoding of the first eventId to be removed
		eventCursor, err := tx.RwCursor(kv.BorEvents)
		if err != nil {
			return err
		}
		defer eventCursor.Close()
		for v, _, err = eventCursor.Seek(v); err == nil && v != nil; v, _, err = eventCursor.Next() {
			if err = eventCursor.DeleteCurrent(); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
	for ; err == nil && k != nil; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	// Removing spans
	spanCursor, err := tx.RwCursor(kv.BorSpans)
	if err != nil {
		return err
	}
	defer spanCursor.Close()
	var lastSpanToKeep uint64
	if u.UnwindPoint > zerothSpanEnd {
		lastSpanToKeep = 1 + (u.UnwindPoint-zerothSpanEnd-1)/spanLength
	}
	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], lastSpanToKeep+1)
	for k, _, err = spanCursor.Seek(spanIdBytes[:]); err == nil && k != nil; k, _, err = spanCursor.Next() {
		if err = spanCursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return
}

func BorHeimdallPrune(s *PruneState, ctx context.Context, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	return
}
