package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const (
	// snapshotPersistInterval Number of blocks after which to persist the vote
	// snapshot to the database
	snapshotPersistInterval = 1024
)

func NewBorHeimdallSpanProcessor(
	cfg BorHeimdallCfg,
	logger log.Logger,
	logPrefix string,
	chainHR consensus.ChainHeaderReader,
	whitelist *whitelist.Service,
	unwinder Unwinder,
	mine bool,
	spanNum uint64,
	startBlockNumBoundary uint64,
	endBlockNumBoundary uint64,
) *BorHeimdallSpanProcessor {
	startBlockNum := span.StartBlockNumber(spanNum)
	endBlockNum := span.EndBlockNumber(spanNum)

	spanIdOfStartBoundary := span.OfBlockNumber(startBlockNumBoundary)
	if spanNum == spanIdOfStartBoundary {
		startBlockNum = math.Max(startBlockNum, startBlockNumBoundary)
	}

	spanIdOfEndBoundary := span.OfBlockNumber(endBlockNumBoundary)
	if spanNum == spanIdOfEndBoundary {
		endBlockNum = math.Min(endBlockNum, endBlockNumBoundary)
	}

	return &BorHeimdallSpanProcessor{
		cfg:           cfg,
		logger:        logger,
		logPrefix:     logPrefix,
		chainHR:       chainHR,
		whitelist:     whitelist,
		unwinder:      unwinder,
		mine:          mine,
		spanNum:       spanNum,
		startBlockNum: startBlockNum,
		endBlockNum:   endBlockNum,
	}
}

type BorHeimdallSpanProcessor struct {
	cfg           BorHeimdallCfg
	logger        log.Logger
	logPrefix     string
	chainHR       consensus.ChainHeaderReader
	whitelist     *whitelist.Service
	unwinder      Unwinder
	mine          bool
	spanNum       uint64
	startBlockNum uint64
	endBlockNum   uint64
	span          *span.HeimdallSpan
}

func (bhsp *BorHeimdallSpanProcessor) FetchSpanAsync(
	ctx context.Context,
	spChan chan *BorHeimdallSpanProcessor,
	errChan chan error,
) {
	go func() {
		heimdallSpan, err := bhsp.cfg.heimdallClient.Span(ctx, bhsp.spanNum)
		if err != nil {
			errChan <- err
		}

		bhsp.span = heimdallSpan
		select {
		case spChan <- bhsp:
			return
		case <-ctx.Done():
			return
		}
	}()
}

func (bhsp *BorHeimdallSpanProcessor) Process(ctx context.Context, tx kv.RwTx) error {
	logTimer := time.NewTicker(logInterval)
	defer logTimer.Stop()

	var eventRecords uint64
	var fetchTime time.Duration

	if err := bhsp.persistSpan(tx); err != nil {
		return err
	}

	//
	// TODO do we need state sync events for miner too? and how do we do this?
	//
	if bhsp.mine {
		return nil
	}

	lastEventId, err := GetLastPersistedStateSyncEventId(tx, bhsp.cfg.blockReader)
	if err != nil {
		return err
	}

	for blockNum := bhsp.startBlockNum; blockNum <= bhsp.endBlockNum; blockNum++ {
		select {
		case <-logTimer.C:
			bhsp.logger.Info(
				fmt.Sprintf("[%s] StateSync Progress", bhsp.logPrefix),
				"spanNum", bhsp.spanNum,
				"blockNum", blockNum,
				"eventRecords", eventRecords,
				"fetchTime", fetchTime,
			)
		default:
		}

		header, err := bhsp.cfg.blockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return err
		}

		if header == nil {
			return fmt.Errorf("[%s] header not found: %d", bhsp.logPrefix, blockNum)
		}

		headerHash := header.Hash()
		// Whitelist service is called to check if the bor chain is
		// on the canonical chain according to milestones
		hs := []*types.Header{header}
		if bhsp.whitelist != nil && !bhsp.whitelist.IsValidChain(blockNum, hs) {
			return bhsp.unwindInvalidChain(ctx, blockNum, headerHash)
		}

		snap := bhsp.loadSnapshot(blockNum, headerHash)
		if snap == nil {
			snap, err = bhsp.initValidatorSets(ctx, tx, blockNum)
			if err != nil {
				return fmt.Errorf("can't initialise validator sets: %w", err)
			}
		}

		err = bhsp.persistValidatorSets(snap, blockNum, headerHash)
		if err != nil {
			return fmt.Errorf("can't persist validator sets: %w", err)
		}

		if bhsp.BorCfg().IsSprintStart(blockNum) {
			prevLastEventId := lastEventId
			callTime := time.Now()
			lastEventId, err = bhsp.processStateSyncEvents(
				ctx,
				tx,
				header,
				lastEventId,
			)
			if err != nil {
				return err
			}

			eventRecords += lastEventId - prevLastEventId
			fetchTime += time.Since(callTime)
		} else if bhsp.BorCfg().IsSprintEnd(blockNum) {
			if err = bhsp.checkHeaderExtraData(header); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bhsp *BorHeimdallSpanProcessor) persistSpan(tx kv.RwTx) error {
	spanBytes, err := json.Marshal(bhsp.span)
	if err != nil {
		return err
	}

	var spanNumBytes [8]byte
	binary.BigEndian.PutUint64(spanNumBytes[:], bhsp.spanNum)
	if err = tx.Put(kv.BorSpans, spanNumBytes[:], spanBytes); err != nil {
		return err
	}

	bhsp.logger.Debug(fmt.Sprintf("[%s] Wrote span", bhsp.logPrefix), "num", bhsp.spanNum)
	return nil
}

func (bhsp *BorHeimdallSpanProcessor) unwindInvalidChain(
	ctx context.Context,
	blockNum uint64,
	headerHash libcommon.Hash,
) error {
	bhsp.logger.Debug(
		fmt.Sprintf("[%s] Verification failed for header", bhsp.logPrefix),
		"height", blockNum,
		"hash", headerHash,
	)

	penalties := []headerdownload.PenaltyItem{
		{
			Penalty: headerdownload.BadBlockPenalty,
			PeerID:  bhsp.cfg.hd.SourcePeerId(headerHash),
		},
	}

	bhsp.cfg.penalize(ctx, penalties)

	dataflow.
		HeaderDownloadStates.
		AddChange(blockNum, dataflow.HeaderInvalidated)

	bhsp.unwinder.UnwindTo(blockNum-1, ForkReset(headerHash))

	return fmt.Errorf(
		"[%s] verification failed for header %d: %x",
		bhsp.logPrefix,
		blockNum,
		headerHash,
	)
}

func (bhsp *BorHeimdallSpanProcessor) loadSnapshot(
	blockNum uint64,
	headerHash libcommon.Hash,
) *bor.Snapshot {
	if s, ok := bhsp.cfg.recents.Get(headerHash); ok {
		return s
	}

	if blockNum%snapshotPersistInterval == 0 {
		s, err := bor.LoadSnapshot(
			bhsp.BorCfg(),
			bhsp.cfg.signatures,
			bhsp.cfg.snapDb,
			headerHash,
		)
		if err == nil {
			bhsp.logger.Trace(
				"Loaded snapshot from disk",
				"number", blockNum,
				"hash", headerHash,
			)
			return s
		}
	}

	return nil
}

func (bhsp *BorHeimdallSpanProcessor) initValidatorSets(
	ctx context.Context,
	tx kv.RwTx,
	blockNum uint64,
) (*bor.Snapshot, error) {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var snap *bor.Snapshot

	// Special handling of the headers in the snapshot
	zeroHeader := bhsp.chainHR.GetHeaderByNumber(0)
	if zeroHeader != nil {
		// get checkpoint data
		hash := zeroHeader.Hash()

		zeroSnap := bhsp.loadSnapshot(0, hash)
		if zeroSnap != nil {
			return nil, nil
		}

		// get validators and current span
		zeroSpanBytes, err := bhsp.cfg.blockReader.Span(ctx, tx, 0)
		if err != nil {
			return nil, err
		}

		if zeroSpanBytes == nil {
			return nil, fmt.Errorf("zero span not found")
		}

		var zeroSpan span.HeimdallSpan
		if err = json.Unmarshal(zeroSpanBytes, &zeroSpan); err != nil {
			return nil, err
		}

		// new snap shot
		snap = bor.NewSnapshot(
			bhsp.BorCfg(),
			bhsp.cfg.signatures,
			0,
			hash,
			zeroSpan.ValidatorSet.Validators,
			bhsp.logger,
		)
		if err := snap.Store(bhsp.cfg.snapDb); err != nil {
			return nil, fmt.Errorf("snap.Store (0): %w", err)
		}

		bhsp.logger.Debug(
			fmt.Sprintf("[%s] Stored proposer snapshot to disk", bhsp.logPrefix),
			"number", 0,
			"hash", hash,
		)

		g := errgroup.Group{}
		g.SetLimit(estimate.AlmostAllCPUs())
		defer func() {
			_ = g.Wait() // no error returned (check goroutines below)
		}()

		batchSize := 128 // must be < inmemorySignatures
		initialHeaders := make([]*types.Header, 0, batchSize)
		parentHeader := zeroHeader
		for i := uint64(1); i <= blockNum; i++ {
			header := bhsp.chainHR.GetHeaderByNumber(i)
			if header == nil {
				return nil, fmt.Errorf(
					"missing header persisting validator sets: (inside loop at %d)",
					i,
				)
			}

			// `snap.apply` bottleneck - is recover of signer.
			// to speedup: recover signer in background goroutines
			// and save in `sigcache`
			// `batchSize` < `inmemorySignatures`: means all current batch will
			// fit in cache - and `snap.apply` will find it there.
			g.Go(func() error {
				if header == nil {
					return nil
				}
				_, _ = bor.Ecrecover(header, bhsp.cfg.signatures, bhsp.BorCfg())
				return nil
			})

			initialHeaders = append(initialHeaders, header)
			if len(initialHeaders) == cap(initialHeaders) {
				snap, err = snap.Apply(parentHeader, initialHeaders, bhsp.logger)
				if err != nil {
					return nil, fmt.Errorf("snap.Apply (inside loop): %w", err)
				}

				parentHeader = initialHeaders[len(initialHeaders)-1]
				initialHeaders = initialHeaders[:0]
			}

			select {
			case <-logEvery.C:
				bhsp.logger.Info(
					fmt.Sprintf(
						"[%s] Computing validator proposer priorities (forward)",
						bhsp.logPrefix,
					),
					"blockNum", i,
				)
			default:
			}
		}

		snap, err = snap.Apply(parentHeader, initialHeaders, bhsp.logger)
		if err != nil {
			return nil, fmt.Errorf("snap.Apply (outside loop): %w", err)
		}
	}

	return snap, nil
}

func (bhsp *BorHeimdallSpanProcessor) persistValidatorSets(
	snap *bor.Snapshot,
	blockNum uint64,
	headerHash libcommon.Hash,
) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	chainHR := bhsp.chainHR
	snapDb := bhsp.cfg.snapDb
	recents := bhsp.cfg.recents
	signatures := bhsp.cfg.signatures

	// Search for a snapshot in memory or on disk for checkpoints
	headers := make([]*types.Header, 0, 16)
	var parent *types.Header

	if s, ok := recents.Get(headerHash); ok {
		snap = s
	}

	//nolint:govet
	for snap == nil {
		// If an on-disk snapshot can be found, use that
		if blockNum%snapshotPersistInterval == 0 {
			s, err := bor.LoadSnapshot(bhsp.BorCfg(), signatures, snapDb, headerHash)
			if err == nil {
				bhsp.logger.Trace(
					"Loaded snapshot from disk",
					"number", blockNum,
					"hash", headerHash,
				)

				snap = s

				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		// No explicit parents (or no more left), reach out to the database
		if parent != nil {
			header = parent
		} else if chainHR != nil {
			header = chainHR.GetHeader(headerHash, blockNum)
		}

		if header == nil {
			return consensus.ErrUnknownAncestor
		}

		if blockNum == 0 {
			break
		}

		headers = append(headers, header)
		blockNum, headerHash = blockNum-1, header.ParentHash
		if chainHR != nil {
			parent = chainHR.GetHeader(headerHash, blockNum)
		}

		// If an in-memory snapshot was found, use that
		if s, ok := recents.Get(headerHash); ok {
			snap = s
			break
		}

		select {
		case <-logEvery.C:
			bhsp.logger.Info(
				fmt.Sprintf(
					"[%s] Gathering headers for validator proposer prorities (backwards)",
					bhsp.logPrefix,
				),
				"blockNum", blockNum,
			)
		default:
		}
	}

	// check if snapshot is nil
	if snap == nil {
		return fmt.Errorf(
			"unknown error while retrieving snapshot at block number %v",
			blockNum,
		)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	if len(headers) > 0 {
		var err error
		snap, err = snap.Apply(parent, headers, bhsp.logger)
		if err != nil {
			if snap != nil {
				var badHash libcommon.Hash
				for _, header := range headers {
					if header.Number.Uint64() == snap.Number+1 {
						badHash = header.Hash()
						break
					}
				}

				bhsp.unwinder.UnwindTo(snap.Number, BadBlock(badHash, err))
			} else {
				return fmt.Errorf(
					"snap.Apply %d, headers %d-%d: %w",
					blockNum,
					headers[0].Number.Uint64(),
					headers[len(headers)-1].Number.Uint64(),
					err,
				)
			}
		}
	}

	recents.Add(snap.Hash, snap)

	// If we've generated a new persistent snapshot, save to disk
	if snap.Number%snapshotPersistInterval == 0 && len(headers) > 0 {
		if err := snap.Store(snapDb); err != nil {
			return fmt.Errorf("snap.Store: %w", err)
		}

		bhsp.logger.Debug(
			fmt.Sprintf("[%s] Stored proposer snapshot to disk", bhsp.logPrefix),
			"number", snap.Number,
			"hash", snap.Hash,
		)
	}

	return nil
}

func (bhsp *BorHeimdallSpanProcessor) processStateSyncEvents(
	ctx context.Context,
	tx kv.RwTx,
	header *types.Header,
	lastEventId uint64,
) (uint64, error) {
	var to time.Time
	from := lastEventId + 1
	blockNum := header.Number.Uint64()
	if bhsp.BorCfg().IsIndore(blockNum) {
		stateSyncDelay := bhsp.BorCfg().CalculateStateSyncDelay(blockNum)
		to = time.Unix(int64(header.Time-stateSyncDelay), 0)
	} else {
		pHeaderNum := blockNum - bhsp.BorCfg().CalculateSprint(blockNum)
		pHeader, err := bhsp.cfg.blockReader.HeaderByNumber(ctx, tx, pHeaderNum)
		if err != nil {
			return 0, err
		}

		to = time.Unix(int64(pHeader.Time), 0)
	}

	bhsp.logger.Debug(
		fmt.Sprintf("[%s] Fetching state updates from Heimdall", bhsp.logPrefix),
		"fromID", from,
		"to", to.Format(time.RFC3339),
	)

	eventRecords, err := bhsp.cfg.heimdallClient.StateSyncEvents(ctx, from, to.Unix())
	if err != nil {
		return 0, err
	}

	if bhsp.BorCfg().OverrideStateSyncRecords != nil {
		val, ok := bhsp.BorCfg().OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]
		if ok {
			eventRecords = eventRecords[0:val]
		}
	}

	if len(eventRecords) > 0 {
		var key, val [8]byte
		binary.BigEndian.PutUint64(key[:], blockNum)
		binary.BigEndian.PutUint64(val[:], lastEventId+1)
	}

	const method = "commitState"
	chainId := bhsp.cfg.chainConfig.ChainID.String()
	wroteIndex := false
	for _, eventRecord := range eventRecords {
		if eventRecord.ID <= lastEventId {
			continue
		}

		expId := lastEventId + 1
		invalidRecord := expId != eventRecord.ID ||
			eventRecord.ChainID != chainId ||
			!eventRecord.Time.Before(to)

		if invalidRecord {
			return lastEventId, fmt.Errorf(
				"invalid event record received %s, %s, %s, %s",
				fmt.Sprintf("blockNum=%d", blockNum),
				fmt.Sprintf("eventId=%d (exp %d)", eventRecord.ID, expId),
				fmt.Sprintf("chainId=%s (exp %s)", eventRecord.ChainID, chainId),
				fmt.Sprintf("time=%s (exp to %s)", eventRecord.Time, to),
			)
		}

		eventRecordWithoutTime := eventRecord.BuildEventRecord()
		recordBytes, err := rlp.EncodeToBytes(eventRecordWithoutTime)
		if err != nil {
			return lastEventId, err
		}

		erTime := big.NewInt(eventRecord.Time.Unix())
		data, err := bhsp.cfg.stateReceiverABI.Pack(method, erTime, recordBytes)
		if err != nil {
			bhsp.logger.Error(
				fmt.Sprintf("[%s] Unable to pack tx for commitState", bhsp.logPrefix),
				"err", err,
			)
			return lastEventId, err
		}

		var eventIdBuf [8]byte
		binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
		if err = tx.Put(kv.BorEvents, eventIdBuf[:], data); err != nil {
			return lastEventId, err
		}

		if !wroteIndex {
			var blockNumBuf [8]byte
			binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
			binary.BigEndian.PutUint64(eventIdBuf[:], eventRecord.ID)
			err = tx.Put(kv.BorEventNums, blockNumBuf[:], eventIdBuf[:])
			if err != nil {
				return lastEventId, err
			}

			wroteIndex = true
		}

		lastEventId++
	}

	return lastEventId, nil
}

func (bhsp *BorHeimdallSpanProcessor) checkHeaderExtraData(header *types.Header) error {
	heimdallSpan := bhsp.span
	producerSet := make([]*valset.Validator, len(heimdallSpan.SelectedProducers))
	for i := range heimdallSpan.SelectedProducers {
		producerSet[i] = &heimdallSpan.SelectedProducers[i]
	}

	sort.Sort(valset.ValidatorsByAddress(producerSet))

	valBytes := bor.GetValidatorBytes(header, bhsp.BorCfg())
	headerVals, err := valset.ParseValidators(valBytes)
	if err != nil {
		return err
	}

	if len(producerSet) != len(headerVals) {
		return bor.ErrInvalidSpanValidators
	}

	for i, val := range producerSet {
		if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
			return bor.ErrInvalidSpanValidators
		}
	}

	return nil
}

func (bhsp *BorHeimdallSpanProcessor) BorCfg() *chain.BorConfig {
	return bhsp.cfg.chainConfig.Bor
}
