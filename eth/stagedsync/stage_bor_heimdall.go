package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/generics"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const (
	inmemorySnapshots       = 128  // Number of recent vote snapshots to keep in memory
	inmemorySignatures      = 4096 // Number of recent block signatures to keep in memory
	snapshotPersistInterval = 1024 // Number of blocks after which to persist the vote snapshot to the database
	extraVanity             = 32   // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal               = 65   // Fixed number of extra-data suffix bytes reserved for signer seal
)

var (
	ErrHeaderValidatorsLengthMismatch = errors.New("header validators length mismatch")
	ErrHeaderValidatorsBytesMismatch  = errors.New("header validators bytes mismatch")
)

type BorHeimdallCfg struct {
	db               kv.RwDB
	snapDb           kv.RwDB // Database to store and retrieve snapshot checkpoints
	miningState      MiningState
	chainConfig      chain.Config
	heimdallClient   heimdall.IHeimdallClient
	blockReader      services.FullBlockReader
	hd               *headerdownload.HeaderDownload
	penalize         func(context.Context, []headerdownload.PenaltyItem)
	stateReceiverABI abi.ABI
	loopBreakCheck   func(int) bool
	recents          *lru.ARCCache[libcommon.Hash, *bor.Snapshot]
	signatures       *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func StageBorHeimdallCfg(
	db kv.RwDB,
	snapDb kv.RwDB,
	miningState MiningState,
	chainConfig chain.Config,
	heimdallClient heimdall.IHeimdallClient,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	penalize func(context.Context, []headerdownload.PenaltyItem),
	loopBreakCheck func(int) bool,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
) BorHeimdallCfg {
	return BorHeimdallCfg{
		db:               db,
		snapDb:           snapDb,
		miningState:      miningState,
		chainConfig:      chainConfig,
		heimdallClient:   heimdallClient,
		blockReader:      blockReader,
		hd:               hd,
		penalize:         penalize,
		stateReceiverABI: contract.StateReceiver(),
		loopBreakCheck:   loopBreakCheck,
		recents:          recents,
		signatures:       signatures,
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

	if err != nil {
		return err
	}

	service := whitelist.GetWhitelistingService()

	if generics.BorMilestoneRewind.Load() != nil && *generics.BorMilestoneRewind.Load() != 0 {
		unwindPoint := *generics.BorMilestoneRewind.Load()
		var reset uint64 = 0
		generics.BorMilestoneRewind.Store(&reset)

		if service != nil && unwindPoint < headNumber {
			header, err = cfg.blockReader.HeaderByNumber(ctx, tx, headNumber)
			logger.Debug("[BorHeimdall] Verification failed for header", "hash", header.Hash(), "height", headNumber, "err", err)
			cfg.penalize(ctx, []headerdownload.PenaltyItem{
				{Penalty: headerdownload.BadBlockPenalty, PeerID: cfg.hd.SourcePeerId(header.Hash())}})

			dataflow.HeaderDownloadStates.AddChange(headNumber, dataflow.HeaderInvalidated)
			s.state.UnwindTo(unwindPoint, ForkReset(header.Hash()))
			return fmt.Errorf("verification failed for header %d: %x", headNumber, header.Hash())
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
					s.state.UnwindTo(minedHeadNumber-1, ForkReset(minedHeader.Hash()))
					return fmt.Errorf("mining on a wrong fork %d:%x", minedHeadNumber, minedHeader.Hash())
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
	type LastFrozen interface {
		LastFrozenEventID() uint64
		LastFrozenSpanID() uint64
	}
	snapshotLastEventId := cfg.blockReader.(LastFrozen).LastFrozenEventID()
	if snapshotLastEventId > lastEventId {
		lastEventId = snapshotLastEventId
	}
	sCursor, err := tx.Cursor(kv.BorSpans)
	if err != nil {
		return err
	}
	defer sCursor.Close()
	k, _, err = sCursor.Last()
	if err != nil {
		return err
	}
	var lastSpanId uint64
	if k != nil {
		lastSpanId = binary.BigEndian.Uint64(k)
	}
	snapshotLastSpanId := cfg.blockReader.(LastFrozen).LastFrozenSpanID()
	if snapshotLastSpanId > lastSpanId {
		lastSpanId = snapshotLastSpanId
	}
	var nextSpanId uint64
	if lastSpanId > 0 {
		nextSpanId = lastSpanId + 1
	}
	var endSpanID uint64
	if span.IDAt(headNumber) > 0 {
		endSpanID = span.IDAt(headNumber + 1)
	}

	if span.BlockInLastSprintOfSpan(headNumber, cfg.chainConfig.Bor) {
		endSpanID++
	}

	lastBlockNum := s.BlockNumber
	if cfg.blockReader.FrozenBorBlocks() > lastBlockNum {
		lastBlockNum = cfg.blockReader.FrozenBorBlocks()
	}
	recents, err := lru.NewARC[libcommon.Hash, *bor.Snapshot](inmemorySnapshots)
	if err != nil {
		return err
	}
	signatures, err := lru.NewARC[libcommon.Hash, libcommon.Address](inmemorySignatures)
	if err != nil {
		return err
	}
	chain := NewChainReaderImpl(&cfg.chainConfig, tx, cfg.blockReader, logger)

	var blockNum uint64
	var fetchTime time.Duration
	var eventRecords int

	logTimer := time.NewTicker(logInterval)
	defer logTimer.Stop()

	if endSpanID >= nextSpanId {
		logger.Info("["+s.LogPrefix()+"] Processing spans...", "from", nextSpanId, "to", endSpanID)
	}
	for spanID := nextSpanId; spanID <= endSpanID; spanID++ {
		if lastSpanId, err = fetchAndWriteSpans(ctx, spanID, tx, cfg.heimdallClient, s.LogPrefix(), logger); err != nil {
			return err
		}
	}
	if !mine {
		logger.Info("["+s.LogPrefix()+"] Processing sync events...", "from", lastBlockNum+1, "to", headNumber)
	}
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
			if header == nil {
				return fmt.Errorf("header not found: %d", blockNum)
			}

			// Whitelist service is called to check if the bor chain is
			// on the cannonical chain according to milestones
			if service != nil {
				if !service.IsValidChain(blockNum, []*types.Header{header}) {
					logger.Debug("["+s.LogPrefix()+"] Verification failed for header", "height", blockNum, "hash", header.Hash())
					cfg.penalize(ctx, []headerdownload.PenaltyItem{
						{Penalty: headerdownload.BadBlockPenalty, PeerID: cfg.hd.SourcePeerId(header.Hash())}})
					dataflow.HeaderDownloadStates.AddChange(blockNum, dataflow.HeaderInvalidated)
					s.state.UnwindTo(blockNum-1, ForkReset(header.Hash()))
					return fmt.Errorf("verification failed for header %d: %x", blockNum, header.Hash())
				}
			}

			sprintLength := cfg.chainConfig.Bor.CalculateSprint(blockNum)
			spanID := span.IDAt(blockNum)
			if (spanID > 0) && ((blockNum+1)%sprintLength == 0) {
				if err = checkHeaderExtraData(u, ctx, chain, blockNum, header, cfg.chainConfig.Bor); err != nil {
					return err
				}
			}
		}

		if blockNum > 0 && blockNum%cfg.chainConfig.Bor.CalculateSprint(blockNum) == 0 {
			var callTime time.Duration
			var records int
			if lastEventId, records, callTime, err = fetchAndWriteBorEvents(ctx, cfg.blockReader, cfg.chainConfig.Bor, header, lastEventId, cfg.chainConfig.ChainID.String(), tx, cfg.heimdallClient, cfg.stateReceiverABI, s.LogPrefix(), logger); err != nil {
				return err
			}

			eventRecords += records
			fetchTime += callTime
		}

		var snap *bor.Snapshot

		if header != nil {
			if cfg.blockReader.BorSnapshots().SegmentsMin() == 0 {
				snap = loadSnapshot(blockNum, header.Hash(), cfg.chainConfig.Bor, recents, signatures, cfg.snapDb, logger)

				if snap == nil {
					snap, err = initValidatorSets(ctx, tx, cfg.blockReader, cfg.chainConfig.Bor,
						cfg.heimdallClient, chain, blockNum, recents, signatures, cfg.snapDb, logger, s.LogPrefix())

					if err != nil {
						return fmt.Errorf("can't initialise validator sets: %w", err)
					}
				}

				if err = persistValidatorSets(ctx, snap, u, tx, cfg.blockReader, cfg.chainConfig.Bor, chain, blockNum, header.Hash(), recents, signatures, cfg.snapDb, logger, s.LogPrefix()); err != nil {
					return fmt.Errorf("can't persist validator sets: %w", err)
				}
			}
		}

		if cfg.loopBreakCheck != nil && cfg.loopBreakCheck(int(blockNum-lastBlockNum)) {
			break
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

func checkHeaderExtraData(
	u Unwinder,
	ctx context.Context,
	chain consensus.ChainHeaderReader,
	blockNum uint64,
	header *types.Header,
	config *chain.BorConfig,
) error {
	spanID := span.IDAt(blockNum + 1)
	spanBytes := chain.BorSpan(spanID)
	var sp span.HeimdallSpan
	if err := json.Unmarshal(spanBytes, &sp); err != nil {
		return err
	}
	producerSet := make([]*valset.Validator, len(sp.SelectedProducers))
	for i := range sp.SelectedProducers {
		producerSet[i] = &sp.SelectedProducers[i]
	}

	sort.Sort(valset.ValidatorsByAddress(producerSet))

	headerVals, err := valset.ParseValidators(bor.GetValidatorBytes(header, config))
	if err != nil {
		return err
	}

	if len(producerSet) != len(headerVals) {
		return ErrHeaderValidatorsLengthMismatch
	}

	for i, val := range producerSet {
		if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
			return ErrHeaderValidatorsBytesMismatch
		}
	}
	return nil
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

	if header == nil {
		return 0, 0, 0, fmt.Errorf("can't fetch events for nil header")
	}

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
	spanId uint64,
	tx kv.RwTx,
	heimdallClient heimdall.IHeimdallClient,
	logPrefix string,
	logger log.Logger,
) (uint64, error) {
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
	logger.Debug(fmt.Sprintf("[%s] Wrote span", logPrefix), "id", spanId)
	return spanId, nil
}

func loadSnapshot(blockNum uint64, hash libcommon.Hash, config *chain.BorConfig, recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger) *bor.Snapshot {

	if s, ok := recents.Get(hash); ok {
		return s
	}

	if blockNum%snapshotPersistInterval == 0 {
		if s, err := bor.LoadSnapshot(config, signatures, snapDb, hash); err == nil {
			logger.Trace("Loaded snapshot from disk", "number", blockNum, "hash", hash)
			return s
		}
	}

	return nil
}

func persistValidatorSets(
	ctx context.Context,
	snap *bor.Snapshot,
	u Unwinder,
	tx kv.Tx,
	blockReader services.FullBlockReader,
	config *chain.BorConfig,
	chain consensus.ChainHeaderReader,
	blockNum uint64,
	hash libcommon.Hash,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger,
	logPrefix string) error {

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	// Search for a snapshot in memory or on disk for checkpoints

	headers := make([]*types.Header, 0, 16)
	var parent *types.Header

	if s, ok := recents.Get(hash); ok {
		snap = s
	}

	//nolint:govet
	for snap == nil {
		// If an on-disk snapshot can be found, use that
		if blockNum%snapshotPersistInterval == 0 {
			if s, err := bor.LoadSnapshot(config, signatures, snapDb, hash); err == nil {
				logger.Trace("Loaded snapshot from disk", "number", blockNum, "hash", hash)

				snap = s

				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		// No explicit parents (or no more left), reach out to the database
		if parent != nil {
			header = parent
		} else if chain != nil {
			header = chain.GetHeader(hash, blockNum)
			//logger.Info(fmt.Sprintf("header %d %x => %+v\n", header.Number.Uint64(), header.Hash(), header))
		}

		if header == nil {
			return consensus.ErrUnknownAncestor
		}

		if blockNum == 0 {
			break
		}

		headers = append(headers, header)
		blockNum, hash = blockNum-1, header.ParentHash
		if chain != nil {
			parent = chain.GetHeader(hash, blockNum)
		}

		// If an in-memory snapshot was found, use that
		if s, ok := recents.Get(hash); ok {
			snap = s
			break
		}

		select {
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[%s] Gathering headers for validator proposer prorities (backwards)", logPrefix), "blockNum", blockNum)
		default:
		}
	}

	// check if snapshot is nil
	if snap == nil {
		return fmt.Errorf("unknown error while retrieving snapshot at block number %v", blockNum)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	if len(headers) > 0 {
		var err error
		if snap, err = snap.Apply(parent, headers, logger); err != nil {
			if snap != nil {
				var badHash common.Hash
				for _, header := range headers {
					if header.Number.Uint64() == snap.Number+1 {
						badHash = header.Hash()
						break
					}
				}
				u.UnwindTo(snap.Number, BadBlock(badHash, err))
			} else {
				return fmt.Errorf("snap.Apply %d, headers %d-%d: %w", blockNum, headers[0].Number.Uint64(), headers[len(headers)-1].Number.Uint64(), err)
			}
		}
	}

	recents.Add(snap.Hash, snap)

	// If we've generated a new persistent snapshot, save to disk
	if snap.Number%snapshotPersistInterval == 0 && len(headers) > 0 {
		if err := snap.Store(snapDb); err != nil {
			return fmt.Errorf("snap.Store: %w", err)
		}

		logger.Debug(fmt.Sprintf("[%s] Stored proposer snapshot to disk", logPrefix), "number", snap.Number, "hash", snap.Hash)
	}

	return nil
}

func initValidatorSets(
	ctx context.Context,
	tx kv.RwTx,
	blockReader services.FullBlockReader,
	config *chain.BorConfig,
	heimdallClient heimdall.IHeimdallClient,
	chain consensus.ChainHeaderReader,
	blockNum uint64,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger,
	logPrefix string) (*bor.Snapshot, error) {

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var snap *bor.Snapshot

	// Special handling of the headers in the snapshot
	zeroHeader := chain.GetHeaderByNumber(0)
	if zeroHeader != nil {
		// get checkpoint data
		hash := zeroHeader.Hash()

		if zeroSnap := loadSnapshot(0, hash, config, recents, signatures, snapDb, logger); zeroSnap != nil {
			return nil, nil
		}

		// get validators and current span
		zeroSpanBytes, err := blockReader.Span(ctx, tx, 0)

		if err != nil {
			if _, err := fetchAndWriteSpans(ctx, 0, tx, heimdallClient, logPrefix, logger); err != nil {
				return nil, err
			}

			zeroSpanBytes, err = blockReader.Span(ctx, tx, 0)

			if err != nil {
				return nil, err
			}
		}

		if zeroSpanBytes == nil {
			return nil, fmt.Errorf("zero span not found")
		}

		var zeroSpan span.HeimdallSpan
		if err = json.Unmarshal(zeroSpanBytes, &zeroSpan); err != nil {
			return nil, err
		}

		// new snap shot
		snap = bor.NewSnapshot(config, signatures, 0, hash, zeroSpan.ValidatorSet.Validators, logger)
		if err := snap.Store(snapDb); err != nil {
			return nil, fmt.Errorf("snap.Store (0): %w", err)
		}
		logger.Debug(fmt.Sprintf("[%s] Stored proposer snapshot to disk", logPrefix), "number", 0, "hash", hash)
		g := errgroup.Group{}
		g.SetLimit(estimate.AlmostAllCPUs())
		defer g.Wait()

		batchSize := 128 // must be < inmemorySignatures
		initialHeaders := make([]*types.Header, 0, batchSize)
		parentHeader := zeroHeader
		for i := uint64(1); i <= blockNum; i++ {
			header := chain.GetHeaderByNumber(i)
			{
				// `snap.apply` bottleneck - is recover of signer.
				// to speedup: recover signer in background goroutines and save in `sigcache`
				// `batchSize` < `inmemorySignatures`: means all current batch will fit in cache - and `snap.apply` will find it there.
				g.Go(func() error {
					if header == nil {
						return nil
					}
					_, _ = bor.Ecrecover(header, signatures, config)
					return nil
				})
			}
			if header == nil {
				return nil, fmt.Errorf("missing header persisting validator sets: (inside loop at %d)", i)
			}
			initialHeaders = append(initialHeaders, header)
			if len(initialHeaders) == cap(initialHeaders) {
				if snap, err = snap.Apply(parentHeader, initialHeaders, logger); err != nil {
					return nil, fmt.Errorf("snap.Apply (inside loop): %w", err)
				}
				parentHeader = initialHeaders[len(initialHeaders)-1]
				initialHeaders = initialHeaders[:0]
			}
			select {
			case <-logEvery.C:
				logger.Info(fmt.Sprintf("[%s] Computing validator proposer prorities (forward)", logPrefix), "blockNum", i)
			default:
			}
		}
		if snap, err = snap.Apply(parentHeader, initialHeaders, logger); err != nil {
			return nil, fmt.Errorf("snap.Apply (outside loop): %w", err)
		}
	}

	return snap, nil
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
	lastSpanToKeep := span.IDAt(u.UnwindPoint)
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
