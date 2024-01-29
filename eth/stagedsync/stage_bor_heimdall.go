package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
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
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/finality"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const (
	inmemorySnapshots       = 128  // Number of recent vote snapshots to keep in memory
	InMemorySignatures      = 4096 // Number of recent block signatures to keep in memory
	snapshotPersistInterval = 1024 // Number of blocks after which to persist the vote snapshot to the database
	extraVanity             = 32   // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal               = 65   // Fixed number of extra-data suffix bytes reserved for signer seal
)

type BorHeimdallCfg struct {
	db               kv.RwDB
	snapDb           kv.RwDB // Database to store and retrieve snapshot checkpoints
	miningState      MiningState
	chainConfig      chain.Config
	borConfig        *borcfg.BorConfig
	heimdallClient   heimdall.HeimdallClient
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
	heimdallClient heimdall.HeimdallClient,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	penalize func(context.Context, []headerdownload.PenaltyItem),
	loopBreakCheck func(int) bool,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
) BorHeimdallCfg {
	var borConfig *borcfg.BorConfig
	if chainConfig.Bor != nil {
		borConfig = chainConfig.Bor.(*borcfg.BorConfig)
	}

	return BorHeimdallCfg{
		db:               db,
		snapDb:           snapDb,
		miningState:      miningState,
		chainConfig:      chainConfig,
		borConfig:        borConfig,
		heimdallClient:   heimdallClient,
		blockReader:      blockReader,
		hd:               hd,
		penalize:         penalize,
		stateReceiverABI: bor.GenesisContractStateReceiverABI(),
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
	logger log.Logger,
) (err error) {
	processStart := time.Now()

	if cfg.borConfig == nil {
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

	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}

	whitelistService := whitelist.GetWhitelistingService()
	if unwindPointPtr := finality.BorMilestoneRewind.Load(); unwindPointPtr != nil && *unwindPointPtr != 0 {
		unwindPoint := *unwindPointPtr
		if whitelistService != nil && unwindPoint < headNumber {
			header, err := cfg.blockReader.HeaderByNumber(ctx, tx, headNumber)
			if err != nil {
				return err
			}

			hash := header.Hash()
			logger.Debug(
				fmt.Sprintf("[%s] Verification failed for header due to milestone rewind", s.LogPrefix()),
				"hash", hash,
				"height", headNumber,
			)
			cfg.penalize(ctx, []headerdownload.PenaltyItem{{
				Penalty: headerdownload.BadBlockPenalty,
				PeerID:  cfg.hd.SourcePeerId(hash),
			}})
			dataflow.HeaderDownloadStates.AddChange(headNumber, dataflow.HeaderInvalidated)
			s.state.UnwindTo(unwindPoint, ForkReset(hash))
			var reset uint64 = 0
			finality.BorMilestoneRewind.Store(&reset)
			return fmt.Errorf("verification failed for header %d: %x", headNumber, header.Hash())
		}
	}

	if s.BlockNumber == headNumber {
		return nil
	}

	lastBlockNum := s.BlockNumber

	if cfg.blockReader.FrozenBorBlocks() > lastBlockNum {
		lastBlockNum = cfg.blockReader.FrozenBorBlocks()
	}

	recents, err := lru.NewARC[libcommon.Hash, *bor.Snapshot](inmemorySnapshots)

	if err != nil {
		return err
	}
	signatures, err := lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
	if err != nil {
		return err
	}

	chain := NewChainReaderImpl(&cfg.chainConfig, tx, cfg.blockReader, logger)

	var blockNum uint64
	var fetchTime time.Duration
	var eventRecords int

	lastSpanID, err := fetchRequiredHeimdallSpansIfNeeded(ctx, headNumber, tx, cfg, s.LogPrefix(), logger)
	if err != nil {
		return err
	}

	lastStateSyncEventID, err := LastStateSyncEventID(tx, cfg.blockReader)
	if err != nil {
		return err
	}

	logTimer := time.NewTicker(logInterval)
	defer logTimer.Stop()

	logger.Info("["+s.LogPrefix()+"] Processing sync events...", "from", lastBlockNum+1, "to", headNumber)

	for blockNum = lastBlockNum + 1; blockNum <= headNumber; blockNum++ {
		select {
		default:
		case <-logTimer.C:
			logger.Info("["+s.LogPrefix()+"] StateSync Progress", "progress", blockNum, "lastSpanID", lastSpanID, "lastStateSyncEventID", lastStateSyncEventID, "total records", eventRecords, "fetch time", fetchTime, "process time", time.Since(processStart))
		}

		header, err := cfg.blockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			return fmt.Errorf("header not found: %d", blockNum)
		}

		// Whitelist whitelistService is called to check if the bor chain is
		// on the cannonical chain according to milestones
		if whitelistService != nil && !whitelistService.IsValidChain(blockNum, []*types.Header{header}) {
			logger.Debug("["+s.LogPrefix()+"] Verification failed for header", "height", blockNum, "hash", header.Hash())
			cfg.penalize(ctx, []headerdownload.PenaltyItem{{
				Penalty: headerdownload.BadBlockPenalty,
				PeerID:  cfg.hd.SourcePeerId(header.Hash()),
			}})
			dataflow.HeaderDownloadStates.AddChange(blockNum, dataflow.HeaderInvalidated)
			s.state.UnwindTo(blockNum-1, ForkReset(header.Hash()))
			return fmt.Errorf("verification failed for header %d: %x", blockNum, header.Hash())
		}

		if blockNum > cfg.blockReader.BorSnapshots().SegmentsMin() {
			// SegmentsMin is only set if running as an uploader process (check SnapshotsCfg.snapshotUploader and
			// UploadLocationFlag) when we remove snapshots based on FrozenBlockLimit and number of uploaded snapshots
			// avoid calling this if block for blockNums <= SegmentsMin to avoid reinsertion of snapshots
			snap := loadSnapshot(blockNum, header.Hash(), cfg.borConfig, recents, signatures, cfg.snapDb, logger)

			if snap == nil {
				snap, err = initValidatorSets(ctx, tx, cfg.blockReader, cfg.borConfig,
					cfg.heimdallClient, chain, blockNum, recents, signatures, cfg.snapDb, logger, s.LogPrefix())

				if err != nil {
					return fmt.Errorf("can't initialise validator sets: %w", err)
				}
			}

			if err = persistValidatorSets(ctx, snap, u, tx, cfg.blockReader, cfg.borConfig, chain, blockNum, header.Hash(), recents, signatures, cfg.snapDb, logger, s.LogPrefix()); err != nil {
				return fmt.Errorf("can't persist validator sets: %w", err)
			}
		}

		if err := checkBorHeaderExtraDataIfRequired(chain, header, cfg.borConfig); err != nil {
			return err
		}

		var callTime time.Duration
		var records int
		lastStateSyncEventID, records, callTime, err = fetchRequiredHeimdallStateSyncEventsIfNeeded(
			ctx,
			header,
			tx,
			cfg,
			s.LogPrefix(),
			logger,
			func() (uint64, error) {
				return lastStateSyncEventID, nil
			},
		)
		if err != nil {
			return err
		}

		eventRecords += records
		fetchTime += callTime

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

	logger.Info("["+s.LogPrefix()+"] Sync events processed", "progress", blockNum-1, "lastSpanID", lastSpanID, "lastStateSyncEventID", lastStateSyncEventID, "total records", eventRecords, "fetch time", fetchTime, "process time", time.Since(processStart))

	return
}

func loadSnapshot(blockNum uint64, hash libcommon.Hash, config *borcfg.BorConfig, recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
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
	config *borcfg.BorConfig,
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
	config *borcfg.BorConfig,
	heimdallClient heimdall.HeimdallClient,
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
			if _, err := fetchAndWriteHeimdallSpan(ctx, 0, tx, heimdallClient, logPrefix, logger); err != nil {
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

		var zeroSpan heimdall.Span
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

		batchSize := 128 // must be < InMemorySignatures
		initialHeaders := make([]*types.Header, 0, batchSize)
		parentHeader := zeroHeader
		for i := uint64(1); i <= blockNum; i++ {
			header := chain.GetHeaderByNumber(i)
			{
				// `snap.apply` bottleneck - is recover of signer.
				// to speedup: recover signer in background goroutines and save in `sigcache`
				// `batchSize` < `InMemorySignatures`: means all current batch will fit in cache - and `snap.apply` will find it there.
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

func checkBorHeaderExtraDataIfRequired(chr consensus.ChainHeaderReader, header *types.Header, cfg *borcfg.BorConfig) error {
	blockNum := header.Number.Uint64()
	sprintLength := cfg.CalculateSprintLength(blockNum)
	if (blockNum+1)%sprintLength != 0 {
		// not last block of a sprint in a span, so no check needed (we only check last block of a sprint)
		return nil
	}

	return checkBorHeaderExtraData(chr, header, cfg)
}

func checkBorHeaderExtraData(chr consensus.ChainHeaderReader, header *types.Header, cfg *borcfg.BorConfig) error {
	spanID := bor.SpanIDAt(header.Number.Uint64() + 1)
	spanBytes := chr.BorSpan(spanID)
	var sp heimdall.Span
	if err := json.Unmarshal(spanBytes, &sp); err != nil {
		return err
	}

	producerSet := make([]*valset.Validator, len(sp.SelectedProducers))
	for i := range sp.SelectedProducers {
		producerSet[i] = &sp.SelectedProducers[i]
	}

	sort.Sort(valset.ValidatorsByAddress(producerSet))

	headerVals, err := valset.ParseValidators(bor.GetValidatorBytes(header, cfg))
	if err != nil {
		return err
	}

	// span 0 at least for mumbai has a header mismatch in
	// its first spam.  Since we control neither the span, not the
	// the headers (they are external data) - we just don't do the
	// check as it will hault further processing
	if len(producerSet) != len(headerVals) && spanID > 0 {
		return ErrHeaderValidatorsLengthMismatch
	}

	for i, val := range producerSet {
		if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
			return ErrHeaderValidatorsBytesMismatch
		}
	}

	return nil
}

func BorHeimdallUnwind(u *UnwindState, ctx context.Context, s *StageState, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	if cfg.borConfig == nil {
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
	lastSpanToKeep := bor.SpanIDAt(u.UnwindPoint)
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
	if cfg.borConfig == nil {
		return
	}
	return
}
