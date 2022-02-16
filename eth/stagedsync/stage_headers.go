package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

// The number of blocks we should be able to re-org sub-second on commodity hardware.
// See https://hackmd.io/TdJtNs0dS56q-In8h-ShSg
const ShortPoSReorgThresholdBlocks = 10

type HeadersCfg struct {
	db                    kv.RwDB
	hd                    *headerdownload.HeaderDownload
	bodyDownload          *bodydownload.BodyDownload
	chainConfig           params.ChainConfig
	headerReqSend         func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool)
	announceNewHashes     func(context.Context, []headerdownload.Announce)
	penalize              func(context.Context, []headerdownload.PenaltyItem)
	batchSize             datasize.ByteSize
	noP2PDiscovery        bool
	tmpdir                string
	newPayloadCh          chan privateapi.PayloadMessage
	forkChoiceCh          chan privateapi.ForkChoiceMessage
	waitingForBeaconChain *uint32 // atomic boolean flag

	snapshots          *snapshotsync.AllSnapshots
	snapshotHashesCfg  *snapshothashes.Config
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        interfaces.FullBlockReader
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	bodyDownload *bodydownload.BodyDownload,
	chainConfig params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool),
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
	noP2PDiscovery bool,
	newPayloadCh chan privateapi.PayloadMessage,
	forkChoiceCh chan privateapi.ForkChoiceMessage,
	waitingForBeaconChain *uint32, // atomic boolean flag
	snapshots *snapshotsync.AllSnapshots,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader interfaces.FullBlockReader,
	tmpdir string,
) HeadersCfg {
	return HeadersCfg{
		db:                    db,
		hd:                    headerDownload,
		bodyDownload:          bodyDownload,
		chainConfig:           chainConfig,
		headerReqSend:         headerReqSend,
		announceNewHashes:     announceNewHashes,
		penalize:              penalize,
		batchSize:             batchSize,
		tmpdir:                tmpdir,
		noP2PDiscovery:        noP2PDiscovery,
		newPayloadCh:          newPayloadCh,
		forkChoiceCh:          forkChoiceCh,
		waitingForBeaconChain: waitingForBeaconChain,
		snapshots:             snapshots,
		snapshotDownloader:    snapshotDownloader,
		blockReader:           blockReader,
		snapshotHashesCfg:     snapshothashes.KnownConfig(chainConfig.ChainName),
	}
}

func SpawnStageHeaders(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	var blockNumber uint64

	if s == nil {
		blockNumber = 0
	} else {
		blockNumber = s.BlockNumber
	}

	isTrans, err := rawdb.Transitioned(tx, blockNumber, cfg.chainConfig.TerminalTotalDifficulty)
	if err != nil {
		return err
	}

	if isTrans {
		return HeadersPOS(s, u, ctx, tx, cfg, useExternalTx)
	} else {
		return HeadersPOW(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx)
	}
}

// HeadersPOS processes Proof-of-Stake requests (newPayload, forkchoiceUpdated)
func HeadersPOS(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	useExternalTx bool,
) error {
	log.Info("Waiting for beacon chain...")

	atomic.StoreUint32(cfg.waitingForBeaconChain, 1)
	defer atomic.StoreUint32(cfg.waitingForBeaconChain, 0)

	var payloadMessage privateapi.PayloadMessage
	var forkChoiceMessage privateapi.ForkChoiceMessage

	// Decide what kind of action we need to take place
	forkChoiceInsteadOfNewPayload := false
	select {
	case <-ctx.Done():
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: errors.New("server is stopping")}
		return nil
	case <-cfg.hd.SkipCycleHack:
		return nil
	case forkChoiceMessage = <-cfg.forkChoiceCh:
		forkChoiceInsteadOfNewPayload = true
	case payloadMessage = <-cfg.newPayloadCh:
	}

	atomic.StoreUint32(cfg.waitingForBeaconChain, 0)

	cfg.hd.ClearPendingPayloadStatus()

	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	headerInserter := headerdownload.NewHeaderInserter(s.LogPrefix(), nil, s.BlockNumber, cfg.blockReader)

	if forkChoiceInsteadOfNewPayload {
		handleForkChoice(&forkChoiceMessage, s, u, ctx, tx, cfg, headerInserter)
	} else {
		if err := handleNewPayload(&payloadMessage, s, ctx, tx, cfg, headerInserter); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func handleForkChoice(
	forkChoiceMessage *privateapi.ForkChoiceMessage,
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	headerInserter *headerdownload.HeaderInserter,
) error {
	headerHash := forkChoiceMessage.HeadBlockHash

	header, err := rawdb.ReadHeaderByHash(tx, headerHash)
	if err != nil {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		return err
	}

	repliedWithSyncStatus := false
	if header == nil {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}
		repliedWithSyncStatus = true

		hashToDownload := headerHash
		heighToDownload := cfg.hd.TopSeenHeight() // approximate
		success, err := downloadMissingPoSHeaders(hashToDownload, heighToDownload, s, ctx, tx, cfg, headerInserter)
		if err != nil {
			return err
		}
		if !success {
			return nil
		}

		// FIXME(yperbasis): HeaderNumber is only populated at Stage 3
		header, err = rawdb.ReadHeaderByHash(tx, headerHash)
		if err != nil {
			return err
		}
	}

	headerNumber := header.Number.Uint64()
	cfg.hd.UpdateTopSeenHeightPoS(headerNumber)

	parent := rawdb.ReadHeader(tx, header.ParentHash, headerNumber-1)

	forkingPoint, err := headerInserter.ForkingPoint(tx, header, parent)
	if err != nil {
		if !repliedWithSyncStatus {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		}
		return err
	}

	if !repliedWithSyncStatus {
		if headerNumber-forkingPoint <= ShortPoSReorgThresholdBlocks {
			// Short range re-org
			// TODO(yperbasis): what if some bodies are missing?
			cfg.hd.SetPendingPayloadStatus(headerHash)
		} else {
			// Long range re-org
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}
		}
	}

	u.UnwindTo(forkingPoint, common.Hash{})

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	err = fixCanonicalChain(s.LogPrefix(), logEvery, headerNumber, headerHash, tx, cfg.blockReader)
	if err != nil {
		return err
	}

	err = rawdb.WriteHeadHeaderHash(tx, headerHash)
	if err != nil {
		return err
	}

	return s.Update(tx, headerNumber)
}

func handleNewPayload(
	payloadMessage *privateapi.PayloadMessage,
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	headerInserter *headerdownload.HeaderInserter,
) error {
	header := payloadMessage.Header
	headerNumber := header.Number.Uint64()
	headerHash := header.Hash()

	cfg.hd.UpdateTopSeenHeightPoS(headerNumber)

	existingCanonicalHash, err := rawdb.ReadCanonicalHash(tx, headerNumber)
	if err != nil {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		return err
	}

	if existingCanonicalHash != (common.Hash{}) && headerHash == existingCanonicalHash {
		// previously received valid header
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
			Status:          remote.EngineStatus_VALID,
			LatestValidHash: headerHash,
		}
		return nil
	}

	// If we have the parent then we can move on with the stagedsync
	parent := rawdb.ReadHeader(tx, header.ParentHash, headerNumber-1)

	transactions, err := types.DecodeTransactions(payloadMessage.Body.Transactions)
	if err != nil {
		log.Warn("Error during Beacon transaction decoding", "err", err.Error())
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			LatestValidHash: header.ParentHash, // TODO(yperbasis): potentially wrong when parent is nil
			ValidationError: err,
		}
		return nil
	}

	if parent != nil {
		success, err := verifyAndSaveNewPoSHeader(s, tx, cfg, header, headerInserter)
		if err != nil || !success {
			return err
		}
	} else {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}

		hashToDownload := header.ParentHash
		heightToDownload := headerNumber - 1
		success, err := downloadMissingPoSHeaders(hashToDownload, heightToDownload, s, ctx, tx, cfg, headerInserter)
		if err != nil || !success {
			return err
		}

		if verificationErr := cfg.hd.VerifyHeader(header); verificationErr != nil {
			log.Warn("Verification failed for header", "hash", headerHash, "height", headerNumber, "error", verificationErr)
			return nil
		}

		err = headerInserter.FeedHeaderPoS(tx, header, headerHash)
		if err != nil {
			return err
		}
	}

	if cfg.bodyDownload != nil {
		block := types.NewBlockFromStorage(headerHash, header, transactions, nil)
		cfg.bodyDownload.AddToPrefetch(block)
	}

	return nil
}

func verifyAndSaveNewPoSHeader(
	s *StageState,
	tx kv.RwTx,
	cfg HeadersCfg,
	header *types.Header,
	headerInserter *headerdownload.HeaderInserter,
) (success bool, err error) {
	headerNumber := header.Number.Uint64()
	headerHash := header.Hash()

	if verificationErr := cfg.hd.VerifyHeader(header); verificationErr != nil {
		log.Warn("Verification failed for header", "hash", headerHash, "height", headerNumber, "error", verificationErr)
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			LatestValidHash: header.ParentHash,
			ValidationError: verificationErr,
		}
		return
	}

	err = headerInserter.FeedHeaderPoS(tx, header, headerHash)
	if err != nil {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		return
	}

	headBlockHash := rawdb.ReadHeadBlockHash(tx)
	if headBlockHash == header.ParentHash {
		// OK, we're on the canonical chain
		cfg.hd.SetPendingPayloadStatus(headerHash)

		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

		// Extend canonical chain by the new header
		err = fixCanonicalChain(s.LogPrefix(), logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader)
		if err != nil {
			return
		}

		err = rawdb.WriteHeadHeaderHash(tx, headerHash)
		if err != nil {
			return
		}

		err = s.Update(tx, headerNumber)
		if err != nil {
			return
		}
	} else {
		// Side chain or something weird
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_ACCEPTED}
		// No canonization, HeadHeaderHash & StageProgress are not updated
	}

	success = true
	return
}

func downloadMissingPoSHeaders(
	hashToDownloadPoS common.Hash,
	heightToDownload uint64,
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	headerInserter *headerdownload.HeaderInserter,
) (success bool, err error) {
	cfg.hd.SetPOSSync(true)
	err = cfg.hd.ReadProgressFromDb(tx)
	if err != nil {
		return
	}

	cfg.hd.SetFetching(true)
	cfg.hd.SetHashToDownloadPoS(hashToDownloadPoS)
	cfg.hd.SetHeightToDownloadPoS(heightToDownload)

	log.Info(fmt.Sprintf("[%s] Downloading PoS headers...", s.LogPrefix()))

	stopped := false
	prevProgress := uint64(0)

	headerCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer headerCollector.Close()
	cfg.hd.SetHeadersCollector(headerCollector)
	// Cleanup after we finish backward sync
	defer func() {
		cfg.hd.SetHeadersCollector(nil)
		cfg.hd.Unsync()
		cfg.hd.SetFetching(false)
	}()

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var req headerdownload.HeaderRequest
	for !stopped {
		sentToPeer := false
		maxRequests := 4096
		for !sentToPeer && !stopped && maxRequests != 0 {
			// TODO(yperbasis): handle the case when are not able to sync a chain
			req = cfg.hd.RequestMoreHeadersForPOS()
			_, sentToPeer = cfg.headerReqSend(ctx, &req)
			maxRequests--
		}

		if cfg.hd.Synced() {
			stopped = true
		}
		// Sleep and check for logs
		timer := time.NewTimer(2 * time.Millisecond)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			if prevProgress == 0 {
				prevProgress = cfg.hd.Progress()
			} else if cfg.hd.Progress() <= prevProgress {
				diff := prevProgress - cfg.hd.Progress()
				log.Info("Wrote Block Headers backwards", "now", cfg.hd.Progress(),
					"blk/sec", float64(diff)/float64(logInterval/time.Second))
				prevProgress = cfg.hd.Progress()
			}
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
		timer.Stop()
	}
	// If the user stopped it, we don't update anything
	if !cfg.hd.Synced() {
		return
	}

	headerLoadFunc := func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		var h types.Header
		if err := rlp.DecodeBytes(value, &h); err != nil {
			return err
		}
		if err := cfg.hd.VerifyHeader(&h); err != nil {
			log.Warn("Verification failed for header", "hash", h.Hash(), "height", h.Number.Uint64(), "error", err)
			return err
		}
		return headerInserter.FeedHeaderPoS(tx, &h, h.Hash())
	}

	err = headerCollector.Load(tx, kv.Headers, headerLoadFunc, etl.TransformArgs{
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
	if err != nil {
		return
	}

	success = true
	return
}

// HeadersPOW progresses Headers stage for Proof-of-Work headers
func HeadersPOW(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
	useExternalTx bool,
) error {
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg); err != nil {
		return err
	}

	var headerProgress uint64
	var err error

	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetPOSSync(false)
	cfg.hd.SetFetching(true)
	defer cfg.hd.SetFetching(false)
	headerProgress = cfg.hd.Progress()
	logPrefix := s.LogPrefix()
	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, err := rawdb.ReadCanonicalHash(tx, headerProgress)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	if hash == (common.Hash{}) {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx, cfg.blockReader); err != nil {
			return err
		}
		if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	// Allow other stages to run 1 cycle if no network available
	if initialCycle && cfg.noP2PDiscovery {
		return nil
	}

	log.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", headerProgress)

	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	if localTd == nil {
		return fmt.Errorf("localTD is nil: %d, %x", headerProgress, hash)
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress, cfg.blockReader)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	var sentToPeer bool
	stopped := false
	prevProgress := headerProgress
Loop:
	for !stopped {

		isTrans, err := rawdb.Transitioned(tx, headerProgress, cfg.chainConfig.TerminalTotalDifficulty)
		if err != nil {
			return err
		}

		if isTrans {
			if err := s.Update(tx, headerProgress); err != nil {
				return err
			}
			break
		}
		currentTime := uint64(time.Now().Unix())
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				// If request was actually sent to a peer, we update retry time to be 5 seconds in the future
				cfg.hd.UpdateRetryTime(req, currentTime, 5 /* timeout */)
				log.Trace("Sent request", "height", req.Number)
			}
		}
		if len(penalties) > 0 {
			cfg.penalize(ctx, penalties)
		}
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && sentToPeer && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				_, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					// If request was actually sent to a peer, we update retry time to be 5 seconds in the future
					cfg.hd.UpdateRetryTime(req, currentTime, 5 /*timeout */)
					log.Trace("Sent request", "height", req.Number)
				}
			}
			if len(penalties) > 0 {
				cfg.penalize(ctx, penalties)
			}
			maxRequests--
		}

		// Send skeleton request if required
		req = cfg.hd.RequestSkeleton()
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				log.Trace("Sent skeleton request", "height", req.Number)
			}
		}
		// Load headers into the database
		var inSync bool
		if inSync, err = cfg.hd.InsertHeaders(headerInserter.NewFeedHeaderFunc(tx, cfg.blockReader), cfg.chainConfig.TerminalTotalDifficulty, logPrefix, logEvery.C); err != nil {
			return err
		}

		announces := cfg.hd.GrabAnnounces()
		if len(announces) > 0 {
			cfg.announceNewHashes(ctx, announces)
		}
		if headerInserter.BestHeaderChanged() { // We do not break unless there best header changed
			if !initialCycle {
				// if this is not an initial cycle, we need to react quickly when new headers are coming in
				break
			}
			// if this is initial cycle, we want to make sure we insert all known headers (inSync)
			if inSync {
				break
			}
		}
		if test {
			break
		}
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := cfg.hd.Progress()
			logProgressHeaders(logPrefix, prevProgress, progress)
			prevProgress = progress
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		case <-cfg.hd.DeliveryNotify:
			log.Trace("headerLoop woken up by the incoming request")
		case <-cfg.hd.SkipCycleHack:
			break Loop
		}
		timer.Stop()
	}
	if headerInserter.Unwind() {
		u.UnwindTo(headerInserter.UnwindPoint(), common.Hash{})
	}
	if headerInserter.GetHighest() != 0 {
		if !headerInserter.Unwind() {
			if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader); err != nil {
				return fmt.Errorf("fix canonical chain: %w", err)
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, headerInserter.GetHighestHash()); err != nil {
			return fmt.Errorf("[%s] marking head header hash as %x: %w", logPrefix, headerInserter.GetHighestHash(), err)
		}
		if err = s.Update(tx, headerInserter.GetHighest()); err != nil {
			return fmt.Errorf("[%s] saving Headers progress: %w", logPrefix, err)
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	if stopped {
		return libcommon.ErrStopped
	}
	// We do not print the following line if the stage was interrupted
	log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest inserted", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)))

	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash common.Hash, tx kv.StatelessRwTx, headerReader interfaces.FullBlockReader) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch common.Hash
	var err error
	for ch, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight); err == nil && ch != ancestorHash; ch, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight) {
		if err = rawdb.WriteCanonicalHash(tx, ancestorHash, ancestorHeight); err != nil {
			return fmt.Errorf("marking canonical header %d %x: %w", ancestorHeight, ancestorHash, err)
		}

		ancestor, err := headerReader.Header(context.Background(), tx, ancestorHash, ancestorHeight)
		if err != nil {
			return err
		}
		if ancestor == nil {
			return fmt.Errorf("ancestor is nil. height %d, hash %x", ancestorHeight, ancestorHash)
		}

		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] write canonical markers", logPrefix), "ancestor", ancestorHeight, "hash", ancestorHash)
		default:
		}
		ancestorHash = ancestor.ParentHash
		ancestorHeight--
	}
	if err != nil {
		return fmt.Errorf("reading canonical hash for %d: %w", ancestorHeight, err)
	}

	return nil
}

func HeadersUnwind(u *UnwindState, s *StageState, tx kv.RwTx, cfg HeadersCfg, test bool) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// Delete canonical hashes that are being unwound
	var headerProgress uint64
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	badBlock := u.BadBlock != (common.Hash{})
	if badBlock {
		cfg.hd.ReportBadHeader(u.BadBlock)
		// Mark all descendants of bad block as bad too
		headerCursor, cErr := tx.Cursor(kv.Headers)
		if cErr != nil {
			return cErr
		}
		defer headerCursor.Close()
		var k, v []byte
		for k, v, err = headerCursor.Seek(dbutils.EncodeBlockNumber(u.UnwindPoint + 1)); err == nil && k != nil; k, v, err = headerCursor.Next() {
			var h types.Header
			if err = rlp.DecodeBytes(v, &h); err != nil {
				return err
			}
			if cfg.hd.IsBadHeader(h.ParentHash) {
				cfg.hd.ReportBadHeader(h.Hash())
			}
		}
		if err != nil {
			return fmt.Errorf("iterate over headers to mark bad headers: %w", err)
		}
	}
	for blockHeight := headerProgress; blockHeight > u.UnwindPoint; blockHeight-- {
		if err = rawdb.DeleteCanonicalHash(tx, blockHeight); err != nil {
			return err
		}
	}
	if badBlock {
		var maxTd big.Int
		var maxHash common.Hash
		var maxNum uint64 = 0

		if test { // If we are not in the test, we can do searching for the heaviest chain in the next cycle
			// Find header with biggest TD
			tdCursor, cErr := tx.Cursor(kv.HeaderTD)
			if cErr != nil {
				return cErr
			}
			defer tdCursor.Close()
			var k, v []byte
			k, v, err = tdCursor.Last()
			if err != nil {
				return err
			}
			for ; err == nil && k != nil; k, v, err = tdCursor.Prev() {
				if len(k) != 40 {
					return fmt.Errorf("key in TD table has to be 40 bytes long: %x", k)
				}
				var hash common.Hash
				copy(hash[:], k[8:])
				if cfg.hd.IsBadHeader(hash) {
					continue
				}
				var td big.Int
				if err = rlp.DecodeBytes(v, &td); err != nil {
					return err
				}
				if td.Cmp(&maxTd) > 0 {
					maxTd.Set(&td)
					copy(maxHash[:], k[8:])
					maxNum = binary.BigEndian.Uint64(k[:8])
				}
			}
			if err != nil {
				return err
			}
		}
		if maxNum == 0 {
			maxNum = u.UnwindPoint
			if maxHash, err = rawdb.ReadCanonicalHash(tx, maxNum); err != nil {
				return err
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, maxHash); err != nil {
			return err
		}
		if err = u.Done(tx); err != nil {
			return err
		}
		if err = s.Update(tx, maxNum); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func logProgressHeaders(logPrefix string, prev, now uint64) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block headers", logPrefix),
		"number", now,
		"blk/second", speed,
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys))

	return now
}

type chainReader struct {
	config      *params.ChainConfig
	tx          kv.RwTx
	blockReader interfaces.FullBlockReader
}

func (cr chainReader) Config() *params.ChainConfig  { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr chainReader) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}

type epochReader struct {
	tx kv.RwTx
}

func (cr epochReader) GetEpoch(hash common.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadEpoch(cr.tx, number, hash)
}
func (cr epochReader) PutEpoch(hash common.Hash, number uint64, proof []byte) error {
	return rawdb.WriteEpoch(cr.tx, number, hash, proof)
}
func (cr epochReader) GetPendingEpoch(hash common.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadPendingEpoch(cr.tx, number, hash)
}
func (cr epochReader) PutPendingEpoch(hash common.Hash, number uint64, proof []byte) error {
	return rawdb.WritePendingEpoch(cr.tx, number, hash, proof)
}
func (cr epochReader) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, err error) {
	return rawdb.FindEpochBeforeOrEqualNumber(cr.tx, number)
}

func HeadersPrune(p *PruneState, tx kv.RwTx, cfg HeadersCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
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
	return nil
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg HeadersCfg) error {
	if cfg.snapshots == nil {
		return nil
	}

	if !cfg.snapshots.SegmentsReady() {
		if err := WaitForDownloader(ctx, tx, cfg); err != nil {
			return err
		}

		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

		// Open segments
		for {
			headers, bodies, txs, err := cfg.snapshots.SegmentsAvailability()
			if err != nil {
				return err
			}
			expect := cfg.snapshotHashesCfg.ExpectBlocks
			if headers >= expect && bodies >= expect && txs >= expect {
				if err := cfg.snapshots.ReopenSegments(); err != nil {
					return err
				}
				if expect > cfg.snapshots.BlocksAvailable() {
					return fmt.Errorf("not enough snapshots available: %d > %d", expect, cfg.snapshots.BlocksAvailable())
				}

				break
			}
			log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
			time.Sleep(10 * time.Second)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
			default:
			}
		}
	}

	// Create .idx files
	if !cfg.snapshots.IndicesReady() {
		if !cfg.snapshots.SegmentsReady() {
			return fmt.Errorf("not all snapshot segments are available")
		}

		// wait for Downloader service to download all expected snapshots
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		headers, bodies, txs, err := cfg.snapshots.IdxAvailability()
		if err != nil {
			return err
		}
		expect := cfg.snapshotHashesCfg.ExpectBlocks
		if headers < expect || bodies < expect || txs < expect {
			chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
			if err := cfg.snapshots.BuildIndices(ctx, *chainID, cfg.tmpdir, 0); err != nil {
				return err
			}
		}

		if err := cfg.snapshots.ReopenIndices(); err != nil {
			return err
		}
		if expect > cfg.snapshots.IndicesAvailable() {
			return fmt.Errorf("not enough snapshots available: %d > %d", expect, cfg.snapshots.BlocksAvailable())
		}
	}

	if s.BlockNumber == 0 {
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

		//tx.ClearBucket(kv.HeaderCanonical)
		//tx.ClearBucket(kv.HeaderTD)
		//tx.ClearBucket(kv.HeaderNumber)

		// fill some small tables from snapshots, in future we may store this data in snapshots also, but
		// for now easier just store them in db
		td := big.NewInt(0)
		if err := snapshotsync.ForEachHeader(ctx, cfg.snapshots, func(header *types.Header) error {
			blockNum, blockHash := header.Number.Uint64(), header.Hash()
			td.Add(td, header.Difficulty)
			if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
				return err
			}
			if err := rawdb.WriteHeaderNumber(tx, blockHash, blockNum); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Writing total difficulty index for snapshots", s.LogPrefix()), "block_num", header.Number.Uint64())
			default:
			}
			return nil
		}); err != nil {
			return err
		}

		sn, ok := cfg.snapshots.Blocks(cfg.snapshots.BlocksAvailable())
		if !ok {
			return fmt.Errorf("snapshot not found for block: %d", cfg.snapshots.BlocksAvailable())
		}

		// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
		lastTxnID := sn.TxnHashIdx.BaseDataID() + uint64(sn.Transactions.Count())
		if err := rawdb.ResetSequence(tx, kv.EthTx, lastTxnID+1); err != nil {
			return err
		}
	}

	// Add last headers from snapshots to HeaderDownloader (as persistent links)
	if s.BlockNumber < cfg.snapshots.BlocksAvailable() {
		if err := cfg.hd.AddHeaderFromSnapshot(tx, cfg.snapshots.BlocksAvailable(), cfg.blockReader); err != nil {
			return err
		}
		if err := s.Update(tx, cfg.snapshots.BlocksAvailable()); err != nil {
			return err
		}
		s.BlockNumber = cfg.snapshots.BlocksAvailable()
	}

	return nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(ctx context.Context, tx kv.RwTx, cfg HeadersCfg) error {
	const readyKey = "snapshots_ready"
	v, err := tx.GetOne(kv.DatabaseInfo, []byte(readyKey))
	if err != nil {
		return err
	}
	if len(v) == 1 && v[0] == 1 {
		return nil
	}
	snapshotsCfg := snapshothashes.KnownConfig(cfg.chainConfig.ChainName)

	// send all hashes to the Downloader service
	preverified := snapshotsCfg.Preverified
	req := &proto_downloader.DownloadRequest{Items: make([]*proto_downloader.DownloadItem, len(preverified))}
	i := 0
	for filePath, infoHashStr := range preverified {
		req.Items[i] = &proto_downloader.DownloadItem{
			TorrentHash: downloadergrpc.String2Proto(infoHashStr),
			Path:        filePath,
		}
		i++
	}
	log.Info("[Snapshots] Fetching torrent files metadata")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := cfg.snapshotDownloader.Download(ctx, req); err != nil {
			log.Error("[Snapshots] Can't call downloader", "err", err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}

	// Print download progress until all segments are available
	for {
		if reply, err := cfg.snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
			log.Warn("Error while waiting for snapshots progress", "err", err)
		} else if int(reply.Torrents) < len(snapshotsCfg.Preverified) {
			log.Warn("Downloader has not enough snapshots (yet)")
		} else if reply.Completed {
			break
		} else {
			readiness := 100 * (float64(reply.BytesCompleted) / float64(reply.BytesTotal))
			log.Info("[Snapshots] download", "progress", fmt.Sprintf("%.2f%%", readiness))
		}
		time.Sleep(10 * time.Second)
	}

	if err := tx.Put(kv.DatabaseInfo, []byte(readyKey), []byte{1}); err != nil {
		return err
	}
	return nil
}
