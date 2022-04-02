package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
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
	"github.com/ledgerwatch/erigon/turbo/engineapi"
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
	db                kv.RwDB
	hd                *headerdownload.HeaderDownload
	bodyDownload      *bodydownload.BodyDownload
	chainConfig       params.ChainConfig
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	tmpdir            string
	snapshotDir       *dir.Rw

	snapshots          *snapshotsync.RoSnapshots
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
	snapshots *snapshotsync.RoSnapshots,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader interfaces.FullBlockReader,
	tmpdir string,
	snapshotDir *dir.Rw,
) HeadersCfg {
	return HeadersCfg{
		db:                 db,
		hd:                 headerDownload,
		bodyDownload:       bodyDownload,
		chainConfig:        chainConfig,
		headerReqSend:      headerReqSend,
		announceNewHashes:  announceNewHashes,
		penalize:           penalize,
		batchSize:          batchSize,
		tmpdir:             tmpdir,
		noP2PDiscovery:     noP2PDiscovery,
		snapshots:          snapshots,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		snapshotHashesCfg:  snapshothashes.KnownConfig(chainConfig.ChainName),
		snapshotDir:        snapshotDir,
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

	unsettledForkChoice, headHeight := cfg.hd.GetUnsettledForkChoice()
	if unsettledForkChoice != nil { // some work left to do after unwind
		return handleUnsettledForkChoice(unsettledForkChoice, headHeight, s, tx, cfg, useExternalTx)
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

func handleUnsettledForkChoice(
	forkChoice *engineapi.ForkChoiceMessage,
	headHeight uint64,
	s *StageState,
	tx kv.RwTx,
	cfg HeadersCfg,
	useExternalTx bool,
) error {
	log.Info(fmt.Sprintf("[%s] Unsettled forkchoice after unwind", s.LogPrefix()), "height", headHeight, "forkchoice", forkChoice)

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if err := fixCanonicalChain(s.LogPrefix(), logEvery, headHeight, forkChoice.HeadBlockHash, tx, cfg.blockReader); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(tx, forkChoice.HeadBlockHash); err != nil {
		return err
	}

	rawdb.WriteForkchoiceHead(tx, forkChoice.HeadBlockHash)
	rawdb.WriteForkchoiceSafe(tx, forkChoice.SafeBlockHash)
	rawdb.WriteForkchoiceFinalized(tx, forkChoice.FinalizedBlockHash)

	if err := s.Update(tx, headHeight); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	cfg.hd.ClearUnsettledForkChoice()
	return nil
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
	log.Info(fmt.Sprintf("[%s] Waiting for Beacon Chain...", s.LogPrefix()))

	onlyNewRequests := cfg.hd.PosStatus() == headerdownload.Syncing
	interrupt, requestId, requestWithStatus := cfg.hd.BeaconRequestList.WaitForRequest(onlyNewRequests)

	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})
	headerInserter := headerdownload.NewHeaderInserter(s.LogPrefix(), nil, s.BlockNumber, cfg.blockReader)

	if interrupt != engineapi.None {
		if interrupt == engineapi.Stopping {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: errors.New("server is stopping")}
		}
		if interrupt == engineapi.Synced {
			verifyAndSaveDownloadedPoSHeaders(tx, cfg, headerInserter)
		}
		if !useExternalTx {
			return tx.Commit()
		}
		return nil
	}

	request := requestWithStatus.Message
	status := requestWithStatus.Status

	// Decide what kind of action we need to take place
	var payloadMessage *engineapi.PayloadMessage
	forkChoiceMessage, forkChoiceInsteadOfNewPayload := request.(*engineapi.ForkChoiceMessage)
	if !forkChoiceInsteadOfNewPayload {
		payloadMessage = request.(*engineapi.PayloadMessage)
	}

	cfg.hd.ClearPendingPayloadStatus()

	if forkChoiceInsteadOfNewPayload {
		handleForkChoice(forkChoiceMessage, status, requestId, s, u, ctx, tx, cfg, headerInserter)
	} else {
		if err := handleNewPayload(payloadMessage, status, requestId, s, ctx, tx, cfg, headerInserter); err != nil {
			return err
		}
	}

	if !useExternalTx {
		return tx.Commit()
	}
	return nil
}

func handleForkChoice(
	forkChoiceMessage *engineapi.ForkChoiceMessage,
	requestStatus engineapi.RequestStatus,
	requestId int,
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	headerInserter *headerdownload.HeaderInserter,
) error {
	headerHash := forkChoiceMessage.HeadBlockHash
	log.Info(fmt.Sprintf("[%s] Handling fork choice", s.LogPrefix()), "headerHash", headerHash)

	currentHeadHash := rawdb.ReadHeadHeaderHash(tx)
	if currentHeadHash == headerHash { // no-op
		log.Info(fmt.Sprintf("[%s] Fork choice no-op", s.LogPrefix()))
		cfg.hd.BeaconRequestList.Remove(requestId)
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
				Status:          remote.EngineStatus_VALID,
				LatestValidHash: currentHeadHash,
			}
		}
		return nil
	}

	header, err := rawdb.ReadHeaderByHash(tx, headerHash)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s] Fork choice err", s.LogPrefix()), "err", err)
		cfg.hd.BeaconRequestList.Remove(requestId)
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		}
		return err
	}

	if header == nil {
		log.Info(fmt.Sprintf("[%s] Fork choice missing header", s.LogPrefix()))
		hashToDownload := headerHash
		heighToDownload := cfg.hd.TopSeenHeight() // approximate
		schedulePoSDownload(requestStatus, requestId, hashToDownload, heighToDownload, s, cfg)
		return nil
	}

	cfg.hd.BeaconRequestList.Remove(requestId)

	headerNumber := header.Number.Uint64()
	cfg.hd.UpdateTopSeenHeightPoS(headerNumber)

	forkingPoint := uint64(0)
	if headerNumber > 0 {
		parent := rawdb.ReadHeader(tx, header.ParentHash, headerNumber-1)
		forkingPoint, err = headerInserter.ForkingPoint(tx, header, parent)
		if err != nil {
			if requestStatus == engineapi.New {
				cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
			}
			return err
		}
	}

	if requestStatus == engineapi.New {
		if headerNumber-forkingPoint <= ShortPoSReorgThresholdBlocks {
			log.Info(fmt.Sprintf("[%s] Short range re-org", s.LogPrefix()), "headerNumber", headerNumber, "forkingPoint", forkingPoint)
			// TODO(yperbasis): what if some bodies are missing and we have to download them?
			cfg.hd.SetPendingPayloadStatus(headerHash)
		} else {
			log.Info(fmt.Sprintf("[%s] Long range re-org", s.LogPrefix()), "headerNumber", headerNumber, "forkingPoint", forkingPoint)
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}
		}
	}

	log.Trace(fmt.Sprintf("[%s] Fork choice beginning unwind", s.LogPrefix()))
	u.UnwindTo(forkingPoint, common.Hash{})
	log.Trace(fmt.Sprintf("[%s] Fork choice unwind finished", s.LogPrefix()))

	cfg.hd.SetUnsettledForkChoice(forkChoiceMessage, headerNumber)

	return nil
}

func handleNewPayload(
	payloadMessage *engineapi.PayloadMessage,
	requestStatus engineapi.RequestStatus,
	requestId int,
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	headerInserter *headerdownload.HeaderInserter,
) error {
	header := payloadMessage.Header
	headerNumber := header.Number.Uint64()
	headerHash := header.Hash()

	log.Trace(fmt.Sprintf("[%s] Handling new payload", s.LogPrefix()), "height", headerNumber, "hash", headerHash)

	cfg.hd.UpdateTopSeenHeightPoS(headerNumber)

	existingCanonicalHash, err := rawdb.ReadCanonicalHash(tx, headerNumber)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s] New payload err", s.LogPrefix()), "err", err)
		cfg.hd.BeaconRequestList.Remove(requestId)
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		}
		return err
	}

	if existingCanonicalHash != (common.Hash{}) && headerHash == existingCanonicalHash {
		log.Info(fmt.Sprintf("[%s] New payload: previously received valid header", s.LogPrefix()))
		cfg.hd.BeaconRequestList.Remove(requestId)
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
				Status:          remote.EngineStatus_VALID,
				LatestValidHash: headerHash,
			}
		}
		return nil
	}

	parent := rawdb.ReadHeader(tx, header.ParentHash, headerNumber-1)
	if parent == nil {
		log.Info(fmt.Sprintf("[%s] New payload missing parent", s.LogPrefix()))
		hashToDownload := header.ParentHash
		heightToDownload := headerNumber - 1
		schedulePoSDownload(requestStatus, requestId, hashToDownload, heightToDownload, s, cfg)
		return nil
	}

	cfg.hd.BeaconRequestList.Remove(requestId)

	transactions, err := types.DecodeTransactions(payloadMessage.Body.Transactions)
	if err != nil {
		log.Warn("Error during Beacon transaction decoding", "err", err.Error())
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
				Status:          remote.EngineStatus_INVALID,
				LatestValidHash: header.ParentHash,
				ValidationError: err,
			}
		}
		return nil
	}

	log.Trace(fmt.Sprintf("[%s] New payload begin verification", s.LogPrefix()))
	success, err := verifyAndSaveNewPoSHeader(requestStatus, s, tx, cfg, header, headerInserter)
	log.Trace(fmt.Sprintf("[%s] New payload verification ended", s.LogPrefix()), "success", success, "err", err)
	if err != nil || !success {
		return err
	}

	if cfg.bodyDownload != nil {
		block := types.NewBlockFromStorage(headerHash, header, transactions, nil)
		cfg.bodyDownload.AddToPrefetch(block)
	}

	return nil
}

func verifyAndSaveNewPoSHeader(
	requestStatus engineapi.RequestStatus,
	s *StageState,
	tx kv.RwTx,
	cfg HeadersCfg,
	header *types.Header,
	headerInserter *headerdownload.HeaderInserter,
) (success bool, err error) {
	headerNumber := header.Number.Uint64()
	headerHash := header.Hash()

	if verificationErr := cfg.hd.VerifyHeader(header); verificationErr != nil {
		log.Warn("Verification failed for header", "hash", headerHash, "height", headerNumber, "err", verificationErr)
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{
				Status:          remote.EngineStatus_INVALID,
				LatestValidHash: header.ParentHash,
				ValidationError: verificationErr,
			}
		}
		return
	}

	err = headerInserter.FeedHeaderPoS(tx, header, headerHash)
	if err != nil {
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
		}
		return
	}

	currentHeadHash := rawdb.ReadHeadHeaderHash(tx)
	if currentHeadHash == header.ParentHash {
		// OK, we're on the canonical chain
		if requestStatus == engineapi.New {
			cfg.hd.SetPendingPayloadStatus(headerHash)
		}

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
		// TODO(yperbasis): considered non-canonical because some missing headers were donloaded but not canonized
		// Or it's not a problem because forkChoice is updated frequently?
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_ACCEPTED}
		}
		// No canonization, HeadHeaderHash & StageProgress are not updated
	}

	success = true
	return
}

func schedulePoSDownload(
	requestStatus engineapi.RequestStatus,
	requestId int,
	hashToDownload common.Hash,
	heightToDownload uint64,
	s *StageState,
	cfg HeadersCfg,
) {
	if requestStatus == engineapi.New {
		cfg.hd.PayloadStatusCh <- privateapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}
	}
	cfg.hd.BeaconRequestList.SetStatus(requestId, engineapi.DataWasMissing)

	if cfg.hd.PosStatus() != headerdownload.Idle {
		log.Trace(fmt.Sprintf("[%s] Postponing PoS download since another one is in progress", s.LogPrefix()), "height", heightToDownload, "hash", hashToDownload)
		return
	}

	log.Info(fmt.Sprintf("[%s] Downloading PoS headers...", s.LogPrefix()), "height", heightToDownload, "hash", hashToDownload, "requestId", requestId)

	cfg.hd.SetPOSSync(true)
	cfg.hd.SetRequestId(requestId)
	cfg.hd.SetHeaderToDownloadPoS(hashToDownload, heightToDownload)

	//nolint
	headerCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	// headerCollector is closed in verifyAndSaveDownloadedPoSHeaders, thus nolint

	cfg.hd.SetHeadersCollector(headerCollector)

	cfg.hd.SetPosStatus(headerdownload.Syncing)
}

func verifyAndSaveDownloadedPoSHeaders(tx kv.RwTx, cfg HeadersCfg, headerInserter *headerdownload.HeaderInserter) error {
	headerLoadFunc := func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		var h types.Header
		if err := rlp.DecodeBytes(value, &h); err != nil {
			return err
		}
		if err := cfg.hd.VerifyHeader(&h); err != nil {
			log.Warn("Verification failed for header", "hash", h.Hash(), "height", h.Number.Uint64(), "err", err)
			return err
		}
		return headerInserter.FeedHeaderPoS(tx, &h, h.Hash())
	}

	err := cfg.hd.HeadersCollector().Load(tx, kv.Headers, headerLoadFunc, etl.TransformArgs{
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})

	if err != nil {
		log.Warn("Removing beacon request due to", "err", err, "requestId", cfg.hd.RequestId())
		cfg.hd.BeaconRequestList.Remove(cfg.hd.RequestId())
	} else {
		log.Info("PoS headers verified and saved", "requestId", cfg.hd.RequestId())
	}

	cfg.hd.HeadersCollector().Close()
	cfg.hd.SetHeadersCollector(nil)
	cfg.hd.SetPosStatus(headerdownload.Idle)

	return err
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
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg, initialCycle); err != nil {
		return err
	}

	var headerProgress uint64
	var err error

	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetPOSSync(false)
	cfg.hd.SetFetchingNew(true)
	defer cfg.hd.SetFetchingNew(false)
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

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, initialCycle bool) error {
	if cfg.snapshots == nil {
		return nil
	}

	if initialCycle {
		if err := WaitForDownloader(ctx, tx, cfg); err != nil {
			return err
		}
		if err := cfg.snapshots.Reopen(); err != nil {
			return fmt.Errorf("ReopenSegments: %w", err)
		}
		expect := cfg.snapshotHashesCfg.ExpectBlocks
		if cfg.snapshots.SegmentsAvailable() < expect {
			c, err := tx.Cursor(kv.Headers)
			if err != nil {
				return err
			}
			defer c.Close()
			firstK, _, err := c.First()
			if err != nil {
				return err
			}
			c.Close()
			hasInDB := binary.BigEndian.Uint64(firstK)
			if cfg.snapshots.SegmentsAvailable() < hasInDB {
				return fmt.Errorf("not enough snapshots available: snapshots=%d, blockInDB=%d, expect=%d", cfg.snapshots.SegmentsAvailable(), hasInDB, expect)
			} else {
				log.Warn(fmt.Sprintf("not enough snapshots available: %d < %d, but we can re-generate them because DB has historical blocks up to: %d", cfg.snapshots.SegmentsAvailable(), expect, hasInDB))
			}
		}
		if err := cfg.snapshots.Reopen(); err != nil {
			return fmt.Errorf("ReopenIndices: %w", err)
		}

		// Create .idx files
		if cfg.snapshots.IndicesAvailable() < cfg.snapshots.SegmentsAvailable() {
			if !cfg.snapshots.SegmentsReady() {
				return fmt.Errorf("not all snapshot segments are available")
			}

			// wait for Downloader service to download all expected snapshots
			if cfg.snapshots.IndicesAvailable() < cfg.snapshots.SegmentsAvailable() {
				chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
				if err := snapshotsync.BuildIndices(ctx, cfg.snapshots, cfg.snapshotDir, *chainID, cfg.tmpdir, cfg.snapshots.IndicesAvailable(), log.LvlInfo); err != nil {
					return err
				}
			}

			if err := cfg.snapshots.Reopen(); err != nil {
				return fmt.Errorf("ReopenIndices: %w", err)
			}
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

		// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
		ok, err := cfg.snapshots.ViewTxs(cfg.snapshots.BlocksAvailable(), func(sn *snapshotsync.TxnSegment) error {
			lastTxnID := sn.IdxTxnHash.BaseDataID() + uint64(sn.Seg.Count())
			if err := rawdb.ResetSequence(tx, kv.EthTx, lastTxnID+1); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshot not found for block: %d", cfg.snapshots.BlocksAvailable())
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
	var prevBytesCompleted uint64
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Print download progress until all segments are available
Loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if reply, err := cfg.snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else if int(reply.Torrents) < len(snapshotsCfg.Preverified) {
				log.Warn("Downloader has not enough snapshots (yet)")
			} else if reply.Completed {
				break Loop
			} else {
				readBytesPerSec := (reply.BytesCompleted - prevBytesCompleted) / uint64(logInterval.Seconds())
				//result.writeBytesPerSec += (result.bytesWritten - prevStats.bytesWritten) / int64(interval.Seconds())

				readiness := 100 * (float64(reply.BytesCompleted) / float64(reply.BytesTotal))
				log.Info("[Snapshots] download", "progress", fmt.Sprintf("%.2f%%", readiness),
					"download", libcommon.ByteCount(readBytesPerSec)+"/s",
				)
				prevBytesCompleted = reply.BytesCompleted
			}
		}
	}

	if err := tx.Put(kv.DatabaseInfo, []byte(readyKey), []byte{1}); err != nil {
		return err
	}
	return nil
}
