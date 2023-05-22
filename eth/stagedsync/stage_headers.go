package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// The number of blocks we should be able to re-org sub-second on commodity hardware.
// See https://hackmd.io/TdJtNs0dS56q-In8h-ShSg
const ShortPoSReorgThresholdBlocks = 10

type HeadersCfg struct {
	db                kv.RwDB
	hd                *headerdownload.HeaderDownload
	bodyDownload      *bodydownload.BodyDownload
	chainConfig       chain.Config
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) ([64]byte, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	tmpdir            string

	snapshots     *snapshotsync.RoSnapshots
	blockReader   services.FullBlockReader
	blockWriter   *blockio.BlockWriter
	forkValidator *engineapi.ForkValidator
	notifications *shards.Notifications
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	bodyDownload *bodydownload.BodyDownload,
	chainConfig chain.Config,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) ([64]byte, bool),
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
	noP2PDiscovery bool,
	snapshots *snapshotsync.RoSnapshots,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	tmpdir string,
	notifications *shards.Notifications,
	forkValidator *engineapi.ForkValidator) HeadersCfg {
	return HeadersCfg{
		db:                db,
		hd:                headerDownload,
		bodyDownload:      bodyDownload,
		chainConfig:       chainConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		penalize:          penalize,
		batchSize:         batchSize,
		tmpdir:            tmpdir,
		noP2PDiscovery:    noP2PDiscovery,
		snapshots:         snapshots,
		blockReader:       blockReader,
		blockWriter:       blockWriter,
		forkValidator:     forkValidator,
		notifications:     notifications,
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
	logger log.Logger,
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
	if initialCycle && cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		if err := cfg.hd.AddHeadersFromSnapshot(tx, cfg.snapshots.BlocksAvailable(), cfg.blockReader); err != nil {
			return err
		}
	}

	var preProgress uint64
	if s == nil {
		preProgress = 0
	} else {
		preProgress = s.BlockNumber
	}

	notBor := cfg.chainConfig.Bor == nil

	unsettledForkChoice, headHeight := cfg.hd.GetUnsettledForkChoice()
	if notBor && unsettledForkChoice != nil { // some work left to do after unwind
		return finishHandlingForkChoice(unsettledForkChoice, headHeight, s, tx, cfg, useExternalTx, logger)
	}

	transitionedToPoS := cfg.chainConfig.TerminalTotalDifficultyPassed
	if notBor && !transitionedToPoS {
		var err error
		transitionedToPoS, err = rawdb.Transitioned(tx, preProgress, cfg.chainConfig.TerminalTotalDifficulty)
		if err != nil {
			return err
		}
		if transitionedToPoS {
			cfg.hd.SetFirstPoSHeight(preProgress)
		}
	}

	if transitionedToPoS {
		libcommon.SafeClose(cfg.hd.QuitPoWMining)
		return HeadersPOS(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx, preProgress, logger)
	} else {
		return HeadersPOW(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx, logger)
	}
}

// HeadersPOS processes Proof-of-Stake requests (newPayload, forkchoiceUpdated).
// It also saves PoS headers downloaded by (*HeaderDownload)StartPoSDownloader into the DB.
func HeadersPOS(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool,
	useExternalTx bool,
	preProgress uint64,
	logger log.Logger,
) error {
	if initialCycle {
		// Let execution and other stages to finish before waiting for CL, but only if other stages aren't ahead.
		// Specifically, this allows to execute snapshot blocks before waiting for CL.
		if execProgress, err := s.ExecutionAt(tx); err != nil {
			return err
		} else if s.BlockNumber >= execProgress {
			return nil
		}
	}

	cfg.hd.SetPOSSync(true)
	syncing := cfg.hd.PosStatus() != headerdownload.Idle
	if !syncing {
		logger.Info(fmt.Sprintf("[%s] Waiting for Consensus Layer...", s.LogPrefix()))
	}
	interrupt, requestId, requestWithStatus := cfg.hd.BeaconRequestList.WaitForRequest(syncing, test)

	cfg.hd.SetHeaderReader(&ChainReaderImpl{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})
	headerInserter := headerdownload.NewHeaderInserter(s.LogPrefix(), nil, s.BlockNumber, cfg.blockReader, cfg.blockWriter)

	interrupted, err := handleInterrupt(interrupt, cfg, tx, headerInserter, useExternalTx, logger)
	if err != nil {
		return err
	}

	if interrupted {
		return nil
	}

	if requestWithStatus == nil {
		logger.Warn(fmt.Sprintf("[%s] Nil beacon request. Should only happen in tests", s.LogPrefix()))
		return nil
	}

	request := requestWithStatus.Message
	requestStatus := requestWithStatus.Status

	// Decide what kind of action we need to take place
	forkChoiceMessage, forkChoiceInsteadOfNewPayload := request.(*engineapi.ForkChoiceMessage)
	cfg.hd.ClearPendingPayloadHash()
	cfg.hd.SetPendingPayloadStatus(nil)

	var payloadStatus *engineapi.PayloadStatus
	if forkChoiceInsteadOfNewPayload {
		payloadStatus, err = startHandlingForkChoice(forkChoiceMessage, requestStatus, requestId, s, u, ctx, tx, cfg, test, headerInserter, preProgress, logger)
	} else {
		payloadMessage := request.(*types.Block)
		payloadStatus, err = handleNewPayload(payloadMessage, requestStatus, requestId, s, ctx, tx, cfg, test, headerInserter, logger)
	}

	if err != nil {
		if requestStatus == engineapi.New {
			cfg.hd.PayloadStatusCh <- engineapi.PayloadStatus{CriticalError: err}
		}
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	if requestStatus == engineapi.New && payloadStatus != nil {
		if payloadStatus.Status == remote.EngineStatus_SYNCING || payloadStatus.Status == remote.EngineStatus_ACCEPTED || !useExternalTx {
			cfg.hd.PayloadStatusCh <- *payloadStatus
		} else {
			// Let the stage loop run to the end so that the transaction is committed prior to replying to CL
			cfg.hd.SetPendingPayloadStatus(payloadStatus)
		}
	}

	return nil
}

func writeForkChoiceHashes(
	forkChoice *engineapi.ForkChoiceMessage,
	s *StageState,
	tx kv.RwTx,
	cfg HeadersCfg,
	logger log.Logger,
) (bool, error) {
	if forkChoice.SafeBlockHash != (libcommon.Hash{}) {
		safeIsCanonical, err := rawdb.IsCanonicalHash(tx, forkChoice.SafeBlockHash)
		if err != nil {
			return false, err
		}
		if !safeIsCanonical {
			logger.Warn(fmt.Sprintf("[%s] Non-canonical SafeBlockHash", s.LogPrefix()), "forkChoice", forkChoice)
			return false, nil
		}
	}

	if forkChoice.FinalizedBlockHash != (libcommon.Hash{}) {
		finalizedIsCanonical, err := rawdb.IsCanonicalHash(tx, forkChoice.FinalizedBlockHash)
		if err != nil {
			return false, err
		}
		if !finalizedIsCanonical {
			logger.Warn(fmt.Sprintf("[%s] Non-canonical FinalizedBlockHash", s.LogPrefix()), "forkChoice", forkChoice)
			return false, nil
		}
	}

	rawdb.WriteForkchoiceHead(tx, forkChoice.HeadBlockHash)
	if forkChoice.SafeBlockHash != (libcommon.Hash{}) {
		rawdb.WriteForkchoiceSafe(tx, forkChoice.SafeBlockHash)
	}
	if forkChoice.FinalizedBlockHash != (libcommon.Hash{}) {
		rawdb.WriteForkchoiceFinalized(tx, forkChoice.FinalizedBlockHash)
	}

	return true, nil
}

func startHandlingForkChoice(
	forkChoice *engineapi.ForkChoiceMessage,
	requestStatus engineapi.RequestStatus,
	requestId int,
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	test bool,
	headerInserter *headerdownload.HeaderInserter,
	preProgress uint64,
	logger log.Logger,
) (*engineapi.PayloadStatus, error) {
	defer cfg.forkValidator.ClearWithUnwind(tx, cfg.notifications.Accumulator, cfg.notifications.StateChangesConsumer)
	headerHash := forkChoice.HeadBlockHash
	logger.Debug(fmt.Sprintf("[%s] Handling fork choice", s.LogPrefix()), "headerHash", headerHash)

	canonical, err := rawdb.IsCanonicalHash(tx, headerHash)
	if err != nil {
		logger.Warn(fmt.Sprintf("[%s] Fork choice err (IsCanonicalHash)", s.LogPrefix()), "err", err)
		cfg.hd.BeaconRequestList.Remove(requestId)
		return nil, err
	}
	if canonical {
		headerNumber := rawdb.ReadHeaderNumber(tx, headerHash)
		ihProgress, err := s.IntermediateHashesAt(tx)
		if err != nil {
			logger.Warn(fmt.Sprintf("[%s] Fork choice err (IntermediateHashesAt)", s.LogPrefix()), "err", err)
			cfg.hd.BeaconRequestList.Remove(requestId)
			return nil, err
		}
		if ihProgress >= *headerNumber {
			// FCU points to a canonical and fully validated block in the past.
			// Treat it as a no-op to avoid unnecessary unwind of block execution and other stages
			// with subsequent rewind on a newer FCU.
			logger.Debug(fmt.Sprintf("[%s] Fork choice no-op", s.LogPrefix()))
			cfg.hd.BeaconRequestList.Remove(requestId)
			canonical, err = writeForkChoiceHashes(forkChoice, s, tx, cfg, logger)
			if err != nil {
				logger.Warn(fmt.Sprintf("[%s] Fork choice err", s.LogPrefix()), "err", err)
				return nil, err
			}
			if canonical {
				return &engineapi.PayloadStatus{
					Status:          remote.EngineStatus_VALID,
					LatestValidHash: headerHash,
				}, nil
			} else {
				return &engineapi.PayloadStatus{
					CriticalError: &privateapi.InvalidForkchoiceStateErr,
				}, nil
			}
		}
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header, err := cfg.blockReader.HeaderByHash(ctx, tx, headerHash)
	if err != nil {
		logger.Warn(fmt.Sprintf("[%s] Fork choice err (reading header by hash %x)", s.LogPrefix(), headerHash), "err", err)
		cfg.hd.BeaconRequestList.Remove(requestId)
		return nil, err
	}

	if header == nil {
		logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", s.LogPrefix(), headerHash))
		if test {
			cfg.hd.BeaconRequestList.Remove(requestId)
		} else {
			schedulePoSDownload(requestId, headerHash, 0 /* header height is unknown, setting to 0 */, headerHash, s, cfg, logger)
		}
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}

	cfg.hd.BeaconRequestList.Remove(requestId)

	headerNumber := header.Number.Uint64()

	if headerHash == cfg.forkValidator.ExtendingForkHeadHash() {
		logger.Info(fmt.Sprintf("[%s] Fork choice update: flushing in-memory state (built by previous newPayload)", s.LogPrefix()))
		if err := cfg.forkValidator.FlushExtendingFork(tx, cfg.notifications.Accumulator); err != nil {
			return nil, err
		}
		canonical, err := writeForkChoiceHashes(forkChoice, s, tx, cfg, logger)
		if err != nil {
			log.Warn(fmt.Sprintf("[%s] Fork choice err", s.LogPrefix()), "err", err)
			return nil, err
		}
		if canonical {
			cfg.hd.SetPendingPayloadHash(headerHash)
			return nil, nil
		} else {
			return &engineapi.PayloadStatus{
				CriticalError: &privateapi.InvalidForkchoiceStateErr,
			}, nil
		}
	}

	forkingPoint, err := forkingPoint(ctx, tx, headerInserter, cfg.blockReader, header)
	if err != nil {
		return nil, err
	}
	if forkingPoint < preProgress {

		logger.Info(fmt.Sprintf("[%s] Fork choice: re-org", s.LogPrefix()), "goal", headerNumber, "from", preProgress, "unwind to", forkingPoint)

		if requestStatus == engineapi.New {
			if headerNumber-forkingPoint <= ShortPoSReorgThresholdBlocks {
				// TODO(yperbasis): what if some bodies are missing and we have to download them?
				cfg.hd.SetPendingPayloadHash(headerHash)
			} else {
				cfg.hd.PayloadStatusCh <- engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}
			}
		}

		u.UnwindTo(forkingPoint, libcommon.Hash{})

		cfg.hd.SetUnsettledForkChoice(forkChoice, headerNumber)
	} else {
		// Extend canonical chain by the new header
		logger.Info(fmt.Sprintf("[%s] Fork choice: chain extension", s.LogPrefix()), "from", preProgress, "to", headerNumber)
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		if err = fixCanonicalChain(s.LogPrefix(), logEvery, headerNumber, headerHash, tx, cfg.blockReader, logger); err != nil {
			return nil, err
		}
		if err = rawdb.WriteHeadHeaderHash(tx, headerHash); err != nil {
			return nil, err
		}

		canonical, err := writeForkChoiceHashes(forkChoice, s, tx, cfg, logger)
		if err != nil {
			return nil, err
		}

		if err := s.Update(tx, headerNumber); err != nil {
			return nil, err
		}

		if canonical {
			return &engineapi.PayloadStatus{
				Status:          remote.EngineStatus_VALID,
				LatestValidHash: headerHash,
			}, nil
		} else {
			return &engineapi.PayloadStatus{
				CriticalError: &privateapi.InvalidForkchoiceStateErr,
			}, nil
		}
	}

	return nil, nil
}

func finishHandlingForkChoice(
	forkChoice *engineapi.ForkChoiceMessage,
	headHeight uint64,
	s *StageState,
	tx kv.RwTx,
	cfg HeadersCfg,
	useExternalTx bool,
	logger log.Logger,
) error {
	logger.Info(fmt.Sprintf("[%s] Unsettled forkchoice after unwind", s.LogPrefix()), "height", headHeight, "forkchoice", forkChoice)

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if err := fixCanonicalChain(s.LogPrefix(), logEvery, headHeight, forkChoice.HeadBlockHash, tx, cfg.blockReader, logger); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(tx, forkChoice.HeadBlockHash); err != nil {
		return err
	}

	canonical, err := writeForkChoiceHashes(forkChoice, s, tx, cfg, logger)
	if err != nil {
		return err
	}

	if err := s.Update(tx, headHeight); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	if !canonical {
		if cfg.hd.GetPendingPayloadHash() != (libcommon.Hash{}) {
			cfg.hd.PayloadStatusCh <- engineapi.PayloadStatus{
				CriticalError: &privateapi.InvalidForkchoiceStateErr,
			}
		}
		cfg.hd.ClearPendingPayloadHash()
	}

	cfg.hd.ClearUnsettledForkChoice()
	return nil
}

func handleNewPayload(
	block *types.Block,
	requestStatus engineapi.RequestStatus,
	requestId int,
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	test bool,
	headerInserter *headerdownload.HeaderInserter,
	logger log.Logger,
) (*engineapi.PayloadStatus, error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	logger.Info(fmt.Sprintf("[%s] Handling new payload", s.LogPrefix()), "height", headerNumber, "hash", headerHash)

	parent, err := cfg.blockReader.HeaderByHash(ctx, tx, header.ParentHash)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		logger.Debug(fmt.Sprintf("[%s] New payload: need to download parent", s.LogPrefix()), "height", headerNumber, "hash", headerHash, "parentHash", header.ParentHash)
		if test {
			cfg.hd.BeaconRequestList.Remove(requestId)
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}
		if !schedulePoSDownload(requestId, header.ParentHash, headerNumber-1, headerHash /* downloaderTip */, s, cfg, logger) {
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}
		currentHeadNumber := rawdb.ReadCurrentBlockNumber(tx)
		if currentHeadNumber != nil && math.AbsoluteDifference(*currentHeadNumber, headerNumber) < 32 {
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			success := false
			for i := 0; i < 10; i++ {
				time.Sleep(10 * time.Millisecond)
				if cfg.hd.PosStatus() == headerdownload.Synced {
					success = true
					break
				}
			}
			if success {
				// If we downloaded the headers in time, then save them and proceed with the new header
				saveDownloadedPoSHeaders(tx, cfg, headerInserter, true /* validate */, logger)
			} else {
				return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
			}
		} else {
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}
	}

	cfg.hd.BeaconRequestList.Remove(requestId)

	logger.Debug(fmt.Sprintf("[%s] New payload begin verification", s.LogPrefix()))
	response, success, err := verifyAndSaveNewPoSHeader(requestStatus, s, ctx, tx, cfg, block, headerInserter, logger)
	logger.Debug(fmt.Sprintf("[%s] New payload verification ended", s.LogPrefix()), "success", success, "err", err)
	if err != nil || !success {
		return response, err
	}

	if cfg.bodyDownload != nil {
		cfg.bodyDownload.AddToPrefetch(header, block.RawBody())
	}

	return response, nil
}

func verifyAndSaveNewPoSHeader(
	requestStatus engineapi.RequestStatus,
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	block *types.Block,
	headerInserter *headerdownload.HeaderInserter,
	logger log.Logger,
) (response *engineapi.PayloadStatus, success bool, err error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	bad, lastValidHash := cfg.hd.IsBadHeaderPoS(headerHash)
	if bad {
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: lastValidHash}, false, nil
	}

	if verificationErr := cfg.hd.VerifyHeader(header); verificationErr != nil {
		logger.Warn("Verification failed for header", "hash", headerHash, "height", headerNumber, "err", verificationErr)
		cfg.hd.ReportBadHeaderPoS(headerHash, header.ParentHash)
		return &engineapi.PayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			LatestValidHash: header.ParentHash,
			ValidationError: verificationErr,
		}, false, nil
	}

	currentHeadHash := rawdb.ReadHeadHeaderHash(tx)

	extendingHash := cfg.forkValidator.ExtendingForkHeadHash()
	extendCanonical := (extendingHash == libcommon.Hash{} && header.ParentHash == currentHeadHash) || extendingHash == header.ParentHash
	status, latestValidHash, validationError, criticalError := cfg.forkValidator.ValidatePayload(tx, header, block.RawBody(), extendCanonical)
	if criticalError != nil {
		return nil, false, criticalError
	}
	success = validationError == nil
	if !success {
		logger.Warn("Validation failed for header", "hash", headerHash, "height", headerNumber, "err", validationError)
		cfg.hd.ReportBadHeaderPoS(headerHash, latestValidHash)
	} else if err := headerInserter.FeedHeaderPoS(tx, header, headerHash); err != nil {
		return nil, false, err
	}
	return &engineapi.PayloadStatus{
		Status:          status,
		LatestValidHash: latestValidHash,
		ValidationError: validationError,
	}, success, nil
}

func schedulePoSDownload(
	requestId int,
	hashToDownload libcommon.Hash,
	heightToDownload uint64,
	downloaderTip libcommon.Hash,
	s *StageState,
	cfg HeadersCfg,
	logger log.Logger,
) bool {
	cfg.hd.BeaconRequestList.SetStatus(requestId, engineapi.DataWasMissing)

	if cfg.hd.PosStatus() != headerdownload.Idle {
		logger.Info(fmt.Sprintf("[%s] Postponing PoS download since another one is in progress", s.LogPrefix()), "height", heightToDownload, "hash", hashToDownload)
		return false
	}

	if heightToDownload == 0 {
		logger.Info(fmt.Sprintf("[%s] Downloading PoS headers...", s.LogPrefix()), "height", "unknown", "hash", hashToDownload, "requestId", requestId)
	} else {
		logger.Info(fmt.Sprintf("[%s] Downloading PoS headers...", s.LogPrefix()), "height", heightToDownload, "hash", hashToDownload, "requestId", requestId)
	}

	cfg.hd.SetRequestId(requestId)
	cfg.hd.SetPoSDownloaderTip(downloaderTip)
	cfg.hd.SetHeaderToDownloadPoS(hashToDownload, heightToDownload)
	cfg.hd.SetPOSSync(true) // This needs to be called after SetHeaderToDownloadPOS because SetHeaderToDownloadPOS sets `posAnchor` member field which is used by ProcessHeadersPOS

	//nolint
	headerCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	// headerCollector is closed in saveDownloadedPoSHeaders, thus nolint

	cfg.hd.SetHeadersCollector(headerCollector)

	cfg.hd.SetPosStatus(headerdownload.Syncing)

	return true
}

func saveDownloadedPoSHeaders(tx kv.RwTx, cfg HeadersCfg, headerInserter *headerdownload.HeaderInserter, validate bool, logger log.Logger) {
	var lastValidHash libcommon.Hash
	var badChainError error
	var foundPow bool

	headerLoadFunc := func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		var h types.Header
		// no header to process
		if value == nil {
			return nil
		}
		if err := rlp.DecodeBytes(value, &h); err != nil {
			return err
		}
		if badChainError != nil {
			cfg.hd.ReportBadHeaderPoS(h.Hash(), lastValidHash)
			return nil
		}
		lastValidHash = h.ParentHash
		if err := cfg.hd.VerifyHeader(&h); err != nil {
			logger.Warn("Verification failed for header", "hash", h.Hash(), "height", h.Number.Uint64(), "err", err)
			badChainError = err
			cfg.hd.ReportBadHeaderPoS(h.Hash(), lastValidHash)
			return nil
		}
		// If we are in PoW range then block validation is not required anymore.
		if foundPow {
			return headerInserter.FeedHeaderPoS(tx, &h, h.Hash())
		}

		foundPow = h.Difficulty.Cmp(libcommon.Big0) != 0
		if foundPow {
			return headerInserter.FeedHeaderPoS(tx, &h, h.Hash())
		}
		// Validate state if possible (bodies will be retrieved through body download)
		if validate {
			_, _, validationError, criticalError := cfg.forkValidator.ValidatePayload(tx, &h, nil, false)
			if criticalError != nil {
				return criticalError
			}
			if validationError != nil {
				badChainError = validationError
				cfg.hd.ReportBadHeaderPoS(h.Hash(), lastValidHash)
				return nil
			}
		}

		return headerInserter.FeedHeaderPoS(tx, &h, h.Hash())
	}

	err := cfg.hd.HeadersCollector().Load(tx, kv.Headers, headerLoadFunc, etl.TransformArgs{
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})

	if err != nil || badChainError != nil {
		if err == nil {
			err = badChainError
		}
		logger.Warn("Removing beacon request due to", "err", err, "requestId", cfg.hd.RequestId())
		cfg.hd.BeaconRequestList.Remove(cfg.hd.RequestId())
		cfg.hd.ReportBadHeaderPoS(cfg.hd.PoSDownloaderTip(), lastValidHash)
	} else {
		logger.Info("PoS headers verified and saved", "requestId", cfg.hd.RequestId(), "fork head", lastValidHash)
	}

	cfg.hd.HeadersCollector().Close()
	cfg.hd.SetHeadersCollector(nil)
	cfg.hd.SetPosStatus(headerdownload.Idle)
}

func forkingPoint(
	ctx context.Context,
	tx kv.RwTx,
	headerInserter *headerdownload.HeaderInserter,
	headerReader services.HeaderReader,
	header *types.Header,
) (uint64, error) {
	headerNumber := header.Number.Uint64()
	if headerNumber == 0 {
		return 0, nil
	}
	parent, err := headerReader.Header(ctx, tx, header.ParentHash, headerNumber-1)
	if err != nil {
		return 0, err
	}
	return headerInserter.ForkingPoint(tx, header, parent)
}

func handleInterrupt(interrupt engineapi.Interrupt, cfg HeadersCfg, tx kv.RwTx, headerInserter *headerdownload.HeaderInserter, useExternalTx bool, logger log.Logger) (bool, error) {
	if interrupt != engineapi.None {
		if interrupt == engineapi.Stopping {
			close(cfg.hd.ShutdownCh)
			return false, fmt.Errorf("server is stopping")
		}
		if interrupt == engineapi.Synced && cfg.hd.HeadersCollector() != nil {
			saveDownloadedPoSHeaders(tx, cfg, headerInserter, false /* validate */, logger)
		}
		if !useExternalTx {
			return true, tx.Commit()
		}
		return true, nil
	}
	return false, nil
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
	logger log.Logger,
) error {
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
	if hash == (libcommon.Hash{}) {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx, cfg.blockReader, logger); err != nil {
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

	logger.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", headerProgress)

	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	if localTd == nil {
		return fmt.Errorf("localTD is nil: %d, %x", headerProgress, hash)
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress, cfg.blockReader, cfg.blockWriter)
	cfg.hd.SetHeaderReader(&ChainReaderImpl{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	stopped := false
	var noProgressCounter uint = 0
	prevProgress := headerProgress
	var wasProgress bool
	var lastSkeletonTime time.Time
	var peer [64]byte
	var sentToPeer bool
Loop:
	for !stopped {

		transitionedToPoS, err := rawdb.Transitioned(tx, headerProgress, cfg.chainConfig.TerminalTotalDifficulty)
		if err != nil {
			return err
		}
		if transitionedToPoS {
			if err := s.Update(tx, headerProgress); err != nil {
				return err
			}
			break
		}

		sentToPeer = false
		currentTime := time.Now()
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				cfg.hd.UpdateStats(req, false /* skeleton */, peer)
				cfg.hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
			}
		}
		if len(penalties) > 0 {
			cfg.penalize(ctx, penalties)
		}
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && sentToPeer && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					cfg.hd.UpdateStats(req, false /* skeleton */, peer)
					cfg.hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
				}
			}
			if len(penalties) > 0 {
				cfg.penalize(ctx, penalties)
			}
			maxRequests--
		}

		// Send skeleton request if required
		if time.Since(lastSkeletonTime) > 1*time.Second {
			req = cfg.hd.RequestSkeleton()
			if req != nil {
				peer, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					cfg.hd.UpdateStats(req, true /* skeleton */, peer)
					lastSkeletonTime = time.Now()
				}
			}
		}
		// Load headers into the database
		var inSync bool
		if inSync, err = cfg.hd.InsertHeaders(headerInserter.NewFeedHeaderFunc(tx, cfg.blockReader), cfg.chainConfig.TerminalTotalDifficulty, logPrefix, logEvery.C, uint64(currentTime.Unix())); err != nil {
			return err
		}

		if test {
			announces := cfg.hd.GrabAnnounces()
			if len(announces) > 0 {
				cfg.announceNewHashes(ctx, announces)
			}
		}

		if headerInserter.BestHeaderChanged() { // We do not break unless there best header changed
			noProgressCounter = 0
			wasProgress = true
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
			logProgressHeaders(logPrefix, prevProgress, progress, logger)
			stats := cfg.hd.ExtractStats()
			if prevProgress == progress {
				noProgressCounter++
			} else {
				noProgressCounter = 0 // Reset, there was progress
			}
			if noProgressCounter >= 5 {
				logger.Info("Req/resp stats", "req", stats.Requests, "reqMin", stats.ReqMinBlock, "reqMax", stats.ReqMaxBlock,
					"skel", stats.SkeletonRequests, "skelMin", stats.SkeletonReqMinBlock, "skelMax", stats.SkeletonReqMaxBlock,
					"resp", stats.Responses, "respMin", stats.RespMinBlock, "respMax", stats.RespMaxBlock, "dups", stats.Duplicates)
				cfg.hd.LogAnchorState()
				if wasProgress {
					logger.Warn("Looks like chain is not progressing, moving to the next stage")
					break Loop
				}
			}
			prevProgress = progress
		case <-timer.C:
			logger.Trace("RequestQueueTime (header) ticked")
		case <-cfg.hd.DeliveryNotify:
			logger.Trace("headerLoop woken up by the incoming request")
		}
		timer.Stop()
	}
	if headerInserter.Unwind() {
		u.UnwindTo(headerInserter.UnwindPoint(), libcommon.Hash{})
	}
	if headerInserter.GetHighest() != 0 {
		if !headerInserter.Unwind() {
			if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader, logger); err != nil {
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
	logger.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest inserted", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)))

	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash libcommon.Hash, tx kv.StatelessRwTx, headerReader services.FullBlockReader, logger log.Logger) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch libcommon.Hash
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
			logger.Info(fmt.Sprintf("[%s] write canonical markers", logPrefix), "ancestor", ancestorHeight, "hash", ancestorHash)
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
	badBlock := u.BadBlock != (libcommon.Hash{})
	if badBlock {
		cfg.hd.ReportBadHeader(u.BadBlock)
		// Mark all descendants of bad block as bad too
		headerCursor, cErr := tx.Cursor(kv.Headers)
		if cErr != nil {
			return cErr
		}
		defer headerCursor.Close()
		var k, v []byte
		for k, v, err = headerCursor.Seek(hexutility.EncodeTs(u.UnwindPoint + 1)); err == nil && k != nil; k, v, err = headerCursor.Next() {
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
	if err := rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, false /* deleteHeaders */); err != nil {
		return err
	}
	if badBlock {
		var maxTd big.Int
		var maxHash libcommon.Hash
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
				var hash libcommon.Hash
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
		/* TODO(yperbasis): Is it safe?
		if err := rawdb.TruncateTd(tx, u.UnwindPoint+1); err != nil {
			return err
		}
		*/
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

func logProgressHeaders(logPrefix string, prev, now uint64, logger log.Logger) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	if speed == 0 {
		logger.Info(fmt.Sprintf("[%s] No block headers to write in this log period", logPrefix), "block number", now)
		return now
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info(fmt.Sprintf("[%s] Wrote block headers", logPrefix),
		"number", now,
		"blk/second", speed,
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys))

	return now
}

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Getter
	blockReader services.FullBlockReader
}

func NewChainReaderImpl(config *chain.Config, tx kv.Getter, blockReader services.FullBlockReader) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader}
}

func (cr ChainReaderImpl) Config() *chain.Config        { return cr.config }
func (cr ChainReaderImpl) CurrentHeader() *types.Header { panic("") }
func (cr ChainReaderImpl) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr ChainReaderImpl) GetHeaderByHash(hash libcommon.Hash) *types.Header {
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
func (cr ChainReaderImpl) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
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
