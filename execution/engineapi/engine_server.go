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

package engineapi

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_logs_spammer"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var caplinEnabledLog = "Caplin is enabled, so the engine API cannot be used. for external CL use --externalcl"
var errCaplinEnabled = &rpc.UnsupportedForkError{Message: "caplin is enabled"}

type EngineServer struct {
	blockDownloader *engine_block_downloader.EngineBlockDownloader
	config          *chain.Config
	// Block proposing for proof-of-stake
	proposing bool
	// Block consuming for proof-of-stake
	consuming        atomic.Bool
	test             bool
	caplin           bool // we need to send errors for caplin.
	internalCL       bool // true when any embedded CL is active (suppresses "no CL" warning)
	executionService execmodule.ExecutionModule
	txpool           txpoolproto.TxpoolClient // needed for getBlobs

	chainRW chainreader.ChainReaderWriterEth1
	filters *rpchelper.Filters
	events  *shards.Events
	lock    sync.Mutex
	logger  log.Logger

	engineLogSpamer *engine_logs_spammer.EngineLogsSpammer
	// TODO Remove this on next release
	printPectraBanner bool
	maxReorgDepth     uint64
}

func NewEngineServer(
	logger log.Logger,
	config *chain.Config,
	executionService execmodule.ExecutionModule,
	blockDownloader *engine_block_downloader.EngineBlockDownloader,
	caplin bool,
	internalCL bool,
	proposing bool,
	consuming bool,
	txPool txpoolproto.TxpoolClient,
	fcuTimeout time.Duration,
	maxReorgDepth uint64,
) *EngineServer {
	chainRW := chainreader.NewChainReaderEth1(config, executionService, fcuTimeout)
	srv := &EngineServer{
		logger:            logger,
		config:            config,
		executionService:  executionService,
		blockDownloader:   blockDownloader,
		chainRW:           chainRW,
		proposing:         proposing,
		caplin:            caplin,
		internalCL:        internalCL,
		engineLogSpamer:   engine_logs_spammer.NewEngineLogsSpammer(logger, config),
		printPectraBanner: true,
		txpool:            txPool,
		maxReorgDepth:     maxReorgDepth,
	}

	srv.consuming.Store(consuming)

	return srv
}

func (e *EngineServer) Start(
	ctx context.Context,
	httpConfig *httpcfg.HttpCfg,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
	filters *rpchelper.Filters,
	stateCache kvcache.Cache,
	engineReader rules.EngineReader,
	eth rpchelper.ApiBackend,
	mining txpoolproto.MiningClient,
	events *shards.Events,
) error {
	e.filters = filters
	e.events = events

	var eg errgroup.Group
	if !e.internalCL {
		eg.Go(func() error {
			defer e.logger.Debug("[EngineServer] engine log spammer goroutine terminated")
			e.engineLogSpamer.Start(ctx)
			return nil
		})
	}
	base := jsonrpc.NewBaseApi(filters, stateCache, blockReader, httpConfig.WithDatadir, httpConfig.EvmCallTimeout, engineReader, httpConfig.Dirs, nil, httpConfig.BlockRangeLimit, httpConfig.GetLogsMaxResults)
	ethImpl := jsonrpc.NewEthAPI(base, db, eth, e.txpool, mining, jsonrpc.NewEthApiConfig(httpConfig), e.logger)

	apiList := []rpc.API{
		{
			Namespace: "eth",
			Public:    true,
			Service:   jsonrpc.EthAPI(ethImpl),
			Version:   "1.0",
		}, {
			Namespace: "engine",
			Public:    true,
			Service:   EngineAPI(e),
			Version:   "1.0",
		}}

	if httpConfig.TestingEnabled {
		e.logger.Warn("[EngineServer] testing_ RPC namespace is ENABLED — do not use on production networks")
		apiList = append(apiList, rpc.API{
			Namespace: "testing",
			Public:    false,
			Service:   TestingAPI(NewTestingImpl(e, true)),
			Version:   "1.0",
		})
	}

	eg.Go(func() error {
		defer e.logger.Debug("[EngineServer] engine rpc server goroutine terminated")
		err := cli.StartRpcServerWithJwtAuthentication(ctx, httpConfig, apiList, e.logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.Error("[EngineServer] rpc server background goroutine failed", "err", err)
		}
		return err
	})
	return eg.Wait()
}

func (s *EngineServer) checkWithdrawalsPresence(time uint64, withdrawals types.Withdrawals) error {
	if s.isWithdrawalsPresenceValid(time, withdrawals) {
		return nil
	}
	if !s.config.IsShanghai(time) {
		return &rpc.InvalidParamsError{Message: "withdrawals before Shanghai"}
	}
	return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
}

func (s *EngineServer) isWithdrawalsPresenceValid(time uint64, withdrawals types.Withdrawals) bool {
	if !s.config.IsShanghai(time) {
		return withdrawals == nil
	}
	return withdrawals != nil
}

func (s *EngineServer) checkRequestsPresence(version clparams.StateVersion, executionRequests []hexutil.Bytes) error {
	if version < clparams.ElectraVersion {
		if executionRequests != nil {
			return &rpc.InvalidParamsError{Message: "requests in EngineAPI not supported before Prague"}
		}
	} else if executionRequests == nil {
		return &rpc.InvalidParamsError{Message: "missing requests list"}
	}

	return nil
}

// EngineNewPayload validates and possibly executes payload
func (s *EngineServer) newPayload(ctx context.Context, req *engine_types.ExecutionPayload,
	expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash, executionRequests []hexutil.Bytes, version clparams.StateVersion,
) (*engine_types.PayloadStatus, error) {
	if !s.consuming.Load() {
		return nil, errors.New("engine payload consumption is not enabled")
	}

	if s.caplin {
		s.logger.Crit(caplinEnabledLog)
		return nil, errCaplinEnabled
	}

	s.engineLogSpamer.RecordRequest()
	s.logger.Debug("[NewPayload] processing new request", "blockNum", req.BlockNumber.Uint64(), "blockHash", req.BlockHash, "parentHash", req.ParentHash)
	if len(req.LogsBloom) != types.BloomByteLength {
		return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("invalid logsBloom length: %d", len(req.LogsBloom))}
	}
	var bloom types.Bloom
	copy(bloom[:], req.LogsBloom)

	txs := [][]byte{}
	for _, transaction := range req.Transactions {
		txs = append(txs, transaction)
	}

	header := types.Header{
		ParentHash:  req.ParentHash,
		Coinbase:    req.FeeRecipient,
		Root:        req.StateRoot,
		Bloom:       bloom,
		BaseFee:     uint256.MustFromBig(req.BaseFeePerGas.ToInt()),
		Extra:       req.ExtraData,
		Number:      *uint256.NewInt(req.BlockNumber.Uint64()),
		GasUsed:     uint64(req.GasUsed),
		GasLimit:    uint64(req.GasLimit),
		Time:        uint64(req.Timestamp),
		MixDigest:   req.PrevRandao,
		UncleHash:   empty.UncleHash,
		Difficulty:  *merge.ProofOfStakeDifficulty,
		Nonce:       merge.ProofOfStakeNonce,
		ReceiptHash: req.ReceiptsRoot,
		TxHash:      types.DeriveSha(types.BinaryTransactions(txs)),
	}

	var withdrawals types.Withdrawals
	if version >= clparams.CapellaVersion {
		withdrawals = req.Withdrawals
	}
	if err := s.checkWithdrawalsPresence(header.Time, withdrawals); err != nil {
		return nil, err
	}
	if withdrawals != nil {
		wh := types.DeriveSha(withdrawals)
		header.WithdrawalsHash = &wh
	}

	var requests types.FlatRequests
	if err := s.checkRequestsPresence(version, executionRequests); err != nil {
		return nil, err
	}
	if version >= clparams.ElectraVersion {
		requests = make(types.FlatRequests, 0)
		lastReqType := -1
		for i, r := range executionRequests {
			if len(r) <= 1 || lastReqType >= 0 && int(r[0]) <= lastReqType {
				return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("Invalid Request at index %d", i)}
			}
			lastReqType = int(r[0])
			requests = append(requests, types.FlatRequest{Type: r[0], RequestData: r[1:]})
		}
		rh := requests.Hash()
		header.RequestsHash = rh
	}

	if version <= clparams.CapellaVersion {
		if req.BlobGasUsed != nil {
			return nil, &rpc.InvalidParamsError{Message: "Unexpected pre-cancun blobGasUsed"}
		}
		if req.ExcessBlobGas != nil {
			return nil, &rpc.InvalidParamsError{Message: "Unexpected pre-cancun excessBlobGas"}
		}
	}

	if version >= clparams.DenebVersion {
		if req.BlobGasUsed == nil || req.ExcessBlobGas == nil || parentBeaconBlockRoot == nil {
			return nil, &rpc.InvalidParamsError{Message: "blobGasUsed/excessBlobGas/beaconRoot missing"}
		}
		header.BlobGasUsed = (*uint64)(req.BlobGasUsed)
		header.ExcessBlobGas = (*uint64)(req.ExcessBlobGas)
		header.ParentBeaconBlockRoot = parentBeaconBlockRoot
	}

	var blockAccessList types.BlockAccessList
	var blockAccessListBytes []byte
	var err error
	if version >= clparams.GloasVersion {
		if req.BlockAccessList == nil {
			return nil, &rpc.InvalidParamsError{Message: "blockAccessList missing"}
		}
		if len(req.BlockAccessList) == 0 {
			blockAccessList = nil
			header.BlockAccessListHash = &empty.BlockAccessListHash
		} else {
			blockAccessList, err = types.DecodeBlockAccessListBytes(req.BlockAccessList)
			if err != nil {
				s.logger.Debug("[NewPayload] failed to decode blockAccessList", "err", err, "raw", hex.EncodeToString(req.BlockAccessList))
				return &engine_types.PayloadStatus{
					Status:          engine_types.InvalidStatus,
					ValidationError: engine_types.NewStringifiedErrorFromString(fmt.Sprintf("invalid block access list decode: %v", err)),
				}, nil
			}
			if err := blockAccessList.Validate(); err != nil {
				return &engine_types.PayloadStatus{
					Status:          engine_types.InvalidStatus,
					ValidationError: engine_types.NewStringifiedErrorFromString(fmt.Sprintf("invalid block access list validate: %v", err)),
				}, nil
			}
			hash := crypto.HashData(req.BlockAccessList)
			header.BlockAccessListHash = &hash
			blockAccessListBytes = req.BlockAccessList
		}
		if req.SlotNumber != nil {
			slotNumber := uint64(*req.SlotNumber)
			header.SlotNumber = &slotNumber
		} else {
			return nil, &rpc.InvalidParamsError{Message: "slotNumber missing"}
		}
	}

	log.Debug(fmt.Sprintf("bal from header: %s", blockAccessList.DebugString()))

	if (!s.config.IsCancun(header.Time) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(header.Time) && version < clparams.DenebVersion) ||
		(!s.config.IsPrague(header.Time) && version >= clparams.ElectraVersion) ||
		(s.config.IsPrague(header.Time) && version < clparams.ElectraVersion) || // osaka has no new newPayload method
		(!s.config.IsAmsterdam(header.Time) && version >= clparams.GloasVersion) ||
		(s.config.IsAmsterdam(header.Time) && version < clparams.GloasVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	blockHash := req.BlockHash
	if header.Hash() != blockHash {
		s.logger.Error(
			"[NewPayload] invalid block hash",
			"stated", blockHash,
			"actual", header.Hash(),
			"parentBeaconBlockRoot", parentBeaconBlockRoot,
			"requests", executionRequests,
		)
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			ValidationError: engine_types.NewStringifiedErrorFromString("invalid block hash"),
		}, nil
	}

	for _, txn := range req.Transactions {
		if types.TypedTransactionMarshalledAsRlpString(txn) {
			s.logger.Warn("[NewPayload] typed txn marshalled as RLP string", "txn", common.Bytes2Hex(txn))
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString("typed txn marshalled as RLP string"),
			}, nil
		}
	}

	transactions, err := types.DecodeTransactions(txs)
	if err != nil {
		s.logger.Warn("[NewPayload] failed to decode transactions", "err", err)
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			ValidationError: engine_types.NewStringifiedError(err),
		}, nil
	}

	if version >= clparams.DenebVersion {
		err := misc.ValidateBlobs(req.BlobGasUsed.Uint64(), s.config.GetMaxBlobGasPerBlock(header.Time), s.config.GetMaxBlobsPerBlock(header.Time), expectedBlobHashes, &transactions)
		if errors.Is(err, misc.ErrNilBlobHashes) {
			return nil, &rpc.InvalidParamsError{Message: "nil blob hashes array"}
		}
		if errors.Is(err, misc.ErrMaxBlobGasUsed) {
			bad, latestValidHash := s.blockDownloader.IsBadHeader(req.ParentHash)
			if !bad {
				latestValidHash = req.ParentHash
			}
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString(err.Error()),
				LatestValidHash: &latestValidHash,
			}, nil
		}
		if errors.Is(err, misc.ErrMismatchBlobHashes) || errors.Is(err, misc.ErrInvalidVersionedHash) {
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString(err.Error()),
			}, nil
		}
	}

	possibleStatus, err := s.getQuickPayloadStatusIfPossible(ctx, blockHash, uint64(req.BlockNumber), header.ParentHash, nil, true)
	if err != nil {
		return nil, err
	}
	if possibleStatus != nil {
		s.logger.Debug("[NewPayload] got quick payload status", "payloadStatus", possibleStatus)
		return possibleStatus, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Debug("[NewPayload] sending block", "height", header.Number, "hash", blockHash)
	block := types.NewBlockFromStorage(blockHash, &header, transactions, nil /* uncles */, withdrawals)
	payloadStatus, err := s.HandleNewPayload(ctx, "NewPayload", block, expectedBlobHashes, blockAccessListBytes)
	if err != nil {
		if errors.Is(err, rules.ErrInvalidBlock) {
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedError(err),
			}, nil
		}
		return nil, err
	}
	s.logger.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	if version == clparams.ElectraVersion && s.printPectraBanner && payloadStatus.Status == engine_types.ValidStatus {
		s.printPectraBanner = false
		log.Info(engine_helpers.PectraBanner)
	}

	return payloadStatus, nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EngineServer) getQuickPayloadStatusIfPossible(ctx context.Context, blockHash common.Hash, blockNumber uint64, parentHash common.Hash, forkchoiceMessage *engine_types.ForkChoiceState, newPayload bool) (*engine_types.PayloadStatus, error) {
	// Determine which prefix to use for logs
	var prefix string
	if newPayload {
		prefix = "NewPayload"
	} else {
		prefix = "ForkChoiceUpdated"
	}
	if s.config.TerminalTotalDifficulty == nil {
		s.logger.Error(fmt.Sprintf("[%s] not a proof-of-stake chain", prefix))
		return nil, errors.New("not a proof-of-stake chain")
	}

	headHash, finalizedHash, safeHash, err := s.chainRW.GetForkChoice(ctx)
	if err != nil {
		return nil, err
	}

	// Some Consensus layer clients sometimes sends us repeated FCUs and make Erigon print a gazillion logs.
	// E.G teku sometimes will end up spamming fcu on the terminal block if it has not synced to that point.
	if forkchoiceMessage != nil &&
		forkchoiceMessage.FinalizedBlockHash == finalizedHash &&
		forkchoiceMessage.HeadHash == headHash &&
		forkchoiceMessage.SafeBlockHash == safeHash {
		return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
	}

	header := s.chainRW.GetHeaderByHash(ctx, blockHash)

	// Retrieve parent and total difficulty.
	var parent *types.Header
	var td *big.Int
	if newPayload {
		parent = s.chainRW.GetHeaderByHash(ctx, parentHash)
		td = s.chainRW.GetTd(ctx, parentHash, blockNumber-1)
	} else {
		td = s.chainRW.GetTd(ctx, blockHash, blockNumber)
	}

	if td != nil && td.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		s.logger.Warn(fmt.Sprintf("[%s] Beacon Chain request before TTD", prefix), "hash", blockHash)
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &common.Hash{}, ValidationError: engine_types.NewStringifiedErrorFromString("Beacon Chain request before TTD")}, nil
	}

	var isCanonical bool
	if header != nil {
		isCanonical, err = s.chainRW.IsCanonicalHash(ctx, blockHash)
	}
	if err != nil {
		return nil, err
	}

	if newPayload && parent != nil && blockNumber != parent.Number.Uint64()+1 {
		s.logger.Warn(fmt.Sprintf("[%s] Invalid block number", prefix), "headerNumber", blockNumber, "parentNumber", parent.Number.Uint64())
		s.blockDownloader.ReportBadHeader(blockHash, parent.Hash())
		parentHash := parent.Hash()
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			LatestValidHash: &parentHash,
			ValidationError: engine_types.NewStringifiedErrorFromString("invalid block number"),
		}, nil
	}
	// Check if we already determined if the hash is attributed to a previously received invalid header.
	bad, lastValidHash := s.blockDownloader.IsBadHeader(blockHash)
	if bad {
		s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash)
	} else if newPayload {
		bad, lastValidHash = s.blockDownloader.IsBadHeader(parentHash)
		if bad {
			s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash, "parentHash", parentHash)
		}
	}
	if bad {
		s.blockDownloader.ReportBadHeader(blockHash, lastValidHash)
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &lastValidHash, ValidationError: engine_types.NewStringifiedErrorFromString("previously known bad block")}, nil
	}

	currentHeader := s.chainRW.CurrentHeader(ctx)
	// If header is already validated or has a missing parent, you can either return VALID or SYNCING.
	if newPayload {
		if header != nil && isCanonical {
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}
		if shouldWait, _ := waitForResponse(50*time.Millisecond, func() (bool, error) {
			if parent == nil {
				parent = s.chainRW.GetHeaderByHash(ctx, parentHash)
			}
			return parent == nil && s.blockDownloader.Status() == engine_block_downloader.Syncing, nil
		}); shouldWait {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	} else {
		if shouldWait, _ := waitForResponse(50*time.Millisecond, func() (bool, error) {
			return header == nil && s.blockDownloader.Status() == engine_block_downloader.Syncing, nil
		}); shouldWait {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS stuff", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		// For NewPayload: if the block is already canonical and known, return VALID immediately.
		// For FCU: we must always go through HandleForkChoice so the EL head actually moves,
		// even if the requested head is a canonical ancestor (e.g. fork choice head regression).
		if newPayload && currentHeader != nil && header != nil && isCanonical {
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}
	}
	waitingForExecutionReady, err := waitForResponse(500*time.Millisecond, func() (bool, error) {
		isReady, err := s.chainRW.Ready(ctx)
		return !isReady, err
	})
	if err != nil {
		return nil, err
	}
	if waitingForExecutionReady {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	return nil, nil
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EngineServer) getPayload(ctx context.Context, payloadId uint64, version clparams.StateVersion) (*engine_types.GetPayloadResponse, error) {
	if s.caplin {
		s.logger.Crit("[NewPayload] caplin is enabled")
		return nil, errCaplinEnabled
	}
	s.engineLogSpamer.RecordRequest()

	if !s.proposing {
		return nil, errors.New("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, errors.New("not a proof-of-stake chain")
	}

	s.logger.Debug("[GetPayload] acquiring lock")
	s.lock.Lock()
	defer s.lock.Unlock()
	s.logger.Debug("[GetPayload] lock acquired")
	var assembled execmodule.AssembledBlockResult
	var err error

	execBusy, err := waitForResponse(time.Duration(s.config.SecondsPerSlot())*time.Second, func() (bool, error) {
		assembled, err = s.executionService.GetAssembledBlock(ctx, payloadId)
		if err != nil {
			return false, err
		}
		return assembled.Busy, nil
	})

	if err != nil {
		return nil, err
	}
	if execBusy {
		s.logger.Warn("Cannot build payload, execution is busy", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}
	if assembled.Block == nil {
		s.logger.Warn("Payload not stored", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}

	br := assembled.Block
	block := br.Block
	header := block.Header()

	if header == nil {
		s.logger.Warn("Payload build failed (nil header)", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}

	ts := header.Time
	if (!s.config.IsCancun(ts) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(ts) && version < clparams.DenebVersion) ||
		(!s.config.IsPrague(ts) && version >= clparams.ElectraVersion) ||
		(s.config.IsPrague(ts) && version < clparams.ElectraVersion) ||
		(!s.config.IsOsaka(ts) && version >= clparams.FuluVersion) ||
		(s.config.IsOsaka(ts) && version < clparams.FuluVersion) ||
		(!s.config.IsAmsterdam(ts) && version >= clparams.GloasVersion) ||
		(s.config.IsAmsterdam(ts) && version < clparams.GloasVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	payload, err := assembledBlockToPayloadResponse(br, assembled.BlockValue, version)
	if err != nil {
		return nil, err
	}

	if version == clparams.FuluVersion {
		if payload.BlobsBundle == nil {
			payload.BlobsBundle = &engine_types.BlobsBundle{
				Commitments: make([]hexutil.Bytes, 0),
				Blobs:       make([]hexutil.Bytes, 0),
				Proofs:      make([]hexutil.Bytes, 0),
			}
		}
		if len(payload.BlobsBundle.Commitments) != len(payload.BlobsBundle.Blobs) || len(payload.BlobsBundle.Proofs) != len(payload.BlobsBundle.Blobs)*int(params.CellsPerExtBlob) {
			return nil, fmt.Errorf("built invalid blobsBundle len(blobs)=%d len(commitments)=%d len(proofs)=%d", len(payload.BlobsBundle.Blobs), len(payload.BlobsBundle.Commitments), len(payload.BlobsBundle.Proofs))
		}
	}
	return payload, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EngineServer) forkchoiceUpdated(ctx context.Context, forkchoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes, version clparams.StateVersion,
) (*engine_types.ForkChoiceUpdatedResponse, error) {
	if !s.consuming.Load() {
		return nil, errors.New("engine payload consumption is not enabled")
	}

	if s.caplin {
		s.logger.Crit("[NewPayload] caplin is enabled")
		return nil, errCaplinEnabled
	}

	s.engineLogSpamer.RecordRequest()
	newReqLogInfoArgs := []any{"head", forkchoiceState.HeadHash}
	if payloadAttributes != nil {
		newReqLogInfoArgs = append(newReqLogInfoArgs, "parentBeaconBlockRoot", payloadAttributes.ParentBeaconBlockRoot)
	}

	s.logger.Debug("[ForkChoiceUpdated] processing new request", newReqLogInfoArgs...)
	status, err := s.getQuickPayloadStatusIfPossible(ctx, forkchoiceState.HeadHash, 0, common.Hash{}, forkchoiceState, false)
	if err != nil {
		return nil, err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if status == nil {
		fcuStart := time.Now()
		s.logger.Debug("[ForkChoiceUpdated] calling HandleForkChoice", "head", forkchoiceState.HeadHash)
		status, err = s.HandleForkChoice(ctx, "ForkChoiceUpdated", forkchoiceState)
		s.logger.Debug("[ForkChoiceUpdated] HandleForkChoice done", "elapsed", time.Since(fcuStart))
		if err != nil {
			if errors.Is(err, rules.ErrInvalidBlock) {
				return &engine_types.ForkChoiceUpdatedResponse{
					PayloadStatus: &engine_types.PayloadStatus{
						Status:          engine_types.InvalidStatus,
						ValidationError: engine_types.NewStringifiedError(err),
					},
				}, nil
			}
			return nil, err
		}
		s.logger.Debug("[ForkChoiceUpdated] HandleForkChoice done", "status", status.Status)

		if status.CriticalError != nil {
			return nil, status.CriticalError
		}
	} else {
		s.logger.Debug("[ForkChoiceUpdated] got quick payload status", "status", status.Status)
	}

	// No need for payload building
	if payloadAttributes == nil || status.Status != engine_types.ValidStatus {
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: status}, nil
	}

	if version < clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot != nil {
		return nil, &engine_helpers.InvalidPayloadAttributesErr // Unexpected Beacon Root
	}
	if version >= clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot == nil {
		return nil, &engine_helpers.InvalidPayloadAttributesErr // Beacon Root missing
	}

	timestamp := uint64(payloadAttributes.Timestamp)
	if !s.config.IsCancun(timestamp) && version >= clparams.DenebVersion { // V3 before cancun
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}
	if s.config.IsCancun(timestamp) && version < clparams.DenebVersion { // Not V3 after cancun
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}
	if version >= clparams.CapellaVersion && !s.isWithdrawalsPresenceValid(timestamp, payloadAttributes.Withdrawals) {
		return nil, &engine_helpers.InvalidPayloadAttributesErr
	}
	if version >= clparams.GloasVersion && payloadAttributes.SlotNumber == nil {
		return nil, &engine_helpers.InvalidPayloadAttributesErr // SlotNumber required for Glamsterdam (EIP-7843)
	}

	if !s.proposing {
		return nil, errors.New("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	headHeader := s.chainRW.GetHeaderByHash(ctx, forkchoiceState.HeadHash)
	if headHeader == nil && s.filters != nil {
		if sd := s.filters.LatestSD(); sd != nil {
			if overlay := sd.BlockOverlay(); overlay != nil {
				headHeader, _ = rawdb.ReadHeaderByHash(overlay, forkchoiceState.HeadHash)
			}
		}
	}
	if headHeader == nil {
		return nil, fmt.Errorf("head header not found for hash %x", forkchoiceState.HeadHash)
	}

	if headHeader.Time >= timestamp {
		s.logger.Debug("[ForkChoiceUpdated] payload time lte head time", "head", headHeader.Time, "payload", timestamp)
		return nil, &engine_helpers.InvalidPayloadAttributesErr
	}

	assembleParams := &builder.Parameters{
		ParentHash:            forkchoiceState.HeadHash,
		Timestamp:             timestamp,
		PrevRandao:            payloadAttributes.PrevRandao,
		SuggestedFeeRecipient: payloadAttributes.SuggestedFeeRecipient,
		SlotNumber:            (*uint64)(payloadAttributes.SlotNumber),
	}

	if version >= clparams.CapellaVersion {
		assembleParams.Withdrawals = payloadAttributes.Withdrawals
	}

	if version >= clparams.DenebVersion {
		assembleParams.ParentBeaconBlockRoot = payloadAttributes.ParentBeaconBlockRoot
	}

	var assembled execmodule.AssembleBlockResult
	// Wait for the execution service to be ready to assemble a block. Wait a full slot duration (12 seconds) to ensure that the execution service is not busy.
	// Blocks are important and 0.5 seconds is not enough to wait for the execution service to be ready.
	execBusy, err := waitForResponse(time.Duration(s.config.SecondsPerSlot())*time.Second, func() (bool, error) {
		assembled, err = s.executionService.AssembleBlock(ctx, assembleParams)
		if err != nil {
			return false, err
		}
		return assembled.Busy, nil
	})
	if err != nil {
		return nil, err
	}
	if execBusy {
		s.logger.Warn("[ForkChoiceUpdated] Execution Service busy, could not fulfil Assemble Block request", "parentHash", forkchoiceState.HeadHash)
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, PayloadId: nil}, nil
	}
	return &engine_types.ForkChoiceUpdatedResponse{
		PayloadStatus: &engine_types.PayloadStatus{
			Status:          engine_types.ValidStatus,
			LatestValidHash: &forkchoiceState.HeadHash,
		},
		PayloadId: engine_types.ConvertPayloadId(assembled.PayloadID),
	}, nil
}

func (s *EngineServer) getPayloadBodiesByHash(ctx context.Context, request []common.Hash) ([]*engine_types.ExecutionPayloadBody, error) {
	if len(request) > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}
	s.engineLogSpamer.RecordRequest()

	bodies, err := s.chainRW.GetBodiesByHashes(ctx, request)
	if err != nil {
		return nil, err
	}

	resp := make([]*engine_types.ExecutionPayloadBody, len(bodies))
	for i, body := range bodies {
		resp[i] = extractPayloadBodyFromBody(body)
	}
	return resp, nil
}

func extractPayloadBodyFromBody(body *types.RawBody) *engine_types.ExecutionPayloadBody {
	if body == nil {
		return nil
	}

	bdTxs := make([]hexutil.Bytes, len(body.Transactions))
	for idx := range body.Transactions {
		bdTxs[idx] = body.Transactions[idx]
	}

	ret := &engine_types.ExecutionPayloadBody{Transactions: bdTxs, Withdrawals: body.Withdrawals}
	return ret
}

func (s *EngineServer) getPayloadBodiesByRange(ctx context.Context, start, count uint64) ([]*engine_types.ExecutionPayloadBody, error) {
	if start == 0 || count == 0 {
		return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("invalid start or count, start: %v count: %v", start, count)}
	}
	if count > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}
	bodies, err := s.chainRW.GetBodiesByRange(ctx, start, count)
	if err != nil {
		return nil, err
	}

	resp := make([]*engine_types.ExecutionPayloadBody, len(bodies))
	for idx, body := range bodies {
		resp[idx] = extractPayloadBodyFromBody(body)
	}
	return resp, nil
}

func compareCapabilities(from []string, to []string) []string {
	result := make([]string, 0)
	for _, f := range from {
		if !slices.Contains(to, f) {
			result = append(result, f)
		}
	}

	return result
}

func (e *EngineServer) HandleNewPayload(
	ctx context.Context,
	logPrefix string,
	block *types.Block,
	versionedHashes []common.Hash,
	blockAccessListBytes []byte,
) (*engine_types.PayloadStatus, error) {
	e.engineLogSpamer.RecordRequest()

	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	e.logger.Info(fmt.Sprintf("[%s] Handling new payload", logPrefix), "height", headerNumber, "hash", headerHash)

	if headerNumber == 0 {
		return nil, errors.New("new payload cannot be used for genesis")
	}

	currentHeader := e.chainRW.CurrentHeader(ctx)
	var currentHeadNumber *uint64
	if currentHeader != nil {
		currentHeadNumber = new(uint64)
		*currentHeadNumber = currentHeader.Number.Uint64()
	}
	parent := e.chainRW.GetHeader(ctx, header.ParentHash, headerNumber-1)
	if parent == nil {
		e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download parent", logPrefix), "height", headerNumber, "hash", headerHash, "parentHash", header.ParentHash)
		if e.test {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if !e.blockDownloader.StartDownloading(header.ParentHash, block, engine_block_downloader.NewPayloadTrigger) {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if currentHeadNumber != nil {
			// wait for the slot duration for full download
			waitTime := time.Duration(e.config.SecondsPerSlot()) * time.Second
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			var respondSyncing bool
			if _, _ = waitForResponse(waitTime, func() (bool, error) {
				status := e.blockDownloader.Status()
				respondSyncing = status != engine_block_downloader.Synced
				// no point in waiting if the downloader is no longer syncing (e.g. it's dropped the download request)
				return status == engine_block_downloader.Syncing, nil
			}); respondSyncing {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
			status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
			if err != nil {
				missingBlkHash, isMissingChainErr := execmodule.GetBlockHashFromMissingSegmentError(err)
				if isMissingChainErr {
					e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
					if e.test {
						return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
					}
					if e.blockDownloader.StartDownloading(missingBlkHash, block, engine_block_downloader.SegmentRecoveryTrigger) {
						e.logger.Warn(fmt.Sprintf("[%s] New payload: need to recover missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
					}
					return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
				}
				return nil, err
			}

			if status == execmodule.ExecutionStatusBusy || status == execmodule.ExecutionStatusTooFarAway {
				e.logger.Debug(fmt.Sprintf("[%s] New payload: Client is still syncing", logPrefix))
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			} else {
				return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &latestValidHash}, nil
			}
		} else {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	}

	var accessLists map[common.Hash][]byte
	if len(blockAccessListBytes) > 0 || block.BlockAccessListHash() != nil {
		accessLists = map[common.Hash][]byte{block.Hash(): blockAccessListBytes}
	}
	if err := e.chainRW.InsertBlocksAndWaitWithAccessLists(ctx, []*types.Block{block}, accessLists); err != nil {
		if errors.Is(err, types.ErrBlockExceedsMaxRlpSize) {
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedError(err),
			}, nil
		}
		return nil, err
	}

	if math.AbsoluteDifference(*currentHeadNumber, headerNumber) >= e.maxReorgDepth {
		return &engine_types.PayloadStatus{Status: engine_types.AcceptedStatus}, nil
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, validationErr, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
		missingBlkHash, isMissingChainErr := execmodule.GetBlockHashFromMissingSegmentError(err)
		if isMissingChainErr {
			e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
			if e.test {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
			if e.blockDownloader.StartDownloading(missingBlkHash, block, engine_block_downloader.SegmentRecoveryTrigger) {
				e.logger.Warn(fmt.Sprintf("[%s] New payload: need to recover missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
			}
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
		return nil, err
	}

	if status == execmodule.ExecutionStatusBadBlock {
		e.blockDownloader.ReportBadHeader(block.Hash(), latestValidHash)
	}

	resp := &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}
	if validationErr != nil {
		resp.ValidationError = engine_types.NewStringifiedErrorFromString(*validationErr)
	}

	return resp, nil
}

func convertGrpcStatusToEngineStatus(status execmodule.ExecutionStatus) engine_types.EngineStatus {
	switch status {
	case execmodule.ExecutionStatusSuccess:
		return engine_types.ValidStatus
	case execmodule.ExecutionStatusMissingSegment:
		return engine_types.AcceptedStatus
	case execmodule.ExecutionStatusTooFarAway:
		return engine_types.AcceptedStatus
	case execmodule.ExecutionStatusBadBlock:
		return engine_types.InvalidStatus
	case execmodule.ExecutionStatusBusy:
		return engine_types.SyncingStatus
	}
	panic(fmt.Sprintf("unhandled execution status: %d", status))
}

// assembledBlockToPayloadResponse converts a native assembled block to an engine-API payload response.
func assembledBlockToPayloadResponse(br *types.BlockWithReceipts, blockValue *uint256.Int, version clparams.StateVersion) (*engine_types.GetPayloadResponse, error) {
	block := br.Block
	header := block.Header()

	encodedTxs, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}
	txs := make([]hexutil.Bytes, len(encodedTxs))
	for i, tx := range encodedTxs {
		txs[i] = tx
	}

	bloom := header.Bloom
	ep := &engine_types.ExecutionPayload{
		ParentHash:    header.ParentHash,
		FeeRecipient:  header.Coinbase,
		StateRoot:     header.Root,
		ReceiptsRoot:  header.ReceiptHash,
		LogsBloom:     bloom[:],
		PrevRandao:    header.MixDigest,
		BlockNumber:   hexutil.Uint64(header.Number.Uint64()),
		GasLimit:      hexutil.Uint64(header.GasLimit),
		GasUsed:       hexutil.Uint64(header.GasUsed),
		Timestamp:     hexutil.Uint64(header.Time),
		ExtraData:     header.Extra,
		BaseFeePerGas: (*hexutil.Big)(header.BaseFee.ToBig()),
		BlockHash:     block.Hash(),
		Transactions:  txs,
	}
	if block.Withdrawals() != nil {
		ep.Withdrawals = block.Withdrawals()
	}
	if header.BlobGasUsed != nil {
		bgu := hexutil.Uint64(*header.BlobGasUsed)
		ep.BlobGasUsed = &bgu
	}
	if header.ExcessBlobGas != nil {
		ebg := hexutil.Uint64(*header.ExcessBlobGas)
		ep.ExcessBlobGas = &ebg
	}
	if header.SlotNumber != nil {
		sn := hexutil.Uint64(*header.SlotNumber)
		ep.SlotNumber = &sn
	}
	if header.BlockAccessListHash != nil && br.BlockAccessList != nil {
		encoded, encErr := types.EncodeBlockAccessListBytes(br.BlockAccessList)
		if encErr == nil {
			ep.BlockAccessList = encoded
		}
	}

	blobsBundle, err := engine_types.BlobsBundleFromTransactions(block.Transactions())
	if err != nil {
		return nil, err
	}

	var executionRequests []hexutil.Bytes
	if version >= clparams.ElectraVersion {
		if br.Requests == nil {
			return nil, errors.New("missing execution requests for Electra+ payload")
		}
		executionRequests = make([]hexutil.Bytes, 0, len(br.Requests))
		for _, r := range br.Requests {
			executionRequests = append(executionRequests, r.Encode())
		}
	}

	return &engine_types.GetPayloadResponse{
		ExecutionPayload:  ep,
		BlockValue:        (*hexutil.Big)(blockValue.ToBig()),
		BlobsBundle:       blobsBundle,
		ExecutionRequests: executionRequests,
	}, nil
}

func (e *EngineServer) HandleForkChoice(
	ctx context.Context,
	logPrefix string,
	forkChoice *engine_types.ForkChoiceState,
) (*engine_types.PayloadStatus, error) {
	e.engineLogSpamer.RecordRequest()

	headerHash := forkChoice.HeadHash

	e.logger.Debug(fmt.Sprintf("[%s] Handling fork choice", logPrefix), "headerHash", headerHash)
	headerNumber, err := e.chainRW.HeaderNumber(ctx, headerHash)
	if err != nil {
		return nil, err
	}

	// We do not have header, download.
	if headerNumber == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(headerHash, nil, engine_block_downloader.FcuTrigger)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header := e.chainRW.GetHeader(ctx, headerHash, *headerNumber)
	if header == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(headerHash, nil, engine_block_downloader.FcuTrigger)
		}

		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Call forkchoice here
	status, validationErr, latestValidHash, err := e.chainRW.UpdateForkChoice(ctx, forkChoice.HeadHash, forkChoice.SafeBlockHash, forkChoice.FinalizedBlockHash)
	if err != nil {
		return nil, err
	}
	if status == execmodule.ExecutionStatusInvalidForkchoice {
		return nil, &engine_helpers.InvalidForkchoiceStateErr
	}
	if status == execmodule.ExecutionStatusBusy {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}
	if status == execmodule.ExecutionStatusBadBlock {
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, ValidationError: engine_types.NewStringifiedErrorFromString("Invalid chain after execution")}, nil
	}
	payloadStatus := &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}

	if validationErr != nil {
		payloadStatus.ValidationError = engine_types.NewStringifiedErrorFromString(*validationErr)
	}
	return payloadStatus, nil
}

func (e *EngineServer) SetConsuming(consuming bool) {
	e.consuming.Store(consuming)
}

func (e *EngineServer) getBlobs(ctx context.Context, blobHashes []common.Hash, version clparams.StateVersion) (any, error) {
	if len(blobHashes) > 128 {
		return nil, &engine_helpers.TooLargeRequestErr
	}
	req := &txpoolproto.GetBlobsRequest{BlobHashes: make([]*typesproto.H256, len(blobHashes))}
	for i := range blobHashes {
		req.BlobHashes[i] = gointerfaces.ConvertHashToH256(blobHashes[i])
	}
	res, err := e.txpool.GetBlobs(ctx, req)
	if err != nil {
		return nil, err
	}
	logLine := []string{}

	if len(blobHashes) != len(res.BlobsWithProofs) {
		log.Warn("[GetBlobs] txpool returned unexpected number of blobs and proofs in response, returning nil blobs list")
		return nil, nil
	}

	switch version {
	case clparams.FuluVersion: // GetBlobsV3
		ret := make([]*engine_types.BlobAndProofV2, len(blobHashes))
		for i, bwp := range res.BlobsWithProofs {
			logHead := fmt.Sprintf("\n%x: ", blobHashes[i])
			if len(bwp.Blob) == 0 {
				logLine = append(logLine, logHead, "nil")
			} else if len(bwp.Proofs) != int(params.CellsPerExtBlob) {
				logLine = append(logLine, logHead, fmt.Sprintf("pre-Fusaka proofs, len(proof)=%d", len(bwp.Proofs)))
			} else {
				ret[i] = &engine_types.BlobAndProofV2{Blob: bwp.Blob, CellProofs: make([]hexutil.Bytes, params.CellsPerExtBlob)}
				for c := range params.CellsPerExtBlob {
					ret[i].CellProofs[c] = bwp.Proofs[c]
				}
				logLine = append(logLine, logHead, fmt.Sprintf("OK, len(blob)=%d", len(bwp.Blob)))
			}
		}
		e.logger.Debug("[GetBlobsV3]", "Responses", logLine)
		return ret, nil
	case clparams.ElectraVersion: // GetBlobsV2
		ret := make([]*engine_types.BlobAndProofV2, len(blobHashes))
		for i, bwp := range res.BlobsWithProofs {
			logHead := fmt.Sprintf("\n%x: ", blobHashes[i])
			if len(bwp.Blob) == 0 {
				// engine_getBlobsV2 MUST return null in case of any missing or older version blobs
				ret = nil
				logLine = append(logLine, logHead, "nil")
				break
			} else if len(bwp.Proofs) != int(params.CellsPerExtBlob) {
				// engine_getBlobsV2 MUST return null in case of any missing or older version blobs
				ret = nil
				logLine = append(logLine, logHead, fmt.Sprintf("pre-Fusaka proofs, len(proof)=%d", len(bwp.Proofs)))
				break
			} else {
				ret[i] = &engine_types.BlobAndProofV2{Blob: bwp.Blob, CellProofs: make([]hexutil.Bytes, params.CellsPerExtBlob)}
				for c := range params.CellsPerExtBlob {
					ret[i].CellProofs[c] = bwp.Proofs[c]
				}
				logLine = append(logLine, logHead, fmt.Sprintf("OK, len(blob)=%d", len(bwp.Blob)))
			}
		}
		e.logger.Debug("[GetBlobsV2]", "Responses", logLine)
		return ret, nil
	case clparams.DenebVersion: // GetBlobsV1
		ret := make([]*engine_types.BlobAndProofV1, len(blobHashes))
		for i, bwp := range res.BlobsWithProofs {
			logHead := fmt.Sprintf("\n%x: ", blobHashes[i])
			if len(bwp.Blob) == 0 {
				logLine = append(logLine, logHead, "nil")
			} else if len(bwp.Proofs) != 1 {
				logLine = append(logLine, logHead, fmt.Sprintf("post-Fusaka proofs, len(proof)=%d", len(bwp.Proofs)))
			} else {
				ret[i] = &engine_types.BlobAndProofV1{Blob: bwp.Blob, Proof: bwp.Proofs[0]}
				logLine = append(logLine, logHead, fmt.Sprintf("OK, len(blob)=%d len(proof)=%d ", len(bwp.Blob), len(bwp.Proofs[0])))
			}
		}
		e.logger.Debug("[GetBlobsV1]", "Responses", logLine)
		return ret, nil
	default:
		return nil, nil
	}
}

func waitForResponse(maxWait time.Duration, waitCondnF func() (bool, error)) (bool, error) {
	shouldWait, err := waitCondnF()
	if err != nil || !shouldWait {
		return false, err
	}
	checkInterval := 10 * time.Millisecond
	maxChecks := int64(maxWait) / int64(checkInterval)
	for i := int64(0); i < maxChecks; i++ {
		time.Sleep(checkInterval)
		shouldWait, err = waitCondnF()
		if err != nil || !shouldWait {
			return shouldWait, err
		}
	}
	return true, nil
}
