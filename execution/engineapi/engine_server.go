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
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_logs_spammer"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/eth1"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
)

var caplinEnabledLog = "Caplin is enabled, so the engine API cannot be used. for external CL use --externalcl"
var errCaplinEnabled = &rpc.UnsupportedForkError{Message: "caplin is enabled"}

type EngineServer struct {
	hd              *headerdownload.HeaderDownload
	blockDownloader *engine_block_downloader.EngineBlockDownloader
	config          *chain.Config
	// Block proposing for proof-of-stake
	proposing bool
	// Block consuming for proof-of-stake
	consuming        atomic.Bool
	test             bool
	caplin           bool // we need to send errors for caplin.
	executionService execution.ExecutionClient
	txpool           txpool.TxpoolClient // needed for getBlobs

	chainRW eth1_chain_reader.ChainReaderWriterEth1
	lock    sync.Mutex
	logger  log.Logger

	engineLogSpamer *engine_logs_spammer.EngineLogsSpammer
	// TODO Remove this on next release
	printPectraBanner bool
}

const fcuTimeout = 1000 // according to mathematics: 1000 millisecods = 1 second

func NewEngineServer(logger log.Logger, config *chain.Config, executionService execution.ExecutionClient,
	hd *headerdownload.HeaderDownload,
	blockDownloader *engine_block_downloader.EngineBlockDownloader, caplin, test, proposing, consuming bool) *EngineServer {
	chainRW := eth1_chain_reader.NewChainReaderEth1(config, executionService, fcuTimeout)
	srv := &EngineServer{
		logger:            logger,
		config:            config,
		executionService:  executionService,
		blockDownloader:   blockDownloader,
		chainRW:           chainRW,
		proposing:         proposing,
		hd:                hd,
		caplin:            caplin,
		engineLogSpamer:   engine_logs_spammer.NewEngineLogsSpammer(logger, config),
		printPectraBanner: true,
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
	engineReader consensus.EngineReader,
	eth rpchelper.ApiBackend,
	txPool txpool.TxpoolClient,
	mining txpool.MiningClient,
) {
	if !e.caplin {
		e.engineLogSpamer.Start(ctx)
	}
	base := jsonrpc.NewBaseApi(filters, stateCache, blockReader, httpConfig.WithDatadir, httpConfig.EvmCallTimeout, engineReader, httpConfig.Dirs, nil)
	ethImpl := jsonrpc.NewEthAPI(base, db, eth, txPool, mining, httpConfig.Gascap, httpConfig.Feecap, httpConfig.ReturnDataLimit, httpConfig.AllowUnprotectedTxs, httpConfig.MaxGetProofRewindBlockCount, httpConfig.WebsocketSubscribeLogsChannelSize, e.logger)
	e.txpool = txPool

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

	if err := cli.StartRpcServerWithJwtAuthentication(ctx, httpConfig, apiList, e.logger); err != nil {
		e.logger.Error(err.Error())
	}

	if e.blockDownloader != nil {
		go func() {
			err := e.blockDownloader.Run(ctx)
			if err != nil {
				e.logger.Error("[EngineBlockDownloader] background goroutine failed", "err", err)
			}
		}()
	}
}

func (s *EngineServer) checkWithdrawalsPresence(time uint64, withdrawals types.Withdrawals) error {
	if !s.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before Shanghai"}
	}
	if s.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
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
		BaseFee:     (*big.Int)(req.BaseFeePerGas),
		Extra:       req.ExtraData,
		Number:      big.NewInt(0).SetUint64(req.BlockNumber.Uint64()),
		GasUsed:     uint64(req.GasUsed),
		GasLimit:    uint64(req.GasLimit),
		Time:        uint64(req.Timestamp),
		MixDigest:   req.PrevRandao,
		UncleHash:   empty.UncleHash,
		Difficulty:  merge.ProofOfStakeDifficulty,
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

	if (!s.config.IsCancun(header.Time) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(header.Time) && version < clparams.DenebVersion) ||
		(!s.config.IsPrague(header.Time) && version >= clparams.ElectraVersion) ||
		(s.config.IsPrague(header.Time) && version < clparams.ElectraVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	blockHash := req.BlockHash
	if header.Hash() != blockHash {
		s.logger.Error("[NewPayload] invalid block hash", "stated", blockHash, "actual", header.Hash(),
			"payload", req, "parentBeaconBlockRoot", parentBeaconBlockRoot, "requests", executionRequests)
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
		checkMaxBlobsPerTxn := version >= clparams.FuluVersion
		err := ethutils.ValidateBlobs(req.BlobGasUsed.Uint64(), s.config.GetMaxBlobGasPerBlock(header.Time), s.config.GetMaxBlobsPerBlock(header.Time), expectedBlobHashes, &transactions, checkMaxBlobsPerTxn)
		if errors.Is(err, ethutils.ErrNilBlobHashes) {
			return nil, &rpc.InvalidParamsError{Message: "nil blob hashes array"}
		}
		if errors.Is(err, ethutils.ErrMaxBlobGasUsed) || errors.Is(err, ethutils.ErrTooManyBlobs) {
			bad, latestValidHash := s.hd.IsBadHeaderPoS(req.ParentHash)
			if !bad {
				latestValidHash = req.ParentHash
			}
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString(err.Error()),
				LatestValidHash: &latestValidHash,
			}, nil
		}
		if errors.Is(err, ethutils.ErrMismatchBlobHashes) || errors.Is(err, ethutils.ErrInvalidVersiondHash) {
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
		return possibleStatus, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Debug("[NewPayload] sending block", "height", header.Number, "hash", blockHash)
	block := types.NewBlockFromStorage(blockHash, &header, transactions, nil /* uncles */, withdrawals)

	payloadStatus, err := s.HandleNewPayload(ctx, "NewPayload", block, expectedBlobHashes)
	if err != nil {
		if errors.Is(err, consensus.ErrInvalidBlock) {
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

	if s.hd == nil {
		return nil, errors.New("headerdownload is nil")
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
		s.hd.ReportBadHeaderPoS(blockHash, parent.Hash())
		parentHash := parent.Hash()
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			LatestValidHash: &parentHash,
			ValidationError: engine_types.NewStringifiedErrorFromString("invalid block number"),
		}, nil
	}
	// Check if we already determined if the hash is attributed to a previously received invalid header.
	bad, lastValidHash := s.hd.IsBadHeaderPoS(blockHash)
	if bad {
		s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash)
	} else if newPayload {
		bad, lastValidHash = s.hd.IsBadHeaderPoS(parentHash)
		if bad {
			s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash, "parentHash", parentHash)
		}
	}
	if bad {
		s.hd.ReportBadHeaderPoS(blockHash, lastValidHash)
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &lastValidHash, ValidationError: engine_types.NewStringifiedErrorFromString("previously known bad block")}, nil
	}

	currentHeader := s.chainRW.CurrentHeader(ctx)
	// If header is already validated or has a missing parent, you can either return VALID or SYNCING.
	if newPayload {
		if header != nil && isCanonical {
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}
		if shouldWait, _ := waitForStuff(50*time.Millisecond, func() (bool, error) {
			return parent == nil && s.blockDownloader.Status() == engine_block_downloader.Syncing, nil
		}); shouldWait {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	} else {
		if shouldWait, _ := waitForStuff(50*time.Millisecond, func() (bool, error) {
			return header == nil && s.blockDownloader.Status() == engine_block_downloader.Syncing, nil
		}); shouldWait {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS stuff", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		// We add the extra restriction blockHash != headHash for the FCU case of canonicalHash == blockHash
		// because otherwise (when FCU points to the head) we want go to stage headers
		// so that it calls writeForkChoiceHashes.
		if currentHeader != nil && blockHash != currentHeader.Hash() && header != nil && isCanonical {
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}
	}
	waitingForExecutionReady, err := waitForStuff(500*time.Millisecond, func() (bool, error) {
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
	resp, err := s.executionService.GetAssembledBlock(ctx, &execution.GetAssembledBlockRequest{
		Id: payloadId,
	})
	if err != nil {
		return nil, err
	}
	if resp.Busy {
		s.logger.Warn("Cannot build payload, execution is busy", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}
	// If the service is busy or there is no data for the given id then respond accordingly.
	if resp.Data == nil {
		s.logger.Warn("Payload not stored", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr

	}

	data := resp.Data
	var executionRequests []hexutil.Bytes
	if version >= clparams.ElectraVersion {
		executionRequests = make([]hexutil.Bytes, 0)
		for _, r := range data.Requests.Requests {
			executionRequests = append(executionRequests, r)
		}
	}

	ts := data.ExecutionPayload.Timestamp
	if (!s.config.IsCancun(ts) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(ts) && version < clparams.DenebVersion) ||
		(!s.config.IsPrague(ts) && version >= clparams.ElectraVersion) ||
		(s.config.IsPrague(ts) && version < clparams.ElectraVersion) ||
		(!s.config.IsOsaka(ts) && version >= clparams.FuluVersion) ||
		(s.config.IsOsaka(ts) && version < clparams.FuluVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	payload := &engine_types.GetPayloadResponse{
		ExecutionPayload:  engine_types.ConvertPayloadFromRpc(data.ExecutionPayload),
		BlockValue:        (*hexutil.Big)(gointerfaces.ConvertH256ToUint256Int(data.BlockValue).ToBig()),
		BlobsBundle:       engine_types.ConvertBlobsFromRpc(data.BlobsBundle),
		ExecutionRequests: executionRequests,
	}

	if version == clparams.FuluVersion {
		if payload.BlobsBundle == nil {
			payload.BlobsBundle = &engine_types.BlobsBundleV1{
				Commitments: make([]hexutil.Bytes, 0),
				Blobs:       make([]hexutil.Bytes, 0),
				Proofs:      make([]hexutil.Bytes, 0),
			}
		}
		if len(payload.BlobsBundle.Commitments) != len(payload.BlobsBundle.Blobs) || len(payload.BlobsBundle.Proofs) != len(payload.BlobsBundle.Blobs)*int(params.CellsPerExtBlob) {
			return nil, errors.New(fmt.Sprintf("built invalid blobsBundle len(proofs)=%d", len(payload.BlobsBundle.Proofs)))
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

	status, err := s.getQuickPayloadStatusIfPossible(ctx, forkchoiceState.HeadHash, 0, common.Hash{}, forkchoiceState, false)
	if err != nil {
		return nil, err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if status == nil {
		s.logger.Debug("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkchoiceState.HeadHash)

		status, err = s.HandlesForkChoice(ctx, "ForkChoiceUpdated", forkchoiceState, 0)
		if err != nil {
			if errors.Is(err, consensus.ErrInvalidBlock) {
				return &engine_types.ForkChoiceUpdatedResponse{
					PayloadStatus: &engine_types.PayloadStatus{
						Status:          engine_types.InvalidStatus,
						ValidationError: engine_types.NewStringifiedError(err),
					},
				}, nil
			}
			return nil, err
		}
		s.logger.Debug("[ForkChoiceUpdated] got reply", "payloadStatus", status)

		if status.CriticalError != nil {
			return nil, status.CriticalError
		}
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

	if !s.proposing {
		return nil, errors.New("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	headHeader := s.chainRW.GetHeaderByHash(ctx, forkchoiceState.HeadHash)

	if headHeader.Time >= timestamp {
		return nil, &engine_helpers.InvalidPayloadAttributesErr
	}

	req := &execution.AssembleBlockRequest{
		ParentHash:            gointerfaces.ConvertHashToH256(forkchoiceState.HeadHash),
		Timestamp:             timestamp,
		PrevRandao:            gointerfaces.ConvertHashToH256(payloadAttributes.PrevRandao),
		SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(payloadAttributes.SuggestedFeeRecipient),
	}

	if version >= clparams.CapellaVersion {
		req.Withdrawals = engine_types.ConvertWithdrawalsToRpc(payloadAttributes.Withdrawals)
	}

	if version >= clparams.DenebVersion {
		req.ParentBeaconBlockRoot = gointerfaces.ConvertHashToH256(*payloadAttributes.ParentBeaconBlockRoot)
	}

	var resp *execution.AssembleBlockResponse
	// Wait for the execution service to be ready to assemble a block. Wait a full slot duration (12 seconds) to ensure that the execution service is not busy.
	// Blocks are important and 0.5 seconds is not enough to wait for the execution service to be ready.
	execBusy, err := waitForStuff(time.Duration(s.config.SecondsPerSlot())*time.Second, func() (bool, error) {
		resp, err = s.executionService.AssembleBlock(ctx, req)
		if err != nil {
			return false, err
		}
		return resp.Busy, nil
	})
	if err != nil {
		return nil, err
	}
	if execBusy {
		s.logger.Warn("[ForkChoiceUpdated] Execution Service busy, could not fulfil Assemble Block request", "req.parentHash", req.ParentHash)
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, PayloadId: nil}, nil
	}
	return &engine_types.ForkChoiceUpdatedResponse{
		PayloadStatus: &engine_types.PayloadStatus{
			Status:          engine_types.ValidStatus,
			LatestValidHash: &forkchoiceState.HeadHash,
		},
		PayloadId: engine_types.ConvertPayloadId(resp.Id),
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
		found := false
		for _, t := range to {
			if f == t {
				found = true
				break
			}
		}
		if !found {
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

		if !e.blockDownloader.StartDownloading(0, header.ParentHash, headerNumber-1, block, engine_block_downloader.NewPayloadTrigger) {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if currentHeadNumber != nil {
			// wait for the slot duration for full download
			waitTime := time.Duration(e.config.SecondsPerSlot()) * time.Second
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			var respondSyncing bool
			if _, _ = waitForStuff(waitTime, func() (bool, error) {
				status := e.blockDownloader.Status()
				respondSyncing = status != engine_block_downloader.Synced
				// no point in waiting if the downloader is no longer syncing (e.g. it's dropped the download request)
				return status == engine_block_downloader.Syncing, nil
			}); respondSyncing {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
			status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
			if err != nil {
				missingBlkHash, isMissingChainErr := eth1.GetBlockHashFromMissingSegmentError(err)
				if isMissingChainErr {
					e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
					if e.test {
						return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
					}
					if e.blockDownloader.StartDownloading(0, missingBlkHash, 0, block, engine_block_downloader.SegmentRecoveryTrigger) {
						e.logger.Warn(fmt.Sprintf("[%s] New payload: need to recover missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
					}
					return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
				}
				return nil, err
			}

			if status == execution.ExecutionStatus_Busy || status == execution.ExecutionStatus_TooFarAway {
				e.logger.Debug(fmt.Sprintf("[%s] New payload: Client is still syncing", logPrefix))
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			} else {
				return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &latestValidHash}, nil
			}
		} else {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	}

	if err := e.chainRW.InsertBlockAndWait(ctx, block); err != nil {
		if errors.Is(err, types.ErrBlockExceedsMaxRlpSize) {
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedError(err),
			}, nil
		}
		return nil, err
	}

	if math.AbsoluteDifference(*currentHeadNumber, headerNumber) >= 32 {
		return &engine_types.PayloadStatus{Status: engine_types.AcceptedStatus}, nil
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, validationErr, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
		missingBlkHash, isMissingChainErr := eth1.GetBlockHashFromMissingSegmentError(err)
		if isMissingChainErr {
			e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
			if e.test {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
			if e.blockDownloader.StartDownloading(0, missingBlkHash, 0, block, engine_block_downloader.SegmentRecoveryTrigger) {
				e.logger.Warn(fmt.Sprintf("[%s] New payload: need to recover missing segment", logPrefix), "height", headerNumber, "hash", headerHash, "missingBlkHash", missingBlkHash)
			}
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
		return nil, err
	}

	if status == execution.ExecutionStatus_BadBlock {
		e.hd.ReportBadHeaderPoS(block.Hash(), latestValidHash)
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

func convertGrpcStatusToEngineStatus(status execution.ExecutionStatus) engine_types.EngineStatus {
	switch status {
	case execution.ExecutionStatus_Success:
		return engine_types.ValidStatus
	case execution.ExecutionStatus_MissingSegment:
		return engine_types.AcceptedStatus
	case execution.ExecutionStatus_TooFarAway:
		return engine_types.AcceptedStatus
	case execution.ExecutionStatus_BadBlock:
		return engine_types.InvalidStatus
	case execution.ExecutionStatus_Busy:
		return engine_types.SyncingStatus
	}
	panic("giulio u stupid.")
}

func (e *EngineServer) HandlesForkChoice(
	ctx context.Context,
	logPrefix string,
	forkChoice *engine_types.ForkChoiceState,
	requestId int,
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
			e.blockDownloader.StartDownloading(requestId, headerHash, 0, nil, engine_block_downloader.FcuTrigger)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header := e.chainRW.GetHeader(ctx, headerHash, *headerNumber)
	if header == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(requestId, headerHash, *headerNumber, nil, engine_block_downloader.FcuTrigger)
		}

		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Call forkchoice here
	status, validationErr, latestValidHash, err := e.chainRW.UpdateForkChoice(ctx, forkChoice.HeadHash, forkChoice.SafeBlockHash, forkChoice.FinalizedBlockHash)
	if err != nil {
		return nil, err
	}
	if status == execution.ExecutionStatus_InvalidForkchoice {
		return nil, &engine_helpers.InvalidForkchoiceStateErr
	}
	if status == execution.ExecutionStatus_Busy {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}
	if status == execution.ExecutionStatus_BadBlock {
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
	req := &txpool.GetBlobsRequest{BlobHashes: make([]*typesproto.H256, len(blobHashes))}
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

	if version == clparams.FuluVersion {
		ret := make([]*engine_types.BlobAndProofV2, len(blobHashes))
		for i, bwp := range res.BlobsWithProofs {
			logHead := fmt.Sprintf("\n%x: ", blobHashes[i])
			if len(bwp.Blob) == 0 {
				// engine_getblobsv2 MUST return null in case of any missing or older version blobs
				ret = nil
				logLine = append(logLine, logHead, "nil")
				break
			} else if len(bwp.Proofs) != int(params.CellsPerExtBlob) {
				// engine_getblobsv2 MUST return null in case of any missing or older version blobs
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
	} else if version == clparams.CapellaVersion {
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
	}
	return nil, nil
}

func waitForStuff(maxWait time.Duration, waitCondnF func() (bool, error)) (bool, error) {
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
