package engineapi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/eth/ethutils"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_block_downloader"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

type EngineServer struct {
	hd              *headerdownload.HeaderDownload
	blockDownloader *engine_block_downloader.EngineBlockDownloader
	config          *chain.Config
	// Block proposing for proof-of-stake
	proposing        bool
	test             bool
	executionService execution.ExecutionClient

	chainRW eth1_chain_reader.ChainReaderWriterEth1
	lock    sync.Mutex
	logger  log.Logger
}

const fcuTimeout = 1000 // according to mathematics: 1000 millisecods = 1 second

func NewEngineServer(logger log.Logger, config *chain.Config, executionService execution.ExecutionClient,
	hd *headerdownload.HeaderDownload,
	blockDownloader *engine_block_downloader.EngineBlockDownloader, test bool, proposing bool) *EngineServer {
	chainRW := eth1_chain_reader.NewChainReaderEth1(config, executionService, fcuTimeout)
	return &EngineServer{
		logger:           logger,
		config:           config,
		executionService: executionService,
		blockDownloader:  blockDownloader,
		chainRW:          chainRW,
		proposing:        proposing,
		hd:               hd,
	}
}

func (e *EngineServer) Start(
	ctx context.Context,
	httpConfig *httpcfg.HttpCfg,
	db kv.RoDB,
	blockReader services.FullBlockReader,
	filters *rpchelper.Filters,
	stateCache kvcache.Cache,
	agg *libstate.Aggregator,
	engineReader consensus.EngineReader,
	eth rpchelper.ApiBackend,
	txPool txpool.TxpoolClient,
	mining txpool.MiningClient,
) {
	base := jsonrpc.NewBaseApi(filters, stateCache, blockReader, agg, httpConfig.WithDatadir, httpConfig.EvmCallTimeout, engineReader, httpConfig.Dirs)

	ethImpl := jsonrpc.NewEthAPI(base, db, eth, txPool, mining, httpConfig.Gascap, httpConfig.Feecap, httpConfig.ReturnDataLimit, httpConfig.AllowUnprotectedTxs, httpConfig.MaxGetProofRewindBlockCount, httpConfig.WebsocketSubscribeLogsChannelSize, e.logger)

	// engineImpl := NewEngineAPI(base, db, engineBackend)
	// e.startEngineMessageHandler()
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

func (s *EngineServer) checkRequestsPresence(time uint64, executionRequests []hexutility.Bytes) error {
	if !s.config.IsPrague(time) {
		if executionRequests != nil {
			return &rpc.InvalidParamsError{Message: "requests before Prague"}
		}
	}
	if s.config.IsPrague(time) {
		if len(executionRequests) < 3 {
			return &rpc.InvalidParamsError{Message: "missing requests list"}
		}
	}
	return nil
}

// EngineNewPayload validates and possibly executes payload
func (s *EngineServer) newPayload(ctx context.Context, req *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash, executionRequests []hexutility.Bytes, version clparams.StateVersion,
) (*engine_types.PayloadStatus, error) {
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
		Number:      big.NewInt(int64(req.BlockNumber)),
		GasUsed:     uint64(req.GasUsed),
		GasLimit:    uint64(req.GasLimit),
		Time:        uint64(req.Timestamp),
		MixDigest:   req.PrevRandao,
		UncleHash:   types.EmptyUncleHash,
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
	if err := s.checkRequestsPresence(header.Time, executionRequests); err != nil {
		return nil, err
	}
	if version >= clparams.ElectraVersion {
		requests = make(types.FlatRequests, len(types.KnownRequestTypes))
		for i, r := range types.KnownRequestTypes {
			if len(executionRequests) == i {
				executionRequests = append(executionRequests, []byte{})
			}
			requests[i] = types.FlatRequest{Type: r, RequestData: executionRequests[i]}
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
		s.logger.Error("[NewPayload] invalid block hash", "stated", blockHash, "actual", header.Hash())
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
		err := ethutils.ValidateBlobs(req.BlobGasUsed.Uint64(), s.config.GetMaxBlobGasPerBlock(), s.config.GetMaxBlobsPerBlock(), expectedBlobHashes, &transactions)
		if errors.Is(err, ethutils.ErrNilBlobHashes) {
			return nil, &rpc.InvalidParamsError{Message: "nil blob hashes array"}
		}
		if errors.Is(err, ethutils.ErrMaxBlobGasUsed) {
			bad, latestValidHash := s.hd.IsBadHeaderPoS(req.ParentHash)
			if !bad {
				latestValidHash = req.ParentHash
			}
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString("blobs/blobgas exceeds max"),
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

	return payloadStatus, nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EngineServer) getQuickPayloadStatusIfPossible(ctx context.Context, blockHash libcommon.Hash, blockNumber uint64, parentHash libcommon.Hash, forkchoiceMessage *engine_types.ForkChoiceState, newPayload bool) (*engine_types.PayloadStatus, error) {
	// Determine which prefix to use for logs
	var prefix string
	if newPayload {
		prefix = "NewPayload"
	} else {
		prefix = "ForkChoiceUpdated"
	}
	if s.config.TerminalTotalDifficulty == nil {
		s.logger.Error(fmt.Sprintf("[%s] not a proof-of-stake chain", prefix))
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	if s.hd == nil {
		return nil, fmt.Errorf("headerdownload is nil")
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
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &libcommon.Hash{}, ValidationError: engine_types.NewStringifiedErrorFromString("Beacon Chain request before TTD")}, nil
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

		if parent == nil && s.hd.PosStatus() == headerdownload.Syncing {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	} else {
		if header == nil && s.hd.PosStatus() == headerdownload.Syncing {
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
	executionReady, err := s.chainRW.Ready(ctx)
	if err != nil {
		return nil, err
	}
	if !executionReady {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	return nil, nil
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EngineServer) getPayload(ctx context.Context, payloadId uint64, version clparams.StateVersion) (*engine_types.GetPayloadResponse, error) {
	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
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
	executionRequests := make([][]byte, len(data.Requests.Requests))
	copy(executionRequests, data.Requests.Requests)

	ts := data.ExecutionPayload.Timestamp
	if (!s.config.IsCancun(ts) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(ts) && version < clparams.DenebVersion) ||
		(!s.config.IsPrague(ts) && version >= clparams.ElectraVersion) ||
		(s.config.IsPrague(ts) && version < clparams.ElectraVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	return &engine_types.GetPayloadResponse{
		ExecutionPayload:  engine_types.ConvertPayloadFromRpc(data.ExecutionPayload),
		BlockValue:        (*hexutil.Big)(gointerfaces.ConvertH256ToUint256Int(data.BlockValue).ToBig()),
		BlobsBundle:       engine_types.ConvertBlobsFromRpc(data.BlobsBundle),
		ExecutionRequests: executionRequests,
	}, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EngineServer) forkchoiceUpdated(ctx context.Context, forkchoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes, version clparams.StateVersion,
) (*engine_types.ForkChoiceUpdatedResponse, error) {
	status, err := s.getQuickPayloadStatusIfPossible(ctx, forkchoiceState.HeadHash, 0, libcommon.Hash{}, forkchoiceState, false)
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
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
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

	resp, err := s.executionService.AssembleBlock(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Busy {
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

func (s *EngineServer) getPayloadBodiesByHash(ctx context.Context, request []libcommon.Hash, version clparams.StateVersion) ([]*engine_types.ExecutionPayloadBody, error) {
	if len(request) > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}
	bodies, err := s.chainRW.GetBodiesByHashes(ctx, request)
	if err != nil {
		return nil, err
	}

	resp := make([]*engine_types.ExecutionPayloadBody, len(bodies))
	for i, body := range bodies {
		resp[i] = extractPayloadBodyFromBody(body, version)
	}
	return resp, nil
}

func extractPayloadBodyFromBody(body *types.RawBody, version clparams.StateVersion) *engine_types.ExecutionPayloadBody {
	if body == nil {
		return nil
	}

	bdTxs := make([]hexutility.Bytes, len(body.Transactions))
	for idx := range body.Transactions {
		bdTxs[idx] = body.Transactions[idx]
	}

	ret := &engine_types.ExecutionPayloadBody{Transactions: bdTxs, Withdrawals: body.Withdrawals}
	return ret
}

func (s *EngineServer) getPayloadBodiesByRange(ctx context.Context, start, count uint64, version clparams.StateVersion) ([]*engine_types.ExecutionPayloadBody, error) {
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
		resp[idx] = extractPayloadBodyFromBody(body, version)
	}
	return resp, nil
}

// Returns the most recent version of the payload(for the payloadID) at the time of receiving the call
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_getpayloadv1
func (e *EngineServer) GetPayloadV1(ctx context.Context, payloadId hexutility.Bytes) (*engine_types.ExecutionPayload, error) {

	decodedPayloadId := binary.BigEndian.Uint64(payloadId)
	e.logger.Info("Received GetPayloadV1", "payloadId", decodedPayloadId)

	response, err := e.getPayload(ctx, decodedPayloadId, clparams.BellatrixVersion)
	if err != nil {
		return nil, err
	}

	return response.ExecutionPayload, nil
}

// Same as [GetPayloadV1] with addition of blockValue
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_getpayloadv2
func (e *EngineServer) GetPayloadV2(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV2", "payloadId", decodedPayloadId)
	return e.getPayload(ctx, decodedPayloadId, clparams.CapellaVersion)
}

// Same as [GetPayloadV2], with addition of blobsBundle containing valid blobs, commitments, proofs
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_getpayloadv3
func (e *EngineServer) GetPayloadV3(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV3", "payloadId", decodedPayloadId)
	return e.getPayload(ctx, decodedPayloadId, clparams.DenebVersion)
}

// Same as [GetPayloadV3], but returning ExecutionPayloadV4 (= ExecutionPayloadV3 + requests)
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_getpayloadv4
func (e *EngineServer) GetPayloadV4(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV4", "payloadId", decodedPayloadId)
	return e.getPayload(ctx, decodedPayloadId, clparams.ElectraVersion)
}

// Updates the forkchoice state after validating the headBlockHash
// Additionally, builds and returns a unique identifier for an initial version of a payload
// (asynchronously updated with transactions), if payloadAttributes is not nil and passes validation
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_forkchoiceupdatedv1
func (e *EngineServer) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.BellatrixVersion)
}

// Same as, and a replacement for, [ForkchoiceUpdatedV1], post Shanghai
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_forkchoiceupdatedv2
func (e *EngineServer) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.CapellaVersion)
}

// Successor of [ForkchoiceUpdatedV2] post Cancun, with stricter check on params
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_forkchoiceupdatedv3
func (e *EngineServer) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.DenebVersion)
}

// NewPayloadV1 processes new payloads (blocks) from the beacon chain without withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_newpayloadv1
func (e *EngineServer) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, nil, clparams.BellatrixVersion)
}

// NewPayloadV2 processes new payloads (blocks) from the beacon chain with withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_newpayloadv2
func (e *EngineServer) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, nil, clparams.CapellaVersion)
}

// NewPayloadV3 processes new payloads (blocks) from the beacon chain with withdrawals & blob gas.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_newpayloadv3
func (e *EngineServer) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, expectedBlobHashes, parentBeaconBlockRoot, nil, clparams.DenebVersion)
}

// NewPayloadV4 processes new payloads (blocks) from the beacon chain with withdrawals, blob gas and requests.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_newpayloadv4
func (e *EngineServer) NewPayloadV4(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash, executionRequests []hexutility.Bytes) (*engine_types.PayloadStatus, error) {
	// TODO(racytech): add proper version or refactor this part
	// add all version ralated checks here so the newpayload doesn't have to deal with checks
	return e.newPayload(ctx, payload, expectedBlobHashes, parentBeaconBlockRoot, executionRequests, clparams.ElectraVersion)
}

// Receives consensus layer's transition configuration and checks if the execution layer has the correct configuration.
// Can also be used to ping the execution layer (heartbeats).
// See https://github.com/ethereum/execution-apis/blob/v1.0.0-beta.1/src/engine/specification.md#engine_exchangetransitionconfigurationv1
func (e *EngineServer) ExchangeTransitionConfigurationV1(ctx context.Context, beaconConfig *engine_types.TransitionConfiguration) (*engine_types.TransitionConfiguration, error) {
	terminalTotalDifficulty := e.config.TerminalTotalDifficulty

	if terminalTotalDifficulty == nil {
		return nil, fmt.Errorf("the execution layer doesn't have a terminal total difficulty. expected: %v", beaconConfig.TerminalTotalDifficulty)
	}

	if terminalTotalDifficulty.Cmp((*big.Int)(beaconConfig.TerminalTotalDifficulty)) != 0 {
		return nil, fmt.Errorf("the execution layer has a wrong terminal total difficulty. expected %v, but instead got: %d", beaconConfig.TerminalTotalDifficulty, terminalTotalDifficulty)
	}

	return &engine_types.TransitionConfiguration{
		TerminalTotalDifficulty: (*hexutil.Big)(terminalTotalDifficulty),
		TerminalBlockHash:       libcommon.Hash{},
		TerminalBlockNumber:     (*hexutil.Big)(libcommon.Big0),
	}, nil
}

// Returns an array of execution payload bodies referenced by their block hashes
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_getpayloadbodiesbyhashv1
func (e *EngineServer) GetPayloadBodiesByHashV1(ctx context.Context, hashes []libcommon.Hash) ([]*engine_types.ExecutionPayloadBody, error) {
	return e.getPayloadBodiesByHash(ctx, hashes, clparams.DenebVersion)
}

// Returns an array of execution payload bodies referenced by their block hashes
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_getpayloadbodiesbyhashv2
func (e *EngineServer) GetPayloadBodiesByHashV2(ctx context.Context, hashes []libcommon.Hash) ([]*engine_types.ExecutionPayloadBody, error) {
	return e.getPayloadBodiesByHash(ctx, hashes, clparams.ElectraVersion)
}

// Returns an ordered (as per canonical chain) array of execution payload bodies, with corresponding execution block numbers from "start", up to "count"
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_getpayloadbodiesbyrangev1
func (e *EngineServer) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBody, error) {
	return e.getPayloadBodiesByRange(ctx, uint64(start), uint64(count), clparams.CapellaVersion)
}

// Returns an ordered (as per canonical chain) array of execution payload bodies, with corresponding execution block numbers from "start", up to "count"
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_getpayloadbodiesbyrangev2
func (e *EngineServer) GetPayloadBodiesByRangeV2(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBody, error) {
	return e.getPayloadBodiesByRange(ctx, uint64(start), uint64(count), clparams.ElectraVersion)
}

var ourCapabilities = []string{
	"engine_forkchoiceUpdatedV1",
	"engine_forkchoiceUpdatedV2",
	"engine_forkchoiceUpdatedV3",
	"engine_newPayloadV1",
	"engine_newPayloadV2",
	"engine_newPayloadV3",
	"engine_newPayloadV4",
	"engine_getPayloadV1",
	"engine_getPayloadV2",
	"engine_getPayloadV3",
	"engine_getPayloadV4",
	"engine_exchangeTransitionConfigurationV1",
	"engine_getPayloadBodiesByHashV1",
	"engine_getPayloadBodiesByHashV2",
	"engine_getPayloadBodiesByRangeV1",
	"engine_getPayloadBodiesByRangeV2",
}

func (e *EngineServer) ExchangeCapabilities(fromCl []string) []string {
	missingOurs := compareCapabilities(fromCl, ourCapabilities)
	missingCl := compareCapabilities(ourCapabilities, fromCl)

	if len(missingCl) > 0 || len(missingOurs) > 0 {
		e.logger.Debug("ExchangeCapabilities mismatches", "cl_unsupported", missingCl, "erigon_unsupported", missingOurs)
	}

	return ourCapabilities
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
	versionedHashes []libcommon.Hash,
) (*engine_types.PayloadStatus, error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	e.logger.Info(fmt.Sprintf("[%s] Handling new payload", logPrefix), "height", headerNumber, "hash", headerHash)

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

		if !e.blockDownloader.StartDownloading(ctx, 0, header.ParentHash, block) {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if currentHeadNumber != nil {
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			success := false
			for i := 0; i < 100; i++ {
				time.Sleep(10 * time.Millisecond)
				if e.blockDownloader.Status() == headerdownload.Synced {
					success = true
					break
				}
			}
			if !success {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}

			status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
			if err != nil {
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
		return nil, err
	}

	if math.AbsoluteDifference(*currentHeadNumber, headerNumber) >= 32 {
		return &engine_types.PayloadStatus{Status: engine_types.AcceptedStatus}, nil
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, validationErr, latestValidHash, err := e.chainRW.ValidateChain(ctx, headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
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
			e.blockDownloader.StartDownloading(ctx, requestId, headerHash, nil)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header := e.chainRW.GetHeader(ctx, headerHash, *headerNumber)
	if header == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(ctx, requestId, headerHash, nil)
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
