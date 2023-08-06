package engineapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
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
	ctx     context.Context
	lock    sync.Mutex
	logger  log.Logger
}

const fcuTimeout = 1000 // according to mathematics: 1000 millisecods = 1 second

func NewEngineServer(ctx context.Context, logger log.Logger, config *chain.Config, executionService execution.ExecutionClient,
	hd *headerdownload.HeaderDownload,
	blockDownloader *engine_block_downloader.EngineBlockDownloader, test bool, proposing bool) *EngineServer {
	chainRW := eth1_chain_reader.NewChainReaderEth1(ctx, config, executionService, fcuTimeout)
	return &EngineServer{
		ctx:              ctx,
		logger:           logger,
		config:           config,
		executionService: executionService,
		blockDownloader:  blockDownloader,
		chainRW:          chainRW,
		proposing:        proposing,
		hd:               hd,
	}
}

func (e *EngineServer) Start(httpConfig httpcfg.HttpCfg, db kv.RoDB, blockReader services.FullBlockReader,
	filters *rpchelper.Filters, stateCache kvcache.Cache, agg *libstate.AggregatorV3, engineReader consensus.EngineReader,
	eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient) {
	base := jsonrpc.NewBaseApi(filters, stateCache, blockReader, agg, httpConfig.WithDatadir, httpConfig.EvmCallTimeout, engineReader, httpConfig.Dirs)

	ethImpl := jsonrpc.NewEthAPI(base, db, eth, txPool, mining, httpConfig.Gascap, httpConfig.ReturnDataLimit, e.logger)

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

	if err := cli.StartRpcServerWithJwtAuthentication(e.ctx, httpConfig, apiList, e.logger); err != nil {
		e.logger.Error(err.Error())
	}
}

func (s *EngineServer) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !s.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if s.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

// EngineNewPayload validates and possibly executes payload
func (s *EngineServer) newPayload(ctx context.Context, req *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash, version clparams.StateVersion,
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
	var withdrawals []*types.Withdrawal
	if version >= clparams.CapellaVersion {
		withdrawals = req.Withdrawals
	}

	if withdrawals != nil {
		wh := types.DeriveSha(types.Withdrawals(withdrawals))
		header.WithdrawalsHash = &wh
	}

	if err := s.checkWithdrawalsPresence(header.Time, withdrawals); err != nil {
		return nil, err
	}

	if version >= clparams.DenebVersion {
		header.BlobGasUsed = (*uint64)(req.BlobGasUsed)
		header.ExcessBlobGas = (*uint64)(req.ExcessBlobGas)
		header.ParentBeaconBlockRoot = parentBeaconBlockRoot
	}

	if (!s.config.IsCancun(header.Time) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(header.Time) && version < clparams.DenebVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	if s.config.IsCancun(header.Time) && (header.BlobGasUsed == nil || header.ExcessBlobGas == nil) {
		return nil, &rpc.InvalidParamsError{Message: "blobGasUsed/excessBlobGas missing"}
	}

	if s.config.IsCancun(header.Time) && header.ParentBeaconBlockRoot == nil {
		return nil, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot missing"}
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
		actualBlobHashes := []libcommon.Hash{}
		for _, tx := range transactions {
			actualBlobHashes = append(actualBlobHashes, tx.GetBlobHashes()...)
		}
		if !reflect.DeepEqual(actualBlobHashes, expectedBlobHashes) {
			s.logger.Warn("[NewPayload] mismatch in blob hashes",
				"expectedBlobHashes", expectedBlobHashes, "actualBlobHashes", actualBlobHashes)
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString("mismatch in blob hashes"),
			}, nil
		}
	}

	possibleStatus, err := s.getQuickPayloadStatusIfPossible(blockHash, uint64(req.BlockNumber), header.ParentHash, nil, true)
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

	payloadStatus, err := s.HandleNewPayload("NewPayload", block)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return payloadStatus, nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EngineServer) getQuickPayloadStatusIfPossible(blockHash libcommon.Hash, blockNumber uint64, parentHash libcommon.Hash, forkchoiceMessage *engine_types.ForkChoiceState, newPayload bool) (*engine_types.PayloadStatus, error) {
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

	headHash, finalizedHash, safeHash, err := s.chainRW.GetForkchoice()
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

	header := s.chainRW.GetHeaderByHash(blockHash)

	// Retrieve parent and total difficulty.
	var parent *types.Header
	var td *big.Int
	if newPayload {
		parent = s.chainRW.GetHeaderByHash(parentHash)
		td = s.chainRW.GetTd(parentHash, blockNumber-1)
	} else {
		td = s.chainRW.GetTd(blockHash, blockNumber)
	}

	if td != nil && td.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		s.logger.Warn(fmt.Sprintf("[%s] Beacon Chain request before TTD", prefix), "hash", blockHash)
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &libcommon.Hash{}}, nil
	}

	var isCanonical bool
	if header != nil {
		isCanonical, err = s.chainRW.IsCanonicalHash(blockHash)
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
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &lastValidHash}, nil
	}

	currentHeader := s.chainRW.CurrentHeader()
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
	executionReady, err := s.chainRW.Ready()
	if err != nil {
		return nil, err
	}
	if !executionReady {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	return nil, nil
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EngineServer) getPayload(ctx context.Context, payloadId uint64) (*engine_types.GetPayloadResponse, error) {
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

	return &engine_types.GetPayloadResponse{
		ExecutionPayload: engine_types.ConvertPayloadFromRpc(data.ExecutionPayload),
		BlockValue:       (*hexutil.Big)(gointerfaces.ConvertH256ToUint256Int(data.BlockValue).ToBig()),
		BlobsBundle:      engine_types.ConvertBlobsFromRpc(data.BlobsBundle),
	}, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EngineServer) forkchoiceUpdated(ctx context.Context, forkchoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes, version clparams.StateVersion,
) (*engine_types.ForkChoiceUpdatedResponse, error) {
	status, err := s.getQuickPayloadStatusIfPossible(forkchoiceState.HeadHash, 0, libcommon.Hash{}, forkchoiceState, false)
	if err != nil {
		return nil, err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if status == nil {
		s.logger.Debug("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkchoiceState.HeadHash)

		status, err = s.HandlesForkChoice("ForkChoiceUpdated", forkchoiceState, 0)
		if err != nil {
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

	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	timestamp := uint64(payloadAttributes.Timestamp)
	if (!s.config.IsCancun(timestamp) && version >= clparams.DenebVersion) ||
		(s.config.IsCancun(timestamp) && version < clparams.DenebVersion) {
		return nil, &rpc.UnsupportedForkError{Message: "Unsupported fork"}
	}

	headHeader := s.chainRW.GetHeaderByHash(forkchoiceState.HeadHash)

	if headHeader.Hash() != forkchoiceState.HeadHash {
		// Per Item 2 of https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.9/src/engine/specification.md#specification-1:
		// Client software MAY skip an update of the forkchoice state and
		// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree.
		// That is, the block referenced by forkchoiceState.headBlockHash is neither the head of the canonical chain nor a block at the tip of any other chain.
		// In the case of such an event, client software MUST return
		// {payloadStatus: {status: VALID, latestValidHash: forkchoiceState.headBlockHash, validationError: null}, payloadId: null}.

		s.logger.Warn("Skipping payload building because forkchoiceState.headBlockHash is not the head of the canonical chain",
			"forkChoice.HeadBlockHash", forkchoiceState.HeadHash, "headHeader.Hash", headHeader.Hash())
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: status}, nil
	}

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
		req.ParentBeaconBlockRoot = gointerfaces.ConvertHashToH256(payloadAttributes.ParentBeaconBlockRoot)
	}

	resp, err := s.executionService.AssembleBlock(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Busy {
		return nil, errors.New("[ForkChoiceUpdated]: execution service is busy, cannot assemble blocks")
	}
	return &engine_types.ForkChoiceUpdatedResponse{
		PayloadStatus: &engine_types.PayloadStatus{
			Status:          engine_types.ValidStatus,
			LatestValidHash: &forkchoiceState.HeadHash,
		},
		PayloadId: engine_types.ConvertPayloadId(resp.Id),
	}, nil
}

func (s *EngineServer) getPayloadBodiesByHash(ctx context.Context, request []libcommon.Hash, _ clparams.StateVersion) ([]*engine_types.ExecutionPayloadBodyV1, error) {

	bodies := make([]*engine_types.ExecutionPayloadBodyV1, len(request))

	for hashIdx, hash := range request {
		block := s.chainRW.GetBlockByHash(hash)
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies[hashIdx] = body
	}

	return bodies, nil
}

func extractPayloadBodyFromBlock(block *types.Block) (*engine_types.ExecutionPayloadBodyV1, error) {
	if block == nil {
		return nil, nil
	}

	txs := block.Transactions()
	bdTxs := make([]hexutility.Bytes, len(txs))
	for idx, tx := range txs {
		var buf bytes.Buffer
		if err := tx.MarshalBinary(&buf); err != nil {
			return nil, err
		} else {
			bdTxs[idx] = buf.Bytes()
		}
	}

	return &engine_types.ExecutionPayloadBodyV1{Transactions: bdTxs, Withdrawals: block.Withdrawals()}, nil
}

func (s *EngineServer) getPayloadBodiesByRange(ctx context.Context, start, count uint64, _ clparams.StateVersion) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	bodies := make([]*engine_types.ExecutionPayloadBodyV1, 0, count)

	for i := uint64(0); i < count; i++ {
		block := s.chainRW.GetBlockByNumber(start + i)

		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		if body == nil {
			break
		}
		bodies = append(bodies, body)
	}

	return bodies, nil
}

func (e *EngineServer) GetPayloadV1(ctx context.Context, payloadId hexutility.Bytes) (*engine_types.ExecutionPayload, error) {

	decodedPayloadId := binary.BigEndian.Uint64(payloadId)
	e.logger.Info("Received GetPayloadV1", "payloadId", decodedPayloadId)

	response, err := e.getPayload(ctx, decodedPayloadId)
	if err != nil {
		return nil, err
	}

	return response.ExecutionPayload, nil
}

func (e *EngineServer) GetPayloadV2(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV2", "payloadId", decodedPayloadId)

	return e.getPayload(ctx, decodedPayloadId)
}

func (e *EngineServer) GetPayloadV3(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV3", "payloadId", decodedPayloadId)

	return e.getPayload(ctx, decodedPayloadId)
}

func (e *EngineServer) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.BellatrixVersion)
}

func (e *EngineServer) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.CapellaVersion)
}

func (e *EngineServer) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.DenebVersion)
}

// NewPayloadV1 processes new payloads (blocks) from the beacon chain without withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_newpayloadv1
func (e *EngineServer) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, clparams.BellatrixVersion)
}

// NewPayloadV2 processes new payloads (blocks) from the beacon chain with withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_newpayloadv2
func (e *EngineServer) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, clparams.CapellaVersion)
}

// NewPayloadV3 processes new payloads (blocks) from the beacon chain with withdrawals & blob gas.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_newpayloadv3
func (e *EngineServer) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, expectedBlobHashes, parentBeaconBlockRoot, clparams.DenebVersion)
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

func (e *EngineServer) GetPayloadBodiesByHashV1(ctx context.Context, hashes []libcommon.Hash) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if len(hashes) > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByHash(ctx, hashes, clparams.DenebVersion)
}

func (e *EngineServer) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if start == 0 || count == 0 {
		return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("invalid start or count, start: %v count: %v", start, count)}
	}
	if count > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByRange(ctx, uint64(start), uint64(count), clparams.CapellaVersion)
}

var ourCapabilities = []string{
	"engine_forkchoiceUpdatedV1",
	"engine_forkchoiceUpdatedV2",
	"engine_newPayloadV1",
	"engine_newPayloadV2",
	"engine_newPayloadV3",
	"engine_getPayloadV1",
	"engine_getPayloadV2",
	"engine_getPayloadV3",
	"engine_exchangeTransitionConfigurationV1",
	"engine_getPayloadBodiesByHashV1",
	"engine_getPayloadBodiesByRangeV1",
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
	logPrefix string,
	block *types.Block,
) (*engine_types.PayloadStatus, error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	e.logger.Info(fmt.Sprintf("[%s] Handling new payload", logPrefix), "height", headerNumber, "hash", headerHash)

	currentHeader := e.chainRW.CurrentHeader()
	var currentHeadNumber *uint64
	if currentHeader != nil {
		currentHeadNumber = new(uint64)
		*currentHeadNumber = currentHeader.Number.Uint64()
	}
	parent := e.chainRW.GetHeader(header.ParentHash, headerNumber-1)
	if parent == nil {
		e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download parent", logPrefix), "height", headerNumber, "hash", headerHash, "parentHash", header.ParentHash)
		if e.test {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if !e.blockDownloader.StartDownloading(0, header.ParentHash, headerHash, block) {
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
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &headerHash}, nil
		} else {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	}
	if err := e.chainRW.InsertHeaderAndBodyAndWait(header, block.RawBody()); err != nil {
		return nil, err
	}

	if math.AbsoluteDifference(*currentHeadNumber, headerNumber) >= 32 {
		return &engine_types.PayloadStatus{Status: engine_types.AcceptedStatus}, nil
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, latestValidHash, err := e.chainRW.ValidateChain(headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
		return nil, err
	}

	if status == execution.ExecutionStatus_BadBlock {
		e.hd.ReportBadHeaderPoS(block.Hash(), latestValidHash)
	}

	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
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
	logPrefix string,
	forkChoice *engine_types.ForkChoiceState,
	requestId int,
) (*engine_types.PayloadStatus, error) {
	headerHash := forkChoice.HeadHash

	e.logger.Debug(fmt.Sprintf("[%s] Handling fork choice", logPrefix), "headerHash", headerHash)
	headerNumber, err := e.chainRW.HeaderNumber(headerHash)
	if err != nil {
		return nil, err
	}

	// We do not have header, download.
	if headerNumber == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash, nil)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header := e.chainRW.GetHeader(headerHash, *headerNumber)
	if header == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if !e.test {
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash, nil)
		}

		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Call forkchoice here
	status, latestValidHash, err := e.chainRW.UpdateForkChoice(forkChoice.HeadHash, forkChoice.SafeBlockHash, forkChoice.FinalizedBlockHash)
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
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus}, nil
	}
	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
}
